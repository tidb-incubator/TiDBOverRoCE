/*
 *
 * Copyright 2021 PingCAP authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <arpa/inet.h>
#include <byteswap.h>
#include <errno.h>
#include <fcntl.h>
#include <grpc/slice.h>
#include <grpc/support/alloc.h>
#include <grpc/support/log.h>
#include <grpc/support/port_platform.h>
#include <grpc/support/string_util.h>
#include <grpc/support/sync.h>
#include <grpc/support/time.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>

#include "src/core/lib/channel/channel_args.h"
#include "src/core/lib/debug/stats.h"
#include "src/core/lib/debug/trace.h"
#include "src/core/lib/gpr/string.h"
#include "src/core/lib/gpr/useful.h"
#include "src/core/lib/gprpp/sync.h"
#include "src/core/lib/iomgr/buffer_list.h"
#include "src/core/lib/iomgr/ev_posix.h"
#include "src/core/lib/iomgr/executor.h"
#include "src/core/lib/iomgr/port.h"
#include "src/core/lib/iomgr/sockaddr_utils.h"
#include "src/core/lib/iomgr/socket_utils_posix.h"
#include "src/core/lib/iomgr/tcp_posix.h"
#include "src/core/lib/profiling/timers.h"
#include "src/core/lib/slice/slice_internal.h"
#include "src/core/lib/slice/slice_string_helpers.h"

#define TCP_OLAP_START_SIZE 2048
#define TCP_MAX_TRANSFER 65536
#define TCP_SNDLOWAT 2048
#define TCP_QP_MIN_SIZE 16
#define TCP_QP_MAX_SIZE 0xFFFE
#define TCP_QP_CTRL_SIZE 4 /* must be power of 2 */
#define TCP_SGL_SIZE 2

static uint16_t def_inline = 64;
static uint16_t def_sqsize = 384;
static uint16_t def_rqsize = 384;
static uint32_t def_mem = (1 << 17);
static uint32_t def_wmem = (1 << 17);
uint32_t polling_time = 10;

enum {
  TCP_OP_DATA,
  TCP_OP_RSVD_DATA_MORE,
  TCP_OP_WRITE, /* opcode is not transmitted over the network */
  TCP_OP_RSVD_DRA_MORE,
  TCP_OP_SGL,
  TCP_OP_RSVD,
  TCP_OP_IOMAP_SGL,
  TCP_OP_CTRL
};
// enum {
// 	RS_CTRL_DISCONNECT,
// 	RS_CTRL_KEEPALIVE,
// 	RS_CTRL_SHUTDOWN
// };
#define tcp_msg_set(op, data) ((op << 29) | (uint32_t)(data))
#define tcp_msg_op(imm_data) (imm_data >> 29)
#define tcp_msg_data(imm_data) (imm_data & 0x1FFFFFFF)
#define TCP_MSG_SIZE sizeof(uint32_t)

#define TCP_WR_ID_FLAG_RECV (((uint64_t)1) << 63)
#define TCP_WR_ID_FLAG_MSG_SEND (((uint64_t)1) << 62) /* See RS_OPT_MSG_SEND \
                                                       */
#define tcp_send_wr_id(data) ((uint64_t)data)
#define tcp_recv_wr_id(data) (TCP_WR_ID_FLAG_RECV | (uint64_t)data)
#define tcp_wr_is_recv(wr_id) (wr_id & TCP_WR_ID_FLAG_RECV)
#define tcp_wr_data(wr_id) ((uint32_t)wr_id)

#define TCP_OPT_SWAP_SGL (1 << 0)

#define TCP_MAX_CTRL_MSG (sizeof(struct grpc_tcp_sge))
#define grpc_tcp_host_is_net() (__BYTE_ORDER == __BIG_ENDIAN)
#define TCP_CONN_FLAG_NET (1 << 0)
// #define CONTAINER_OF(ptr, type, field)                                        \
//   ((type *) ((char *) (ptr) - ((char *) &((type *) 0)->field)))
extern grpc_core::TraceFlag grpc_tcp_trace;
extern grpc_core::TraceFlag grpc_rdma_trace;

uint64_t grpc_tcp_time_us(void) {
  struct timespec now;

  clock_gettime(CLOCK_MONOTONIC, &now);
  return now.tv_sec * 1000000 + now.tv_nsec / 1000;
}

struct grpc_tcp_msg {
  uint32_t op;
  uint32_t data;
};

#define _Atomic(X) std::atomic<X>

typedef struct {
  sem_t sem;
  _Atomic(int) cnt;
} fastlock_t;

static inline void fastlock_init(fastlock_t* lock) {
  sem_init(&lock->sem, 0, 0);
  atomic_store(&lock->cnt, 0);
}

static inline void fastlock_destroy(fastlock_t* lock) {
  sem_destroy(&lock->sem);
}

static inline void fastlock_acquire(fastlock_t* lock) {
  if (atomic_fetch_add(&lock->cnt, 1) > 0) {
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      uint64_t start_time = grpc_tcp_time_us();
      sem_wait(&lock->sem);
      gpr_log(GPR_INFO, "wait %lu for fastlock",
              grpc_tcp_time_us() - start_time);
    } else {
      sem_wait(&lock->sem);
    }
  }
}
static inline void fastlock_release(fastlock_t* lock) {
  if (atomic_fetch_sub(&lock->cnt, 1) > 1) {
    sem_post(&lock->sem);
  }
}

struct grpc_tcp {
  /* This is our C++ class derivation emulation. */
  grpc_endpoint base;

  fastlock_t slock;
  fastlock_t rlock;
  fastlock_t cq_lock;
  fastlock_t cq_wait_lock;

  struct rdma_cm_id* cm_id;
  grpc_fd* em_fd;
  grpc_fd* cm_fd;
  double target_length;
  double bytes_read_this_round;
  /* Refcounting how many operations are in progress. */
  grpc_core::RefCount refcount;

  int min_read_chunk_size;
  int max_read_chunk_size;

  /* garbage after the last read */
  grpc_slice_buffer last_read_buffer;

  grpc_slice_buffer* incoming_buffer;
  grpc_slice_buffer* outgoing_buffer;
  /* byte within outgoing_buffer->slices[0] to write next */
  size_t outgoing_byte_idx;

  grpc_closure* read_cb;
  grpc_closure* write_cb;

  grpc_closure read_done_closure;
  grpc_closure write_done_closure;
  grpc_closure error_closure;

  unsigned int ctrl_seqno;
  unsigned int ctrl_max_seqno;
  uint16_t sseq_no;
  uint16_t sseq_comp;
  uint16_t rseq_no;
  uint16_t rseq_comp;

  int remote_sge;
  struct grpc_tcp_sge remote_sgl;

  struct ibv_mr* target_mr;
  int target_sge;
  void* target_buffer_list;
  volatile struct grpc_tcp_sge* target_sgl;

  int rbuf_bytes_avail;
  int rbuf_free_offset;
  int rbuf_offset;
  struct ibv_mr* rmr;
  uint8_t* rbuf;

  int sbuf_bytes_avail;
  struct ibv_mr* smr;
  struct ibv_sge ssgl[2];

  int opts;
  int cq_armed;

  int sqe_avail;
  uint32_t sbuf_size;
  uint16_t sq_size;
  uint16_t sq_inline;

  uint32_t rbuf_size;
  uint16_t rq_size;
  int rmsg_head;
  int rmsg_tail;

  struct grpc_tcp_msg* rmsg;

  uint8_t* sbuf;
  int unack_cqe;

  grpc_resource_user* resource_user;
  grpc_resource_user_slice_allocator slice_allocator;

  enum { CONNECTED, SHUTDOWN, TCPERROR } state;

  std::string peer_string;
  std::string local_address;
};

struct backup_poller {
  gpr_mu* pollset_mu;
  grpc_closure run_poller;
};

#define BACKUP_POLLER_POLLSET(b) ((grpc_pollset*)((b) + 1))

static gpr_atm g_uncovered_notifications_pending;
static gpr_atm g_backup_poller; /* backup_poller* */

static inline int ERR(int err) {
  errno = err;
  return -1;
}

static inline int grpc_tcp_seterrno(int ret) {
  if (ret) {
    errno = ret;
    ret = -1;
  }
  return ret;
}

static void tcp_free(grpc_tcp* tcp) {
  gpr_log(GPR_INFO, "TCP:%p tcp_free", tcp);
  grpc_fd_orphan(tcp->em_fd, nullptr, nullptr, "tcp_unref_orphan");
  grpc_slice_buffer_destroy_internal(&tcp->last_read_buffer);
  grpc_resource_user_unref(tcp->resource_user);
  if (tcp->rmsg) {
    gpr_free_aligned(tcp->rmsg);
  }
  if (tcp->sbuf) {
    if (tcp->smr) {
      rdma_dereg_mr(tcp->smr);
    }
    gpr_free_aligned(tcp->sbuf);
  }
  if (tcp->rbuf) {
    if (tcp->rmr) {
      rdma_dereg_mr(tcp->rmr);
    }
    gpr_free_aligned(tcp->rbuf);
  }
  if (tcp->target_buffer_list) {
    if (tcp->target_mr) {
      rdma_dereg_mr(tcp->target_mr);
    }
    gpr_free_aligned(tcp->target_buffer_list);
  }
  if (tcp->cm_id) {
    if (tcp->cm_id->qp) {
      fastlock_acquire(&tcp->cq_lock);
      ibv_ack_cq_events(tcp->cm_id->recv_cq, tcp->unack_cqe);
      rdma_destroy_qp(
          tcp->cm_id);  // fixme: if ibv_poll_cq is polling, destroy qp will
                        // execute ibv_destroy_cq which may cause SIGSEGV in
                        // providers/rxe/rxe_queue.h:73 queue_empty
      fastlock_release(&tcp->cq_lock);
      // gpr_log(GPR_INFO, "TCP:%p tcp_free rdma_destroy_qp", tcp);
    }
    if (tcp->cm_id->pd) {
      ibv_dealloc_pd(tcp->cm_id->pd);
    }
    // gpr_log(GPR_INFO, "%p tcp_free will rdma_destroy_id,
    // tcp->cm_id->channel->fd:%d, ev_channel->fd:%d ",tcp,
    // tcp->cm_id->channel->fd,tcp->cm_id->channel->fd);
    rdma_destroy_id(tcp->cm_id);
  }

  fastlock_destroy(&tcp->cq_wait_lock);
  fastlock_destroy(&tcp->cq_lock);
  fastlock_destroy(&tcp->rlock);
  fastlock_destroy(&tcp->slock);
  delete tcp;
}

#ifndef NDEBUG
#define TCP_UNREF(tcp, reason) tcp_unref((tcp), (reason), DEBUG_LOCATION)
#define TCP_REF(tcp, reason) tcp_ref((tcp), (reason), DEBUG_LOCATION)
static void tcp_unref(grpc_tcp* tcp, const char* reason,
                      const grpc_core::DebugLocation& debug_location) {
  if (GPR_UNLIKELY(tcp->refcount.Unref(debug_location, reason))) {
    tcp_free(tcp);
  }
}

static void tcp_ref(grpc_tcp* tcp, const char* reason,
                    const grpc_core::DebugLocation& debug_location) {
  tcp->refcount.Ref(debug_location, reason);
}
#else
#define TCP_UNREF(tcp, reason) tcp_unref((tcp))
#define TCP_REF(tcp, reason) tcp_ref((tcp))
static void tcp_unref(grpc_tcp* tcp) {
  if (GPR_UNLIKELY(tcp->refcount.Unref())) {
    tcp_free(tcp);
  }
}

static void tcp_ref(grpc_tcp* tcp) { tcp->refcount.Ref(); }
#endif

static void done_poller(void* bp, grpc_error* /*error_ignored*/) {
  backup_poller* p = static_cast<backup_poller*>(bp);
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "BACKUP_POLLER:%p destroy", p);
  }
  grpc_pollset_destroy(BACKUP_POLLER_POLLSET(p));
  gpr_free(p);
}

static void run_poller(void* bp, grpc_error* /*error_ignored*/) {
  backup_poller* p = static_cast<backup_poller*>(bp);
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "BACKUP_POLLER:%p run", p);
  }
  gpr_mu_lock(p->pollset_mu);
  grpc_millis deadline = grpc_core::ExecCtx::Get()->Now() + 10 * GPR_MS_PER_SEC;
  GRPC_LOG_IF_ERROR(
      "backup_poller:pollset_work",
      grpc_pollset_work(BACKUP_POLLER_POLLSET(p), nullptr, deadline));
  gpr_mu_unlock(p->pollset_mu);
  /* last "uncovered" notification is the ref that keeps us polling, if we get
   * there try a cas to release it */
  if (gpr_atm_no_barrier_load(&g_uncovered_notifications_pending) == 1 &&
      gpr_atm_full_cas(&g_uncovered_notifications_pending, 1, 0)) {
    gpr_mu_lock(p->pollset_mu);
    bool cas_ok =
        gpr_atm_full_cas(&g_backup_poller, reinterpret_cast<gpr_atm>(p), 0);
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "BACKUP_POLLER:%p done cas_ok=%d", p, cas_ok);
    }
    gpr_mu_unlock(p->pollset_mu);
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "BACKUP_POLLER:%p shutdown", p);
    }
    grpc_pollset_shutdown(BACKUP_POLLER_POLLSET(p),
                          GRPC_CLOSURE_INIT(&p->run_poller, done_poller, p,
                                            grpc_schedule_on_exec_ctx));
  } else {
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "BACKUP_POLLER:%p reschedule", p);
    }
    grpc_core::Executor::Run(&p->run_poller, GRPC_ERROR_NONE,
                             grpc_core::ExecutorType::DEFAULT,
                             grpc_core::ExecutorJobType::LONG);
  }
}

static void drop_uncovered(grpc_tcp* /*tcp*/) {
  backup_poller* p =
      reinterpret_cast<backup_poller*>(gpr_atm_acq_load(&g_backup_poller));
  gpr_atm old_count =
      gpr_atm_full_fetch_add(&g_uncovered_notifications_pending, -1);
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "BACKUP_POLLER:%p uncover cnt %d->%d", p,
            static_cast<int>(old_count), static_cast<int>(old_count) - 1);
  }
  GPR_ASSERT(old_count != 1);
}

// gRPC API considers a Write operation to be done the moment it clears ‘flow
// control’ i.e., not necessarily sent on the wire. This means that the
// application MIGHT not call `grpc_completion_queue_next/pluck` in a timely
// manner when its `Write()` API is acked.
//
// We need to ensure that the fd is 'covered' (i.e being monitored by some
// polling thread and progress is made) and hence add it to a backup poller here
static void cover_self(grpc_tcp* tcp) {
  backup_poller* p;
  gpr_atm old_count =
      gpr_atm_no_barrier_fetch_add(&g_uncovered_notifications_pending, 2);
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "BACKUP_POLLER: cover cnt %d->%d",
            static_cast<int>(old_count), 2 + static_cast<int>(old_count));
  }
  if (old_count == 0) {
    GRPC_STATS_INC_TCP_BACKUP_POLLERS_CREATED();
    p = static_cast<backup_poller*>(
        gpr_zalloc(sizeof(*p) + grpc_pollset_size()));
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "BACKUP_POLLER:%p create", p);
    }
    grpc_pollset_init(BACKUP_POLLER_POLLSET(p), &p->pollset_mu);
    gpr_atm_rel_store(&g_backup_poller, (gpr_atm)p);
    grpc_core::Executor::Run(
        GRPC_CLOSURE_INIT(&p->run_poller, run_poller, p, nullptr),
        GRPC_ERROR_NONE, grpc_core::ExecutorType::DEFAULT,
        grpc_core::ExecutorJobType::LONG);
  } else {
    while ((p = reinterpret_cast<backup_poller*>(
                gpr_atm_acq_load(&g_backup_poller))) == nullptr) {
      // spin waiting for backup poller
    }
  }
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "BACKUP_POLLER:%p add %p", p, tcp);
  }
  grpc_pollset_add_fd(BACKUP_POLLER_POLLSET(p), tcp->em_fd);
  if (old_count != 0) {
    drop_uncovered(tcp);
  }
}

static void notify_on_read(grpc_tcp* tcp) {
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p notify_on_read", tcp);
  }
  grpc_fd_notify_on_read(tcp->em_fd, &tcp->read_done_closure);
}

static void notify_on_write(grpc_tcp* tcp) {
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p notify_on_write", tcp);
  }
  if (!grpc_event_engine_run_in_background()) {
    cover_self(tcp);
  }
  grpc_fd_notify_on_write(tcp->em_fd, &tcp->write_done_closure);
}

static void rdma_handle_write(void* arg /* grpc_tcp */, grpc_error* error);
static void tcp_handle_error(void* arg /* grpc_tcp */, grpc_error* error);

static void rdma_drop_uncovered_then_handle_write(void* arg,
                                                  grpc_error* error) {
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p got_write: %s", arg, grpc_error_string(error));
  }
  drop_uncovered(static_cast<grpc_tcp*>(arg));
  rdma_handle_write(arg, error);
}

static bool grpc_tcp_have_rdata(grpc_tcp* tcp) {
  if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
    gpr_log(
        GPR_INFO,
        "TCP:%p rbuf_bytes_avail: %d, rbuf_free_offset: %d, rbuf_offset: %d",
        tcp, tcp->rbuf_bytes_avail, tcp->rbuf_free_offset, tcp->rbuf_offset);
  }
  return (tcp->rmsg_head != tcp->rmsg_tail);
}

static void rdma_read(grpc_endpoint* ep, grpc_slice_buffer* incoming_buffer,
                      grpc_closure* cb, bool /* urgent */) {
  grpc_tcp* tcp = reinterpret_cast<grpc_tcp*>(ep);
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p rdma_read", tcp);
  }
  GPR_ASSERT(tcp->read_cb == nullptr);
  tcp->read_cb = cb;
  tcp->incoming_buffer = incoming_buffer;
  grpc_slice_buffer_reset_and_unref_internal(incoming_buffer);
  grpc_slice_buffer_swap(incoming_buffer, &tcp->last_read_buffer);
  TCP_REF(tcp, "read");
  grpc_core::Closure::Run(DEBUG_LOCATION, &tcp->read_done_closure,
                          GRPC_ERROR_NONE);
}

static size_t get_target_read_size(grpc_tcp* tcp) {
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p get_target_read_size", tcp);
  }
  grpc_resource_quota* rq = grpc_resource_user_quota(tcp->resource_user);
  double pressure = grpc_resource_quota_get_memory_pressure(rq);
  double target =
      tcp->target_length * (pressure > 0.8 ? (1.0 - pressure) / 0.2 : 1.0);
  size_t sz = ((static_cast<size_t> GPR_CLAMP(target, tcp->min_read_chunk_size,
                                              tcp->max_read_chunk_size)) +
               255) &
              ~static_cast<size_t>(255);
  /* don't use more than 1/16th of the overall resource quota for a single read
   * alloc */
  size_t rqmax = grpc_resource_quota_peek_size(rq);
  if (sz > rqmax / 16 && rqmax > 1024) {
    sz = rqmax / 16;
  }
  return sz;
}

static grpc_error* tcp_annotate_error(grpc_error* src_error, grpc_tcp* tcp) {
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p tcp_annotate_error", tcp);
  }
  return grpc_error_set_str(
      grpc_error_set_int(grpc_error_set_int(src_error, GRPC_ERROR_INT_FD,
                                            grpc_fd_wrapped_fd(tcp->em_fd)),
                         /* All tcp errors are marked with UNAVAILABLE so that
                          * application may choose to retry. */
                         GRPC_ERROR_INT_GRPC_STATUS, GRPC_STATUS_UNAVAILABLE),
      GRPC_ERROR_STR_TARGET_ADDRESS,
      grpc_slice_from_copied_string(tcp->peer_string.c_str()));
}

static uint32_t tcp_sbuf_left(grpc_tcp* tcp) {
  return (uint32_t)(((uint64_t)(uintptr_t)&tcp->sbuf[tcp->sbuf_size]) -
                    tcp->ssgl[0].addr);
}

static bool grpc_tcp_can_send(grpc_tcp* tcp) {
  if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
    gpr_log(GPR_INFO,
            "TCP:%p sqe_avail:%d, tcp->sbuf_bytes_avail:%d, sseq_no:%d, "
            "sseq_comp:%d, tcp->target_sgl[%d].length: %d",
            tcp, tcp->sqe_avail, tcp->sbuf_bytes_avail, tcp->sseq_no,
            tcp->sseq_comp, tcp->target_sge,
            tcp->target_sgl[tcp->target_sge].length);
  }
  return tcp->sqe_avail && (tcp->sbuf_bytes_avail >= TCP_SNDLOWAT) &&
         (tcp->sseq_no != tcp->sseq_comp) &&
         (tcp->target_sgl[tcp->target_sge].length != 0);
}

static void grpc_tcp_copy_from_slices(void* dst, const grpc_slice_buffer* buf,
                                      size_t* outgoing_slice_idx,
                                      size_t* outgoing_byte_idx, size_t len) {
  size_t size;
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "grpc_tcp_copy_from_slices");
  }

  while (len) {
    void* src = GRPC_SLICE_START_PTR(buf->slices[*outgoing_slice_idx]) +
                *outgoing_byte_idx;
    size = GRPC_SLICE_LENGTH(buf->slices[*outgoing_slice_idx]) -
           *outgoing_byte_idx;
    if (size > len) {
      memcpy(dst, src, len);
      *outgoing_byte_idx += len;
      break;
    }

    memcpy(dst, src, size);
    len -= size;
    dst = (uint8_t*)dst + size;
    *outgoing_slice_idx += 1;
    *outgoing_byte_idx = 0;
  }
}

static inline bool grpc_tcp_ctrl_avail(grpc_tcp* tcp) {
  return tcp->ctrl_seqno != tcp->ctrl_max_seqno;
}

static bool grpc_tcp_give_credits(grpc_tcp* tcp) {
  return ((tcp->rbuf_bytes_avail >= (tcp->rbuf_size >> 1)) ||
          ((short)((short)tcp->rseq_no - (short)tcp->rseq_comp) >= 0)) &&
         grpc_tcp_ctrl_avail(tcp);
}

static void* grpc_tcp_get_ctrl_buf(grpc_tcp* tcp) {
  return tcp->sbuf + tcp->sbuf_size +
         TCP_MAX_CTRL_MSG * (tcp->ctrl_seqno & (TCP_QP_CTRL_SIZE - 1));
}

static int grpc_tcp_post_msg(grpc_tcp* tcp, uint32_t msg) {
  // if (tcp->cm_id->qp)
  //   gpr_log(GPR_INFO,"TCP:%p grpc_tcp_post_msg qp(%p) qp_num(%u) qp_state(%d)
  //   msg(%u)",tcp,tcp->cm_id->qp,tcp->cm_id->qp->qp_num,tcp->cm_id->qp->state,msg);
  // else
  //   gpr_log(GPR_INFO,"TCP:%p grpc_tcp_post_msg qp(none) msg(%u)",tcp,msg);
  struct ibv_send_wr wr, *bad;
  struct ibv_sge sge;

  wr.wr_id = tcp_send_wr_id(msg);
  wr.next = NULL;
  wr.sg_list = NULL;
  wr.num_sge = 0;
  wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
  wr.send_flags = 0;
  wr.imm_data = htobe32(msg);
  GPR_TIMER_SCOPE("grpc_tcp_post_msg", 0);
  return grpc_tcp_seterrno(ibv_post_send(tcp->cm_id->qp, &wr, &bad));
}

static int grpc_tcp_post_write_msg(grpc_tcp* tcp, struct ibv_sge* sgl, int nsge,
                                   uint32_t msg, int flags, uint64_t addr,
                                   uint32_t rkey) {
  struct ibv_send_wr wr, *bad;
  struct ibv_sge sge;
  int ret;
  // if (tcp->cm_id->qp)
  //   gpr_log(GPR_INFO,"TCP:%p grpc_tcp_post_write_msg qp(%p) qp_num(%u)
  //   qp_state(%d) msg(%u) remote_addr(%lu)
  //   rkey(%u)",tcp,tcp->cm_id->qp,tcp->cm_id->qp->qp_num,tcp->cm_id->qp->state,msg,addr,rkey);
  // else
  //   gpr_log(GPR_INFO,"TCP:%p grpc_tcp_post_write_msg qp 0x0 msg(%u)
  //   remote_addr(%lu) rkey(%u)",tcp,msg,addr,rkey);
  if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
    gpr_log(GPR_INFO, "TCP:%p grpc_tcp_post_write_msg rkey:%d", tcp, rkey);
  }

  wr.next = NULL;
  wr.wr_id = tcp_send_wr_id(msg);
  wr.sg_list = sgl;
  wr.num_sge = nsge;
  wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
  wr.send_flags = flags;
  wr.imm_data = htobe32(msg);
  wr.wr.rdma.remote_addr = addr;
  wr.wr.rdma.rkey = rkey;
  GPR_TIMER_SCOPE("grpc_tcp_post_write_msg", 0);
  return grpc_tcp_seterrno(ibv_post_send(tcp->cm_id->qp, &wr, &bad));
}

static int grpc_tcp_write_data(grpc_tcp* tcp, struct ibv_sge* sgl, int nsge,
                               uint32_t length, int flags) {
  uint64_t addr;
  uint32_t rkey;

  tcp->sseq_no++;
  tcp->sqe_avail--;
  tcp->sbuf_bytes_avail -= length;

  addr = tcp->target_sgl[tcp->target_sge].addr;
  rkey = tcp->target_sgl[tcp->target_sge].key;

  tcp->target_sgl[tcp->target_sge].addr += length;
  tcp->target_sgl[tcp->target_sge].length -= length;

  if (!tcp->target_sgl[tcp->target_sge].length) {
    if (++tcp->target_sge == TCP_SGL_SIZE) {
      tcp->target_sge = 0;
      if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
        gpr_log(GPR_INFO, "TCP:%p reset target_sge", tcp);
      }
    }
  }

  if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
    gpr_log(GPR_INFO, "TCP:%p write data length: %d", tcp, length);
    gpr_log(GPR_INFO, "TCP:%p sseq_no: %d, seq_avail: %d, sbuf_bytes_avail: %d",
            tcp, tcp->sseq_no, tcp->sqe_avail, tcp->sbuf_bytes_avail);
  }

  return grpc_tcp_post_write_msg(
      tcp, sgl, nsge, tcp_msg_set(TCP_OP_DATA, length), flags, addr, rkey);
}

static void grpc_tcp_send_credits(grpc_tcp* tcp) {
  if (tcp->remote_sgl.key ==
      0) {  // try to fix grpc_tcp_poll_fd called before save_conn_data
    if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
      gpr_log(GPR_INFO, "TCP:%p grpc_tcp_send_credits remote_sgl.key %d", tcp,
              tcp->remote_sgl.key);
    }
    return;
  }
  struct ibv_sge ibsge;
  grpc_tcp_sge sge, *sge_buf;
  int flags;

  tcp->ctrl_seqno++;
  tcp->rseq_comp = tcp->rseq_no + (tcp->rq_size >> 1);
  if (tcp->rbuf_bytes_avail >= (tcp->rbuf_size >> 1)) {
    if (!(tcp->opts & TCP_OPT_SWAP_SGL)) {
      sge.addr = (uintptr_t)&tcp->rbuf[tcp->rbuf_free_offset];
      sge.key = tcp->rmr->rkey;
      sge.length = tcp->rbuf_size >> 1;
    } else {
      sge.addr = bswap_64((uintptr_t)&tcp->rbuf[tcp->rbuf_free_offset]);
      sge.key = bswap_32(tcp->rmr->rkey);
      sge.length = bswap_32(tcp->rbuf_size >> 1);
    }

    if (tcp->sq_inline < sizeof(sge)) {
      sge_buf = static_cast<grpc_tcp_sge*>(grpc_tcp_get_ctrl_buf(tcp));
      memcpy(sge_buf, &sge, sizeof(sge));
      ibsge.addr = (uintptr_t)sge_buf;
      ibsge.lkey = tcp->smr->lkey;
      flags = 0;
    } else {
      ibsge.addr = (uintptr_t)&sge;
      ibsge.lkey = 0;
      flags = IBV_SEND_INLINE;
    }
    ibsge.length = sizeof(sge);

    if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
      gpr_log(GPR_INFO,
              "TCP:%p ctrl_seqno: %d, rseq_comp: %d, rbuf_free_offset: %d, "
              "remote_sge: %d, remote sseq_comp: %d",
              tcp, tcp->ctrl_seqno, tcp->rseq_comp, tcp->rbuf_free_offset,
              tcp->remote_sge, tcp->rseq_no + tcp->rq_size);
    }

    grpc_tcp_post_write_msg(
        tcp, &ibsge, 1, tcp_msg_set(TCP_OP_SGL, tcp->rseq_no + tcp->rq_size),
        flags,
        tcp->remote_sgl.addr + tcp->remote_sge * sizeof(struct grpc_tcp_sge),
        tcp->remote_sgl.key);

    tcp->rbuf_bytes_avail -= tcp->rbuf_size >> 1;
    tcp->rbuf_free_offset += tcp->rbuf_size >> 1;

    if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
      gpr_log(GPR_INFO,
              "TCP:%p update rbuf_bytes_avail: %d rbuf_free_offset: %d", tcp,
              tcp->rbuf_bytes_avail, tcp->rbuf_free_offset);
    }

    if (tcp->rbuf_free_offset >= tcp->rbuf_size) {
      if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
        gpr_log(GPR_INFO, "TCP:%p rbuf_free_offset(%d) >= rbuf_size(%d), reset",
                tcp, tcp->rbuf_free_offset, tcp->rbuf_size);
      }
      tcp->rbuf_free_offset = 0;
    }
    if (++tcp->remote_sge == tcp->remote_sgl.length) {
      tcp->remote_sge = 0;
      if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
        gpr_log(GPR_INFO, "TCP:%p reset remote_sge", tcp);
      }
    }
  } else {
    if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
      gpr_log(GPR_INFO, "TCP:%p rbuf_bytes_avail: %d, remote sseq_comp: %d",
              tcp, tcp->rbuf_bytes_avail, tcp->rseq_no + tcp->rq_size);
    }
    grpc_tcp_post_msg(tcp,
                      tcp_msg_set(TCP_OP_SGL, tcp->rseq_no + tcp->rq_size));
  }
}

static inline int grpc_tcp_post_recv(grpc_tcp* tcp) {
  struct ibv_recv_wr wr, *bad;
  struct ibv_sge sge;

  wr.next = NULL;
  wr.wr_id = tcp_recv_wr_id(0);
  wr.sg_list = NULL;
  wr.num_sge = 0;
  return grpc_tcp_seterrno(ibv_post_recv(tcp->cm_id->qp, &wr, &bad));
}

static int grpc_tcp_poll_cq(grpc_tcp* tcp) {
  struct ibv_wc wc;
  uint32_t msg;
  int ret, rcnt = 0;
  // gpr_log(GPR_INFO, "TCP:%p grpc_tcp_poll_cq",tcp);

  // while (tcp->state==tcp->CONNECTED && tcp->cm_id->recv_cq && (ret =
  // ibv_poll_cq(tcp->cm_id->recv_cq, 1, &wc)) > 0) {// fixme: maybe
  // tcp->cm_id->recv_cq maybe 0x0
  while (tcp->cm_id->recv_cq &&
         ((ret = ibv_poll_cq(tcp->cm_id->recv_cq, 1, &wc)) > 0)) {
    if (tcp_wr_is_recv(wc.wr_id)) {
      if (wc.status != IBV_WC_SUCCESS) {
        if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
          gpr_log(GPR_INFO, "TCP:%p wc status: %d, errno: %d", tcp, wc.status,
                  errno);
        }
        continue;
      }
      rcnt++;

      GPR_ASSERT(wc.wc_flags & IBV_WC_WITH_IMM);
      msg = be32toh(wc.imm_data);
      switch (tcp_msg_op(msg)) {
        case TCP_OP_SGL:
          tcp->sseq_comp = (uint16_t)tcp_msg_data(msg);
          if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
            gpr_log(GPR_INFO,
                    "TCP:%p sseq_comp: %d, 0's length: %d, 1's length: %d", tcp,
                    tcp->sseq_comp, tcp->target_sgl[0].length,
                    tcp->target_sgl[1].length);
          }
          break;
        case TCP_OP_WRITE:
          /* We really shouldn't be here. */
          GPR_ASSERT(1);
          break;
        default:
          tcp->rmsg[tcp->rmsg_tail].op = tcp_msg_op(msg);
          tcp->rmsg[tcp->rmsg_tail].data = tcp_msg_data(msg);
          if (++tcp->rmsg_tail == tcp->rq_size + 1) {
            tcp->rmsg_tail = 0;
            if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
              gpr_log(GPR_INFO, "TCP:%p reset rmsg_tail", tcp);
            }
          }
          if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
            gpr_log(GPR_INFO, "TCP:%p recv rmsg_tail: %d", tcp, tcp->rmsg_tail);
          }
          break;
      }
    } else {
      switch (tcp_msg_op(tcp_wr_data(wc.wr_id))) {
        case TCP_OP_SGL:
          tcp->ctrl_max_seqno++;
          if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
            gpr_log(GPR_INFO, "TCP:%p ctrl_max_seqno: %d", tcp,
                    tcp->ctrl_max_seqno);
          }
          break;
        case TCP_OP_CTRL:
          tcp->ctrl_max_seqno++;
          if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
            gpr_log(GPR_INFO, "TCP:%p ctrl_max_seqno: %d", tcp,
                    tcp->ctrl_max_seqno);
          }
          break;
        default:
          tcp->sqe_avail++;
          tcp->sbuf_bytes_avail += tcp_msg_data(tcp_wr_data(wc.wr_id));
          if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
            gpr_log(GPR_INFO, "TCP:%p sqe_avail: %d, sbuf_bytes_avail: %d", tcp,
                    tcp->sqe_avail, tcp->sbuf_bytes_avail);
          }
          break;
      }
      if (wc.status != IBV_WC_SUCCESS) {
        gpr_log(GPR_ERROR,
                "TCP:%p failed to handle RDMA cq, error status: %d, errno: %d",
                tcp, wc.status, errno);
        tcp->state = tcp->TCPERROR;
        break;
        // return -1;
      }
    }
  }

  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p polled %d recv from cq", tcp, rcnt);
  }

  while (!ret && rcnt--) {
    ret = grpc_tcp_post_recv(tcp);
  }

  if (ret) {
    gpr_log(GPR_ERROR, "TCP:%p failed to post recv: %d", tcp, errno);
  }
  return ret;
}

static void grpc_tcp_update_credits(grpc_tcp* tcp) {
  if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
    gpr_log(GPR_INFO, "TCP:%p grpc_tcp_update_credits", tcp);
  }
  if (grpc_tcp_give_credits(tcp)) {
    grpc_tcp_send_credits(tcp);
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "TCP:%p update credits", tcp);
    }
  }
}

static int __grpc_tcp_get_cq_event(grpc_tcp* tcp) {
  struct ibv_cq* cq;
  void* context;
  int ret;
  if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
    gpr_log(GPR_INFO, "TCP:%p __grpc_tcp_get_cq_event", tcp);
  }

  if (!tcp->cq_armed) {
    return 0;
  }

  ret = ibv_get_cq_event(tcp->cm_id->recv_cq_channel, &cq, &context);
  if (!ret) {
    if (++tcp->unack_cqe >= tcp->sq_size + tcp->rq_size) {
      ibv_ack_cq_events(tcp->cm_id->recv_cq, tcp->unack_cqe);
      if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
        gpr_log(GPR_INFO, "TCP:%p ack %d events", tcp, tcp->unack_cqe);
      }
      tcp->unack_cqe = 0;
    }
    tcp->cq_armed = 0;
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "TCP:%p set cq_armed to zero", tcp);
    }
  } else if (!(errno == EAGAIN || errno == EINTR)) {
    gpr_log(GPR_ERROR, "TCP:%p failed to get cq event: %d", tcp, errno);
  }

  return ret;
}

int grpc_tcp_get_cq_event(grpc_tcp* tcp) {
  int ret;
  fastlock_acquire(&tcp->cq_wait_lock);
  ret = __grpc_tcp_get_cq_event(tcp);
  fastlock_release(&tcp->cq_wait_lock);
  return ret;
}

static int grpc_tcp_process_cq(grpc_tcp* tcp, int nonblock,
                               bool (*cb)(grpc_tcp* tcp)) {
  int ret;
  if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
    // gpr_log(GPR_INFO, "TCP:%p grpc_tcp_process_cq",tcp);
  }

  GPR_TIMER_SCOPE("grpc_tcp_process_cq", 0);

  fastlock_acquire(&tcp->cq_lock);
  do {
    grpc_tcp_update_credits(tcp);
    ret = grpc_tcp_poll_cq(tcp);
    if (cb(tcp)) {
      ret = 0;
      if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
        gpr_log(GPR_INFO, "TCP:%p cb return true", tcp);
      }
      break;
    } else if (ret) {
      if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
        gpr_log(GPR_INFO, "TCP:%p ret: %d", tcp, ret);
      }
      break;
    } else if (nonblock) {
      ret = ERR(EWOULDBLOCK);
      if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
        gpr_log(GPR_INFO, "TCP:%p EWOULDBLOCK", tcp);
      }
    } else if (!tcp->cq_armed) {
      int ret = ibv_req_notify_cq(tcp->cm_id->recv_cq, 0);
      if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
        if (ret < 0) {
          gpr_log(GPR_INFO,
                  "TCP:%p ibv_req_notify_cq failed: %d, set cq_armed to 1", tcp,
                  errno);
        } else {
          gpr_log(GPR_INFO, "TCP:%p ibv_req_notify_cq, set cq_armed to 1", tcp);
        }
      }
      tcp->cq_armed = 1;
    } else {
      grpc_tcp_update_credits(tcp);
      fastlock_acquire(&tcp->cq_wait_lock);
      fastlock_release(&tcp->cq_lock);

      ret = __grpc_tcp_get_cq_event(tcp);
      fastlock_release(&tcp->cq_wait_lock);
      fastlock_acquire(&tcp->cq_lock);
    }
  } while (!ret);

  grpc_tcp_update_credits(tcp);
  fastlock_release(&tcp->cq_lock);
  return ret;
}

static int grpc_tcp_get_comp(grpc_tcp* tcp, int nonblock,
                             bool (*cb)(grpc_tcp* tcp)) {
  uint64_t start_time = 0;
  uint32_t poll_time;
  int ret;
  if (GRPC_TRACE_FLAG_ENABLED(grpc_rdma_trace)) {
    gpr_log(GPR_INFO, "TCP:%p grpc_tcp_get_comp", tcp);
  }

  GPR_TIMER_SCOPE("grpc_tcp_get_comp", 0);

  do {
    ret = grpc_tcp_process_cq(tcp, 1, cb);
    if (!ret || errno != EWOULDBLOCK) {
      return ret;
    }
    if (!start_time) {
      start_time = grpc_tcp_time_us();
    }
    poll_time = (uint32_t)(grpc_tcp_time_us() - start_time);
  } while (poll_time <= polling_time);

  ret = grpc_tcp_process_cq(tcp, 0, cb);
  return ret;
}

static bool rdma_flush(grpc_tcp* tcp, grpc_error** error) {
  size_t left, len, offset = 0;
  uint32_t xfer_size, olen = TCP_OLAP_START_SIZE;
  int i, ret = 0;
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p rdma_flush", tcp);
  }

  len = GRPC_SLICE_LENGTH(tcp->outgoing_buffer->slices[0]) -
        tcp->outgoing_byte_idx;
  for (i = 1; i < tcp->outgoing_buffer->count; ++i) {
    len += GRPC_SLICE_LENGTH(tcp->outgoing_buffer->slices[i]);
  }
  left = len;

  size_t unwind_slice_idx;
  size_t unwind_byte_idx;

  // We always start at zero, because we eagerly unref and trim the slice
  // buffer as we write
  size_t outgoing_slice_idx = 0;

  fastlock_acquire(&tcp->slock);
  for (; left; left -= xfer_size) {
    unwind_slice_idx = outgoing_slice_idx;
    unwind_byte_idx = tcp->outgoing_byte_idx;

    if (tcp->state == tcp->SHUTDOWN) {  //||tcp->state == tcp->TCPERROR
      if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
        gpr_log(GPR_INFO, "TCP:%p already shutdown", tcp);
      }
      *error = tcp_annotate_error(
          GRPC_ERROR_CREATE_FROM_STATIC_STRING("Socket closed"), tcp);
      goto out;
    }

    if (!grpc_tcp_can_send(tcp)) {
      if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
        gpr_log(GPR_INFO, "TCP:%p cann't send need to get comp", tcp);
      }
      ret = grpc_tcp_get_comp(tcp, 1, grpc_tcp_can_send);
      if (ret) {
        goto delayed;
      }
    }

    if (olen < left) {
      xfer_size = olen;
      if (olen < TCP_MAX_TRANSFER) {
        olen <<= 1;
      }
    } else {
      xfer_size = left;
    }

    if (xfer_size > tcp->sbuf_bytes_avail) {
      xfer_size = tcp->sbuf_bytes_avail;
    }
    if (xfer_size > tcp->target_sgl[tcp->target_sge].length) {
      xfer_size = tcp->target_sgl[tcp->target_sge].length;
    }

    if (xfer_size <= tcp_sbuf_left(tcp)) {
      grpc_tcp_copy_from_slices((void*)(uintptr_t)tcp->ssgl[0].addr,
                                tcp->outgoing_buffer, &outgoing_slice_idx,
                                &tcp->outgoing_byte_idx, xfer_size);
      tcp->ssgl[0].length = xfer_size;
      ret = grpc_tcp_write_data(
          tcp, tcp->ssgl, 1, xfer_size,
          xfer_size <= tcp->sq_inline ? IBV_SEND_INLINE : 0);
      if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
        gpr_log(GPR_INFO, "TCP:%p send with 1 sgl", tcp);
      }
      if (xfer_size < tcp_sbuf_left(tcp)) {
        tcp->ssgl[0].addr += xfer_size;
      } else {
        tcp->ssgl[0].addr = (uintptr_t)tcp->sbuf;
      }
    } else {
      tcp->ssgl[0].length = tcp_sbuf_left(tcp);
      grpc_tcp_copy_from_slices((void*)(uintptr_t)tcp->ssgl[0].addr,
                                tcp->outgoing_buffer, &outgoing_slice_idx,
                                &tcp->outgoing_byte_idx, tcp->ssgl[0].length);
      tcp->ssgl[1].length = xfer_size - tcp->ssgl[0].length;
      grpc_tcp_copy_from_slices(tcp->sbuf, tcp->outgoing_buffer,
                                &outgoing_slice_idx, &tcp->outgoing_byte_idx,
                                tcp->ssgl[1].length);
      ret = grpc_tcp_write_data(
          tcp, tcp->ssgl, 2, xfer_size,
          xfer_size <= tcp->sq_inline ? IBV_SEND_INLINE : 0);
      if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
        gpr_log(GPR_INFO, "TCP:%p send with 2 sgls", tcp);
      }
      tcp->ssgl[0].addr = (uintptr_t)tcp->sbuf + tcp->ssgl[1].length;
    }
    if (ret) {
      // TODO: any other errors can delayed?
      if (errno == ENOMEM) {
        if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
          gpr_log(GPR_INFO, "TCP:%p grpc_tcp_write_data failed, errno: %d", tcp,
                  errno);
        }
        goto delayed;
      } else {
        goto err_out;
      }
    }
  }

  if (!ret) {
    *error = GRPC_ERROR_NONE;
    goto out;
  }

delayed:
  fastlock_release(&tcp->slock);
  tcp->outgoing_byte_idx = unwind_byte_idx;
  // unref all and forget about all slices that have been written to this
  // point
  for (size_t idx = 0; idx < unwind_slice_idx; ++idx) {
    grpc_slice_buffer_remove_first(tcp->outgoing_buffer);
  }
  return false;
err_out:
  *error = tcp_annotate_error(GRPC_OS_ERROR(errno, "ibv_post_send"), tcp);
out:
  fastlock_release(&tcp->slock);
  grpc_slice_buffer_reset_and_unref_internal(tcp->outgoing_buffer);
  return true;
}

static void rdma_add_to_pollset(grpc_endpoint* ep, grpc_pollset* pollset) {
  grpc_tcp* tcp = reinterpret_cast<grpc_tcp*>(ep);
  gpr_log(GPR_DEBUG, "TCP:%p rdma_add_to_pollset", tcp);
  grpc_pollset_add_fd(pollset, tcp->em_fd);
}

void grpc_tcp_add_to_pollset(grpc_endpoint* ep, grpc_pollset* pollset) {
  rdma_add_to_pollset(ep, pollset);
}

grpc_fd* grpc_tcp_get_cq_fd(grpc_endpoint* ep) {
  grpc_tcp* tcp = reinterpret_cast<grpc_tcp*>(ep);
  gpr_log(GPR_DEBUG, "TCP:%p grpc_tcp_get_cq_fd", tcp);
  return tcp->em_fd;
}

void grpc_tcp_shutdown_ep(grpc_endpoint* ep) {
  grpc_tcp* tcp = reinterpret_cast<grpc_tcp*>(ep);
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p grpc_tcp_shutdown_ep", tcp);
  }
  tcp->state = tcp->SHUTDOWN;
}

static void rdma_add_to_pollset_set(grpc_endpoint* ep,
                                    grpc_pollset_set* pollset_set) {
  grpc_tcp* tcp = reinterpret_cast<grpc_tcp*>(ep);
  gpr_log(GPR_DEBUG, "TCP:%p rdma_add_to_pollset_set", tcp);
  grpc_pollset_set_add_fd(pollset_set, tcp->em_fd);
}

static void rdma_delete_from_pollset_set(grpc_endpoint* ep,
                                         grpc_pollset_set* pollset_set) {
  grpc_tcp* tcp = reinterpret_cast<grpc_tcp*>(ep);
  gpr_log(GPR_DEBUG, "TCP:%p rdma_delete_from_pollset_set", tcp);
  grpc_pollset_set_del_fd(pollset_set, tcp->em_fd);
}

static void rdma_shutdown(grpc_endpoint* ep, grpc_error* why) {
  grpc_tcp* tcp = reinterpret_cast<grpc_tcp*>(ep);
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p rdma_shutdown", tcp);
  }
  if (!grpc_fd_is_shutdown(tcp->em_fd)) {
    grpc_fd_shutdown(tcp->em_fd, why);
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "TCP:%p shutdown compl channel fd", tcp);
    }
  } else {
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "TCP:%p compl channel fd shutdown at other place", tcp);
    }
  }
  if (tcp->cm_fd &&
      !grpc_fd_is_shutdown(
          tcp->cm_fd)) {  // cm_fd 对于client，记录的是 async_connect 实例化时
                          // 通过 cm_id->channel->fd 封装得到的
                          // grpc_fd，grcp_fd->fd 字段就是 cm_id->channel->fd
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "TCP:%p rdma_shutdown rdma_disconnect", tcp);
    }
    rdma_disconnect(tcp->cm_id);
    grpc_fd_shutdown(tcp->cm_fd,
                     why);  // grpc_fd_shutdown 的实现位于 ev_epoll1_linux.cc/
                            // fd_shutdown(grpc_fd,why) ，实际执行了 shutdown
                            // 系统调用 shutdown(fd->fd, SHUT_RDWR)
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "TCP:%p shutdown cm fd", tcp);
    }
  } else {
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "TCP:%p cm fd shutdown at other place", tcp);
    }
  }
  grpc_resource_user_shutdown(tcp->resource_user);
}

grpc_fd* grpc_tcp_get_rdma_compl_channel_fd(grpc_endpoint* ep) {
  grpc_tcp* tcp = reinterpret_cast<grpc_tcp*>(ep);
  return tcp->em_fd;
}

static void rdma_destroy(grpc_endpoint* ep) {
  grpc_tcp* tcp = reinterpret_cast<grpc_tcp*>(ep);
  gpr_log(GPR_DEBUG, "TCP:%p rdma_destroy", tcp);
  grpc_slice_buffer_reset_and_unref_internal(&tcp->last_read_buffer);
  TCP_UNREF(tcp, "destroy");
}

static grpc_resource_user* rdma_get_resource_user(grpc_endpoint* ep) {
  grpc_tcp* tcp = (grpc_tcp*)ep;
  gpr_log(GPR_DEBUG, "TCP:%p rdma_get_resource_user", tcp);
  return tcp->resource_user;
}

static absl::string_view rdma_get_peer(grpc_endpoint* ep) {
  grpc_tcp* tcp = (grpc_tcp*)ep;
  gpr_log(GPR_DEBUG, "TCP:%p rdma_get_peer", tcp);
  return tcp->peer_string;
}

static absl::string_view rdma_get_local_address(grpc_endpoint* ep) {
  grpc_tcp* tcp = (grpc_tcp*)ep;
  gpr_log(GPR_DEBUG, "TCP:%p rdma_get_local_address", tcp);
  return tcp->peer_string;
}

static int rdma_get_fd(grpc_endpoint* ep) {
  grpc_tcp* tcp = (grpc_tcp*)ep;
  gpr_log(GPR_DEBUG, "TCP:%p rdma_get_fd", tcp);
  return -1;
}

static bool rdma_can_track_err(grpc_endpoint* ep) { return false; }

static void call_read_cb(grpc_tcp* tcp, grpc_error* error) {
  grpc_closure* cb = tcp->read_cb;
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p call_read_cb", tcp);
  }

  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p call_cb %p %p:%p", tcp, cb, cb->cb, cb->cb_arg);
    size_t i;
    const char* str = grpc_error_string(error);
    gpr_log(GPR_INFO, "READ %p (peer=%s) error=%s", tcp,
            tcp->peer_string.c_str(), str);

    if (gpr_should_log(GPR_LOG_SEVERITY_DEBUG)) {
      for (i = 0; i < tcp->incoming_buffer->count; i++) {
        char* dump = grpc_dump_slice(tcp->incoming_buffer->slices[i],
                                     GPR_DUMP_HEX | GPR_DUMP_ASCII);
        gpr_log(GPR_DEBUG, "TCP:%p DATA: %s", tcp, dump);
        gpr_free(dump);
      }
    }
    gpr_log(GPR_INFO, "TCP:%p READ length: %ld", tcp,
            tcp->incoming_buffer->length);
  }

  tcp->read_cb = nullptr;
  tcp->incoming_buffer = nullptr;
  grpc_core::Closure::Run(DEBUG_LOCATION, cb, error);
}

static void rdma_do_read(grpc_tcp* tcp) {
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p rdma_do_read", tcp);
  }
  GPR_TIMER_SCOPE("rdma_do_read", 0);
  ssize_t left, i, len = 0;
  uint32_t end_size, rsize;
  int ret = 0;

  /*
   * Simple, straightforward implementation for now that only tries to fill
   * in the first vector.
   */
  void* buf = GRPC_SLICE_START_PTR(tcp->incoming_buffer->slices[0]);
  len = GRPC_SLICE_LENGTH(tcp->incoming_buffer->slices[0]);
  left = len;

  fastlock_acquire(&tcp->rlock);
  do {
    if (!grpc_tcp_have_rdata(tcp)) {
      if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
        gpr_log(GPR_INFO, "TCP:%p no data to read need to get comp", tcp);
      }
      ret = grpc_tcp_get_comp(tcp, 1, grpc_tcp_have_rdata);
      if (ret) {
        break;
      }
    }

    for (; left && grpc_tcp_have_rdata(tcp); left -= rsize) {
      if (left < tcp->rmsg[tcp->rmsg_head].data) {
        rsize = left;
        tcp->rmsg[tcp->rmsg_head].data -= left;
      } else {
        tcp->rseq_no++;
        rsize = tcp->rmsg[tcp->rmsg_head].data;
        if (++tcp->rmsg_head == tcp->rq_size + 1) tcp->rmsg_head = 0;
      }

      end_size = tcp->rbuf_size - tcp->rbuf_offset;
      if (rsize > end_size) {
        memcpy(buf, &tcp->rbuf[tcp->rbuf_offset], end_size);
        tcp->rbuf_offset = 0;
        buf = (uint8_t*)buf + end_size;
        rsize -= end_size;
        left -= end_size;
        tcp->rbuf_bytes_avail += end_size;
      }
      memcpy(buf, &tcp->rbuf[tcp->rbuf_offset], rsize);
      tcp->rbuf_offset += rsize;
      buf = (uint8_t*)buf + rsize;
      tcp->rbuf_bytes_avail += rsize;
    }
  } while (left);
  fastlock_release(&tcp->rlock);

  if (tcp->state == tcp->SHUTDOWN) {  //||tcp->state == tcp->TCPERROR
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "TCP:%p already shutdown", tcp);
    }
    grpc_slice_buffer_reset_and_unref_internal(tcp->incoming_buffer);
    call_read_cb(
        tcp, tcp_annotate_error(
                 GRPC_ERROR_CREATE_FROM_STATIC_STRING("Socket closed"), tcp));
    TCP_UNREF(tcp, "read");
    return;
  }

  size_t total_read_bytes = len - left;
  if (total_read_bytes > 0) {
    if (total_read_bytes < tcp->incoming_buffer->length) {
      if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
        gpr_log(GPR_INFO, "TCP:%p trim incoming buffer %ld", tcp,
                tcp->incoming_buffer->length - total_read_bytes);
      }
      grpc_slice_buffer_trim_end(
          tcp->incoming_buffer, tcp->incoming_buffer->length - total_read_bytes,
          &tcp->last_read_buffer);
    }
    call_read_cb(tcp, GRPC_ERROR_NONE);
    TCP_UNREF(tcp, "read");
  } else {
    notify_on_read(tcp);
  }
}

static void tcp_read_allocation_done(void* tcpp, grpc_error* error) {
  grpc_tcp* tcp = static_cast<grpc_tcp*>(tcpp);
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p tcp_read_allocation_done", tcp);
  }
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p read_allocation_done: %s", tcp,
            grpc_error_string(error));
  }
  if (GPR_UNLIKELY(error != GRPC_ERROR_NONE)) {
    grpc_slice_buffer_reset_and_unref_internal(tcp->incoming_buffer);
    grpc_slice_buffer_reset_and_unref_internal(&tcp->last_read_buffer);
    call_read_cb(tcp, GRPC_ERROR_REF(error));
    TCP_UNREF(tcp, "read");
  } else {
    rdma_do_read(tcp);
  }
}

#define MAX_READ_IOVEC 1

static void rdma_continue_read(grpc_tcp* tcp) {
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p rdma_continue_read", tcp);
  }
  size_t target_read_size = get_target_read_size(tcp);
  /* Wait for allocation only when there is no buffer left. */
  if (tcp->incoming_buffer->length == 0 &&
      tcp->incoming_buffer->count < MAX_READ_IOVEC) {
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "TCP:%p alloc_slices, size: %ld", tcp,
              target_read_size);
    }
    if (GPR_UNLIKELY(!grpc_resource_user_alloc_slices(&tcp->slice_allocator,
                                                      target_read_size, 1,
                                                      tcp->incoming_buffer))) {
      if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
        gpr_log(GPR_INFO, "TCP:%p wait for allocation", tcp);
      }
      // Wait for allocation.
      return;
    }
  }
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p do_read", tcp);
  }
  rdma_do_read(tcp);
}

static void rdma_write(grpc_endpoint* ep, grpc_slice_buffer* buf,
                       grpc_closure* cb, void* /* arg */) {
  GPR_TIMER_SCOPE("rdma_write", 0);
  grpc_tcp* tcp = reinterpret_cast<grpc_tcp*>(ep);
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p rdma_write", tcp);
  }
  grpc_error* error = GRPC_ERROR_NONE;

  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    size_t i;

    gpr_log(GPR_INFO, "WRITE %p (peer=%s)", tcp, tcp->peer_string.c_str());
    for (i = 0; i < buf->count; i++) {
      if (gpr_should_log(GPR_LOG_SEVERITY_DEBUG)) {
        char* data =
            grpc_dump_slice(buf->slices[i], GPR_DUMP_HEX | GPR_DUMP_ASCII);
        gpr_log(GPR_DEBUG, "TCP:%p DATA: %s", tcp, data);
        gpr_free(data);
      }
    }
    gpr_log(GPR_INFO, "TCP:%p WRITE length: %ld, buf size: %ld", tcp,
            buf->length, buf->count);
  }

  GPR_ASSERT(tcp->write_cb == nullptr);

  if (buf->length == 0) {
    grpc_core::Closure::Run(
        DEBUG_LOCATION, cb,
        grpc_fd_is_shutdown(tcp->em_fd)
            ? tcp_annotate_error(GRPC_ERROR_CREATE_FROM_STATIC_STRING("EOF"),
                                 tcp)
            : GRPC_ERROR_NONE);
    return;
  }

  tcp->outgoing_buffer = buf;
  tcp->outgoing_byte_idx = 0;

  bool flush_result = rdma_flush(tcp, &error);
  if (!flush_result) {
    TCP_REF(tcp, "write");
    tcp->write_cb = cb;
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "TCP:%p write: delayed", tcp);
    }
    notify_on_write(tcp);
  } else {
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      const char* str = grpc_error_string(error);
      gpr_log(GPR_INFO, "TCP:%p write: %s", tcp, str);
    }
    grpc_core::Closure::Run(DEBUG_LOCATION, cb, error);
  }
}

bool grpc_tcp_poll_all(grpc_tcp* tcp) { return true; }

bool grpc_tcp_is_cq_armed(grpc_tcp* tcp) { return tcp->cq_armed; }

int grpc_tcp_poll_fd(grpc_tcp* tcp, int events, int nonblock,
                     bool (*test)(grpc_tcp* tcp)) {
  grpc_tcp_process_cq(
      tcp, nonblock,
      test);  // 2020/01/29 fixme: tcp->target_sgl.rkey may be 0 until
              // save_conn_data, but upper pollable_poll_events/check/arm will
              // call grpc_tcp_poll_fd before save_conn_data
  int revents = 0;
  if ((events & EPOLLIN) && grpc_tcp_have_rdata(tcp)) {
    revents |= EPOLLIN;
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "TCP:%p got EPOLLIN", tcp);
    }
  }
  if ((events & EPOLLOUT) && grpc_tcp_can_send(tcp) && tcp->write_cb) {
    revents |= EPOLLOUT;
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "TCP:%p got EPOLLOUT", tcp);
    }
  }
  if (tcp->state == tcp->TCPERROR) {  // fixed
    revents |= EPOLLERR;
  }
  if (events & EPOLLERR) {
    revents |= EPOLLERR;
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "TCP:%p got EPOLLERR", tcp);
    }
  } else if (tcp->state == tcp->SHUTDOWN) {
    revents |= EPOLLHUP;
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "TCP:%p got EPOLLHUP", tcp);
    }
  }
  return revents;
}

static void tcp_handle_error(void* arg /* grpc_tcp */, grpc_error* error) {
  grpc_tcp* tcp = static_cast<grpc_tcp*>(arg);
  // if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
  gpr_log(GPR_INFO, "TCP:%p got_error: %s", tcp, grpc_error_string(error));
  // }

  // if (error != GRPC_ERROR_NONE ||
  //     static_cast<bool>(gpr_atm_acq_load(&tcp->stop_error_notification))) {
  //   /* We aren't going to register to hear on error anymore, so it is safe to
  //    * unref. */
  TCP_UNREF(tcp, "error-handle");
  return;
  // }
}

static void rdma_handle_write(void* arg /* grpc_tcp */, grpc_error* error) {
  grpc_tcp* tcp = static_cast<grpc_tcp*>(arg);
  grpc_closure* cb;
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p rdma_handle_write", tcp);
  }

  if (error != GRPC_ERROR_NONE) {
    cb = tcp->write_cb;
    tcp->write_cb = nullptr;
    grpc_core::Closure::Run(DEBUG_LOCATION, cb, GRPC_ERROR_REF(error));
    TCP_UNREF(tcp, "write");
    return;
  }

  bool flush_result = rdma_flush(tcp, &error);
  if (!flush_result) {
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "TCP:%p write: delayed", tcp);
    }
    notify_on_write(tcp);
    // tcp_flush does not populate error if it has returned false.
    GPR_DEBUG_ASSERT(error == GRPC_ERROR_NONE);
  } else {
    cb = tcp->write_cb;
    tcp->write_cb = nullptr;
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      const char* str = grpc_error_string(error);
      gpr_log(GPR_INFO, "TCP:%p write: %s", tcp, str);
    }
    // No need to take a ref on error since tcp_flush provides a ref.
    grpc_core::Closure::Run(DEBUG_LOCATION, cb, error);
    TCP_UNREF(tcp, "write");
  }
}

static void rdma_handle_read(void* arg /* grpc_tcp */, grpc_error* error) {
  grpc_tcp* tcp = static_cast<grpc_tcp*>(arg);
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p rdma_handle_read", tcp);
  }
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p got_read: %s", tcp, grpc_error_string(error));
  }

  if (GPR_UNLIKELY(error != GRPC_ERROR_NONE)) {
    grpc_slice_buffer_reset_and_unref_internal(tcp->incoming_buffer);
    grpc_slice_buffer_reset_and_unref_internal(&tcp->last_read_buffer);
    call_read_cb(tcp, GRPC_ERROR_REF(error));
    TCP_UNREF(tcp, "read");
  } else {
    rdma_continue_read(tcp);
  }
}

static const grpc_endpoint_vtable vtable = {rdma_read,
                                            rdma_write,
                                            rdma_add_to_pollset,
                                            rdma_add_to_pollset_set,
                                            rdma_delete_from_pollset_set,
                                            rdma_shutdown,
                                            rdma_destroy,
                                            rdma_get_resource_user,
                                            rdma_get_peer,
                                            rdma_get_local_address,
                                            rdma_get_fd,
                                            rdma_can_track_err};

static grpc_error* grpc_tcp_init_bufs(grpc_tcp* tcp) {
  size_t len, pg_size = sysconf(_SC_PAGESIZE);
  uint32_t total_rbuf_size, total_sbuf_size;
  grpc_error* error;

  len = (tcp->rq_size + 1) * sizeof(*tcp->rmsg);
  tcp->rmsg = static_cast<grpc_tcp_msg*>(gpr_malloc_aligned(len, pg_size));
  memset(tcp->rmsg, 0, len);
  total_sbuf_size = tcp->sbuf_size;
  if (tcp->sq_inline < TCP_MAX_CTRL_MSG)
    total_sbuf_size += TCP_MAX_CTRL_MSG * TCP_QP_CTRL_SIZE;
  tcp->sbuf =
      static_cast<uint8_t*>(gpr_malloc_aligned(total_sbuf_size, pg_size));
  memset(tcp->sbuf, 0, total_sbuf_size);
  tcp->smr = rdma_reg_msgs(tcp->cm_id, tcp->sbuf, total_sbuf_size);
  if (!tcp->smr) {
    error = grpc_error_set_int(
        GRPC_ERROR_CREATE_FROM_STATIC_STRING("failed to call rmda_reg_msgs"),
        GRPC_ERROR_INT_ERRNO, errno);
    goto err_out1;
  }
  len = sizeof(*tcp->target_sgl) * TCP_SGL_SIZE;
  tcp->target_buffer_list = gpr_malloc_aligned(len, pg_size);
  tcp->target_mr = rdma_reg_write(tcp->cm_id, tcp->target_buffer_list, len);
  if (!tcp->target_mr) {
    error = grpc_error_set_int(
        GRPC_ERROR_CREATE_FROM_STATIC_STRING("failed to call rmda_reg_write"),
        GRPC_ERROR_INT_ERRNO, errno);
    goto err_out2;
  }
  memset(tcp->target_buffer_list, 0, len);
  tcp->target_sgl =
      static_cast<volatile grpc_tcp_sge*>(tcp->target_buffer_list);
  total_rbuf_size = tcp->rbuf_size;
  tcp->rbuf =
      static_cast<uint8_t*>(gpr_malloc_aligned(total_rbuf_size, pg_size));
  tcp->rmr = rdma_reg_write(tcp->cm_id, tcp->rbuf, total_rbuf_size);
  if (!tcp->rmr) {
    error = grpc_error_set_int(
        GRPC_ERROR_CREATE_FROM_STATIC_STRING("failed to call rmda_reg_write"),
        GRPC_ERROR_INT_ERRNO, errno);
    goto err_out3;
  }

  tcp->ssgl[0].addr = tcp->ssgl[1].addr = (uintptr_t)tcp->sbuf;
  tcp->sbuf_bytes_avail = tcp->sbuf_size;
  tcp->ssgl[0].lkey = tcp->ssgl[1].lkey = tcp->smr->lkey;

  tcp->rbuf_free_offset = tcp->rbuf_size >> 1;
  tcp->rbuf_bytes_avail = tcp->rbuf_size >> 1;
  tcp->sqe_avail = tcp->sq_size - tcp->ctrl_max_seqno;
  tcp->rseq_comp = tcp->rq_size >> 1;

  return GRPC_ERROR_NONE;

err_out3:
  gpr_free_aligned(tcp->rbuf);
err_out2:
  gpr_free_aligned(tcp->target_buffer_list);
err_out1:
  gpr_free_aligned(tcp->rmsg);
  gpr_free_aligned(tcp->sbuf);
  return error;
}

static grpc_error* grpc_tcp_create_cq(grpc_tcp* tcp) {
  struct rdma_cm_id* cm_id = tcp->cm_id;
  grpc_error* error;
  // // create pd explicitly
  cm_id->pd = ibv_alloc_pd(cm_id->verbs);
  if (!cm_id->pd) {
    return grpc_error_set_int(
        GRPC_ERROR_CREATE_FROM_STATIC_STRING("failed to call ibv_alloc_pd"),
        GRPC_ERROR_INT_ERRNO, errno);
  }
  cm_id->recv_cq_channel = ibv_create_comp_channel(cm_id->verbs);
  if (!cm_id->recv_cq_channel) {
    error = grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                   "failed to call ibv_create_comp_channel"),
                               GRPC_ERROR_INT_ERRNO, errno);
    goto err_out0;
  }
  // grpc_error* error;
  cm_id->recv_cq = ibv_create_cq(tcp->cm_id->verbs, tcp->sq_size + tcp->rq_size,
                                 cm_id, cm_id->recv_cq_channel, 0);
  if (!cm_id->recv_cq) {
    error = grpc_error_set_int(
        GRPC_ERROR_CREATE_FROM_STATIC_STRING("failed to call ibv_create_cq"),
        GRPC_ERROR_INT_ERRNO, errno);
    goto err_out1;
  }
  {
    int flags, fd = cm_id->recv_cq_channel->fd;
    flags = fcntl(fd, F_GETFL);
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
      error = grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                     "grpc tcp fcntl comp channel failed"),
                                 GRPC_ERROR_INT_ERRNO, errno);
      goto err_out2;
    }
  }
  ibv_req_notify_cq(cm_id->recv_cq, 0);
  cm_id->send_cq_channel = cm_id->recv_cq_channel;
  cm_id->send_cq = cm_id->recv_cq;
  return GRPC_ERROR_NONE;

err_out2:
  ibv_destroy_cq(cm_id->recv_cq);
  cm_id->recv_cq = nullptr;
err_out1:
  ibv_destroy_comp_channel(cm_id->recv_cq_channel);
  cm_id->recv_cq_channel = nullptr;
err_out0:
  ibv_dealloc_pd(cm_id->pd);
  return error;
}

static void grpc_tcp_setup_qp_size(grpc_tcp* tcp) {
  uint16_t max_size;

  // TODO: min(ucma_max_qpsize(tcp->cm_id), TCP_QP_MAX_SIZE);
  max_size = TCP_QP_MAX_SIZE;

  if (tcp->sq_size > max_size)
    tcp->sq_size = max_size;
  else if (tcp->sq_size < TCP_QP_MIN_SIZE)
    tcp->sq_size = TCP_QP_MIN_SIZE;

  if (tcp->rq_size > max_size)
    tcp->rq_size = max_size;
  else if (tcp->rq_size < TCP_QP_MIN_SIZE)
    tcp->rq_size = TCP_QP_MIN_SIZE;
}

void grpc_tcp_format_conn_data(grpc_endpoint* ep, grpc_tcp_conn_data* conn) {
  grpc_tcp* tcp = reinterpret_cast<grpc_tcp*>(
      ep);  // grpc_endpoint_vtable 定义在 iomgr/endpoint.h 中，grpc_tcp 实现了
            // grpc_endpoint_vtable 定义的所有成员函数，因此ep 可以解释转化为tcp
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p grpc_tcp_format_conn_data", tcp);
  }
  conn->version = 1;
  conn->flags = grpc_tcp_host_is_net() ? TCP_CONN_FLAG_NET : 0;
  conn->credits = htobe16(tcp->rq_size);
  memset(conn->reserved, 0, sizeof(conn->reserved));

  conn->target_sgl.addr = (uint64_t)htobe64((uintptr_t)tcp->target_sgl);
  conn->target_sgl.length = (uint32_t)htobe32(TCP_SGL_SIZE);
  conn->target_sgl.key = (uint32_t)htobe32(tcp->target_mr->rkey);

  conn->data_buf.addr = (uint64_t)htobe64((uintptr_t)tcp->rbuf);
  conn->data_buf.length = (uint32_t)htobe32(tcp->rbuf_size >> 1);
  conn->data_buf.key = (uint32_t)htobe32(tcp->rmr->rkey);

  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p conn version: %d, flags: %d, credits: %d", tcp,
            conn->version, conn->flags, tcp->rq_size);
    gpr_log(GPR_INFO,
            "TCP:%p conn target_sgl's addr: 0x%p, length: %d, key: %d", tcp,
            tcp->target_sgl, TCP_SGL_SIZE, tcp->target_mr->rkey);
    gpr_log(GPR_INFO, "TCP:%p conn data_buf's addr: 0x%p, length: %d, key: %d",
            tcp, tcp->rbuf, tcp->rbuf_size >> 1, tcp->rmr->rkey);
  }
}

void grpc_tcp_save_conn_data(grpc_endpoint* ep, grpc_tcp_conn_data* conn) {
  grpc_tcp* tcp = reinterpret_cast<grpc_tcp*>(ep);
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p grpc_tcp_save_conn_data", tcp);
  }
  tcp->remote_sgl.addr = be64toh((__be64)conn->target_sgl.addr);
  tcp->remote_sgl.length = be32toh((__be32)conn->target_sgl.length);
  tcp->remote_sgl.key = be32toh((__be32)conn->target_sgl.key);
  tcp->remote_sge = 1;
  if ((grpc_tcp_host_is_net() && !(conn->flags & TCP_CONN_FLAG_NET)) ||
      (!grpc_tcp_host_is_net() && (conn->flags & TCP_CONN_FLAG_NET)))
    tcp->opts = TCP_OPT_SWAP_SGL;

  tcp->target_sgl[0].addr = be64toh((__be64)conn->data_buf.addr);
  tcp->target_sgl[0].length = be32toh((__be32)conn->data_buf.length);
  tcp->target_sgl[0].key = be32toh((__be32)conn->data_buf.key);

  tcp->sseq_comp = be16toh(conn->credits);

  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO,
            "TCP:%p tcp remote_sgl's addr: 0x%lx, length: %d, key: %d", tcp,
            tcp->remote_sgl.addr, tcp->remote_sgl.length, tcp->remote_sgl.key);
    gpr_log(GPR_INFO,
            "TCP:%p tcp target_sgl[0]'s addr: 0x%lx, length: %d, key: %d", tcp,
            tcp->target_sgl[0].addr, tcp->target_sgl[0].length,
            tcp->target_sgl[0].key);
    gpr_log(GPR_INFO, "TCP:%p tcp sseq_comp: %d", tcp, tcp->sseq_comp);
  }
}

#define MAX_CHUNK_SIZE (32 * 1024 * 1024)

grpc_error* grpc_tcp_create(grpc_endpoint** ep, grpc_fd* cm_fd,
                            struct rdma_cm_id* id,
                            const grpc_channel_args* args,
                            const char* peer_string) {
  *ep = nullptr;
  int tcp_read_chunk_size = TCP_MAX_TRANSFER;
  int tcp_max_read_chunk_size = 4 * 1024 * 1024;
  int tcp_min_read_chunk_size = 256;
  grpc_resource_quota* resource_quota = grpc_resource_quota_create(nullptr);
  if (args != nullptr) {
    for (size_t i = 0; i < args->num_args; i++) {
      if (0 == strcmp(args->args[i].key, GRPC_ARG_TCP_READ_CHUNK_SIZE)) {
        grpc_integer_options options = {tcp_read_chunk_size, 1, MAX_CHUNK_SIZE};
        tcp_read_chunk_size =
            grpc_channel_arg_get_integer(&args->args[i], options);
      } else if (0 ==
                 strcmp(args->args[i].key, GRPC_ARG_TCP_MIN_READ_CHUNK_SIZE)) {
        grpc_integer_options options = {tcp_read_chunk_size, 1, MAX_CHUNK_SIZE};
        tcp_min_read_chunk_size =
            grpc_channel_arg_get_integer(&args->args[i], options);
      } else if (0 ==
                 strcmp(args->args[i].key, GRPC_ARG_TCP_MAX_READ_CHUNK_SIZE)) {
        grpc_integer_options options = {tcp_read_chunk_size, 1, MAX_CHUNK_SIZE};
        tcp_max_read_chunk_size =
            grpc_channel_arg_get_integer(&args->args[i], options);
      } else if (0 == strcmp(args->args[i].key, GRPC_ARG_RESOURCE_QUOTA)) {
        grpc_resource_quota_unref_internal(resource_quota);
        resource_quota = grpc_resource_quota_ref_internal(
            static_cast<grpc_resource_quota*>(args->args[i].value.pointer.p));
      }
    }
  }

  if (tcp_min_read_chunk_size > tcp_max_read_chunk_size) {
    tcp_min_read_chunk_size = tcp_max_read_chunk_size;
  }
  tcp_read_chunk_size = GPR_CLAMP(tcp_read_chunk_size, tcp_min_read_chunk_size,
                                  tcp_max_read_chunk_size);

  grpc_tcp* tcp = new grpc_tcp;
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "new TCP:%p", tcp);
  }
  tcp->base.vtable = &vtable;
  tcp->peer_string = peer_string;
  grpc_resolved_address resolved_local_addr;
  memset(&resolved_local_addr, 0, sizeof(resolved_local_addr));
  resolved_local_addr.len = sizeof(resolved_local_addr.addr);
  memcpy(&resolved_local_addr.addr, rdma_get_local_addr(id),
         resolved_local_addr.len);
  tcp->local_address = grpc_sockaddr_to_uri(&resolved_local_addr);
  /* paired with unref in grpc_tcp_destroy */
  new (&tcp->refcount) grpc_core::RefCount(
      1, GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace) ? "tcp" : nullptr);
  fastlock_init(&tcp->slock);
  fastlock_init(&tcp->rlock);
  fastlock_init(&tcp->cq_lock);
  fastlock_init(&tcp->cq_wait_lock);
  tcp->cm_id = id;
  tcp->cm_fd = cm_fd;
  tcp->target_length = static_cast<double>(tcp_read_chunk_size);
  tcp->bytes_read_this_round = 0;
  tcp->min_read_chunk_size = tcp_min_read_chunk_size;
  tcp->max_read_chunk_size = tcp_max_read_chunk_size;
  tcp->incoming_buffer = nullptr;
  tcp->outgoing_buffer = nullptr;
  tcp->outgoing_byte_idx = 0;
  tcp->read_cb = nullptr;
  tcp->write_cb = nullptr;
  tcp->ctrl_seqno = 0;
  tcp->ctrl_max_seqno = TCP_QP_CTRL_SIZE;
  tcp->sseq_no = 0;
  tcp->sseq_comp = 0;
  tcp->rseq_no = 0;
  tcp->rseq_comp = 0;
  tcp->remote_sge = 0;
  tcp->target_sge = 0;
  tcp->rbuf_bytes_avail = 0;
  tcp->rbuf_free_offset = 0;
  tcp->rbuf_offset = 0;
  tcp->sbuf_bytes_avail = 0;
  tcp->opts = 0;
  tcp->cq_armed = 0;
  tcp->sqe_avail = 0;
  tcp->sbuf_size = def_wmem;    // TODO: add to args
  tcp->sq_size = def_sqsize;    // TODO: add to args
  tcp->sq_inline = def_inline;  // TODO: add to args
  tcp->rbuf_size = def_mem;     // TODO: add to args
  tcp->rq_size = def_rqsize;    // TODO: add to args
  tcp->rmsg_head = 0;
  tcp->rmsg_tail = 0;
  tcp->unack_cqe = 0;
  tcp->state = tcp->CONNECTED;  // It's not very accurate here
  grpc_slice_buffer_init(&tcp->last_read_buffer);
  tcp->resource_user = grpc_resource_user_create(resource_quota, peer_string);
  grpc_resource_user_slice_allocator_init(
      &tcp->slice_allocator, tcp->resource_user, tcp_read_allocation_done, tcp);
  grpc_resource_quota_unref_internal(resource_quota);

  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "TCP:%p target_length: %lf", tcp, tcp->target_length);
    gpr_log(GPR_INFO, "TCP:%p min_read_chunk_size: %d, max_read_chunk_size: %d",
            tcp, tcp->min_read_chunk_size, tcp->max_read_chunk_size);
    gpr_log(GPR_INFO, "TCP:%p sbuf_size: %d, rbuf_size: %d", tcp,
            tcp->sbuf_size, tcp->rbuf_size);
    gpr_log(GPR_INFO, "TCP:%p sq_inline: %d, sq_size: %d, rq_size: %d", tcp,
            tcp->sq_inline, tcp->sq_size, tcp->rq_size);
    gpr_log(GPR_INFO, "TCP:%p ctrl_max_seqno: %d", tcp, tcp->ctrl_max_seqno);
  }

  grpc_error* error = grpc_tcp_create_cq(tcp);
  if (error != GRPC_ERROR_NONE) {
    gpr_log(GPR_ERROR, "TCP:%p grpc tcp failed to create cq", tcp);
    goto err_out;
  }

  {
    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof qp_attr);
    qp_attr.qp_context = tcp;
    qp_attr.send_cq = tcp->cm_id->send_cq;
    qp_attr.recv_cq = tcp->cm_id->recv_cq;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.sq_sig_all = 1;
    qp_attr.cap.max_send_wr = tcp->sq_size;
    qp_attr.cap.max_recv_wr = tcp->rq_size;
    qp_attr.cap.max_send_sge = 2;
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.cap.max_inline_data = tcp->sq_inline;

    int ret = rdma_create_qp(tcp->cm_id, id->pd, &qp_attr);
    if (ret < 0) {
      gpr_log(GPR_ERROR, "TCP:%p grpc tcp failed to create cq, errno: %d", tcp,
              errno);
      goto err_out;
    }
    tcp->sq_inline = qp_attr.cap.max_inline_data;
    tcp->em_fd =
        grpc_fd_create(id->recv_cq_channel->fd, peer_string, true, tcp);
    error = grpc_tcp_init_bufs(tcp);
    if (error != GRPC_ERROR_NONE) {
      goto err_out0;
    }
    // {// show self's ip addr and port
    //     struct sockaddr_in *ipv4 = (struct sockaddr_in
    //     *)resolved_local_addr.addr; char ipAddress[INET_ADDRSTRLEN];
    //     inet_ntop(AF_INET, &(ipv4->sin_addr), ipAddress, INET_ADDRSTRLEN);
    //     int port = ntohs(ipv4->sin_port);
    //     gpr_log(GPR_INFO, "new TCP:%p cm_id:%p, create qp_num:%d,
    //     qp_state:%d, qp_context:%p, recv_cq_channel->fd:%d , %s %d",
    //     tcp,tcp->cm_id,tcp->cm_id->qp->qp_num,tcp->cm_id->qp->state,tcp->cm_id->qp->qp_context,tcp->cm_id->recv_cq_channel->fd,ipAddress,port);
    // }

    for (int i = 0; i < tcp->rq_size; i++) {
      ret = grpc_tcp_post_recv(tcp);
      if (ret < 0) {
        error = grpc_error_set_int(
            GRPC_ERROR_CREATE_FROM_STATIC_STRING("grpc tcp post recv failed"),
            GRPC_ERROR_INT_ERRNO, errno);
        goto err_out0;
      }
    }
  }
  GRPC_CLOSURE_INIT(&tcp->read_done_closure, rdma_handle_read, tcp,
                    grpc_schedule_on_exec_ctx);
  if (grpc_event_engine_run_in_background()) {
    // If there is a polling engine always running in the background, there is
    // no need to run the backup poller.
    GRPC_CLOSURE_INIT(&tcp->write_done_closure, rdma_handle_write, tcp,
                      grpc_schedule_on_exec_ctx);
  } else {
    GRPC_CLOSURE_INIT(&tcp->write_done_closure,
                      rdma_drop_uncovered_then_handle_write, tcp,
                      grpc_schedule_on_exec_ctx);
  }
  // handle rdma poll err
  TCP_REF(tcp, "error-handle");
  GRPC_CLOSURE_INIT(&tcp->error_closure, tcp_handle_error, tcp,
                    grpc_schedule_on_exec_ctx);
  grpc_fd_notify_on_error(tcp->em_fd, &tcp->error_closure);
  *ep = &tcp->base;
  return GRPC_ERROR_NONE;
err_out0:
  rdma_destroy_qp(tcp->cm_id);
err_out:
  delete tcp;
  return error;
}
