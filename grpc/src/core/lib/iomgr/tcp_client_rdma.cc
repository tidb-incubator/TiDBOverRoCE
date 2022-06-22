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

#include <grpc/support/port_platform.h>

#include "src/core/lib/iomgr/port.h"

#include "src/core/lib/iomgr/tcp_client.h"

#include <cstdint>
#include <cerrno>
#include <fcntl.h>

#include <grpc/support/alloc.h>
#include <grpc/support/log.h>
#include <grpc/support/time.h>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"

#include "src/core/lib/channel/channel_args.h"
#include "src/core/lib/gpr/string.h"
#include "src/core/lib/iomgr/ev_posix.h"
#include "src/core/lib/iomgr/iomgr_posix.h"
#include "src/core/lib/iomgr/sockaddr.h"
#include "src/core/lib/iomgr/sockaddr_utils.h"
#include "src/core/lib/iomgr/socket_mutator.h"
#include "src/core/lib/iomgr/socket_utils_posix.h"
#include "src/core/lib/iomgr/tcp_posix.h"
#include "src/core/lib/iomgr/timer.h"
#include "src/core/lib/iomgr/unix_sockets_posix.h"
#include "src/core/lib/slice/slice_internal.h"

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

// TODO: FIX it, use deadline
#define TIMEOUT_IN_MS  500

extern grpc_core::TraceFlag grpc_tcp_trace;

struct async_connect {
  struct rdma_event_channel* ev_chan;
  struct rdma_cm_id* cm_id;
  gpr_mu mu;
  grpc_fd* fd;
  grpc_timer alarm;
  grpc_closure on_alarm;
  int refs;
  grpc_closure read_closure;
  grpc_pollset_set* interested_parties;
  std::string addr_str;
  grpc_endpoint** ep;
  grpc_endpoint* saved_ep;
  grpc_closure* closure;
  bool closure_called;
  grpc_channel_args* channel_args;
};

static void tc_on_alarm(void* acp, grpc_error* error) {
  int done;
  async_connect* ac = static_cast<async_connect*>(acp);
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    const char* str = grpc_error_string(error);
    gpr_log(GPR_INFO, "CLIENT_CONNECT: %s: on_alarm: error=%s",
            ac->addr_str.c_str(), str);
  }
  gpr_mu_lock(&ac->mu);
  if (!ac->closure_called && ac->fd != nullptr) {
    grpc_fd_shutdown(
        ac->fd, GRPC_ERROR_CREATE_FROM_STATIC_STRING("connect() timed out"));
  }
  done = (--ac->refs == 0);
  gpr_mu_unlock(&ac->mu);
  if (done) {
    gpr_mu_destroy(&ac->mu);
    grpc_channel_args_destroy(ac->channel_args);
    rdma_destroy_id(ac->cm_id);
    rdma_destroy_event_channel(ac->ev_chan);
    delete ac;
  }
}

static grpc_error* on_addr_resolved(struct rdma_cm_id* id, grpc_fd* fd,
                                    async_connect* ac) {
  if (rdma_resolve_route(id, TIMEOUT_IN_MS) < 0) {
    return grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                 "failed to call rdma_resolve_route"),
                               GRPC_ERROR_INT_ERRNO, errno);
  }
  grpc_fd_notify_on_read(fd, &ac->read_closure);
  return GRPC_ERROR_NONE;
}

static grpc_error* on_route_resolved(struct rdma_cm_id* id, grpc_fd* fd,
                                     async_connect* ac) {
  grpc_error* error;
  gpr_log(GPR_INFO, "on_route_resolved()");
  error = grpc_tcp_create(ac->ep, ac->fd, id, ac->channel_args,
                          ac->addr_str.c_str());
  if (error != GRPC_ERROR_NONE) {
    return error;
  }
  ac->saved_ep = *ac->ep;
  struct grpc_tcp_conn_data cresp;
  grpc_tcp_format_conn_data(ac->saved_ep, &cresp);
  struct rdma_conn_param param;
  memset(&param, 0, sizeof(param));
  param.private_data = &cresp;
  param.private_data_len = sizeof(cresp);
  param.flow_control = 1;
  param.retry_count = 7;
  param.rnr_retry_count = 7;
  if (rdma_connect(id, &param) < 0) {
    return grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                "failed to call rdma_connect"),
                              GRPC_ERROR_INT_ERRNO, errno);
  }
  grpc_fd_notify_on_read(fd, &ac->read_closure);
  return GRPC_ERROR_NONE;
}

static grpc_error* on_connection(struct rdma_cm_id* id, grpc_fd* fd,
                                 async_connect* ac) {
  grpc_tcp_conn_data* creq =
    const_cast<grpc_tcp_conn_data*>(static_cast<const grpc_tcp_conn_data*>
                                    (id->event->param.conn.private_data));
  if (creq->version != 1) {
    return grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                "unsupported conn version"),
                              GRPC_ERROR_INT_ERRNO, ENOTSUP);
  }
  grpc_tcp_save_conn_data(ac->saved_ep, creq);
  grpc_fd_notify_on_read(fd, &ac->read_closure);
  return GRPC_ERROR_NONE;
}

static grpc_error* on_disconnect(struct rdma_cm_id* /*id*/, grpc_fd* /*fd*/,
                                 async_connect* ac) {
  grpc_error* error = GRPC_ERROR_CREATE_FROM_STATIC_STRING("Socket closed");
  if (ac->saved_ep) {
    grpc_fd* fd = grpc_tcp_get_cq_fd(ac->saved_ep);
    if (fd && !grpc_fd_is_shutdown(fd)) {
      grpc_tcp_shutdown_ep(ac->saved_ep);
      if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
        gpr_log(GPR_INFO, "on_disconnect: shutdown compl channel fd");
      }
      grpc_fd_shutdown(fd, error);
    }
  } else {
    if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
      gpr_log(GPR_INFO, "on_disconnect: ep is nullptr");
    }
  }
  return error;
}

static void on_readable(void* acp, grpc_error* error) {
  async_connect* ac = static_cast<async_connect*>(acp);
  struct rdma_cm_event* event = nullptr;
  grpc_closure* closure = ac->closure;
  grpc_fd* fd = ac->fd;
  int done;

  GPR_ASSERT(fd);

  GRPC_ERROR_REF(error);

  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    const char* str = grpc_error_string(error);
    gpr_log(GPR_INFO, "CLIENT_CONNECT: %s: on_readable: error=%s",
            ac->addr_str.c_str(), str);
  }

  gpr_mu_lock(&ac->mu);
  if (error != GRPC_ERROR_NONE) {
    error =
        grpc_error_set_str(error, GRPC_ERROR_STR_OS_ERROR,
                           grpc_slice_from_static_string("Timeout occurred"));
    goto err_out;
  }

  if (rdma_get_cm_event(ac->ev_chan, &event) < 0) {
    error = grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                 "grpc tcp resolve rdma addr failed"),
                               GRPC_ERROR_INT_ERRNO, errno);
    goto err_out;
  }

  event->id->event = nullptr;
  switch (event->event) {
    case RDMA_CM_EVENT_ADDR_RESOLVED:
      gpr_log(GPR_INFO, "RDMA_CM_EVENT_ADDR_RESOLVED");
      error = on_addr_resolved(event->id, fd, ac);
      break;
    case RDMA_CM_EVENT_ROUTE_RESOLVED:
      gpr_log(GPR_INFO, "RDMA_CM_EVENT_ROUTE_RESOLVED");
      error = on_route_resolved(event->id, fd, ac);
      gpr_log(GPR_INFO, "rdma-cm-id=%d, qp=%d", event->id, event->id->qp->qp_num);
      break;
    case RDMA_CM_EVENT_ESTABLISHED:
      gpr_log(GPR_INFO, "RDMA_CM_EVENT_ESTABLISHED");
      event->id->event = event;
      error = on_connection(event->id, fd, ac);
      if (error == GRPC_ERROR_NONE) {
        ac->closure_called = true;
        gpr_mu_unlock(&ac->mu);
        grpc_timer_cancel(&ac->alarm);
        grpc_core::ExecCtx::Run(DEBUG_LOCATION, closure, GRPC_ERROR_NONE);
        rdma_ack_cm_event(event);
        return;
      }
      goto err_out;
    case RDMA_CM_EVENT_DISCONNECTED:
      gpr_log(GPR_INFO, "client recv RDMA_CM_EVENT_DISCONNECTED");
      error = on_disconnect(event->id, fd, ac);
      goto err_out;
    default:
      gpr_log(GPR_ERROR, "unrecognized rdma cm event:%d", event->event);
      gpr_log(GPR_ERROR, "the event->stauts is %d:", event->status);
      error = grpc_error_set_str(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                   rdma_event_str(event->event)),
                                 GRPC_ERROR_STR_DESCRIPTION,
                                 grpc_slice_from_static_string(
                                   "unsupported rdma cm event"));
      break;
  }

  if (error == GRPC_ERROR_NONE) {
    rdma_ack_cm_event(event);
    gpr_mu_unlock(&ac->mu);
    return;
  }

err_out:
  rdma_ack_cm_event(event);
  if (fd != nullptr) {
    gpr_log(GPR_DEBUG, "delete cm fd from pollset");
    grpc_pollset_set_del_fd(ac->interested_parties, fd);
    grpc_fd_orphan(fd, nullptr, nullptr, "tcp_client_rdma_orphan");
    fd = nullptr;
  }
  done = (--ac->refs == 0);
  // Create a copy of the data from "ac" to be accessed after the unlock, as
  // "ac" and its contents may be deallocated by the time they are read.
  const grpc_slice addr_str_slice = grpc_slice_from_cpp_string(ac->addr_str);
  bool closure_called = ac->closure_called;
  gpr_mu_unlock(&ac->mu);
  grpc_slice str;
  bool ret = grpc_error_get_str(error, GRPC_ERROR_STR_DESCRIPTION, &str);
  GPR_ASSERT(ret);
  std::string description = absl::StrCat("Failed to connect to remote host: ",
                                         grpc_core::StringViewFromSlice(str));
  error =
    grpc_error_set_str(error, GRPC_ERROR_STR_DESCRIPTION,
                       grpc_slice_from_cpp_string(std::move(description)));
  error = grpc_error_set_str(error, GRPC_ERROR_STR_TARGET_ADDRESS,
                             addr_str_slice /* takes ownership */);
  if (done) {
    // This is safe even outside the lock, because "done", the sentinel, is
    // populated *inside* the lock.
    gpr_mu_destroy(&ac->mu);
    grpc_channel_args_destroy(ac->channel_args);
    rdma_destroy_id(ac->cm_id);
    rdma_destroy_event_channel(ac->ev_chan);
    delete ac;
  }
  if (!closure_called) {
    grpc_core::ExecCtx::Run(DEBUG_LOCATION, closure, error);
  }
}

grpc_error* grpc_tcp_client_prepare_cm(struct rdma_event_channel** ev_chan_,
                                       struct rdma_cm_id** cm_id_) {
  struct rdma_event_channel* ev_chan;
  struct rdma_cm_id* cm_id;
  grpc_error* error;
  *ev_chan_ = nullptr;
  *cm_id_ = nullptr;

  ev_chan = rdma_create_event_channel();
  if (!ev_chan) {
    return grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                "grpc tcp create rdma event channel failed"),
                              GRPC_ERROR_INT_ERRNO, errno);
  }
  if (rdma_create_id(ev_chan, &cm_id, NULL, RDMA_PS_TCP) < 0) {
    error = grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                 "grpc tcp create rdma cm id failed"),
                               GRPC_ERROR_INT_ERRNO, errno);
    goto err_out;
  }

  {
    int flags = fcntl(cm_id->channel->fd, F_GETFL);
    if (fcntl(cm_id->channel->fd, F_SETFL, flags | O_NONBLOCK) < 0) {
      error = grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                   "grpc tcp get/set rdma channel flag failed"),
                                 GRPC_ERROR_INT_ERRNO, errno);
      goto err_out;
    }
  }

  *ev_chan_ = ev_chan;
  *cm_id_ = cm_id;
  return GRPC_ERROR_NONE;

err_out:
  if (cm_id) {
    rdma_destroy_id(cm_id);
  }
  rdma_destroy_event_channel(ev_chan);
  return error;
}

void grpc_tcp_client_create_from_prepared_cm(
    grpc_pollset_set* interested_parties, grpc_closure* closure,
    struct rdma_event_channel* ev_chan, struct rdma_cm_id* cm_id,
    const grpc_channel_args* channel_args, const grpc_resolved_address* addr,
    grpc_millis deadline, grpc_endpoint** ep) {
  if (rdma_resolve_addr(cm_id, NULL, (struct sockaddr*)addr->addr,
                        TIMEOUT_IN_MS) < 0) {
    grpc_error* error = grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                             "grpc tcp resolve rdma addr failed"),
                                           GRPC_ERROR_INT_ERRNO, errno);
    rdma_destroy_id(cm_id);
    rdma_destroy_event_channel(ev_chan);
    grpc_core::ExecCtx::Run(DEBUG_LOCATION, closure, error);
    return;
  }

  std::string name = absl::StrCat("tcp-client:", grpc_sockaddr_to_uri(addr));
  grpc_fd* fdobj = grpc_fd_create(cm_id->channel->fd, name.c_str(), true,
                                  nullptr);
  grpc_pollset_set_add_fd(interested_parties, fdobj);
  async_connect* ac = new async_connect();
  ac->ev_chan = ev_chan;
  ac->cm_id = cm_id;
  ac->fd = fdobj;
  ac->closure = closure;
  ac->closure_called = false;
  ac->ep = ep;
  ac->saved_ep = nullptr;
  ac->interested_parties = interested_parties;
  ac->addr_str = grpc_sockaddr_to_uri(addr);
  gpr_mu_init(&ac->mu);
  ac->refs = 2;
  GRPC_CLOSURE_INIT(&ac->read_closure, on_readable, ac,
                    grpc_schedule_on_exec_ctx);
  ac->channel_args = grpc_channel_args_copy(channel_args);

 // if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "CLIENT_CONNECT: %s: asynchronously connecting fd %p",
            ac->addr_str.c_str(), fdobj);
 // }

  gpr_mu_lock(&ac->mu);
  GRPC_CLOSURE_INIT(&ac->on_alarm, tc_on_alarm, ac, grpc_schedule_on_exec_ctx);
  grpc_timer_init(&ac->alarm, deadline, &ac->on_alarm);
  grpc_fd_notify_on_read(ac->fd, &ac->read_closure);
  gpr_mu_unlock(&ac->mu);
}

static void tcp_connect(grpc_closure* closure, grpc_endpoint** ep,
                        grpc_pollset_set* interested_parties,
                        const grpc_channel_args* channel_args,
                        const grpc_resolved_address* addr,
                        grpc_millis deadline) {
  struct rdma_event_channel* ev_chan;
  struct rdma_cm_id *cm_id;
  grpc_error *error;
  *ep = nullptr;
  if ((error = grpc_tcp_client_prepare_cm(&ev_chan, &cm_id))
      != GRPC_ERROR_NONE) {
    grpc_core::ExecCtx::Run(DEBUG_LOCATION, closure, error);
    return;
  }
  grpc_tcp_client_create_from_prepared_cm(interested_parties, closure, ev_chan,
                                          cm_id, channel_args, addr, deadline,
                                          ep);
}

grpc_tcp_client_vtable grpc_rdma_tcp_client_vtable = {tcp_connect};
