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

#include "src/core/lib/iomgr/sockaddr.h"

#include <cerrno>
#include <fcntl.h>

#include <string>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"

#include <grpc/support/alloc.h>
#include <grpc/support/log.h>
#include <grpc/support/sync.h>
#include <grpc/support/time.h>

#include "src/core/lib/iomgr/ev_posix.h"
#include "src/core/lib/channel/channel_args.h"
#include "src/core/lib/gpr/string.h"
#include "src/core/lib/gprpp/memory.h"
#include "src/core/lib/iomgr/exec_ctx.h"
#include "src/core/lib/iomgr/resolve_address.h"
#include "src/core/lib/iomgr/sockaddr_utils.h"
#include "src/core/lib/iomgr/tcp_server.h"
#include "src/core/lib/iomgr/tcp_posix.h"

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

/* one listening port */
typedef struct grpc_tcp_listener {
  struct rdma_event_channel* ev_chan;
  struct rdma_cm_id* cm_id;
  grpc_fd* emfd;
  grpc_tcp_server* server;
  grpc_resolved_address addr;
  int port;
  unsigned port_index;
  grpc_closure read_closure;
  grpc_closure destroyed_closure;
  struct grpc_tcp_listener* next;
} grpc_tcp_listener;

/* the overall RDMA server */
struct grpc_tcp_server {
  gpr_refcount refs;
  /* Called whenever accept() succeeds on a server port. */
  grpc_tcp_server_cb on_accept_cb;
  void* on_accept_cb_arg;

  gpr_mu mu;

  /* active port count: how many ports are actually still listening */
  int active_ports;
  int destroyed_ports;

  /* linked list of server ports */
  grpc_tcp_listener* head;
  grpc_tcp_listener* tail;
  unsigned nports;

  /* is this server shutting down? */
  bool shutdown;
  /* have listeners been shutdown? */
  bool shutdown_listeners;

  /* List of closures passed to shutdown_starting_add(). */
  grpc_closure_list shutdown_starting;

  /* shutdown callback */
  grpc_closure* shutdown_complete;

  /* all pollsets interested in new connections. The object pointed at is not
   * owned by this struct */
  const std::vector<grpc_pollset*>* pollsets;

  /* next pollset to assign a channel to */
  gpr_atm next_pollset_to_assign;

  grpc_channel_args* channel_args;
};

extern grpc_core::TraceFlag grpc_tcp_trace;

#define MIN_SAFE_ACCEPT_QUEUE_SIZE 100

static gpr_once s_init_max_accept_queue_size = GPR_ONCE_INIT;
static int s_max_accept_queue_size;

/* get max listen queue size on linux */
static void init_max_accept_queue_size(void) {
  int n = SOMAXCONN;
  char buf[64];
  FILE* fp = fopen("/proc/sys/net/core/somaxconn", "r");
  if (fp == nullptr) {
    /* 2.4 kernel. */
    s_max_accept_queue_size = SOMAXCONN;
    return;
  }
  if (fgets(buf, sizeof buf, fp)) {
    char* end;
    long i = strtol(buf, &end, 10);
    if (i > 0 && i <= INT_MAX && end && *end == '\n') {
      n = static_cast<int>(i);
    }
  }
  fclose(fp);
  s_max_accept_queue_size = n;

  if (s_max_accept_queue_size < MIN_SAFE_ACCEPT_QUEUE_SIZE) {
    gpr_log(GPR_INFO,
            "Suspiciously small accept queue (%d) will probably lead to "
            "connection drops",
            s_max_accept_queue_size);
  }
}

static grpc_error* tcp_server_create(grpc_closure* shutdown_complete,
                                     const grpc_channel_args* args,
                                     grpc_tcp_server** server) {
  grpc_tcp_server* s = static_cast<grpc_tcp_server*>(gpr_zalloc(sizeof(*s)));
  gpr_ref_init(&s->refs, 1);
  gpr_mu_init(&s->mu);
  s->active_ports = 0;
  s->destroyed_ports = 0;
  s->shutdown = false;
  s->shutdown_starting.head = nullptr;
  s->shutdown_starting.tail = nullptr;
  s->shutdown_complete = shutdown_complete;
  s->on_accept_cb = nullptr;
  s->on_accept_cb_arg = nullptr;
  s->head = nullptr;
  s->tail = nullptr;
  s->nports = 0;
  s->channel_args = grpc_channel_args_copy(args);
  gpr_atm_no_barrier_store(&s->next_pollset_to_assign, 0);
  *server = s;
  gpr_log(GPR_DEBUG, "tcp_server_create");
  return GRPC_ERROR_NONE;
}

static int get_max_accept_queue_size(void) {
  gpr_once_init(&s_init_max_accept_queue_size, init_max_accept_queue_size);
  return s_max_accept_queue_size;
}

static
grpc_error* grpc_tcp_server_prepare_cm(const grpc_resolved_address* addr,
                                       struct rdma_event_channel** ev_chan_,
                                       struct rdma_cm_id** cm_id_,
                                       int* fd) {
  struct rdma_event_channel* ev_chan;
  struct rdma_cm_id* cm_id;
  grpc_error* error;
  int flags;
  *ev_chan_ = nullptr;
  *cm_id_ = nullptr;
  *fd = -1;

  ev_chan = rdma_create_event_channel();
  if (!ev_chan) {
    return grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                "RDMA create event channel failed"),
                              GRPC_ERROR_INT_ERRNO, errno);
  }
  if (rdma_create_id(ev_chan, &cm_id, NULL, RDMA_PS_TCP) < 0) {
    error = grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                 "RDMA create id failed"),
                               GRPC_ERROR_INT_ERRNO, errno);
    goto err_out;
  }
  if (rdma_bind_addr(cm_id, (struct sockaddr*)addr->addr) < 0) {
    error = grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                 "RDMA bind addr failed"),
                               GRPC_ERROR_INT_ERRNO, errno);
    goto err_out;
  }
  if (rdma_listen(cm_id, get_max_accept_queue_size()) < 0) {
    error = grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                 "RDMA listen failed"),
                               GRPC_ERROR_INT_ERRNO, errno);
    goto err_out;
  }
  flags = fcntl(cm_id->channel->fd, F_GETFL);
  if (fcntl(cm_id->channel->fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    error = grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                 "RDMA fcntl failed"),
                               GRPC_ERROR_INT_ERRNO, errno);
    goto err_out;
  }

  *ev_chan_ = ev_chan;
  *cm_id_ = cm_id;
  *fd = cm_id->channel->fd;
  return GRPC_ERROR_NONE;

err_out:
  rdma_destroy_id(cm_id);
  rdma_destroy_event_channel(ev_chan);
  return error;
}

grpc_error* grpc_tcp_add_cm_to_server(grpc_tcp_server* s,
                                      const grpc_resolved_address* addr,
                                      unsigned port_index, int port,
                                      grpc_tcp_listener** listener) {
  *listener = nullptr;
  grpc_tcp_listener* sp = nullptr;
  struct rdma_event_channel* ev_chan;
  struct rdma_cm_id* cm_id;
  int fd;

  grpc_error* error =
    grpc_tcp_server_prepare_cm(addr, &ev_chan, &cm_id, &fd);
  if (error == GRPC_ERROR_NONE) {
    GPR_ASSERT(fd > 0);
    std::string addr_str = grpc_sockaddr_to_string(addr, true);
    std::string name = absl::StrCat("tcp-server-listener:", addr_str);
    gpr_mu_lock(&s->mu);
    s->nports++;
    GPR_ASSERT(!s->on_accept_cb && "must add ports before starting server");
    sp = static_cast<grpc_tcp_listener*>(gpr_malloc(sizeof(*sp)));
    sp->next = nullptr;
    if (s->head == nullptr) {
      s->head = sp;
    } else {
      s->tail->next = sp;
    }
    s->tail = sp;
    sp->server = s;
    sp->ev_chan = ev_chan;
    sp->cm_id = cm_id;
    sp->emfd = grpc_fd_create(fd, name.c_str(), true, nullptr);
    memcpy(&sp->addr, addr, sizeof(grpc_resolved_address));
    sp->port = port;
    sp->port_index = port_index;
    GPR_ASSERT(sp->ev_chan);
    GPR_ASSERT(sp->cm_id);
    GPR_ASSERT(sp->emfd);
    gpr_mu_unlock(&s->mu);
  }

  gpr_log(GPR_DEBUG, "grpc_tcp_add_cm_to_server");
  *listener = sp;
  return error;
}

static grpc_error* tcp_server_add_port(grpc_tcp_server* s,
                                       const grpc_resolved_address* addr,
                                       int* out_port) {
  int requested_port = grpc_sockaddr_get_port(addr);
  unsigned port_index = 0;
  grpc_tcp_listener* sp;
  grpc_error* err;

  *out_port = -1;
  if (s->tail != nullptr) {
    port_index = s->tail->port_index + 1;
  }

  /* TODO: handle the wildcard add | port, ipv6 if it is */

  if (requested_port == 0) {
    gpr_log(GPR_ERROR, "Unsupport wildcard port");
    return grpc_error_set_str(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                "tcp_server_add_port: "),
                              GRPC_ERROR_STR_DESCRIPTION,
                              grpc_slice_from_static_string(
                                "Unsupport wildcard port"));
  }

  err = grpc_tcp_add_cm_to_server(s, addr, port_index, requested_port, &sp);
  if (err == GRPC_ERROR_NONE) {
    *out_port = sp->port;
  }
  gpr_log(GPR_DEBUG, "tcp_server_add_port: %d", *out_port);
  return err;
}

static unsigned tcp_server_port_fd_count(grpc_tcp_server* /*s*/,
                                         unsigned /*port_index*/) {
  gpr_log(GPR_DEBUG, "tcp_server_port_fd_count");
  return 0;
}

static int tcp_server_port_fd(grpc_tcp_server* /*s*/, unsigned /*port_index*/,
                              unsigned /*fd_index*/) {
  gpr_log(GPR_DEBUG, "tcp_server_port_fd");
  return -1;
}

static grpc_error *on_connect_request(struct rdma_cm_id *id,
                                      grpc_tcp_listener* sp) {
  gpr_log(GPR_DEBUG, "on_connect_request, id: %p", id);
  grpc_tcp_conn_data cresp, *creq =
    const_cast<grpc_tcp_conn_data*>(static_cast<const grpc_tcp_conn_data*>
                                    (id->event->param.conn.private_data));
  if (creq->version != 1) {
    rdma_reject(id, nullptr, 0);
    return grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                "unsupported conn version"),
                              GRPC_ERROR_INT_ERRNO, ENOTSUP);
  }
  grpc_resolved_address addr;
  memset(&addr, 0, sizeof(addr));
  addr.len = static_cast<socklen_t>(sizeof(struct sockaddr_storage));
  memcpy(&addr.addr, rdma_get_peer_addr(id), addr.len);
  std::string addr_str = grpc_sockaddr_to_uri(&addr);
  if (GRPC_TRACE_FLAG_ENABLED(grpc_tcp_trace)) {
    gpr_log(GPR_INFO, "SERVER_CONNECT: incoming connection: %s",
            addr_str.c_str());
  }
  grpc_endpoint* ep;
  grpc_error* error = grpc_tcp_create(&ep, nullptr, id,
                                      sp->server->channel_args,
                                      addr_str.c_str());
  if (error != GRPC_ERROR_NONE) {
    goto err_out;
  }
  id->context = static_cast<void*>(ep);
  grpc_tcp_save_conn_data(ep, creq);
  struct rdma_conn_param param;
  param = id->event->param.conn;
  grpc_tcp_format_conn_data(ep, &cresp);
  param.private_data = &cresp;
  param.private_data_len = sizeof(cresp);
  if (rdma_accept(id, &param) < 0) {
    error = grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                 "failed to call rdma_accept"),
                               GRPC_ERROR_INT_ERRNO, errno);
    goto err_out;
  }

  return GRPC_ERROR_NONE;

err_out:
  rdma_reject(id, nullptr, 0);
  return error;
}

static grpc_error *on_connection(struct rdma_cm_id* id,
                                 grpc_tcp_listener* sp) {
  gpr_log(GPR_DEBUG, "on_connection, id: %p", id);
  grpc_pollset* read_notifier_pollset = (*(sp->server->pollsets))
    [static_cast<size_t>(gpr_atm_no_barrier_fetch_add(
                           &sp->server->next_pollset_to_assign, 1)) %
     sp->server->pollsets->size()];
  grpc_endpoint* ep = static_cast<grpc_endpoint*>(id->context);
  grpc_tcp_add_to_pollset(ep, read_notifier_pollset);
  // Create acceptor.
  grpc_tcp_server_acceptor* acceptor =
    static_cast<grpc_tcp_server_acceptor*>(gpr_malloc(sizeof(*acceptor)));
  acceptor->from_server = sp->server;
  acceptor->port_index = sp->port_index;
  acceptor->fd_index = 0; // TODO need fd_index?
  acceptor->external_connection = false;
  sp->server->on_accept_cb(sp->server->on_accept_cb_arg, ep,
                           read_notifier_pollset, acceptor);
  return GRPC_ERROR_NONE;
}

static grpc_error* on_disconnect(struct rdma_cm_id* id,
                                 grpc_tcp_listener* sp) {
  gpr_log(GPR_DEBUG, "on_disconnect, id: %p", id);
  grpc_error *error = GRPC_ERROR_CREATE_FROM_STATIC_STRING("Socket closed");
  grpc_endpoint* ep = static_cast<grpc_endpoint*>(id->context);
  if (ep) {
    grpc_fd *fd = grpc_tcp_get_cq_fd(ep);
    if (fd && !grpc_fd_is_shutdown(fd)) {
      grpc_tcp_shutdown_ep(ep);
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
  return GRPC_ERROR_NONE;
}

static void finish_shutdown(grpc_tcp_server* s) {
  gpr_mu_lock(&s->mu);
  GPR_ASSERT(s->shutdown);
  gpr_mu_unlock(&s->mu);
  if (s->shutdown_complete != nullptr) {
    grpc_core::ExecCtx::Run(DEBUG_LOCATION, s->shutdown_complete,
                            GRPC_ERROR_NONE);
  }

  gpr_mu_destroy(&s->mu);

  while (s->head) {
    grpc_tcp_listener* sp = s->head;
    s->head = sp->next;
    rdma_destroy_id(sp->cm_id);
    rdma_destroy_event_channel(sp->ev_chan);
    gpr_free(sp);
  }
  grpc_channel_args_destroy(s->channel_args);
  gpr_free(s);
}

static void destroyed_port(void* server, grpc_error* /*error*/) {
  grpc_tcp_server* s = static_cast<grpc_tcp_server*>(server);
  gpr_mu_lock(&s->mu);
  s->destroyed_ports++;
  if (s->destroyed_ports == s->nports) {
    gpr_mu_unlock(&s->mu);
    finish_shutdown(s);
  } else {
    GPR_ASSERT(s->destroyed_ports < s->nports);
    gpr_mu_unlock(&s->mu);
  }
}

/* called when all listening endpoints have been shutdown, so no further
   events will be received on them - at this point it's safe to destroy
   things */
static void deactivated_all_ports(grpc_tcp_server* s) {
  /* delete ALL the things */
  gpr_mu_lock(&s->mu);

  GPR_ASSERT(s->shutdown);

  if (s->head) {
    grpc_tcp_listener* sp;
    for (sp = s->head; sp; sp = sp->next) {
      GRPC_CLOSURE_INIT(&sp->destroyed_closure, destroyed_port, s,
                        grpc_schedule_on_exec_ctx);
      grpc_fd_orphan(sp->emfd, &sp->destroyed_closure, nullptr,
                     "tcp_listener_shutdown");
    }
    gpr_mu_unlock(&s->mu);
  } else {
    gpr_mu_unlock(&s->mu);
    finish_shutdown(s);
  }
}

static void tcp_server_destroy(grpc_tcp_server* s) {
  gpr_log(GPR_DEBUG, "tcp_server_destroy");
  gpr_mu_lock(&s->mu);

  GPR_ASSERT(!s->shutdown);
  s->shutdown = true;

  /* shutdown all fd's */
  if (s->active_ports) {
    grpc_tcp_listener* sp;
    for (sp = s->head; sp; sp = sp->next) {
      grpc_fd_shutdown(
          sp->emfd, GRPC_ERROR_CREATE_FROM_STATIC_STRING("Server destroyed"));
    }
    gpr_mu_unlock(&s->mu);
  } else {
    gpr_mu_unlock(&s->mu);
    deactivated_all_ports(s);
  }
}

/* event manager callback when reads are ready */
static void on_read(void *arg, grpc_error *err) {
  grpc_tcp_listener* sp = static_cast<grpc_tcp_listener*>(arg);
  struct rdma_event_channel* ev_chan = sp->ev_chan;
  struct rdma_cm_event* event = NULL;

  gpr_log(GPR_DEBUG, "on_read");
  if (err != GRPC_ERROR_NONE) {
    gpr_log(GPR_ERROR, "on_read with error: %s", grpc_error_string(err));
    goto error;
  }

  /* loop until rdma_get_cm_event returns EAGAIN, and then re-arm notification */
  for (;;) {
    if (rdma_get_cm_event(ev_chan, &event) < 0) {
      switch (errno) {
        case EINTR:
          continue;
        case EAGAIN:
          grpc_fd_notify_on_read(sp->emfd, &sp->read_closure);
          return;
        default:
          err = grpc_error_set_int(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                     "rdma_get_cm_event failed"),
                                   GRPC_ERROR_INT_ERRNO, errno);
          gpr_mu_lock(&sp->server->mu);
          if (!sp->server->shutdown_listeners) {
            gpr_log(GPR_ERROR, "Failed rdma_get_cm_event: %s",
                    grpc_error_string(err));
          }
          gpr_mu_unlock(&sp->server->mu);
          goto error;
      }
    }

    event->id->event = nullptr;
    switch (event->event) {
      case RDMA_CM_EVENT_CONNECT_REQUEST:
        event->id->event = event;
        err = on_connect_request(event->id, sp);
        break;
      case RDMA_CM_EVENT_ESTABLISHED: {
        err = on_connection(event->id, sp);
        break;
      }
      case RDMA_CM_EVENT_DISCONNECTED:
        gpr_log(GPR_INFO, "server recv RDMA_CM_EVENT_DISCONNECTED");
        err = on_disconnect(event->id, sp);
        break;
      default:
        err = grpc_error_set_str(GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                                   rdma_event_str(event->event)),
                                 GRPC_ERROR_STR_DESCRIPTION,
                                 grpc_slice_from_static_string(
                                   "unknown tcp rdma cm event"));
        break;
    }
    rdma_ack_cm_event(event);

    if (err != GRPC_ERROR_NONE) {
      gpr_log(GPR_ERROR, "Failed to handle cm event: %s",
              grpc_error_string(err));
      goto error;
    }
  }

  GPR_UNREACHABLE_CODE(return );

error:
  gpr_mu_lock(&sp->server->mu);
  if (0 == --sp->server->active_ports && sp->server->shutdown) {
    gpr_mu_unlock(&sp->server->mu);
    deactivated_all_ports(sp->server);
  } else {
    gpr_mu_unlock(&sp->server->mu);
  }
}

static void tcp_server_start(grpc_tcp_server* s,
                             const std::vector<grpc_pollset*>* pollsets,
                             grpc_tcp_server_cb on_accept_cb,
                             void* on_accept_cb_arg) {
  size_t i;
  grpc_tcp_listener* sp;
  GPR_ASSERT(on_accept_cb);
  gpr_mu_lock(&s->mu);
  GPR_ASSERT(!s->on_accept_cb);
  GPR_ASSERT(s->active_ports == 0);
  s->on_accept_cb = on_accept_cb;
  s->on_accept_cb_arg = on_accept_cb_arg;
  s->pollsets = pollsets;
  sp = s->head;
  while (sp != nullptr) {
      for (i = 0; i < pollsets->size(); i++) {
        grpc_pollset_add_fd((*pollsets)[i], sp->emfd);
      }
      GRPC_CLOSURE_INIT(&sp->read_closure, on_read, sp,
                        grpc_schedule_on_exec_ctx);
      grpc_fd_notify_on_read(sp->emfd, &sp->read_closure);
      s->active_ports++;
      sp = sp->next;
  }
  gpr_mu_unlock(&s->mu);
  gpr_log(GPR_DEBUG, "tcp_server_start, add to %ld pollsets", i);
}

static grpc_tcp_server* tcp_server_ref(grpc_tcp_server* s) {
  gpr_ref_non_zero(&s->refs);
  gpr_log(GPR_DEBUG, "tcp_server_ref");
  return s;
}

static void tcp_server_shutdown_starting_add(grpc_tcp_server* s,
                                             grpc_closure* shutdown_starting) {
  gpr_mu_lock(&s->mu);
  grpc_closure_list_append(&s->shutdown_starting, shutdown_starting,
                           GRPC_ERROR_NONE);
  gpr_mu_unlock(&s->mu);
  gpr_log(GPR_DEBUG, "tcp_server_shutdown_starting_add");
}

static void tcp_server_shutdown_listeners(grpc_tcp_server* s) {
  gpr_mu_lock(&s->mu);
  s->shutdown_listeners = true;
  /* shutdown all fd's */
  if (s->active_ports) {
    grpc_tcp_listener* sp;
    for (sp = s->head; sp; sp = sp->next) {
      grpc_fd_shutdown(sp->emfd,
                       GRPC_ERROR_CREATE_FROM_STATIC_STRING("Server shutdown"));
    }
  }
  gpr_mu_unlock(&s->mu);
  gpr_log(GPR_DEBUG, "tcp_server_shutdown_listerners");
}

static void tcp_server_unref(grpc_tcp_server* s) {
  gpr_log(GPR_DEBUG, "tcp_server_unref");
  if (gpr_unref(&s->refs)) {
    gpr_log(GPR_DEBUG, "in tcp_server_unref shutdown & destroy tcp server");
    tcp_server_shutdown_listeners(s);
    gpr_mu_lock(&s->mu);
    grpc_core::ExecCtx::RunList(DEBUG_LOCATION, &s->shutdown_starting);
    gpr_mu_unlock(&s->mu);
    tcp_server_destroy(s);
  }
}

static grpc_core::TcpServerFdHandler* tcp_server_create_fd_handler(
    grpc_tcp_server* s) {
  gpr_log(GPR_DEBUG, "tcp_server_create_fd_handler");
  return nullptr;
}

grpc_tcp_server_vtable grpc_rdma_tcp_server_vtable = {
    tcp_server_create,        tcp_server_start,
    tcp_server_add_port,      tcp_server_create_fd_handler,
    tcp_server_port_fd_count, tcp_server_port_fd,
    tcp_server_ref,           tcp_server_shutdown_starting_add,
    tcp_server_unref,         tcp_server_shutdown_listeners};
