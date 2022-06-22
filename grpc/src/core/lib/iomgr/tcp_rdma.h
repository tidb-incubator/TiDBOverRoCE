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

#ifndef GRPC_CORE_LIB_IOMGR_TCP_RDMA_H
#define GRPC_CORE_LIB_IOMGR_TCP_RDMA_H
/*
   Low level TCP "bottom half" implementation, for use by transports built on
   top of a TCP connection.

   Note that this file does not (yet) include APIs for creating the socket in
   the first place.

   All calls passing slice transfer ownership of a slice refcount unless
   otherwise specified.
*/

#include <grpc/support/port_platform.h>

#include "src/core/lib/iomgr/port.h"

#include "src/core/lib/debug/trace.h"
#include "src/core/lib/iomgr/buffer_list.h"
#include "src/core/lib/iomgr/endpoint.h"
#include "src/core/lib/iomgr/ev_posix.h"

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

#include <grpc/slice.h>
#include <grpc/support/alloc.h>
#include <grpc/support/log.h>
#include <grpc/support/string_util.h>
#include <grpc/support/sync.h>
#include <grpc/support/time.h>

#include "src/core/lib/channel/channel_args.h"
#include "src/core/lib/debug/stats.h"
#include "src/core/lib/debug/trace.h"
#include "src/core/lib/gpr/string.h"
#include "src/core/lib/gpr/useful.h"
#include "src/core/lib/gprpp/sync.h"
#include "src/core/lib/iomgr/buffer_list.h"
#include "src/core/lib/iomgr/ev_posix.h"
#include "src/core/lib/iomgr/executor.h"
#include "src/core/lib/iomgr/tcp_rdma.h"
#include "src/core/lib/iomgr/sockaddr_utils.h"
#include "src/core/lib/iomgr/socket_utils_posix.h"
#include "src/core/lib/profiling/timers.h"
#include "src/core/lib/slice/slice_internal.h"
#include "src/core/lib/slice/slice_string_helpers.h"

struct grpc_tcp_sge {
  uint64_t addr;
  uint32_t key;
  uint32_t length;
};

struct grpc_tcp_conn_data {
  uint8_t version;
  uint8_t flags;
  __be16 credits;
  uint8_t reserved[3];
  struct grpc_tcp_sge target_sgl;
  struct grpc_tcp_sge data_buf;
};

extern uint32_t polling_time;

typedef struct grpc_tcp grpc_tcp;

/* Create a tcp endpoint given a file desciptor and a read slice size.
   Takes ownership of fd. */
grpc_error* grpc_tcp_create(grpc_endpoint** ep, grpc_fd* cm_fd,
                            struct rdma_cm_id *id,
                            const grpc_channel_args* args,
                            const char* peer_string);
void grpc_tcp_format_conn_data(grpc_endpoint* ep,
                               grpc_tcp_conn_data* conn);
void grpc_tcp_save_conn_data(grpc_endpoint* ep,
                             grpc_tcp_conn_data* conn);
void grpc_tcp_add_to_pollset(grpc_endpoint* ep, grpc_pollset *pollset);
grpc_fd* grpc_tcp_get_cq_fd(grpc_endpoint* ep);
void grpc_tcp_shutdown_ep(grpc_endpoint* ep);

bool grpc_tcp_poll_all(grpc_tcp* tcp);
bool grpc_tcp_is_cq_armed(grpc_tcp* tcp);
int grpc_tcp_poll_fd(grpc_tcp* tcp, int events, int nonblock,
                     bool (*test)(grpc_tcp* tcp));
int grpc_tcp_get_cq_event(grpc_tcp* tcp);

uint64_t grpc_tcp_time_us(void);

#endif /* GRPC_CORE_LIB_IOMGR_TCP_POSIX_H */
