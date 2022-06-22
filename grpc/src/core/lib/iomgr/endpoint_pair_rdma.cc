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

#include "src/core/lib/iomgr/endpoint_pair.h"
#include "src/core/lib/iomgr/socket_utils_posix.h"
#include "src/core/lib/iomgr/unix_sockets_posix.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <string>

#include "absl/strings/str_cat.h"

#include <grpc/support/alloc.h>
#include <grpc/support/log.h>
#include "src/core/lib/gpr/string.h"
#include "src/core/lib/iomgr/tcp_posix.h"

grpc_endpoint_pair grpc_iomgr_create_endpoint_pair(const char* name,
                                                   grpc_channel_args* args) {
    gpr_log(GPR_ERROR, "Should never reach here.");
    abort();
}
