/*
 *  Copyright (c) 2023 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Curve
 * Created Date: 2023-11-20
 * Author: Jingli Chen (Wine93)
 */

// perf context
// (1) 整体设计参考 rocksdb，需要更灵活
// (2) IO 链路上的请求需要尽可能细的拆分: memory, diskcache, s3, memcache
// (3) 元数据操作主要是 rpc(如果可以拆分网络和磁盘等更好)

#ifndef CURVEFS_SRC_CLIENT_BASE_PERF_CONTEXT_H_
#define CURVEFS_SRC_CLIENT_BASE_PERF_CONTEXT_H_

namespace curvefs {
namespace client {
namespace base {

}  // namespace base
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BASE_PERF_CONTEXT_H_
