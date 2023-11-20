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

// 日志的采集器：
// (1) 需要支持 buffer
// (2) 配置可以指定多个后端地址，采用轮询进行负载
// (3) 并且发送需要有 retry 机制
// (4) 需要采集比例的可配置，如 4/100、100/100
// (5) 由于采集器的逻辑经常变更，可以考虑以插件的形式实现，以 so 加载或者 lua
// (6) 需要和 access log 和 perf_context 结合起来，日志要同时发送到文件与 TCP
//     实现不允许有冗余代码， 所以 log 的输出逻辑需要足够灵活，考虑采用 multi-sender

#ifndef CURVEFS_SRC_CLIENT_BASE_LOG_SENDER_H_
#define CURVEFS_SRC_CLIENT_BASE_LOG_SENDER_H_

namespace curvefs {
namespace client {
namespace base {

}  // namespace base
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BASE_LOG_SENDER_H_
