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
 * Project: curve
 * Created Date: 2023-10-19
 * Author: chengyi01
 */

#include "src/client/client_metric.h"

namespace curve {
namespace client {

void CollectMetrics(InterfaceMetric* interface, int count, uint64_t u_elapsed) {
    interface->bps.count << count;
    interface->qps.count << 1;
    interface->latency << u_elapsed;
}

}  // namespace client
}  // namespace curve
