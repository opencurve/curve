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
 * Created Date: 2023-06-29
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/fuse_client.h"

namespace curvefs {
namespace client {
namespace fuse {

#define MetricGuard(REQUEST) \
    InflightGuard iGuard(&g_clientOpMetric->op##REQUEST.inflightOpNum); \
    CodeGuard cGuard(&rc, &g_clientOpMetric->op##REQUEST.ecount); \
    LatencyUpdater updater(&g_clientOpMetric->op##REQUEST.latency)

FuseClient* Client() {
    return g_ClientInstance;
}

}  // namespace fuse
}  // namespace client
}  // namespace curvefs

