/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: April  13th 2021
 * Author: huyao
 */

#include "src/chunkserver/scan_service.h"

namespace curve {
namespace chunkserver {

void ScanServiceImpl::FollowScanMap(RpcController *controller,
                       const FollowScanMapRequest *request,
                       FollowScanMapResponse *response,
                       Closure *done) {
    return;
}
}  // namespace chunkserver
}  // namespace curve
