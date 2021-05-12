/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 21-4-8
 * Author: huyao
 */

#ifndef SRC_CHUNKSERVER_SCAN_SERVICE_H_
#define SRC_CHUNKSERVER_SCAN_SERVICE_H_

#include "proto/scan.pb.h"
#include "src/chunkserver/scan_manager.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

class ScanServiceImpl : public ScanService {
 public:
    explicit  ScanServiceImpl(ScanManager* scanManager) :
           scanManager_(scanManager) {}
    ~ScanServiceImpl() {}
    void FollowScanMap(RpcController *controller,
                       const FollowScanMapRequest *request,
                       FollowScanMapResponse *response,
                       Closure *done);
 private:
    ScanManager* scanManager_;
};
}  // namespace chunkserver
}  // namespace curve
#endif  // SRC_CHUNKSERVER_SCAN_SERVICE_H_
