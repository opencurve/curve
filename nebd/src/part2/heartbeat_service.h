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
 * Project: nebd
 * Created Date: 2020-02-03
 * Author: charisu
 */

#ifndef NEBD_SRC_PART2_HEARTBEAT_SERVICE_H_
#define NEBD_SRC_PART2_HEARTBEAT_SERVICE_H_

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <memory>

#include "nebd/proto/heartbeat.pb.h"
#include "nebd/src/part2/heartbeat_manager.h"

namespace nebd {
namespace server {

class NebdHeartbeatServiceImpl : public nebd::client::NebdHeartbeatService {
 public:
    explicit NebdHeartbeatServiceImpl(
        std::shared_ptr<HeartbeatManager> heartbeatManager)
        : heartbeatManager_(heartbeatManager) {}
    virtual ~NebdHeartbeatServiceImpl() {}
    virtual void KeepAlive(google::protobuf::RpcController* cntl_base,
                           const nebd::client::HeartbeatRequest* request,
                           nebd::client::HeartbeatResponse* response,
                           google::protobuf::Closure* done);

 private:
    std::shared_ptr<HeartbeatManager> heartbeatManager_;
};

}  // namespace server
}  // namespace nebd

#endif  // NEBD_SRC_PART2_HEARTBEAT_SERVICE_H_
