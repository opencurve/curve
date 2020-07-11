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

/**
 * Project: nebd
 * Create Date: 2020-01-20
 * Author: wuhanqing
 */

#ifndef NEBD_TEST_PART1_FAKE_HEARTBEAT_SERVICE_H_
#define NEBD_TEST_PART1_FAKE_HEARTBEAT_SERVICE_H_

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <string>
#include <vector>

#include "nebd/proto/heartbeat.pb.h"

namespace nebd {
namespace client {

class FakeHeartbeatService : public NebdHeartbeatService {
 public:
    FakeHeartbeatService() = default;
    virtual ~FakeHeartbeatService() = default;

    void KeepAlive(::google::protobuf::RpcController* controller,
                   const ::nebd::client::HeartbeatRequest* request,
                   ::nebd::client::HeartbeatResponse* response,
                   ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard doneGuard(done);
        ++invokeTimes_;
        latestFileInfos_.clear();

        response->set_retcode(RetCode::kOK);

        for (int i = 0; i < request->info_size(); ++i) {
           latestFileInfos_.push_back(request->info(i));
        }
    }

    std::vector<HeartbeatFileInfo> GetLatestRequestFileInfos() const {
       return latestFileInfos_;
    }

    void ClearInvokeTimes() {
       invokeTimes_ = 0;
    }

    int GetInvokeTimes() const {
       return invokeTimes_;
    }

 private:
    int invokeTimes_{0};
    std::vector<HeartbeatFileInfo> latestFileInfos_;
};

}  // namespace client
}  // namespace nebd

#endif  // NEBD_TEST_PART1_FAKE_HEARTBEAT_SERVICE_H_
