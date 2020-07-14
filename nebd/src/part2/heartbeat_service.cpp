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

#include "nebd/src/common/timeutility.h"
#include "nebd/src/part2/heartbeat_service.h"
#include "nebd/src/part2/define.h"

namespace nebd {
namespace server {

using nebd::common::TimeUtility;

void NebdHeartbeatServiceImpl::KeepAlive(
        google::protobuf::RpcController* cntl_base,
        const nebd::client::HeartbeatRequest* request,
        nebd::client::HeartbeatResponse* response,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    bool ok = true;
    uint64_t curTime = TimeUtility::GetTimeofDayMs();
    heartbeatManager_->UpdateNebdClientInfo(request->pid(),
                                            request->nebdversion(), curTime);
    for (int i = 0; i < request->info_size(); ++i) {
        const auto& info = request->info(i);
        bool res = heartbeatManager_->UpdateFileTimestamp(info.fd(), curTime);
        if (!res) {
            LOG(WARNING) << "Update file timestamp fail, fd: "
                         << info.fd() << ", name: " << info.name();
            ok = false;
        }
    }

    if (ok) {
        response->set_retcode(nebd::client::RetCode::kOK);
    } else {
        response->set_retcode(nebd::client::RetCode::kNoOK);
    }
}

}  // namespace server
}  // namespace nebd
