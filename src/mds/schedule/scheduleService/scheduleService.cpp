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
 * Created Date: 2020-01-02
 * Author: lixiaocui
 */

#include <sstream>
#include <vector>
#include <map>
#include "src/common/authenticator.h"
#include "src/mds/schedule/scheduleService/scheduleService.h"

namespace curve {
namespace mds {
namespace schedule {

using curve::common::Authenticator;

void ScheduleServiceImpl::RapidLeaderSchedule(
    google::protobuf::RpcController* cntl_base,
    const RapidLeaderScheduleRequst* request,
    RapidLeaderScheduleResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    // auth
    auto ret = Authenticator::GetInstance().VerifyCredential(
        request->authtoken());
    if (!ret) {
        LOG(WARNING) << "RapidLeaderSchedule auth failed, request = "
                     << request->ShortDebugString();
        response->set_statuscode(kScheduleErrAuthFail);
        return;
    }

    auto errCode = coordinator_->RapidLeaderSchedule(request->logicalpoolid());
    response->set_statuscode(errCode);
    if (errCode == kScheduleErrCodeSuccess) {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [RapidLeaderScheduleResponse] "
                  << response->DebugString();
    } else {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [RapidLeaderScheduleResponse] "
                   << response->DebugString();
    }
}

void ScheduleServiceImpl::QueryChunkServerRecoverStatus(
    google::protobuf::RpcController* cntl_base,
    const QueryChunkServerRecoverStatusRequest *request,
    QueryChunkServerRecoverStatusResponse *response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    // auth
    auto ret = Authenticator::GetInstance().VerifyCredential(
        request->authtoken());
    if (!ret) {
        LOG(WARNING) << "QueryChunkServerRecoverStatus auth failed, request = "
                     << request->ShortDebugString();
        response->set_statuscode(kScheduleErrAuthFail);
        return;
    }

    std::vector<ChunkServerIdType> ids;
    for (int i = 0; i < request->chunkserverid_size(); i++) {
        ids.emplace_back(request->chunkserverid(i));
    }

    std::map<ChunkServerIdType, bool> statusMap;
    auto errCode = coordinator_->QueryChunkServerRecoverStatus(ids, &statusMap);
    response->set_statuscode(errCode);
    if (errCode == kScheduleErrCodeSuccess) {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [QueryChunkServerRecoverStatusResponse] "
                  << response->DebugString();
        for (auto item : statusMap) {
            response->mutable_recoverstatusmap()->insert(
                { item.first, item.second});
        }
    } else {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [QueryChunkServerRecoverStatusResponse] "
                   << response->DebugString();
    }
}

}  // namespace schedule
}  // namespace mds
}  // namespace curve
