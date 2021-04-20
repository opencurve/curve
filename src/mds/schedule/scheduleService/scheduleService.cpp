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

#include <vector>
#include <map>
#include "src/mds/schedule/scheduleService/scheduleService.h"

namespace curve {
namespace mds {
namespace schedule {
void ScheduleServiceImpl::RapidLeaderSchedule(
    google::protobuf::RpcController* cntl_base,
    const RapidLeaderScheduleRequst* request,
    RapidLeaderScheduleResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [RapidLeaderScheduleRequest] "
              << request->DebugString();

    int errCode = coordinator_->RapidLeaderSchedule(request->logicalpoolid());
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

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [QueryChunkServerRecoverStatusRequest] "
              << request->DebugString();

    std::vector<ChunkServerIdType> ids;
    for (int i = 0; i < request->chunkserverid_size(); i++) {
        ids.emplace_back(request->chunkserverid(i));
    }

    std::map<ChunkServerIdType, bool> statusMap;
    int errCode = coordinator_->QueryChunkServerRecoverStatus(ids, &statusMap);
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

void ScheduleServiceImpl::CancelScanSchedule(
        google::protobuf::RpcController* cntl_base,
        const CancelScanScheduleRequest* request,
        CancelScanScheduleResponse* response,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [CancelScanScheduleRequest] "
              << request->DebugString();
    int errCode = coordinator_->CancelScanSchedule(request->logicalpoolid());
    response->set_statuscode(errCode);
    if (errCode == kScheduleErrCodeSuccess) {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [CancelScanScheduleResponse] "
                  << response->DebugString();
    } else {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [CancelScanScheduleResponse] "
                   << response->DebugString();
    }

    return;
}

}  // namespace schedule
}  // namespace mds
}  // namespace curve
