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
 * @Project: curve
 * @Date: 2021-11-23 19:25:51
 * @Author: chenwei
 */

#include "curvefs/src/mds/schedule/scheduleService/scheduleService.h"
#include <map>
#include <vector>

namespace curvefs {
namespace mds {
namespace schedule {
void ScheduleServiceImpl::QueryMetaServerRecoverStatus(
    google::protobuf::RpcController* cntl_base,
    const QueryMetaServerRecoverStatusRequest* request,
    QueryMetaServerRecoverStatusResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id() << "] from "
              << cntl->remote_side() << " to " << cntl->local_side()
              << ". [QueryMetaServerRecoverStatusRequest] "
              << request->DebugString();

    std::vector<MetaServerIdType> ids;
    for (int i = 0; i < request->metaserverid_size(); i++) {
        ids.emplace_back(request->metaserverid(i));
    }

    std::map<MetaServerIdType, bool> statusMap;
    ScheduleStatusCode errCode =
        coordinator_->QueryMetaServerRecoverStatus(ids, &statusMap);
    response->set_statuscode(errCode);
    if (errCode == ScheduleStatusCode::Success) {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id() << "] from "
                  << cntl->local_side() << " to " << cntl->remote_side()
                  << ". [QueryMetaServerRecoverStatusResponse] "
                  << response->DebugString();
        for (auto item : statusMap) {
            response->mutable_recoverstatusmap()->insert(
                {item.first, item.second});
        }
    } else {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id() << "] from "
                   << cntl->local_side() << " to " << cntl->remote_side()
                   << ". [QueryMetaServerRecoverStatusResponse] "
                   << response->DebugString();
    }
}
}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
