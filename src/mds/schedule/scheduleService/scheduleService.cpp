/*
 * Project: curve
 * Created Date: 2020-01-02
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <vector>
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

}  // namespace schedule
}  // namespace mds
}  // namespace curve
