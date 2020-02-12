/*
 * Project: nebd
 * Created Date: 2020-02-03
 * Author: charisu
 * Copyright (c) 2020 netease
 */

#include "src/common/timeutility.h"
#include "src/part2/heartbeat_service.h"

namespace nebd {
namespace server {

using common::TimeUtility;

void NebdHeartbeatServiceImpl::KeepAlive(
        google::protobuf::RpcController* cntl_base,
        const nebd::client::HeartbeatRequest* request,
        nebd::client::HeartbeatResponse* response,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    bool ok = true;
    uint64_t curTime = TimeUtility::GetTimeofDayMs();
    for (int i = 0; i < request->info_size(); ++i) {
        const auto& info = request->info(i);
        int res = heartbeatManager_->UpdateFileTimestamp(info.fd(), curTime);
        if (res != 0) {
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

