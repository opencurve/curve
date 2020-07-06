/*
 * Project: nebd
 * Created Date: 2020-02-03
 * Author: charisu
 * Copyright (c) 2020 netease
 */

#include "nebd/src/part2/heartbeat_service.h"
#include "nebd/src/part2/define.h"
#include "src/common/timeutility.h"

namespace nebd {
namespace server {

using curve::common::TimeUtility;

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
