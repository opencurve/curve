/*
 * Project: curve
 * Created Date: Sat Jan 05 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include "src/mds/heartbeat/heartbeat_service.h"

namespace curve {
namespace mds {
namespace heartbeat {
HeartbeatServiceImpl::HeartbeatServiceImpl(
    std::shared_ptr<HeartbeatManager> heartbeatManager) {
    this->heartbeatManager_ = heartbeatManager;
}

void HeartbeatServiceImpl::ChunkServerHeartbeat(
    ::google::protobuf::RpcController *controller,
    const ::curve::mds::heartbeat::ChunkServerHeartbeatRequest *request,
    ::curve::mds::heartbeat::ChunkServerHeartbeatResponse *response,
    ::google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    heartbeatManager_->ChunkServerHeartbeat(*request, response);
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve


