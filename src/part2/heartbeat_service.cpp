#include "src/part2/heartbeat_service.h"

namespace nebd {
namespace server {

void NebdHeartbeatServiceImpl::KeepAlive(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::HeartbeatRequest* request,
    nebd::client::HeartbeatResponse* response,
    google::protobuf::Closure* done) {
    // TODO
}

}  // namespace server
}  // namespace nebd

