/*
 * Project: nebd
 * Created Date: 2020-02-03
 * Author: charisu
 * Copyright (c) 2020 netease
 */

#ifndef SRC_PART2_HEARTBEAT_SERVICE_H_
#define SRC_PART2_HEARTBEAT_SERVICE_H_

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <memory>

#include "nebd/proto/heartbeat.pb.h"
#include "nebd/src/part2/heartbeat_manager.h"

namespace nebd {
namespace server {

class NebdHeartbeatServiceImpl : public nebd::client::NebdHeartbeatService {
 public:
    explicit NebdHeartbeatServiceImpl(
        std::shared_ptr<HeartbeatManager> heartbeatManager)
        : heartbeatManager_(heartbeatManager) {}
    virtual ~NebdHeartbeatServiceImpl() {}
    virtual void KeepAlive(google::protobuf::RpcController* cntl_base,
                           const nebd::client::HeartbeatRequest* request,
                           nebd::client::HeartbeatResponse* response,
                           google::protobuf::Closure* done);

 private:
    std::shared_ptr<HeartbeatManager> heartbeatManager_;
};

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_HEARTBEAT_SERVICE_H_
