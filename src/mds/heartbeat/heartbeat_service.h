/*
 * Project: curve
 * Created Date: Tue Nov 20 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_HEARTBEAT_HEARTBEAT_SERVICE_H_
#define SRC_MDS_HEARTBEAT_HEARTBEAT_SERVICE_H_

#include <brpc/server.h>
#include <boost/shared_ptr.hpp>

#include "src/mds/heartbeat/heartbeat_manager.h"
#include "proto/heartbeat.pb.h"

using ::curve::mds::heartbeat::HeartbeatManager;

namespace curve {
namespace mds {
namespace heartbeat {

class HeartbeatServiceImpl : public HeartbeatService {
 public:
  HeartbeatServiceImpl() = default;
  explicit HeartbeatServiceImpl(
      std::shared_ptr<HeartbeatManager> heartbeatManager);
  ~HeartbeatServiceImpl() override = default;

  void ChunkServerHeartbeat(
      google::protobuf::RpcController *cntl_base,
      const ChunkServerHeartbeatRequest *request,
      ChunkServerHeartbeatResponse *response,
      google::protobuf::Closure *done) override;

 private:
  std::shared_ptr<HeartbeatManager> heartbeatManager_;
};
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve



#endif  // SRC_MDS_HEARTBEAT_HEARTBEAT_SERVICE_H_
