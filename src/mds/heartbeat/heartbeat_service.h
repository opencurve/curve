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
 * Created Date: Tue Nov 20 2018
 * Author: xuchaojie
 */

#ifndef SRC_MDS_HEARTBEAT_HEARTBEAT_SERVICE_H_
#define SRC_MDS_HEARTBEAT_HEARTBEAT_SERVICE_H_

#include <brpc/server.h>
#include <memory>

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
