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
 * Created Date: 18-8-23
 * Author: wudemiao
 */

#ifndef SRC_CHUNKSERVER_BRAFT_CLI_SERVICE_H_
#define SRC_CHUNKSERVER_BRAFT_CLI_SERVICE_H_

#include <butil/status.h>
#include <braft/node.h>

#include <string>

#include "src/chunkserver/copyset_node.h"
#include "proto/cli.pb.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

/**
 * @brief: This is a service for braft configuration changes.
 */
class BRaftCliServiceImpl : public CliService {
 public:
    // @brief: Add a peer.
    void add_peer(RpcController *controller,
                  const AddPeerRequest *request,
                  AddPeerResponse *response,
                  Closure *done);

    // @brief: Remove peer.
    void remove_peer(RpcController *controller,
                     const RemovePeerRequest *request,
                     RemovePeerResponse *response,
                     Closure *done);

    // @brief: Get the leader of copyset.
    void get_leader(RpcController *controller,
                    const GetLeaderRequest *request,
                    GetLeaderResponse *response,
                    Closure *done);

    // @brief: Transfer leader.
    void transfer_leader(RpcController *controller,
                         const TransferLeaderRequest *request,
                         TransferLeaderResponse *response,
                         Closure *done);

 private:
    butil::Status get_node(scoped_refptr<braft::NodeImpl> *node,
                           const LogicPoolID &logicPoolId,
                           const CopysetID &copysetId,
                           const std::string &peer_id);
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_BRAFT_CLI_SERVICE_H_
