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

#ifndef SRC_CHUNKSERVER_BRAFT_CLI_SERVICE2_H_
#define SRC_CHUNKSERVER_BRAFT_CLI_SERVICE2_H_

#include <butil/status.h>
#include <braft/node.h>

#include <string>

#include "src/chunkserver/copyset_node.h"
#include "proto/cli2.pb.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

/**
 * braft's configuration about Rpc Service
 */
class BRaftCliServiceImpl2 : public CliService2 {
 public:
    // add a peer
    void AddPeer(RpcController *controller,
                 const AddPeerRequest2 *request,
                 AddPeerResponse2 *response,
                 Closure *done);

    // remove a peer
    void RemovePeer(RpcController *controller,
                    const RemovePeerRequest2 *request,
                    RemovePeerResponse2 *response,
                    Closure *done);

    // change peers
    void ChangePeers(RpcController *controller,
                     const ChangePeersRequest2 *request,
                     ChangePeersResponse2 *response,
                     Closure *done);

    // get copyset's leader
    void GetLeader(RpcController *controller,
                   const GetLeaderRequest2 *request,
                   GetLeaderResponse2 *response,
                   Closure *done);

    // transfer leader
    void TransferLeader(RpcController *controller,
                        const TransferLeaderRequest2 *request,
                        TransferLeaderResponse2 *response,
                        Closure *done);

    // reset copyset's peer
    void ResetPeer(RpcController* controller,
                   const ResetPeerRequest2* request,
                   ResetPeerResponse2* response,
                   Closure* done);

    // trigger snapshot
    void Snapshot(RpcController* controller,
                  const SnapshotRequest2* request,
                  SnapshotResponse2* response,
                  Closure* done);

    // make snapshots of all copysets on current chunkserver
    void SnapshotAll(RpcController* controller,
                     const SnapshotAllRequest* request,
                     SnapshotAllResponse* response,
                     Closure* done);

 private:
    /**
     * @brief: get specific raft node
     * @param node[out]: result raft node
     * @param logicPoolId[in]: logicpool id
     * @param copysetId[in]: copysetpool id
     * @param peer_id[in]: peer id
     * @return if succeeded，return Status::OK()；otherwise，return a non- Status::OK() value
     */
    butil::Status get_node(scoped_refptr<braft::NodeImpl> *node,
                           const LogicPoolID &logicPoolId,
                           const CopysetID &copysetId,
                           const std::string &peer_id);
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_BRAFT_CLI_SERVICE2_H_
