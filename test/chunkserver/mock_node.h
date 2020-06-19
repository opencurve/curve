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
 * Created Date: 19-05-03
 * Author: wudemiao
 */

#ifndef TEST_CHUNKSERVER_MOCK_NODE_H_
#define TEST_CHUNKSERVER_MOCK_NODE_H_

#include <gmock/gmock.h>
#include <gmock/gmock-generated-function-mockers.h>
#include <gmock/internal/gmock-generated-internal-utils.h>
#include <braft/raft.h>

#include <string>
#include <vector>

#include "src/chunkserver/raft_node.h"

namespace curve {
namespace chunkserver {

using ::braft::UserLog;
using ::braft::Task;
using ::braft::NodeId;

class MockNode : public RaftNode {
 public:
    MockNode(const LogicPoolID &logicPoolId,
             const CopysetID &copysetId)
        : RaftNode(ToGroupIdString(logicPoolId, copysetId),
                   PeerId("127.0.0.1:3200:0")) {
    }

    MOCK_METHOD0(node_id, NodeId());
    MOCK_METHOD0(leader_id, PeerId());
    MOCK_METHOD0(is_leader, bool());
    MOCK_METHOD1(init, int(const NodeOptions&));
    MOCK_METHOD1(shutdown, void(braft::Closure*));
    MOCK_METHOD0(join, void(void));
    MOCK_METHOD1(apply, void(const Task&));
    MOCK_METHOD1(list_peers, butil::Status(std::vector<PeerId>*));
    MOCK_METHOD2(add_peer, void(const PeerId&, braft::Closure*));
    MOCK_METHOD2(remove_peer, void(const PeerId&, braft::Closure*));
    MOCK_METHOD2(change_peers, void(const Configuration&, braft::Closure*));
    MOCK_METHOD1(reset_peers, butil::Status(const Configuration&));
    MOCK_METHOD1(snapshot, void(braft::Closure*));
    MOCK_METHOD1(vote, void(int));
    MOCK_METHOD1(reset_election_timeout_ms, void(int));
    MOCK_METHOD1(transfer_leadership_to, int(const PeerId&));
    MOCK_METHOD2(read_committed_user_log, butil::Status(const int64_t,
                                                        UserLog*));
    MOCK_METHOD1(get_status, void(NodeStatus*));
    MOCK_METHOD0(enter_readonly_mode, void(void));
    MOCK_METHOD0(leave_readonly_mode, void(void));
    MOCK_METHOD0(readonly, bool());
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_MOCK_NODE_H_
