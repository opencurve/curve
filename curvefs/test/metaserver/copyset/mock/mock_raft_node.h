/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: Tue Sep  7 11:24:19 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_TEST_METASERVER_COPYSET_MOCK_MOCK_RAFT_NODE_H_
#define CURVEFS_TEST_METASERVER_COPYSET_MOCK_MOCK_RAFT_NODE_H_

#include <gmock/gmock.h>

#include <vector>

#include "curvefs/src/metaserver/copyset/raft_node.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::curvefs::metaserver::copyset::RaftNode;

class MockRaftNode : public RaftNode {
 public:
    MockRaftNode() : RaftNode({}, {}) {}

    MOCK_METHOD0(node_id, braft::NodeId());
    MOCK_METHOD0(leader_id, braft::PeerId());
    MOCK_METHOD0(is_leader, bool());
    MOCK_METHOD1(init, int(const braft::NodeOptions&));
    MOCK_METHOD1(shutdown, void(braft::Closure*));
    MOCK_METHOD0(join, void());
    MOCK_METHOD1(apply, void(const braft::Task&));
    MOCK_METHOD1(list_peers, butil::Status(std::vector<braft::PeerId>*));
    MOCK_METHOD2(add_peer, void(const braft::PeerId&, braft::Closure*));
    MOCK_METHOD2(remove_peer, void(const braft::PeerId&, braft::Closure*));
    MOCK_METHOD2(change_peers,
                 void(const braft::Configuration&, braft::Closure*));
    MOCK_METHOD1(reset_peers, butil::Status(const braft::Configuration&));
    MOCK_METHOD1(snapshot, void(braft::Closure*));
    MOCK_METHOD1(reset_election_timeout_ms, void(int));
    MOCK_METHOD1(transfer_leadership_to, int(const braft::PeerId&));
    MOCK_METHOD2(read_committed_user_log,
                 butil::Status(const int64_t, braft::UserLog*));
    MOCK_METHOD1(get_status, void(braft::NodeStatus*));
    MOCK_METHOD(void, get_leader_lease_status, (braft::LeaderLeaseStatus*), (override));  // NOLINT
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_TEST_METASERVER_COPYSET_MOCK_MOCK_RAFT_NODE_H_
