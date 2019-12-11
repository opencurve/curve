/*
 * Project: curve
 * Created Date: 19-05-03
 * Author: wudemiao
 * Copyright (c) 2019 netease
 */

#ifndef TEST_CHUNKSERVER_MOCK_NODE_H_
#define TEST_CHUNKSERVER_MOCK_NODE_H_

#include <gmock/gmock.h>
#include <gmock/gmock-generated-function-mockers.h>
#include <gmock/internal/gmock-generated-internal-utils.h>
#include <braft/raft.h>

#include <string>
#include <vector>

#include "include/chunkserver/chunkserver_common.h"

namespace curve {
namespace chunkserver {

using ::braft::UserLog;
using ::braft::Task;
using ::braft::NodeId;

class MockNode : public braft::Node {
 public:
    MockNode(const LogicPoolID &logicPoolId,
             const CopysetID &copysetId)
        : braft::Node(ToGroupIdString(logicPoolId, copysetId),
                      PeerId("127.0.0.1:3200:0")) {
    }

    MOCK_METHOD0(node_id, NodeId());
    MOCK_METHOD0(leader_id, PeerId());
    MOCK_METHOD0(is_leader, bool());
    MOCK_METHOD4(conf_changes, bool(Configuration *old_conf,
                                    Configuration *adding,
                                    Configuration *removing,
                                    PeerId *transferee_peer));
    MOCK_METHOD1(init, int(const NodeOptions&));
    MOCK_METHOD1(shutdown, void(Closure*));
    MOCK_METHOD0(join, void(void));
    MOCK_METHOD1(apply, void(const Task&));
    MOCK_METHOD1(list_peers, butil::Status(std::vector<PeerId>*));
    MOCK_METHOD2(add_peer, void(const PeerId&, Closure*));
    MOCK_METHOD2(remove_peer, void(const PeerId&, Closure*));
    MOCK_METHOD2(change_peers, void(const Configuration&, Closure*));
    MOCK_METHOD1(reset_peers, butil::Status(const Configuration&));
    MOCK_METHOD1(snapshot, void(Closure*));
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
