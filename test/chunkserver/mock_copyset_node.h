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
 * Created Date: Monday April 1st 2019
 * Author: yangyaokai
 */

#ifndef TEST_CHUNKSERVER_MOCK_COPYSET_NODE_H_
#define TEST_CHUNKSERVER_MOCK_COPYSET_NODE_H_

#include <gmock/gmock.h>
#include <memory>
#include <string>
#include <vector>

#include "src/chunkserver/copyset_node.h"

namespace curve {
namespace chunkserver {


class MockCopysetNode : public CopysetNode {
 public:
    MockCopysetNode() = default;
    ~MockCopysetNode() = default;

    MOCK_METHOD1(Init, int(const CopysetNodeOptions&));
    MOCK_METHOD0(Run, int());
    MOCK_METHOD0(Fini, void());
    MOCK_CONST_METHOD0(IsLeaderTerm, bool());
    MOCK_METHOD(bool, IsLeaseLeader, (const braft::LeaderLeaseStatus&), (const, override));  // NOLINT
    MOCK_CONST_METHOD0(GetLeaderId, PeerId());
    MOCK_METHOD1(ListPeers, void(std::vector<Peer>*));
    MOCK_CONST_METHOD0(GetConfEpoch, uint64_t());
    MOCK_METHOD1(UpdateAppliedIndex, void(uint64_t));
    MOCK_CONST_METHOD0(GetAppliedIndex, uint64_t());
    MOCK_METHOD3(GetConfChange, int(ConfigChangeType*, Configuration*, Peer*));
    MOCK_METHOD1(GetHash, int(std::string*));
    MOCK_METHOD1(GetStatus, void(NodeStatus*));
    MOCK_METHOD1(GetLeaderStatus, bool(NodeStatus*));
    MOCK_METHOD(void, GetLeaderLeaseStatus, (braft::LeaderLeaseStatus*), (override));  // NOLINT
    MOCK_CONST_METHOD0(GetDataStore, std::shared_ptr<CSDataStore>());
    MOCK_CONST_METHOD0(GetConcurrentApplyModule, ConcurrentApplyModule*());
    MOCK_METHOD0(GetFailedScanMap, std::vector<ScanMap>&());
    MOCK_METHOD1(Propose, void(const braft::Task&));
    MOCK_METHOD1(SetScan, void(bool));
    MOCK_CONST_METHOD0(GetScan, bool());
    MOCK_METHOD1(SetLastScan, void(uint64_t));
    MOCK_CONST_METHOD0(GetLastScan, uint64_t());

    MOCK_METHOD1(on_apply, void(::braft::Iterator&));
    MOCK_METHOD0(on_shutdown, void());
    MOCK_METHOD2(on_snapshot_save, void(::braft::SnapshotWriter*,
                                        ::braft::Closure*));
    MOCK_METHOD1(on_snapshot_load, int(::braft::SnapshotReader*));
    MOCK_METHOD1(on_leader_start, void(int64_t));
    MOCK_METHOD1(on_leader_stop, void(const butil::Status&));
    MOCK_METHOD1(on_error, void(const ::braft::Error&));
    MOCK_METHOD1(on_configuration_committed, void(const ::braft::Configuration&));  //NOLINT
    MOCK_METHOD1(on_stop_following, void(const ::braft::LeaderChangeContext&));
    MOCK_METHOD1(on_start_following, void(const ::braft::LeaderChangeContext&));
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_MOCK_COPYSET_NODE_H_
