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
 * Date: Wednesday Dec 01 19:37:04 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_TEST_METASERVER_COPYSET_MOCK_MOCK_COPYSET_NODE_H_
#define CURVEFS_TEST_METASERVER_COPYSET_MOCK_MOCK_COPYSET_NODE_H_

#include <gmock/gmock.h>

#include <vector>

#include "curvefs/src/metaserver/copyset/copyset_node.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

class MockCopysetNode : public CopysetNode {
 public:
    MockCopysetNode() : CopysetNode(0, 0, {}, nullptr) {}

    MOCK_CONST_METHOD0(GetConfEpoch, uint64_t());
    MOCK_METHOD1(TransferLeader, butil::Status(const Peer&));
    MOCK_METHOD2(AddPeer, void(const Peer&, braft::Closure*));
    MOCK_METHOD2(RemovePeer, void(const Peer&, braft::Closure*));
    MOCK_METHOD2(ChangePeers, void(const std::vector<Peer>&, braft::Closure*));
    MOCK_CONST_METHOD1(ListPeers, void(std::vector<Peer>*));
    MOCK_CONST_METHOD0(IsLeaderTerm, bool());
    MOCK_METHOD1(Propose, void(const braft::Task& task));
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_TEST_METASERVER_COPYSET_MOCK_MOCK_COPYSET_NODE_H_
