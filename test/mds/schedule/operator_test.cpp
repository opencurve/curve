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
 * Created Date: Mon Dec 24 2018
 * Author: lixiaocui
 */

#include <gtest/gtest.h>
#include "src/mds/schedule/operator.h"
#include "test/mds/schedule/common.h"

using std::chrono::steady_clock;
using ::curve::mds::topology::EpochType;

namespace curve {
namespace mds {
namespace schedule {

TEST(OperatorTest, OperatorTest_Apply_Test) {
    EpochType startEpoch = 1;
    CopySetKey copySetKey;
    copySetKey.first = 1;
    copySetKey.second = 2;
    Operator testOperator(startEpoch, copySetKey,
                          OperatorPriority::NormalPriority,
                          steady_clock::now(),
                          std::make_shared<RemovePeer>(3));

    // 1. order remove peer
    auto originCopySetInfo = GetCopySetInfoForTest();
    originCopySetInfo.peers.emplace_back(
        PeerInfo(4, 3, 4, "192.168.10.4", 9000));
    CopySetConf copySetConf;
    auto applyStatus = testOperator.Apply(originCopySetInfo, &copySetConf);
    ASSERT_EQ(ApplyStatus::Ordered, applyStatus);
    ASSERT_EQ(3, copySetConf.configChangeItem);
    ASSERT_EQ(ConfigChangeType::REMOVE_PEER, copySetConf.type);

    // 2. finish remove peer
    originCopySetInfo.candidatePeerInfo = PeerInfo(3, 1, 1, "", 9000);
    originCopySetInfo.peers.erase(originCopySetInfo.peers.end() - 2);
    auto replica = new ::curve::common::Peer();
    replica->set_id(1);
    replica->set_address("192.168.10.1:9000:0");
    originCopySetInfo.configChangeInfo.set_allocated_peer(replica);
    originCopySetInfo.configChangeInfo.set_type(ConfigChangeType::REMOVE_PEER);
    originCopySetInfo.configChangeInfo.set_finished(true);
    applyStatus = testOperator.Apply(originCopySetInfo, &copySetConf);
    ASSERT_EQ(ApplyStatus::Finished, applyStatus);

    // 3. test remove peer failed
    originCopySetInfo = GetCopySetInfoForTest();
    std::string *errMsg = new std::string("remove err");
    CandidateError *candidateErr = new CandidateError();
    candidateErr->set_allocated_errmsg(errMsg);
    candidateErr->set_errtype(2);
    originCopySetInfo.candidatePeerInfo = PeerInfo(3, 1, 1, "", 9000);
    originCopySetInfo.configChangeInfo.set_finished(false);
    replica = new ::curve::common::Peer();
    replica->set_id(1);
    replica->set_address("192.168.10.1:9000:0");
    originCopySetInfo.configChangeInfo.set_allocated_peer(replica);
    originCopySetInfo.configChangeInfo.set_type(ConfigChangeType::REMOVE_PEER);
    originCopySetInfo.configChangeInfo.set_allocated_err(candidateErr);
    originCopySetInfo.configChangeInfo.CheckInitialized();
    applyStatus = testOperator.Apply(originCopySetInfo, &copySetConf);
    ASSERT_EQ(ApplyStatus::Failed, applyStatus);

    // 4. test remove peer unfinished
    originCopySetInfo.configChangeInfo.clear_err();
    applyStatus = testOperator.Apply(originCopySetInfo, &copySetConf);
    ASSERT_EQ(ApplyStatus::OnGoing, applyStatus);
}

TEST(OperatorTest, OperatorTest_Function_Test) {
    EpochType startEpoch = 1;
    CopySetKey copySetKey;
    copySetKey.first = 1;
    copySetKey.second = 2;
    Operator testOperator(startEpoch, copySetKey,
                          OperatorPriority::NormalPriority,
                          steady_clock::now(),
                          std::make_shared<TransferLeader>(1, 3));

    // 1. test AffectedChunkServers
    auto affects = testOperator.AffectedChunkServers();
    ASSERT_EQ(0, affects.size());
    testOperator.step = std::make_shared<AddPeer>(5);
    affects = testOperator.AffectedChunkServers();
    ASSERT_EQ(1, affects.size());
    ASSERT_EQ(5, affects[0]);
    testOperator.step = std::make_shared<ChangePeer>(1, 5);
    ASSERT_EQ(1, affects.size());
    ASSERT_EQ(5, affects[0]);

    // 2. test IsTimeout
    testOperator.createTime = steady_clock::now() - std::chrono::seconds(2);
    testOperator.timeLimit = std::chrono::seconds(2);
    ASSERT_TRUE(testOperator.IsTimeout());
    testOperator.createTime = steady_clock::now();
    ASSERT_FALSE(testOperator.IsTimeout());
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve

