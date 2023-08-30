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
 * Created Date: Fri Dec 21 2018
 * Author: lixiaocui
 */

#include "test/mds/schedule/mock_topoAdapter.h"
#include "test/mds/mock/mock_topology.h"
#include "test/mds/schedule/common.h"
#include "src/mds/schedule/scheduleMetrics.h"
#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/operatorFactory.h"

using ::curve::mds::topology::MockTopology;

using ::testing::_;
using ::testing::Return;
using ::testing::AtLeast;
using ::testing::SetArgPointee;
using ::testing::DoAll;

namespace curve {
namespace mds {
namespace schedule {
class TestRapidLeaderSchedule : public ::testing::Test {
 protected:
    void SetUp() override {
        auto topo = std::make_shared<MockTopology>();
        auto metric = std::make_shared<ScheduleMetrics>(topo);
        opController_ = std::make_shared<OperatorController>(2, metric);
        topoAdapter_ = std::make_shared<MockTopoAdapter>();

        opt_.transferLeaderTimeLimitSec = 10;
        opt_.removePeerTimeLimitSec = 100;
        opt_.addPeerTimeLimitSec = 1000;
        opt_.changePeerTimeLimitSec = 1000;
        opt_.recoverSchedulerIntervalSec = 1;
        opt_.scatterWithRangePerent = 0.2;

        auto testCopySetInfo = GetCopySetInfoForTest();
        ChunkServerInfo csInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
            DiskState::DISKNORMAL, ChunkServerStatus::READWRITE,
            1, 100, 100, ChunkServerStatisticInfo{});
        ChunkServerInfo csInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
            DiskState::DISKNORMAL, ChunkServerStatus::READWRITE,
            0, 100, 100, ChunkServerStatisticInfo{});
        ChunkServerInfo csInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
            DiskState::DISKNORMAL, ChunkServerStatus::READWRITE,
            0, 100, 100, ChunkServerStatisticInfo{});
        chunkServerInfos_.emplace_back(csInfo1);
        chunkServerInfos_.emplace_back(csInfo2);
        chunkServerInfos_.emplace_back(csInfo3);
    }

 protected:
    std::shared_ptr<MockTopoAdapter> topoAdapter_;
    std::shared_ptr<OperatorController> opController_;
    std::vector<ChunkServerInfo> chunkServerInfos_;
    ScheduleOption opt_;
};

TEST_F(TestRapidLeaderSchedule, test_logicalPool_not_exist) {
    std::shared_ptr<RapidLeaderScheduler> rapidLeaderScheduler;
    //1 Mds does not have any logicalpool
    {
        rapidLeaderScheduler = std::make_shared<RapidLeaderScheduler>(
            opt_, topoAdapter_, opController_, 2);
        EXPECT_CALL(*topoAdapter_, GetLogicalpools())
            .WillOnce(Return(std::vector<PoolIdType>{}));
        ASSERT_EQ(kScheduleErrCodeInvalidLogicalPool,
            rapidLeaderScheduler->Schedule());

        rapidLeaderScheduler = std::make_shared<RapidLeaderScheduler>(
            opt_, topoAdapter_, opController_, 0);
        EXPECT_CALL(*topoAdapter_, GetLogicalpools())
            .WillOnce(Return(std::vector<PoolIdType>{}));
        ASSERT_EQ(kScheduleErrCodeSuccess, rapidLeaderScheduler->Schedule());
    }

    //2 No logicalpool specified in the mds logical pool list
    {
        rapidLeaderScheduler = std::make_shared<RapidLeaderScheduler>(
            opt_, topoAdapter_, opController_, 2);
        EXPECT_CALL(*topoAdapter_, GetLogicalpools())
            .WillOnce(Return(std::vector<PoolIdType>{1}));
        ASSERT_EQ(kScheduleErrCodeInvalidLogicalPool,
            rapidLeaderScheduler->Schedule());
    }
}

TEST_F(TestRapidLeaderSchedule, test_initResource_no_need_schedule) {
    std::shared_ptr<RapidLeaderScheduler> rapidLeaderScheduler;
    {
        //1 There is no chunkserver in the specified logicalpool
        EXPECT_CALL(*topoAdapter_, GetLogicalpools())
            .WillOnce(Return(std::vector<PoolIdType>{1}));
        EXPECT_CALL(*topoAdapter_, GetChunkServersInLogicalPool(1))
            .WillOnce(Return(std::vector<ChunkServerInfo>{}));
        EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(1))
            .WillOnce(Return(std::vector<CopySetInfo>{}));
        rapidLeaderScheduler = std::make_shared<RapidLeaderScheduler>(
            opt_, topoAdapter_, opController_, 1);
        ASSERT_EQ(kScheduleErrCodeSuccess, rapidLeaderScheduler->Schedule());
        ASSERT_EQ(0, opController_->GetOperators().size());
    }

    {
        //2 There is no copyset in the specified logicalpool
        EXPECT_CALL(*topoAdapter_, GetLogicalpools())
            .WillOnce(Return(std::vector<PoolIdType>{1}));
        EXPECT_CALL(*topoAdapter_, GetChunkServersInLogicalPool(1))
            .WillOnce(Return(std::vector<ChunkServerInfo>{ChunkServerInfo{}}));
        EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(1))
            .WillOnce(Return(std::vector<CopySetInfo>{}));
        rapidLeaderScheduler = std::make_shared<RapidLeaderScheduler>(
            opt_, topoAdapter_, opController_, 1);
        ASSERT_EQ(kScheduleErrCodeSuccess, rapidLeaderScheduler->Schedule());
        ASSERT_EQ(0, opController_->GetOperators().size());
    }
}

TEST_F(TestRapidLeaderSchedule, test_select_target_fail) {
    std::shared_ptr<RapidLeaderScheduler> rapidLeaderScheduler;
    rapidLeaderScheduler = std::make_shared<RapidLeaderScheduler>(
        opt_, topoAdapter_, opController_, 1);

    {
        //1 The number of copies for copyset is 1, and migration will not occur
        EXPECT_CALL(*topoAdapter_, GetLogicalpools())
            .WillOnce(Return(std::vector<PoolIdType>{1}));
        EXPECT_CALL(*topoAdapter_, GetChunkServersInLogicalPool(1))
            .WillOnce(Return(chunkServerInfos_));
        auto copysetInfo = GetCopySetInfoForTest();
        PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
        copysetInfo.peers.clear();
        copysetInfo.peers.emplace_back(peer1);
        EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(1))
            .WillOnce(Return(std::vector<CopySetInfo>{copysetInfo}));

        ASSERT_EQ(kScheduleErrCodeSuccess, rapidLeaderScheduler->Schedule());
        ASSERT_EQ(0, opController_->GetOperators().size());
    }

    {
        //2 The maximum difference in the number of leaders owned on chunkserver is 1, and migration will not occur
        //      chunkserver-1        chunkserver-2        chunkserver-3
        //      copyset-1(leader)      copyset-1            copyset-1
        EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Return(std::vector<PoolIdType>{1}));
        EXPECT_CALL(*topoAdapter_, GetChunkServersInLogicalPool(1))
            .WillOnce(Return(chunkServerInfos_));
        EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(1))
            .WillOnce(Return(
                std::vector<CopySetInfo>{GetCopySetInfoForTest()}));

        ASSERT_EQ(kScheduleErrCodeSuccess, rapidLeaderScheduler->Schedule());
        ASSERT_EQ(0, opController_->GetOperators().size());
    }
}

TEST_F(TestRapidLeaderSchedule, test_rapid_schedule_success) {
    //Fast balancing successful
    //      chunkserver-1        chunkserver-2        chunkserver-3
    //      copyset-1(leader)      copyset-1            copyset-1
    //      copyset-2(leader)      copyset-2            copyset-2
    //      copyset-3(leader)      copyset-3            copyset-3
    std::shared_ptr<RapidLeaderScheduler> rapidLeaderScheduler;
    rapidLeaderScheduler = std::make_shared<RapidLeaderScheduler>(
        opt_, topoAdapter_, opController_, 1);

    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Return(std::vector<PoolIdType>{1}));
    auto chunkserverInfosBak = chunkServerInfos_;
    chunkserverInfosBak[0].leaderCount = 3;
    EXPECT_CALL(*topoAdapter_, GetChunkServersInLogicalPool(1))
            .WillOnce(Return(chunkserverInfosBak));

    auto copyset1 = GetCopySetInfoForTest();
    auto copyset2 = GetCopySetInfoForTest();
    copyset2.id = CopySetKey{1, 2};
    auto copyset3 = GetCopySetInfoForTest();
    copyset3.id = CopySetKey{1, 3};
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(1))
            .WillOnce(Return(
                std::vector<CopySetInfo>{copyset1, copyset2, copyset3}));
    OperatorFactory factory;
    opController_->AddOperator(factory.CreateRemovePeerOperator(
        copyset2, 2, OperatorPriority::NormalPriority));

    ASSERT_EQ(kScheduleErrCodeSuccess, rapidLeaderScheduler->Schedule());
    auto operators = opController_->GetOperators();
    ASSERT_EQ(3, operators.size());
    auto op1 = dynamic_cast<TransferLeader *>(operators[0].step.get());
    ASSERT_TRUE(nullptr != op1);
    ASSERT_EQ(2, op1->GetTargetPeer());
    ASSERT_EQ(1, operators[0].copysetID.second);
    auto op2 = dynamic_cast<TransferLeader *>(operators[2].step.get());
    ASSERT_TRUE(nullptr != op2);
    ASSERT_EQ(3, op2->GetTargetPeer());
    ASSERT_EQ(3, operators[2].copysetID.second);
}

TEST_F(TestRapidLeaderSchedule, test_rapid_schedule_pendding) {
    //Fast balancing successful
    //      chunkserver-1        chunkserver-2        chunkserver-3
    //      copyset-1(leader)      copyset-1            copyset-1
    //      copyset-2(leader)      copyset-2            copyset-2
    //      copyset-3(leader)      copyset-3            copyset-3
    std::shared_ptr<RapidLeaderScheduler> rapidLeaderScheduler;
    rapidLeaderScheduler = std::make_shared<RapidLeaderScheduler>(
        opt_, topoAdapter_, opController_, 1);

    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Return(std::vector<PoolIdType>{1}));
    auto chunkserverInfosBak = chunkServerInfos_;
    chunkserverInfosBak[0].leaderCount = 3;
    chunkserverInfosBak[0].status = ChunkServerStatus::PENDDING;
    EXPECT_CALL(*topoAdapter_, GetChunkServersInLogicalPool(1))
            .WillOnce(Return(chunkserverInfosBak));

    auto copyset1 = GetCopySetInfoForTest();
    auto copyset2 = GetCopySetInfoForTest();
    copyset2.id = CopySetKey{1, 2};
    auto copyset3 = GetCopySetInfoForTest();
    copyset3.id = CopySetKey{1, 3};
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(1))
            .WillOnce(Return(
                std::vector<CopySetInfo>{copyset1, copyset2, copyset3}));

    ASSERT_EQ(kScheduleErrCodeSuccess, rapidLeaderScheduler->Schedule());
    auto operators = opController_->GetOperators();
    ASSERT_EQ(0, operators.size());
}

}  // namespace schedule
}  // namespace mds
}  // namespace curve
