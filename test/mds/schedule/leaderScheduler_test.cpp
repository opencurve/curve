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
 * Created Date: Mon March 11 2019
 * Author: lixiaocui
 */

#include <sys/time.h>
#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/scheduleMetrics.h"
#include "test/mds/schedule/mock_topoAdapter.h"
#include "test/mds/mock/mock_topology.h"
#include "test/mds/schedule/common.h"
#include "src/common/timeutility.h"

using ::curve::mds::topology::MockTopology;

using ::testing::_;
using ::testing::Return;
using ::testing::AtLeast;
using ::testing::SetArgPointee;
using ::testing::DoAll;

namespace curve {
namespace mds {
namespace schedule {
class TestLeaderSchedule : public ::testing::Test {
 protected:
    TestLeaderSchedule() {}
    ~TestLeaderSchedule() {}

    void SetUp() override {
        auto topo = std::make_shared<MockTopology>();
        auto metric = std::make_shared<ScheduleMetrics>(topo);
        opController_ = std::make_shared<OperatorController>(2, metric);
        topoAdapter_ = std::make_shared<MockTopoAdapter>();

        ScheduleOption opt;
        opt.transferLeaderTimeLimitSec = 10;
        opt.removePeerTimeLimitSec = 100;
        opt.addPeerTimeLimitSec = 1000;
        opt.changePeerTimeLimitSec = 1000;
        opt.scatterWithRangePerent = 0.2;
        opt.leaderSchedulerIntervalSec = 1;
        opt.chunkserverCoolingTimeSec = 0;
        leaderScheduler_ = std::make_shared<LeaderScheduler>(
            opt, topoAdapter_, opController_);
    }

    void TearDown() override {
        topoAdapter_ = nullptr;
        opController_ = nullptr;
        leaderScheduler_ = nullptr;
    }

 protected:
    std::shared_ptr<MockTopoAdapter> topoAdapter_;
    std::shared_ptr<OperatorController> opController_;
    std::shared_ptr<LeaderScheduler> leaderScheduler_;
};

TEST_F(TestLeaderSchedule, test_no_chunkserverInfos) {
    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Return(std::vector<PoolIdType>({1})));
    EXPECT_CALL(*topoAdapter_, GetChunkServersInLogicalPool(_))
        .WillOnce(Return(std::vector<ChunkServerInfo>()));
    leaderScheduler_->Schedule();
    ASSERT_EQ(0, opController_->GetOperators().size());
}

TEST_F(TestLeaderSchedule, test_has_chunkServer_offline) {
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    auto onlineState = ::curve::mds::topology::OnlineState::ONLINE;
    auto offlineState = ::curve::mds::topology::OnlineState::OFFLINE;
    auto statInfo = ::curve::mds::heartbeat::ChunkServerStatisticInfo();
    auto diskState = ::curve::mds::topology::DiskState::DISKNORMAL;
    ChunkServerInfo csInfo1(
        peer1, offlineState, diskState, ChunkServerStatus::READWRITE,
        0, 100, 10, statInfo);
    ChunkServerInfo csInfo2(
        peer2, onlineState, diskState, ChunkServerStatus::READWRITE,
        2, 100, 10, statInfo);
    ChunkServerInfo csInfo3(
        peer3, onlineState, diskState, ChunkServerStatus::READWRITE,
        0, 100, 10, statInfo);
    std::vector<ChunkServerInfo> csInfos({csInfo1, csInfo2, csInfo3});

    PoolIdType poolId = 1;
    CopySetIdType copysetId = 1;
    CopySetKey copySetKey;
    copySetKey.first = poolId;
    copySetKey.second = copysetId;
    EpochType epoch = 1;
    ChunkServerIdType leader = 2;
    CopySetInfo copySet1(copySetKey, epoch, leader,
        std::vector<PeerInfo>({peer1, peer2, peer3}),
        ConfigChangeInfo{}, CopysetStatistics{});
    std::vector<CopySetInfo> copySetInfos({copySet1});

    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Return(std::vector<PoolIdType>({1})));
    EXPECT_CALL(*topoAdapter_, GetChunkServersInLogicalPool(1))
        .WillOnce(Return(csInfos));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(1))
        .WillRepeatedly(Return(copySetInfos));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(1, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo1), Return(true)));

    leaderScheduler_->Schedule();
    ASSERT_EQ(0, opController_->GetOperators().size());
}

TEST_F(TestLeaderSchedule, test_copySet_has_candidate) {
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    auto onlineState = ::curve::mds::topology::OnlineState::ONLINE;
    auto diskState = ::curve::mds::topology::DiskState::DISKNORMAL;
    auto statInfo = ::curve::mds::heartbeat::ChunkServerStatisticInfo();
    ChunkServerInfo csInfo1(
        peer1, onlineState, diskState, ChunkServerStatus::READWRITE,
        0, 100, 10, statInfo);
    ChunkServerInfo csInfo2(
        peer2, onlineState, diskState, ChunkServerStatus::READWRITE,
        2, 100, 10, statInfo);
    ChunkServerInfo csInfo3(
        peer3, onlineState, diskState, ChunkServerStatus::READWRITE,
        0, 100, 10, statInfo);
    std::vector<ChunkServerInfo> csInfos({csInfo1, csInfo2, csInfo3});
    PoolIdType poolId = 1;
    CopySetIdType copysetId = 1;
    CopySetKey copySetKey;
    copySetKey.first = poolId;
    copySetKey.second = copysetId;
    EpochType epoch = 1;
    ChunkServerIdType leader = 2;
    CopySetInfo copySet1(copySetKey, epoch, leader,
        std::vector<PeerInfo>({peer1, peer2, peer3}),
        ConfigChangeInfo{}, CopysetStatistics{});
    copySet1.candidatePeerInfo = PeerInfo(1, 1, 1, "192.168.10.1", 9000);
    std::vector<CopySetInfo> copySetInfos({copySet1});

    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Return(std::vector<PoolIdType>({1})));
    EXPECT_CALL(*topoAdapter_, GetChunkServersInLogicalPool(1))
        .WillOnce(Return(csInfos));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(1))
        .WillRepeatedly(Return(copySetInfos));

    leaderScheduler_->Schedule();
    ASSERT_EQ(0, opController_->GetOperators().size());}

TEST_F(TestLeaderSchedule, test_cannot_get_chunkServerInfo) {
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    auto onlineState = ::curve::mds::topology::OnlineState::ONLINE;
    auto diskState = ::curve::mds::topology::DiskState::DISKNORMAL;
    auto statInfo = ::curve::mds::heartbeat::ChunkServerStatisticInfo();
    ChunkServerInfo csInfo1(
        peer1, onlineState, diskState, ChunkServerStatus::READWRITE,
        0, 100, 10, statInfo);
    ChunkServerInfo csInfo2(
        peer2, onlineState, diskState, ChunkServerStatus::READWRITE,
        2, 100, 10, statInfo);
    ChunkServerInfo csInfo3(
        peer3, onlineState, diskState, ChunkServerStatus::READWRITE,
        0, 100, 10, statInfo);
    std::vector<ChunkServerInfo> csInfos({csInfo1, csInfo2, csInfo3});

    PoolIdType poolId = 1;
    CopySetIdType copysetId = 1;
    CopySetKey copySetKey;
    copySetKey.first = poolId;
    copySetKey.second = copysetId;
    EpochType epoch = 1;
    ChunkServerIdType leader = 2;
    CopySetInfo copySet1(copySetKey, epoch, leader,
        std::vector<PeerInfo>({peer1, peer2, peer3}),
        ConfigChangeInfo{}, CopysetStatistics{});
    std::vector<CopySetInfo> copySetInfos({copySet1});

    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Return(std::vector<PoolIdType>({1})));
    EXPECT_CALL(*topoAdapter_, GetChunkServersInLogicalPool(1))
        .WillOnce(Return(csInfos));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(1))
        .WillRepeatedly(Return(copySetInfos));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(1, _))
        .WillRepeatedly(Return(false));


    leaderScheduler_->Schedule();
    ASSERT_EQ(0, opController_->GetOperators().size());
}

TEST_F(TestLeaderSchedule, test_no_need_tranferLeaderOut) {
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    auto onlineState = ::curve::mds::topology::OnlineState::ONLINE;
    auto diskState = ::curve::mds::topology::DiskState::DISKNORMAL;
    auto statInfo = ::curve::mds::heartbeat::ChunkServerStatisticInfo();
    ChunkServerInfo csInfo1(
        peer1, onlineState, diskState, ChunkServerStatus::READWRITE,
        0, 100, 10, statInfo);
    ChunkServerInfo csInfo2(
        peer2, onlineState, diskState, ChunkServerStatus::READWRITE,
        1, 100, 10, statInfo);
    ChunkServerInfo csInfo3(
        peer3, onlineState, diskState, ChunkServerStatus::READWRITE,
        0, 100, 10, statInfo);
    csInfo3.startUpTime = 3;
    std::vector<ChunkServerInfo> csInfos({csInfo1, csInfo2, csInfo3});

    PoolIdType poolId = 1;
    CopySetIdType copysetId = 1;
    CopySetKey copySetKey;
    copySetKey.first = poolId;
    copySetKey.second = copysetId;
    EpochType epoch = 1;
    ChunkServerIdType leader = 2;
    CopySetInfo copySet1(copySetKey, epoch, leader,
        std::vector<PeerInfo>({peer1, peer2, peer3}),
        ConfigChangeInfo{}, CopysetStatistics{});
    std::vector<CopySetInfo> copySetInfos({copySet1});

    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Return(std::vector<PoolIdType>({1})));
    EXPECT_CALL(*topoAdapter_, GetChunkServersInLogicalPool(1))
        .WillOnce(Return(csInfos));

    leaderScheduler_->Schedule();
    ASSERT_EQ(0, opController_->GetOperators().size());
}

TEST_F(TestLeaderSchedule, test_tranferLeaderout_normal) {
    //              chunkserver1    chunkserver2     chunkserver3
    // leaderCount       1                2                0
    // copyset           1                1                1
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    PeerInfo peer4(4, 4, 4, "192.168.10.4", 9000);
    PeerInfo peer5(5, 5, 5, "192.168.10.5", 9000);
    PeerInfo peer6(6, 6, 6, "192.168.10.6", 9000);
    auto onlineState = ::curve::mds::topology::OnlineState::ONLINE;
    auto diskState = ::curve::mds::topology::DiskState::DISKNORMAL;
    auto statInfo = ::curve::mds::heartbeat::ChunkServerStatisticInfo();
    ChunkServerInfo csInfo1(
        peer1, onlineState, diskState, ChunkServerStatus::READWRITE,
        1, 100, 10, statInfo);
    ChunkServerInfo csInfo2(
        peer2, onlineState, diskState, ChunkServerStatus::READWRITE,
        2, 100, 10, statInfo);
    ChunkServerInfo csInfo3(
        peer3, onlineState, diskState, ChunkServerStatus::READWRITE,
        0, 100, 10, statInfo);

    ChunkServerInfo csInfo4(
        peer4, onlineState, diskState, ChunkServerStatus::READWRITE,
        1, 100, 10, statInfo);
    ChunkServerInfo csInfo5(
        peer5, onlineState, diskState, ChunkServerStatus::READWRITE,
        2, 100, 10, statInfo);
    ChunkServerInfo csInfo6(
        peer6, onlineState, diskState, ChunkServerStatus::READWRITE,
        0, 100, 10, statInfo);
    struct timeval tm;
    gettimeofday(&tm, NULL);
    csInfo3.startUpTime = tm.tv_sec - 2;
    csInfo6.startUpTime = tm.tv_sec - 2;
    std::vector<ChunkServerInfo> csInfos1({csInfo1, csInfo2, csInfo3});
    std::vector<ChunkServerInfo> csInfos2({csInfo4, csInfo5, csInfo6});

    PoolIdType poolId = 1;
    CopySetIdType copysetId = 1;
    CopySetKey copySetKey;
    copySetKey.first = poolId;
    copySetKey.second = copysetId;
    EpochType epoch = 1;
    ChunkServerIdType leader = 2;
    CopySetInfo copySet1(copySetKey, epoch, leader,
        std::vector<PeerInfo>({peer1, peer2, peer3}),
        ConfigChangeInfo{}, CopysetStatistics{});
    CopySetInfo copySet2(CopySetKey{2, 1}, epoch, 5,
        std::vector<PeerInfo>({peer4, peer5, peer6}),
        ConfigChangeInfo{}, CopysetStatistics{});
    std::vector<CopySetInfo> copySetInfos1({copySet1});
    std::vector<CopySetInfo> copySetInfos2({copySet2});

    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Return(std::vector<PoolIdType>({1, 2})));
    EXPECT_CALL(*topoAdapter_, GetChunkServersInLogicalPool(1))
        .WillOnce(Return(csInfos1));
    EXPECT_CALL(*topoAdapter_, GetChunkServersInLogicalPool(2))
        .WillOnce(Return(csInfos2));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(1))
        .WillOnce(Return(copySetInfos1));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(2))
        .WillOnce(Return(copySetInfos2));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo1), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo2), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(3, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo3), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(4, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo4), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(5, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo5), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(6, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo6), Return(true)));

    leaderScheduler_->Schedule();
    Operator op;
    ASSERT_TRUE(opController_->GetOperatorById(copySet1.id, &op));
    ASSERT_EQ(OperatorPriority::NormalPriority, op.priority);
    ASSERT_EQ(std::chrono::seconds(10), op.timeLimit);
    TransferLeader *res = dynamic_cast<TransferLeader *>(op.step.get());
    ASSERT_TRUE(res != nullptr);
    ASSERT_EQ(csInfo3.info.id, res->GetTargetPeer());

    ASSERT_TRUE(opController_->GetOperatorById(copySet2.id, &op));
    ASSERT_EQ(OperatorPriority::NormalPriority, op.priority);
    ASSERT_EQ(std::chrono::seconds(10), op.timeLimit);
    res = dynamic_cast<TransferLeader *>(op.step.get());
    ASSERT_TRUE(res != nullptr);
    ASSERT_EQ(csInfo6.info.id, res->GetTargetPeer());
}

TEST_F(TestLeaderSchedule, test_tranferLeaderout_pendding) {
    //              chunkserver1    chunkserver2     chunkserver3
    // leaderCount       1                2                0
    // copyset           1                1                1
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    PeerInfo peer4(4, 4, 4, "192.168.10.4", 9000);
    PeerInfo peer5(5, 5, 5, "192.168.10.5", 9000);
    PeerInfo peer6(6, 6, 6, "192.168.10.6", 9000);
    auto onlineState = ::curve::mds::topology::OnlineState::ONLINE;
    auto diskState = ::curve::mds::topology::DiskState::DISKNORMAL;
    auto statInfo = ::curve::mds::heartbeat::ChunkServerStatisticInfo();
    ChunkServerInfo csInfo1(
        peer1, onlineState, diskState, ChunkServerStatus::PENDDING,
        0, 100, 10, statInfo);
    ChunkServerInfo csInfo2(
        peer2, onlineState, diskState, ChunkServerStatus::READWRITE,
        5, 100, 10, statInfo);
    ChunkServerInfo csInfo3(
        peer3, onlineState, diskState, ChunkServerStatus::READWRITE,
        0, 100, 10, statInfo);

    ChunkServerInfo csInfo4(
        peer4, onlineState, diskState, ChunkServerStatus::READWRITE,
        4, 100, 10, statInfo);
    ChunkServerInfo csInfo5(
        peer5, onlineState, diskState, ChunkServerStatus::READWRITE,
        5, 100, 10, statInfo);
    ChunkServerInfo csInfo6(
        peer6, onlineState, diskState, ChunkServerStatus::PENDDING,
        0, 100, 10, statInfo);
    struct timeval tm;
    gettimeofday(&tm, NULL);
    csInfo3.startUpTime = tm.tv_sec - 2;
    csInfo6.startUpTime = tm.tv_sec - 2;
    std::vector<ChunkServerInfo> csInfos1({csInfo1, csInfo2, csInfo3});
    std::vector<ChunkServerInfo> csInfos2({csInfo4, csInfo5, csInfo6});

    PoolIdType poolId = 1;
    CopySetIdType copysetId = 1;
    CopySetKey copySetKey;
    copySetKey.first = poolId;
    copySetKey.second = copysetId;
    EpochType epoch = 1;
    ChunkServerIdType leader = 2;
    CopySetInfo copySet1(copySetKey, epoch, leader,
        std::vector<PeerInfo>({peer1, peer2, peer3}),
        ConfigChangeInfo{}, CopysetStatistics{});
    CopySetInfo copySet2(CopySetKey{2, 1}, epoch, 5,
        std::vector<PeerInfo>({peer4, peer5, peer6}),
        ConfigChangeInfo{}, CopysetStatistics{});
    std::vector<CopySetInfo> copySetInfos1({copySet1});
    std::vector<CopySetInfo> copySetInfos2({copySet2});

    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Return(std::vector<PoolIdType>({1, 2})));
    EXPECT_CALL(*topoAdapter_, GetChunkServersInLogicalPool(1))
        .WillOnce(Return(csInfos1));
    EXPECT_CALL(*topoAdapter_, GetChunkServersInLogicalPool(2))
        .WillOnce(Return(csInfos2));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(1))
        .WillOnce(Return(copySetInfos1));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(2))
        .WillOnce(Return(copySetInfos2));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo1), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo2), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(3, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo3), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(4, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo4), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(5, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo5), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(6, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo6), Return(true)));

    leaderScheduler_->Schedule();
    Operator op;
    ASSERT_TRUE(opController_->GetOperatorById(copySet1.id, &op));
    ASSERT_EQ(OperatorPriority::NormalPriority, op.priority);
    ASSERT_EQ(std::chrono::seconds(10), op.timeLimit);
    TransferLeader *res = dynamic_cast<TransferLeader *>(op.step.get());
    ASSERT_TRUE(res != nullptr);
    ASSERT_EQ(csInfo3.info.id, res->GetTargetPeer());

    ASSERT_FALSE(opController_->GetOperatorById(copySet2.id, &op));
}

TEST_F(TestLeaderSchedule, test_transferLeaderIn_normal) {
    //              chunkserver1    chunkserver2    chunkserver3    chunkserver4
    // leaderCount        0              3                 2               1
    // copyset            1              1                 1(with operator)
    //                    2              2                 2
    //                                   3                 3               3
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    PeerInfo peer4(3, 4, 4, "192.168.10.4", 9000);
    auto onlineState = ::curve::mds::topology::OnlineState::ONLINE;
    auto diskState = ::curve::mds::topology::DiskState::DISKNORMAL;
    auto statInfo = ::curve::mds::heartbeat::ChunkServerStatisticInfo();
    ChunkServerInfo csInfo1(
        peer1, onlineState, diskState, ChunkServerStatus::READWRITE,
        0, 100, 10, statInfo);
    csInfo1.startUpTime = ::curve::common::TimeUtility::GetTimeofDaySec() - 4;
    ChunkServerInfo csInfo2(
        peer2, onlineState, diskState, ChunkServerStatus::READWRITE,
        3, 100, 10, statInfo);
    ChunkServerInfo csInfo3(
        peer3, onlineState, diskState, ChunkServerStatus::READWRITE,
        2, 100, 10, statInfo);
    ChunkServerInfo csInfo4(
        peer4, onlineState, diskState, ChunkServerStatus::READWRITE,
        1, 100, 10, statInfo);
    std::vector<ChunkServerInfo> csInfos({csInfo1, csInfo2, csInfo3, csInfo4});

    PoolIdType poolId = 1;
    CopySetIdType copysetId = 1;
    CopySetKey copySetKey;
    copySetKey.first = poolId;
    copySetKey.second = copysetId;
    EpochType epoch = 1;
    ChunkServerIdType leader = 2;
    CopySetInfo copySet1(copySetKey, epoch, leader,
        std::vector<PeerInfo>({peer1, peer2, peer3}),
        ConfigChangeInfo{}, CopysetStatistics{});
    copySetKey.second = 2;
    leader = 3;
    CopySetInfo copySet2(copySetKey, epoch, leader,
        std::vector<PeerInfo>({peer1, peer2, peer3}),
        ConfigChangeInfo{}, CopysetStatistics{});
    copySetKey.second = 3;
    leader = 4;
    CopySetInfo copySet3(copySetKey, epoch, leader,
        std::vector<PeerInfo>({peer2, peer3, peer4}),
        ConfigChangeInfo{}, CopysetStatistics{});

    copySetKey.second = 1;
    Operator testOperator(1, copySetKey, OperatorPriority::NormalPriority,
                          steady_clock::now(), std::make_shared<AddPeer>(1));
    ASSERT_TRUE(opController_->AddOperator(testOperator));

    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Return(std::vector<PoolIdType>({1})));
    EXPECT_CALL(*topoAdapter_, GetChunkServersInLogicalPool(1))
        .WillOnce(Return(csInfos));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(1))
        .Times(2)
        .WillOnce(Return(std::vector<CopySetInfo>({copySet1})))
        .WillOnce(Return(std::vector<CopySetInfo>({copySet3, copySet2})));
     EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(1, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo1), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(3, _))
        .Times(3)
        .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo3), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(2, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo2), Return(true)));

    ASSERT_EQ(1, leaderScheduler_->Schedule());
    Operator op;
    ASSERT_TRUE(opController_->GetOperatorById(copySet2.id, &op));
    ASSERT_EQ(OperatorPriority::NormalPriority, op.priority);
    ASSERT_EQ(std::chrono::seconds(10), op.timeLimit);
    TransferLeader *res = dynamic_cast<TransferLeader *>(op.step.get());
    ASSERT_TRUE(res != nullptr);
    ASSERT_EQ(1, res->GetTargetPeer());
}

TEST_F(TestLeaderSchedule, test_transferLeaderIn_pendding) {
    //              chunkserver1    chunkserver2    chunkserver3    chunkserver4
    // leaderCount        0              3                 2               1
    // copyset            1              1                 1(with operator)
    //                    2              2                 2
    //                                   3                 3  
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    PeerInfo peer4(3, 4, 4, "192.168.10.4", 9000);
    auto onlineState = ::curve::mds::topology::OnlineState::ONLINE;
    auto diskState = ::curve::mds::topology::DiskState::DISKNORMAL;
    auto statInfo = ::curve::mds::heartbeat::ChunkServerStatisticInfo();
    ChunkServerInfo csInfo1(
        peer1, onlineState, diskState, ChunkServerStatus::READWRITE,
        0, 100, 10, statInfo);
    csInfo1.startUpTime = ::curve::common::TimeUtility::GetTimeofDaySec() - 4;
    ChunkServerInfo csInfo2(
        peer2, onlineState, diskState, ChunkServerStatus::READWRITE,
        3, 100, 10, statInfo);
    ChunkServerInfo csInfo3(
        peer3, onlineState, diskState, ChunkServerStatus::PENDDING,
        2, 100, 10, statInfo);
    ChunkServerInfo csInfo4(
        peer4, onlineState, diskState, ChunkServerStatus::READWRITE,
        1, 100, 10, statInfo);
    std::vector<ChunkServerInfo> csInfos({csInfo1, csInfo2, csInfo3, csInfo4});

    PoolIdType poolId = 1;
    CopySetIdType copysetId = 1;
    CopySetKey copySetKey;
    copySetKey.first = poolId;
    copySetKey.second = copysetId;
    EpochType epoch = 1;
    ChunkServerIdType leader = 2;
    CopySetInfo copySet1(copySetKey, epoch, leader,
        std::vector<PeerInfo>({peer1, peer2, peer3}),
        ConfigChangeInfo{}, CopysetStatistics{});
    copySetKey.second = 2;
    leader = 3;
    CopySetInfo copySet2(copySetKey, epoch, leader,
        std::vector<PeerInfo>({peer1, peer2, peer3}),
        ConfigChangeInfo{}, CopysetStatistics{});
    copySetKey.second = 3;
    leader = 4;
    CopySetInfo copySet3(copySetKey, epoch, leader,
        std::vector<PeerInfo>({peer2, peer3, peer4}),
        ConfigChangeInfo{}, CopysetStatistics{});

    copySetKey.second = 1;
    Operator testOperator(1, copySetKey, OperatorPriority::NormalPriority,
                          steady_clock::now(), std::make_shared<AddPeer>(1));
    ASSERT_TRUE(opController_->AddOperator(testOperator));

    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Return(std::vector<PoolIdType>({1})));
    EXPECT_CALL(*topoAdapter_, GetChunkServersInLogicalPool(1))
        .WillOnce(Return(csInfos));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(1))
        .Times(2)
        .WillOnce(Return(std::vector<CopySetInfo>({copySet1})))
        .WillOnce(Return(std::vector<CopySetInfo>({copySet3, copySet2})));
     EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(1, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo1), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(3, _))
        .Times(3)
        .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo3), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(2, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo2), Return(true)));

    ASSERT_EQ(1, leaderScheduler_->Schedule());
    Operator op;
    ASSERT_TRUE(opController_->GetOperatorById(copySet2.id, &op));
    ASSERT_EQ(OperatorPriority::NormalPriority, op.priority);
    ASSERT_EQ(std::chrono::seconds(10), op.timeLimit);
    TransferLeader *res = dynamic_cast<TransferLeader *>(op.step.get());
    ASSERT_TRUE(res != nullptr);
    ASSERT_EQ(1, res->GetTargetPeer());
}

}  // namespace schedule
}  // namespace mds
}  // namespace curve




