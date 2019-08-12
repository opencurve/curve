/*
 * Project: curve
 * Created Date: Mon March 11 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
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
        leaderScheduler_ = std::make_shared<LeaderScheduler>(
            opController_, 1, 0, 10, 100, 1000, 0.2, topoAdapter_);
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
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfos())
        .WillOnce(Return(std::vector<ChunkServerInfo>()));
    ASSERT_EQ(0, leaderScheduler_->Schedule());
}

TEST_F(TestLeaderSchedule, test_has_chunkServer_offline) {
    PeerInfo peer1(1, 1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, 1, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, 1, "192.168.10.3", 9000);
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

    EXPECT_CALL(*topoAdapter_, GetChunkServerInfos())
        .WillOnce(Return(csInfos));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillRepeatedly(Return(copySetInfos));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(1, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo1), Return(true)));

    ASSERT_EQ(0, leaderScheduler_->Schedule());
}

TEST_F(TestLeaderSchedule, test_copySet_has_candidate) {
    PeerInfo peer1(1, 1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, 1, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, 1, "192.168.10.3", 9000);
    auto onlineState = ::curve::mds::topology::OnlineState::ONLINE;
    auto offlineState = ::curve::mds::topology::OnlineState::OFFLINE;
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
    copySet1.candidatePeerInfo = PeerInfo(1, 1, 1, 1, "192.168.10.1", 9000);
    std::vector<CopySetInfo> copySetInfos({copySet1});

    EXPECT_CALL(*topoAdapter_, GetChunkServerInfos())
        .WillOnce(Return(csInfos));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillRepeatedly(Return(copySetInfos));

    ASSERT_EQ(0, leaderScheduler_->Schedule());
}

TEST_F(TestLeaderSchedule, test_cannot_get_chunkServerInfo) {
    PeerInfo peer1(1, 1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, 1, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, 1, "192.168.10.3", 9000);
    auto onlineState = ::curve::mds::topology::OnlineState::ONLINE;
    auto offlineState = ::curve::mds::topology::OnlineState::OFFLINE;
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

    EXPECT_CALL(*topoAdapter_, GetChunkServerInfos())
        .WillOnce(Return(csInfos));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillRepeatedly(Return(copySetInfos));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(1, _))
        .WillRepeatedly(Return(false));

    ASSERT_EQ(0, leaderScheduler_->Schedule());
}

TEST_F(TestLeaderSchedule, test_no_need_tranferLeaderOut) {
    PeerInfo peer1(1, 1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, 1, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, 1, "192.168.10.3", 9000);
    auto onlineState = ::curve::mds::topology::OnlineState::ONLINE;
    auto offlineState = ::curve::mds::topology::OnlineState::OFFLINE;
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

    EXPECT_CALL(*topoAdapter_, GetChunkServerInfos())
        .WillOnce(Return(csInfos));
    ASSERT_EQ(0, leaderScheduler_->Schedule());
}

TEST_F(TestLeaderSchedule, test_tranferLeaderout_normal) {
    //              chunkserver1    chunkserver2     chunkserver3
    // leaderCount       1                2                0
    // copyset           1                1                1
    PeerInfo peer1(1, 1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, 1, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, 1, "192.168.10.3", 9000);
    auto onlineState = ::curve::mds::topology::OnlineState::ONLINE;
    auto offlineState = ::curve::mds::topology::OnlineState::OFFLINE;
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
    struct timeval tm;
    gettimeofday(&tm, NULL);
    csInfo3.startUpTime = tm.tv_sec - 2;
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

    EXPECT_CALL(*topoAdapter_, GetChunkServerInfos())
        .WillOnce(Return(csInfos));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(copySetInfos));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo1), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo2), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(3, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo3), Return(true)));

    ASSERT_EQ(1, leaderScheduler_->Schedule());
    Operator op;
    ASSERT_TRUE(opController_->GetOperatorById(copySet1.id, &op));
    ASSERT_EQ(OperatorPriority::NormalPriority, op.priority);
    ASSERT_EQ(std::chrono::seconds(10), op.timeLimit);
    TransferLeader *res = dynamic_cast<TransferLeader *>(op.step.get());
    ASSERT_TRUE(res != nullptr);
    ASSERT_EQ(csInfo3.info.id, res->GetTargetPeer());
}

TEST_F(TestLeaderSchedule, test_transferLeaderIn_normal) {
    //              chunkserver1    chunkserver2    chunkserver3    chunkserver4
    // leaderCount        0              3                 2               1
    // copyset            1              1                 1(有operator)
    //                    2              2                 2
    //                                   3                 3               3
    PeerInfo peer1(1, 1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, 1, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, 1, "192.168.10.3", 9000);
    PeerInfo peer4(3, 4, 4, 1, "192.168.10.4", 9000);
    auto onlineState = ::curve::mds::topology::OnlineState::ONLINE;
    auto offlineState = ::curve::mds::topology::OnlineState::OFFLINE;
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

    EXPECT_CALL(*topoAdapter_, GetChunkServerInfos())
        .WillOnce(Return(csInfos));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
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




