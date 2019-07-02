/*
 * Project: curve
 * Created Date: Tue Feb 19 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include "src/mds/schedule/scheduler.h"
#include "test/mds/schedule/mock_topoAdapter.h"
#include "test/mds/schedule/common.h"

using ::testing::_;
using ::testing::Return;
using ::testing::AtLeast;
using ::testing::SetArgPointee;
using ::testing::DoAll;

namespace curve {
namespace mds {
namespace schedule {
class TestReplicaSchedule : public ::testing::Test {
 protected:
  TestReplicaSchedule() {}
  ~TestReplicaSchedule() {}

  void SetUp() override {
      opController_ = std::make_shared<OperatorController>(2);
      topoAdapter_ = std::make_shared<MockTopoAdapter>();
      replicaScheduler_ = std::make_shared<ReplicaScheduler>(
          opController_, 1, 10, 100, 1000, 0.2, 90, topoAdapter_);
  }

  void TearDown() override {
      topoAdapter_ = nullptr;
      opController_ = nullptr;
      replicaScheduler_ = nullptr;
  }

 protected:
  std::shared_ptr<MockTopoAdapter> topoAdapter_;
  std::shared_ptr<OperatorController> opController_;
  std::shared_ptr<ReplicaScheduler> replicaScheduler_;
};

TEST_F(TestReplicaSchedule, test_copySet_already_has_operator) {
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({GetCopySetInfoForTest()})));
    CopySetKey copySetKey;
    copySetKey.first = 1;
    copySetKey.second = 1;

    Operator testOperator(1, copySetKey, OperatorPriority::HighPriority,
        steady_clock::now(), std::make_shared<AddPeer>(1));
    ASSERT_TRUE(opController_->AddOperator(testOperator));
    ASSERT_EQ(0, replicaScheduler_->Schedule());
}

TEST_F(TestReplicaSchedule, test_copySet_has_configChangeInfo) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    testCopySetInfo.candidatePeerInfo = PeerInfo(1, 1, 1, 1, "", 9000);
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({testCopySetInfo})));
    ASSERT_EQ(0, replicaScheduler_->Schedule());
}

TEST_F(TestReplicaSchedule, test_copySet_has_standard_replica) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInLogicalPool(_))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({testCopySetInfo})));

    ASSERT_EQ(0, replicaScheduler_->Schedule());
}

TEST_F(TestReplicaSchedule, test_copySet_has_smaller_replicaNum_selectNone) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    PeerInfo peer1(1, 1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, 1, "192.168.10.2", 9000);
    testCopySetInfo.peers = std::vector<PeerInfo>({peer1, peer2});
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInLogicalPool(_))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({testCopySetInfo})));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(_, _)).WillOnce(Return(true));
    EXPECT_CALL(*topoAdapter_, GetChunkServersInPhysicalPool(_))
            .WillOnce(Return(std::vector<ChunkServerInfo>{}));
    ASSERT_EQ(0, replicaScheduler_->Schedule());
}

TEST_F(TestReplicaSchedule, test_copySet_has_smaller_replicaNum_conExceed) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    ChunkServerInfo csInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                        DiskState::DISKNORMAL, ChunkServerStatus::READWRITE,
                        2, 100, 100, ChunkServerStatisticInfo{});
    ChunkServerInfo csInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                        DiskState::DISKNORMAL, ChunkServerStatus::READWRITE,
                        2, 100, 100, ChunkServerStatisticInfo{});
    ChunkServerInfo csInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                        DiskState::DISKNORMAL, ChunkServerStatus::READWRITE,
                        2, 100, 100, ChunkServerStatisticInfo{});
    PeerInfo peer1(1, 1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, 1, "192.168.10.2", 9000);
    testCopySetInfo.peers = std::vector<PeerInfo>({peer1, peer2});
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInLogicalPool(_))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .Times(2)
        .WillRepeatedly(Return(std::vector<CopySetInfo>({testCopySetInfo})));

    std::vector<ChunkServerInfo> chunkserverList(
        {csInfo1, csInfo2, csInfo3});
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(_, _)).WillOnce(Return(true));
    EXPECT_CALL(*topoAdapter_, GetChunkServersInPhysicalPool(_))
        .WillOnce(Return(chunkserverList));
    EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInLogicalPool(_))
        .WillOnce(Return(3));

    Operator testOperator1(1, CopySetKey{1, 3}, OperatorPriority::HighPriority,
        steady_clock::now(), std::make_shared<AddPeer>(3));
    Operator testOperator2(2, CopySetKey{1, 4}, OperatorPriority::HighPriority,
        steady_clock::now(), std::make_shared<AddPeer>(3));
    ASSERT_TRUE(opController_->AddOperator(testOperator1));
    ASSERT_TRUE(opController_->AddOperator(testOperator2));
    ASSERT_EQ(0, replicaScheduler_->Schedule());
}

TEST_F(TestReplicaSchedule, test_copySet_has_smaller_replicaNum_selectCorrect) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    ChunkServerInfo csInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                        DiskState::DISKNORMAL, ChunkServerStatus::READWRITE,
                        2, 100, 100, ChunkServerStatisticInfo{});
    ChunkServerInfo csInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                        DiskState::DISKNORMAL, ChunkServerStatus::READWRITE,
                        2, 100, 100, ChunkServerStatisticInfo{});
    ChunkServerInfo csInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                        DiskState::DISKNORMAL, ChunkServerStatus::READWRITE,
                        2, 100, 100, ChunkServerStatisticInfo{});
    PeerInfo peer1(1, 1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, 1, "192.168.10.2", 9000);
    testCopySetInfo.peers = std::vector<PeerInfo>({peer1, peer2});
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInLogicalPool(_))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .Times(2)
        .WillRepeatedly(Return(std::vector<CopySetInfo>({testCopySetInfo})));

    std::vector<ChunkServerInfo> chunkserverList(
        {csInfo1, csInfo2, csInfo3});
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(_, _)).WillOnce(Return(true));
    EXPECT_CALL(*topoAdapter_, GetChunkServersInPhysicalPool(_))
        .WillOnce(Return(chunkserverList));
    EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInLogicalPool(_))
        .WillOnce(Return(3));
    std::map<ChunkServerIdType, int> map1{{2, 1}};
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(1, _))
        .WillOnce(SetArgPointee<1>(map1));
    std::map<ChunkServerIdType, int> map2{{1, 1}};
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(2, _))
        .WillOnce(SetArgPointee<1>(map2));
    std::map<ChunkServerIdType, int> map3;
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(3, _))
        .WillOnce(SetArgPointee<1>(map3));
    EXPECT_CALL(*topoAdapter_, CreateCopySetAtChunkServer(_, _))
        .WillOnce(Return(true));
    ASSERT_EQ(1, replicaScheduler_->Schedule());
    Operator op;
    ASSERT_TRUE(opController_->GetOperatorById(testCopySetInfo.id, &op));
    ASSERT_EQ(testCopySetInfo.id, op.copsetID);
    ASSERT_EQ(testCopySetInfo.epoch, op.startEpoch);
    ASSERT_EQ(OperatorPriority::HighPriority, op.priority);
    ASSERT_EQ(std::chrono::seconds(1000), op.timeLimit);
    AddPeer *res = dynamic_cast<AddPeer *>(op.step.get());
    ASSERT_EQ(3, res->GetTargetPeer());
}

TEST_F(TestReplicaSchedule, test_copySet_has_smaller_replicaNum_createErr) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    ChunkServerInfo csInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                        DiskState::DISKNORMAL, ChunkServerStatus::READWRITE,
                        2, 100, 100, ChunkServerStatisticInfo{});
    ChunkServerInfo csInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                        DiskState::DISKNORMAL, ChunkServerStatus::READWRITE,
                        2, 100, 100, ChunkServerStatisticInfo{});
    ChunkServerInfo csInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                        DiskState::DISKNORMAL, ChunkServerStatus::READWRITE,
                        2, 100, 100, ChunkServerStatisticInfo{});
    PeerInfo peer1(1, 1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, 1, "192.168.10.2", 9000);
    testCopySetInfo.peers = std::vector<PeerInfo>({peer1, peer2});
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInLogicalPool(_))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .Times(2)
        .WillRepeatedly(Return(std::vector<CopySetInfo>({testCopySetInfo})));

    std::vector<ChunkServerInfo> chunkserverList(
        {csInfo1, csInfo2, csInfo3});
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(_, _)).WillOnce(Return(true));
    EXPECT_CALL(*topoAdapter_, GetChunkServersInPhysicalPool(_))
        .WillOnce(Return(chunkserverList));
    EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInLogicalPool(_))
        .WillOnce(Return(3));
    std::map<ChunkServerIdType, int> map1{{2, 1}};
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(1, _))
        .WillOnce(SetArgPointee<1>(map1));
    std::map<ChunkServerIdType, int> map2{{1, 1}};
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(2, _))
        .WillOnce(SetArgPointee<1>(map2));
    std::map<ChunkServerIdType, int> map3;
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(3, _))
        .WillOnce(SetArgPointee<1>(map3));
    EXPECT_CALL(*topoAdapter_, CreateCopySetAtChunkServer(_, _))
        .WillOnce(Return(false));
    ASSERT_EQ(0, replicaScheduler_->Schedule());
}

TEST_F(TestReplicaSchedule, test_copySet_has_larger_replicaNum_selectNone) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    PeerInfo peer1(1, 1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, 1, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, 1, "192.168.10.3", 9000);
    PeerInfo peer4(4, 4, 4, 1, "192.168.10.4", 9000);
    testCopySetInfo.peers = std::vector<PeerInfo>({peer1, peer2, peer3, peer4});
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInLogicalPool(_))
        .WillOnce(Return(3)).WillOnce(Return(0));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({testCopySetInfo})));

    ASSERT_EQ(0, replicaScheduler_->Schedule());
}

TEST_F(TestReplicaSchedule, test_copySet_has_larger_replicaNum_selectCorrect) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    PeerInfo peer1(1, 1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, 1, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, 1, "192.168.10.3", 9000);
    PeerInfo peer4(4, 4, 4, 1, "192.168.10.4", 9000);
    testCopySetInfo.peers = std::vector<PeerInfo>({peer1, peer2, peer3, peer4});
    ChunkServerInfo csInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                            DiskState::DISKNORMAL, ChunkServerStatus::READWRITE,
                            2, 100, 100, ChunkServerStatisticInfo{});
    ChunkServerInfo csInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                            DiskState::DISKNORMAL, ChunkServerStatus::READWRITE,
                            2, 100, 100, ChunkServerStatisticInfo{});
    ChunkServerInfo csInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                            DiskState::DISKNORMAL, ChunkServerStatus::READWRITE,
                            2, 100, 100, ChunkServerStatisticInfo{});
    ChunkServerInfo csInfo4(testCopySetInfo.peers[3], OnlineState::ONLINE,
                            DiskState::DISKNORMAL, ChunkServerStatus::READWRITE,
                            2, 100, 100, ChunkServerStatisticInfo{});
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInLogicalPool(_))
        .Times(2).WillRepeatedly(Return(3));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({testCopySetInfo})));
    EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInLogicalPool(_))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInChunkServer(_))
        .Times(4)
        .WillRepeatedly(Return(std::vector<CopySetInfo>{testCopySetInfo}));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo1), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo2), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(3, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo3), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(4, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo4), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(1, _))
        .WillRepeatedly(SetArgPointee<1>(
            std::map<ChunkServerIdType, int>{{2, 1}, {3, 1}, {4, 1}}));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(2, _))
        .WillRepeatedly(SetArgPointee<1>(
            std::map<ChunkServerIdType, int>{{1, 1}, {3, 1}, {4, 1}}));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(3, _))
        .WillRepeatedly(SetArgPointee<1>(
            std::map<ChunkServerIdType, int>{{1, 1}, {2, 1}, {4, 1}}));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(4, _))
        .WillRepeatedly(SetArgPointee<1>(
            std::map<ChunkServerIdType, int>{{2, 1}, {3, 1}, {1, 1}}));

    ASSERT_EQ(1, replicaScheduler_->Schedule());
    Operator op;
    ASSERT_TRUE(opController_->GetOperatorById(testCopySetInfo.id, &op));
    ASSERT_EQ(testCopySetInfo.id, op.copsetID);
    ASSERT_EQ(testCopySetInfo.epoch, op.startEpoch);
    ASSERT_EQ(OperatorPriority::HighPriority, op.priority);
    ASSERT_EQ(std::chrono::seconds(100), op.timeLimit);
    RemovePeer *res = dynamic_cast<RemovePeer *>(op.step.get());
    ASSERT_FALSE(res == nullptr);
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
