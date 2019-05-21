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
          opController_, 1, 10, 100, 1000);
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
    ASSERT_EQ(0, replicaScheduler_->Schedule(topoAdapter_));
}

TEST_F(TestReplicaSchedule, test_copySet_has_configChangeInfo) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    testCopySetInfo.candidatePeerInfo = PeerInfo(1, 1, 1, "", 9000);
    auto replica = new ::curve::common::Peer();
    replica->set_id(1);
    replica->set_address("192.10.10.1:9000");
    testCopySetInfo.configChangeInfo.set_allocated_peer(replica);
    testCopySetInfo.configChangeInfo.set_finished(false);
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({testCopySetInfo})));
    ASSERT_EQ(0, replicaScheduler_->Schedule(topoAdapter_));
}

TEST_F(TestReplicaSchedule, test_copySet_has_standard_replica) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInLogicalPool(_))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({testCopySetInfo})));

    ASSERT_EQ(0, replicaScheduler_->Schedule(topoAdapter_));
}

TEST_F(TestReplicaSchedule, test_copySet_has_smaller_replicaNum_selectNone) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    testCopySetInfo.peers = std::vector<PeerInfo>({peer1, peer2});
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInLogicalPool(_))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({testCopySetInfo})));
    EXPECT_CALL(*topoAdapter_, SelectBestPlacementChunkServer(_, _))
        .WillOnce(Return(::curve::mds::topology::UNINTIALIZE_ID));

    ASSERT_EQ(0, replicaScheduler_->Schedule(topoAdapter_));
}

TEST_F(TestReplicaSchedule, test_copySet_has_smaller_replicaNum_selectCorrect) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    testCopySetInfo.peers = std::vector<PeerInfo>({peer1, peer2});
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInLogicalPool(_))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({testCopySetInfo})));
    EXPECT_CALL(*topoAdapter_, SelectBestPlacementChunkServer(_, _))
        .WillOnce(Return(3));

    ASSERT_EQ(1, replicaScheduler_->Schedule(topoAdapter_));
    Operator op;
    ASSERT_TRUE(opController_->GetOperatorById(testCopySetInfo.id, &op));
    ASSERT_EQ(testCopySetInfo.id, op.copsetID);
    ASSERT_EQ(testCopySetInfo.epoch, op.startEpoch);
    ASSERT_EQ(OperatorPriority::HighPriority, op.priority);
    ASSERT_EQ(std::chrono::seconds(1000), op.timeLimit);
    AddPeer *res = dynamic_cast<AddPeer *>(op.step.get());
    ASSERT_EQ(3, res->GetTargetPeer());
}

TEST_F(TestReplicaSchedule, test_copySet_has_larger_replicaNum_selectNone) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    PeerInfo peer4(4, 4, 4, "192.168.10.4", 9000);
    testCopySetInfo.peers = std::vector<PeerInfo>({peer1, peer2, peer3, peer4});
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInLogicalPool(_))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({testCopySetInfo})));
    EXPECT_CALL(*topoAdapter_, SelectRedundantReplicaToRemove(_))
        .WillOnce(Return(::curve::mds::topology::UNINTIALIZE_ID));

    ASSERT_EQ(0, replicaScheduler_->Schedule(topoAdapter_));
}

TEST_F(TestReplicaSchedule, test_copySet_has_larger_replicaNum_selectCorrect) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    PeerInfo peer4(4, 4, 4, "192.168.10.4", 9000);
    testCopySetInfo.peers = std::vector<PeerInfo>({peer1, peer2, peer3, peer4});
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInLogicalPool(_))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({testCopySetInfo})));
    EXPECT_CALL(*topoAdapter_, SelectRedundantReplicaToRemove(_))
        .WillOnce(Return(4));

    ASSERT_EQ(1, replicaScheduler_->Schedule(topoAdapter_));
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
