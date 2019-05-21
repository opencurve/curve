/*
 * Project: curve
 * Created Date: Tue Dec 25 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/operatorController.h"
#include "src/mds/topology/topology_id_generator.h"
#include "src/mds/common/mds_define.h"
#include "test/mds/schedule/mock_topoAdapter.h"
#include "test/mds/schedule/common.h"

using ::testing::_;
using ::testing::Return;
using ::testing::AtLeast;
using ::testing::SetArgPointee;
using ::testing::DoAll;

using ::curve::mds::topology::TopologyIdGenerator;

namespace curve {
namespace mds {
namespace schedule {
class TestRecoverSheduler : public ::testing::Test {
 protected:
  TestRecoverSheduler() {}
  ~TestRecoverSheduler() {}

  void SetUp() override {
      opController_ = std::make_shared<OperatorController>(2);
      topoAdapter_ = std::make_shared<MockTopoAdapter>();
      int64_t runInterval = 1;
      recoverScheduler_ =
          std::make_shared<RecoverScheduler>(opController_,
                                             runInterval,
                                             10,
                                             100,
                                             1000);
  }
  void TearDown() override {
      opController_ = nullptr;
      topoAdapter_ = nullptr;
      recoverScheduler_ = nullptr;
  }

 protected:
  std::shared_ptr<MockTopoAdapter> topoAdapter_;
  std::shared_ptr<OperatorController> opController_;
  std::shared_ptr<RecoverScheduler> recoverScheduler_;
};

TEST_F(TestRecoverSheduler, test_copySet_already_has_operator) {
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({GetCopySetInfoForTest()})));
    CopySetKey copySetKey;
    copySetKey.
        first = 1;
    copySetKey.
        second = 1;
    Operator testOperator(1, copySetKey, OperatorPriority::NormalPriority,
                          steady_clock::now(), std::make_shared<AddPeer>(1));
    ASSERT_TRUE(opController_->AddOperator(testOperator));
    ASSERT_EQ(0, recoverScheduler_->Schedule(topoAdapter_));
}

TEST_F(TestRecoverSheduler, test_copySet_has_configChangeInfo) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    testCopySetInfo.candidatePeerInfo = PeerInfo(1, 1, 1, "", 9000);
    auto replica = new ::curve::common::Peer();
    replica->set_id(1);
    replica->set_address("192.10.10.1:9000:0");
    testCopySetInfo.configChangeInfo.set_allocated_peer(replica);
    testCopySetInfo.configChangeInfo.set_finished(false);
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({testCopySetInfo})));
    ASSERT_EQ(0, recoverScheduler_->Schedule(topoAdapter_));
}

TEST_F(TestRecoverSheduler, test_chunkServer_cannot_get) {
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos()).
        WillOnce(Return(std::vector<CopySetInfo>({GetCopySetInfoForTest()})));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(_, _))
        .Times(3)
        .WillRepeatedly(Return(false));
    ASSERT_EQ(0, recoverScheduler_->Schedule(topoAdapter_));
}

TEST_F(TestRecoverSheduler, test_all_chunkServer_online_offline) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillRepeatedly(Return(std::vector<CopySetInfo>({testCopySetInfo})));

    ChunkServerInfo csInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                            2, 100, 100, 1234, ChunkServerStatisticInfo{});
    ChunkServerInfo csInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                            2, 100, 100, 1234, ChunkServerStatisticInfo{});
    ChunkServerInfo csInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                            2, 100, 100, 1234, ChunkServerStatisticInfo{});
    ChunkServerIdType id1 = 1;
    ChunkServerIdType id2 = 2;
    ChunkServerIdType id3 = 3;

    // 1. test all chunkServers online
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(id1, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo1),
                        Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(id2, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo2),
                              Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(id3, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo3),
                              Return(true)));
    ASSERT_EQ(0, recoverScheduler_->Schedule(topoAdapter_));

    // 2. replica num larger then standard, leader offline
    csInfo1.state = OnlineState::OFFLINE;
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(id1, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo1),
                        Return(true)));
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInLogicalPool(_))
        .Times(2).WillRepeatedly(Return(2));
    ASSERT_EQ(1, recoverScheduler_->Schedule(topoAdapter_));
    Operator op;
    ASSERT_TRUE(opController_->GetOperatorById(testCopySetInfo.id, &op));
    ASSERT_TRUE(
        dynamic_cast<RemovePeer *>(op.step.get()) != nullptr);
    ASSERT_EQ(std::chrono::seconds(100), op.timeLimit);

    // 3. relpica num larger then standard, not leader offline
    opController_->RemoveOperator(op.copsetID);
    csInfo1.state = OnlineState::ONLINE;
    csInfo2.state = OnlineState::OFFLINE;
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(id1, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo1),
                              Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(id2, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo2),
                              Return(true)));
    ASSERT_EQ(1, recoverScheduler_->Schedule(topoAdapter_));
    ASSERT_TRUE(opController_->GetOperatorById(testCopySetInfo.id, &op));
    ASSERT_TRUE(dynamic_cast<RemovePeer *>(op.step.get()) != nullptr);
    ASSERT_EQ(std::chrono::seconds(100), op.timeLimit);

    // 4. test chunkServer offline and replica num equal standard
    opController_->RemoveOperator(op.copsetID);
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInLogicalPool(_))
        .WillRepeatedly(Return(3));
    EXPECT_CALL(*topoAdapter_, SelectBestPlacementChunkServer(_, _))
        .WillOnce(Return(5));
    EXPECT_CALL(*topoAdapter_, CreateCopySetAtChunkServer(_, _))
        .WillOnce(Return(true));
    ASSERT_EQ(1, recoverScheduler_->Schedule(topoAdapter_));
    ASSERT_TRUE(opController_->GetOperatorById(testCopySetInfo.id, &op));
    ASSERT_TRUE(
        dynamic_cast<AddPeer *>(op.step.get()) != nullptr);
    ASSERT_EQ(std::chrono::seconds(1000), op.timeLimit);

    // 5. test select placement chunkServer fail
    opController_->RemoveOperator(op.copsetID);
    EXPECT_CALL(*topoAdapter_, SelectBestPlacementChunkServer(_, _))
        .WillOnce(Return(::curve::mds::topology::UNINTIALIZE_ID));
    ASSERT_EQ(0, recoverScheduler_->Schedule(topoAdapter_));

    // 6. test create copySet at chunkServer fail
    EXPECT_CALL(*topoAdapter_, SelectBestPlacementChunkServer(_, _))
        .WillOnce(Return(5));
    EXPECT_CALL(*topoAdapter_, CreateCopySetAtChunkServer(_, _))
        .WillOnce(Return(false));
    ASSERT_EQ(0, recoverScheduler_->Schedule(topoAdapter_));
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve


