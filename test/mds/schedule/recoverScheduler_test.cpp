/*
 * Project: curve
 * Created Date: Tue Dec 25 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
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
          std::make_shared<RecoverScheduler>(opController_, runInterval,
            10, 100, 1000, 0.2, 90, 3, topoAdapter_);
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
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfos())
        .WillOnce(Return(std::vector<ChunkServerInfo>{}));
    CopySetKey copySetKey;
    copySetKey.
        first = 1;
    copySetKey.
        second = 1;
    Operator testOperator(1, copySetKey, OperatorPriority::NormalPriority,
                          steady_clock::now(), std::make_shared<AddPeer>(1));
    ASSERT_TRUE(opController_->AddOperator(testOperator));
    ASSERT_EQ(0, recoverScheduler_->Schedule());
}

TEST_F(TestRecoverSheduler, test_copySet_has_configChangeInfo) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    testCopySetInfo.candidatePeerInfo = PeerInfo(1, 1, 1, 1, "", 9000);
    auto replica = new ::curve::common::Peer();
    replica->set_id(1);
    replica->set_address("192.10.10.1:9000:0");
    testCopySetInfo.configChangeInfo.set_allocated_peer(replica);
    testCopySetInfo.configChangeInfo.set_finished(false);
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfos())
        .WillOnce(Return(std::vector<ChunkServerInfo>{}));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({testCopySetInfo})));
    ASSERT_EQ(0, recoverScheduler_->Schedule());
}

TEST_F(TestRecoverSheduler, test_chunkServer_cannot_get) {
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfos())
        .WillOnce(Return(std::vector<ChunkServerInfo>{}));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos()).
        WillOnce(Return(std::vector<CopySetInfo>({GetCopySetInfoForTest()})));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(_, _))
        .Times(3)
        .WillRepeatedly(Return(false));
    ASSERT_EQ(0, recoverScheduler_->Schedule());
}

TEST_F(TestRecoverSheduler, test_server_has_more_offline_chunkserver) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillRepeatedly(Return(std::vector<CopySetInfo>({testCopySetInfo})));
    ChunkServerInfo csInfo1(testCopySetInfo.peers[0], OnlineState::OFFLINE,
                            DiskState::DISKNORMAL,
                            2, 100, 100, ChunkServerStatisticInfo{});
    ChunkServerInfo csInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                            DiskState::DISKNORMAL,
                            2, 100, 100, ChunkServerStatisticInfo{});
    ChunkServerInfo csInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                            DiskState::DISKNORMAL,
                            2, 100, 100, ChunkServerStatisticInfo{});
    PeerInfo peer4(4, 1, 1, 1, "192.168.10.1", 9001);
    PeerInfo peer5(5, 1, 1, 1, "192.168.10.1", 9002);
    ChunkServerInfo csInfo4(peer4, OnlineState::OFFLINE, DiskState::DISKNORMAL,
            2, 100, 100, ChunkServerStatisticInfo{});
    ChunkServerInfo csInfo5(peer5, OnlineState::OFFLINE, DiskState::DISKNORMAL,
            2, 100, 100, ChunkServerStatisticInfo{});
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfos())
        .WillOnce(Return(std::vector<ChunkServerInfo>{
            csInfo1, csInfo2, csInfo3, csInfo4, csInfo5}));

    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(csInfo1.info.id , _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo1), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(csInfo2.info.id , _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo2), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(csInfo3.info.id , _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo2), Return(true)));
    ASSERT_EQ(0, recoverScheduler_->Schedule());
}

TEST_F(TestRecoverSheduler, test_all_chunkServer_online_offline) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillRepeatedly(Return(std::vector<CopySetInfo>({testCopySetInfo})));
    EXPECT_CALL(*topoAdapter_, GetChunkServerInfos())
        .WillRepeatedly(Return(std::vector<ChunkServerInfo>{}));
    ChunkServerInfo csInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                            DiskState::DISKNORMAL,
                            2, 100, 100, ChunkServerStatisticInfo{});
    ChunkServerInfo csInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                            DiskState::DISKNORMAL,
                            2, 100, 100, ChunkServerStatisticInfo{});
    ChunkServerInfo csInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                            DiskState::DISKNORMAL,
                            2, 100, 100, ChunkServerStatisticInfo{});
    PeerInfo peer4(4, 4, 4, 1, "192.168.10.4", 9000);
    ChunkServerInfo csInfo4(peer4, OnlineState::ONLINE,
                            DiskState::DISKNORMAL,
                            2, 100, 100, ChunkServerStatisticInfo{});
    ChunkServerIdType id1 = 1;
    ChunkServerIdType id2 = 2;
    ChunkServerIdType id3 = 3;
    ChunkServerIdType id4 = 4;
    Operator op;
    {
        // 1. 所有chunkserveronline
        EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(id1, _))
            .WillOnce(DoAll(SetArgPointee<1>(csInfo1),
                            Return(true)));
        EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(id2, _))
            .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo2),
                                Return(true)));
        EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(id3, _))
            .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo3),
                                Return(true)));
        ASSERT_EQ(0, recoverScheduler_->Schedule());
    }

    {
        // 2. 副本数量大于标准，leader挂掉
        csInfo1.state = OnlineState::OFFLINE;
        EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(id1, _))
            .WillOnce(DoAll(SetArgPointee<1>(csInfo1),
                            Return(true)));
        EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInLogicalPool(_))
            .Times(2).WillRepeatedly(Return(2));
        ASSERT_EQ(1, recoverScheduler_->Schedule());
        ASSERT_TRUE(opController_->GetOperatorById(testCopySetInfo.id, &op));
        ASSERT_TRUE(
            dynamic_cast<RemovePeer *>(op.step.get()) != nullptr);
        ASSERT_EQ(std::chrono::seconds(100), op.timeLimit);
    }

    {
        // 3. 副本数量大于标准，follower挂掉
        opController_->RemoveOperator(op.copsetID);
        csInfo1.state = OnlineState::ONLINE;
        csInfo2.state = OnlineState::OFFLINE;
        EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(id1, _))
            .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo1),
                                Return(true)));
        EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(id2, _))
            .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo2),
                                Return(true)));
        ASSERT_EQ(1, recoverScheduler_->Schedule());
        ASSERT_TRUE(opController_->GetOperatorById(testCopySetInfo.id, &op));
        ASSERT_TRUE(dynamic_cast<RemovePeer *>(op.step.get()) != nullptr);
        ASSERT_EQ(std::chrono::seconds(100), op.timeLimit);
    }

    {
        // 4. 副本数目等于标准， follower挂掉
        opController_->RemoveOperator(op.copsetID);
        EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInLogicalPool(_))
            .WillRepeatedly(Return(3));
        std::vector<ChunkServerInfo> chunkserverList(
            {csInfo1, csInfo2, csInfo3, csInfo4});
        EXPECT_CALL(*topoAdapter_, GetChunkServersInPhysicalPool(_))
            .WillOnce(Return(chunkserverList));
        EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInLogicalPool(_))
            .WillOnce(Return(3));
        std::map<ChunkServerIdType, int> map1{{3, 1}};
        EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(1, _))
            .WillOnce(SetArgPointee<1>(map1));
        EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(2, _))
            .WillOnce(SetArgPointee<1>(map1));
        std::map<ChunkServerIdType, int> map3{{1, 1}};
        EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(3, _))
            .WillOnce(SetArgPointee<1>(map3));
        std::map<ChunkServerIdType, int> map4;
        EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(4, _))
            .WillOnce(SetArgPointee<1>(map4));
        EXPECT_CALL(*topoAdapter_, CreateCopySetAtChunkServer(_, _))
            .WillOnce(Return(true));
        ASSERT_EQ(1, recoverScheduler_->Schedule());
        ASSERT_TRUE(opController_->GetOperatorById(testCopySetInfo.id, &op));
        ASSERT_TRUE(
            dynamic_cast<AddPeer *>(op.step.get()) != nullptr);
        ASSERT_EQ(std::chrono::seconds(1000), op.timeLimit);
    }

    {
        // 5. 选不出替换chunkserver
        opController_->RemoveOperator(op.copsetID);
        EXPECT_CALL(*topoAdapter_, GetChunkServersInPhysicalPool(_))
            .WillOnce(Return(std::vector<ChunkServerInfo>{}));
        ASSERT_EQ(0, recoverScheduler_->Schedule());
    }

    {
        // 6. 在chunkserver上创建copyset失败
        EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInLogicalPool(_))
            .WillRepeatedly(Return(3));
        std::vector<ChunkServerInfo> chunkserverList(
            {csInfo1, csInfo2, csInfo3, csInfo4});
        EXPECT_CALL(*topoAdapter_, GetChunkServersInPhysicalPool(_))
            .WillOnce(Return(chunkserverList));
        EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInLogicalPool(_))
            .WillOnce(Return(3));
        std::map<ChunkServerIdType, int> map1{{3, 1}};
        EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(1, _))
            .WillOnce(SetArgPointee<1>(map1));
        EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(2, _))
            .WillOnce(SetArgPointee<1>(map1));
        std::map<ChunkServerIdType, int> map3{{1, 1}};
        EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(3, _))
            .WillOnce(SetArgPointee<1>(map3));
        std::map<ChunkServerIdType, int> map4;
        EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(4, _))
            .WillOnce(SetArgPointee<1>(map4));
        EXPECT_CALL(*topoAdapter_, CreateCopySetAtChunkServer(_, _))
            .WillOnce(Return(false));
        ASSERT_EQ(0, recoverScheduler_->Schedule());
    }
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve


