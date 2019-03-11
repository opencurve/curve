/*
 * Project: curve
 * Created Date: Wed Dec 26 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include "test/mds/schedule/mock_topology.h"
#include "test/mds/schedule/mock_topology_service_manager.h"
#include "test/mds/schedule/common.h"

using ::curve::mds::topology::ChunkServerStatus;

using ::testing::_;
using ::testing::Return;
using ::testing::AtLeast;
using ::testing::SetArgPointee;
using ::testing::DoAll;
using ::testing::InSequence;

namespace curve {
namespace mds {
namespace schedule {
class TestTopoAdapterImpl : public ::testing::Test {
 protected:
  TestTopoAdapterImpl() {}
  ~TestTopoAdapterImpl() {}

  void SetUp() override {
      mockTopo_ = std::make_shared<MockTopology>();
      mockTopoManager_ = std::make_shared<MockTopologyServiceManager>(
          mockTopo_,
          std::make_shared<::curve::mds::copyset::CopysetManager>());
      topoAdapter_ = std::make_shared<TopoAdapterImpl>(mockTopo_,
                                                       mockTopoManager_);
  }
  void TearDown() override {
      mockTopo_ = nullptr;
      mockTopoManager_ = nullptr;
      topoAdapter_ = nullptr;
  }

 protected:
  std::shared_ptr<MockTopology> mockTopo_;
  std::shared_ptr<MockTopologyServiceManager> mockTopoManager_;
  std::shared_ptr<TopoAdapterImpl> topoAdapter_;
};

TEST_F(TestTopoAdapterImpl, no_enough_chunkServers_or_invalid_zone_num) {
    auto copySetInfo = GetCopySetInfoForTest();
    EXPECT_CALL(*mockTopo_, GetChunkServerInLogicalPool(_))
        .WillOnce(Return(std::list<ChunkServerIdType>()))
        .WillOnce(Return(std::list<ChunkServerIdType>({1, 2, 3, 4})));
    EXPECT_CALL(*mockTopo_, GetLogicalPool(_, _))
        .WillOnce(Return(false));
    ASSERT_EQ(::curve::mds::topology::UNINTIALIZE_ID,
              topoAdapter_->SelectBestPlacementChunkServer(copySetInfo, 1));
    ASSERT_EQ(::curve::mds::topology::UNINTIALIZE_ID,
              topoAdapter_->SelectBestPlacementChunkServer(copySetInfo, 1));
}

TEST_F(TestTopoAdapterImpl, test_chunkServer_or_server_not_satisfy) {
    auto copySetInfo = GetCopySetInfoForTest();
    EXPECT_CALL(*mockTopo_, GetChunkServerInLogicalPool(_))
        .WillOnce(Return(std::list<ChunkServerIdType>({1, 2, 3, 4, 5, 6})));

    // GetChunkServer
    EXPECT_CALL(*mockTopo_, GetLogicalPool(_, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(GetLogicalPoolForTest()),
                              Return(true)));
    EXPECT_CALL(*mockTopo_, GetChunkServer(_, _))
        .Times(3).WillRepeatedly(Return(false));
    EXPECT_CALL(*mockTopo_, GetChunkServer(4, _)).WillOnce(Return(false));
    EXPECT_CALL(*mockTopo_, GetChunkServer(5, _)).WillOnce(Return(true));
    EXPECT_CALL(*mockTopo_, GetChunkServer(6, _)).WillOnce(Return(true));

    // GetServer
    // called by 4,5
    EXPECT_CALL(*mockTopo_, GetServer(_, _)).
        WillOnce(Return(false)).WillOnce(Return(true));

    ASSERT_EQ(::curve::mds::topology::UNINTIALIZE_ID,
              topoAdapter_->SelectBestPlacementChunkServer(copySetInfo, 1));
}

TEST_F(TestTopoAdapterImpl, test_same_zone_or_same_server) {
    auto copySetInfo = GetCopySetInfoForTest();
    EXPECT_CALL(*mockTopo_, GetChunkServerInLogicalPool(_))
        .WillOnce(Return(std::list<ChunkServerIdType>({1, 2, 3, 4, 5})));
    EXPECT_CALL(*mockTopo_, GetLogicalPool(_, _)).
        WillOnce(DoAll(SetArgPointee<1>(GetLogicalPoolForTest()),
                       Return(true)));

    // GetChunkSever
    EXPECT_CALL(*mockTopo_, GetChunkServer(_, _))
        .Times(3).WillRepeatedly(Return(false));
    EXPECT_CALL(*mockTopo_, GetChunkServer(4, _)).WillOnce(Return(true));
    EXPECT_CALL(*mockTopo_, GetChunkServer(5, _)).WillOnce(Return(true));

    // GetServer
    Server forServer4(4, "", "", 0, "", 0, 2, 1, "");
    Server forServer5(2, "", "", 0, "", 0, 4, 1, "");
    EXPECT_CALL(*mockTopo_, GetServer(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(forServer4), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(forServer5), Return(true)));


    // zone or server not satisfy
    ASSERT_EQ(::curve::mds::topology::UNINTIALIZE_ID,
              topoAdapter_->SelectBestPlacementChunkServer(copySetInfo, 1));
}

TEST_F(TestTopoAdapterImpl, test_dissatisfy_healthy) {
    auto copySetInfo = GetCopySetInfoForTest();
    EXPECT_CALL(*mockTopo_, GetChunkServerInLogicalPool(_))
        .WillOnce(Return(std::list<ChunkServerIdType>({1, 2, 3, 4, 5})));
    EXPECT_CALL(*mockTopo_, GetLogicalPool(_, _)).
        WillOnce(DoAll(SetArgPointee<1>(GetLogicalPoolForTest()),
                       Return(true)));

    // GetChunkSever
    EXPECT_CALL(*mockTopo_, GetChunkServer(_, _))
        .Times(3).WillRepeatedly(Return(false));
    // chunkServer4-offline
    ChunkServer chunkServer4(4, "", "", 4, "", 9000, "",
                             ChunkServerStatus::READWRITE);
    ChunkServerState state4;
    state4.SetOnlineState(OnlineState::OFFLINE);
    chunkServer4.SetChunkServerState(state4);
    EXPECT_CALL(*mockTopo_, GetChunkServer(4, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer4), Return(true)));

    // chunkServer5-diskError
    ChunkServer chunkServer5(5, "", "", 4, "", 9000, "",
                             ChunkServerStatus::READWRITE);
    ChunkServerState state5;
    state5.SetOnlineState(OnlineState::ONLINE);
    state5.SetDiskState(DiskState::DISKERROR);
    chunkServer5.SetChunkServerState(state5);
    EXPECT_CALL(*mockTopo_, GetChunkServer(5, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer5), Return(true)));

    // GetServer
    Server forServer4(4, "", "", 0, "", 0, 4, 1, "");
    EXPECT_CALL(*mockTopo_, GetServer(4, _))
        .Times(2).WillRepeatedly(DoAll(SetArgPointee<1>(forServer4),
                                       Return(true)));

    // chunkServer offline
    ASSERT_EQ(::curve::mds::topology::UNINTIALIZE_ID,
              topoAdapter_->SelectBestPlacementChunkServer(copySetInfo, 1));
}

/* logicalPoolID=1
 *               server1    server2    server3    server4    server5    server6
 * copySetId        1          1          1          2(×)       5          6
 *                  4          2          2          3          4
 *                  5          3          3          4          6
 *                                        6          5
 *-----------------------------------------------------------------------------
 * state         offline     online     online     online      online    online
 * factor-origin                                    1.33          1        0.5
 * factor-possible                                  1.67          1        0.67
 * gap                                             -0.33          0       -0.17
 */
TEST_F(TestTopoAdapterImpl, test_choose_chunkServer_normal) {
    auto copySetInfo = GetCopySetInfoForTest();
    EXPECT_CALL(*mockTopo_, GetChunkServerInLogicalPool(_))
        .WillOnce(Return(std::list<ChunkServerIdType>({1, 2, 3, 4, 5, 6})));
    EXPECT_CALL(*mockTopo_, GetLogicalPool(_, _)).
        WillOnce(DoAll(SetArgPointee<1>(GetLogicalPoolForTest()),
                       Return(true)));

    // GetChunkSever
    // chunkserver1-3
    ChunkServer chunkServer1(1, "", "", 1, "", 9000, "",
                             ChunkServerStatus::READWRITE);
    ChunkServerState state1;
    state1.SetOnlineState(OnlineState::OFFLINE);
    chunkServer1.SetChunkServerState(state1);
    EXPECT_CALL(*mockTopo_, GetChunkServer(1, _))
        .Times(AtLeast(3))
        .WillOnce(Return(false))
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));

    ChunkServer chunkServer2(2, "", "", 2, "", 9000, "",
                             ChunkServerStatus::READWRITE);
    ChunkServerState state2;
    state2.SetOnlineState(OnlineState::ONLINE);
    chunkServer2.SetChunkServerState(state2);
    EXPECT_CALL(*mockTopo_, GetChunkServer(2, _))
        .Times(5)
        .WillOnce(Return(false))
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkServer2), Return(true)));

    ChunkServer chunkServer3(3, "", "", 3, "", 9000, "",
                             ChunkServerStatus::READWRITE);
    ChunkServerState state3;
    state3.SetOnlineState(OnlineState::ONLINE);
    chunkServer3.SetChunkServerState(state3);
    EXPECT_CALL(*mockTopo_, GetChunkServer(3, _))
        .Times(7)
        .WillOnce(Return(false))
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkServer3), Return(true)));

    // chunkserver4
    ChunkServer chunkServer4(4, "", "", 4, "", 9000, "",
                             ChunkServerStatus::READWRITE);
    ChunkServerState state4;
    state4.SetOnlineState(OnlineState::ONLINE);
    state4.SetDiskState(DiskState::DISKNORMAL);
    state4.SetDiskCapacity(100);
    state4.SetDiskUsed(10);
    chunkServer4.SetChunkServerState(state4);
    EXPECT_CALL(*mockTopo_, GetChunkServer(4, _))
        .Times(3)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkServer4), Return(true)));

    // chunkServer5
    ChunkServer chunkServer5(5, "", "", 4, "", 9000, "",
                             ChunkServerStatus::READWRITE);
    chunkServer5.SetChunkServerState(state4);
    EXPECT_CALL(*mockTopo_, GetChunkServer(5, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkServer5), Return(true)));


    // chunkServer6
    ChunkServer chunkServer6(6, "", "", 4, "", 9000, "",
                             ChunkServerStatus::RETIRED);
    chunkServer6.SetChunkServerState(state4);
    EXPECT_CALL(*mockTopo_, GetChunkServer(6, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkServer6), Return(true)));


    // GetServer
    Server forServer4(4, "", "", 0, "", 0, 4, 1, "");
    EXPECT_CALL(*mockTopo_, GetServer(4, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(forServer4),
                              Return(true)));

    CopySetKey key2 = std::pair<PoolIdType, CopySetIdType>(1, 2);
    CopySetKey key3 = std::pair<PoolIdType, CopySetIdType>(1, 3);
    CopySetKey key4 = std::pair<PoolIdType, CopySetIdType>(1, 4);
    CopySetKey key5 = std::pair<PoolIdType, CopySetIdType>(1, 5);
    CopySetKey key6 = std::pair<PoolIdType, CopySetIdType>(1, 6);
    EXPECT_CALL(*mockTopo_, GetCopySetsInChunkServer(4))
        .WillOnce(Return(
            std::vector<::curve::mds::topology::CopySetKey>(
                {key2, key3, key4, key5})));
    EXPECT_CALL(*mockTopo_, GetCopySetsInChunkServer(5))
        .WillOnce(Return(
            std::vector<::curve::mds::topology::CopySetKey>(
                {key4, key5, key6})));
    EXPECT_CALL(*mockTopo_, GetCopySetsInChunkServer(6))
        .WillOnce(Return(
            std::vector<::curve::mds::topology::CopySetKey>({key6})));

    ::curve::mds::topology::CopySetInfo copySetInfo3(1, 3);
    copySetInfo3.SetCopySetMembers(std::set<ChunkServerIdType>({2, 3, 4}));
    ::curve::mds::topology::CopySetInfo copySetInfo4(1, 4);
    copySetInfo4.SetCopySetMembers(std::set<ChunkServerIdType>({1, 4, 5}));
    ::curve::mds::topology::CopySetInfo copySetInfo5(1, 5);
    copySetInfo5.SetCopySetMembers(std::set<ChunkServerIdType>({1, 4, 5}));
    ::curve::mds::topology::CopySetInfo copySetInfo6(1, 6);
    copySetInfo6.SetCopySetMembers(std::set<ChunkServerIdType>({3, 5, 6}));
    EXPECT_CALL(*mockTopo_, GetCopySet(key2, _))
        .WillOnce(Return(false));
    EXPECT_CALL(*mockTopo_, GetCopySet(key3, _))
        .WillOnce(DoAll(SetArgPointee<1>(copySetInfo3), Return(true)));
    EXPECT_CALL(*mockTopo_, GetCopySet(key4, _))
        .Times(2).WillRepeatedly(DoAll(SetArgPointee<1>(copySetInfo4),
                                       Return(true)));
    EXPECT_CALL(*mockTopo_, GetCopySet(key5, _))
        .Times(2).WillRepeatedly(DoAll(SetArgPointee<1>(copySetInfo5),
                                       Return(true)));
    EXPECT_CALL(*mockTopo_, GetCopySet(key6, _))
        .Times(2).WillRepeatedly(DoAll(SetArgPointee<1>(copySetInfo6),
                                       Return(true)));

    ASSERT_EQ(5, topoAdapter_->SelectBestPlacementChunkServer(copySetInfo, 1));
}

TEST_F(TestTopoAdapterImpl, test_GetCopySetInfo) {
    CopySetKey key;
    key.first = 1;
    key.second = 2;
    ::curve::mds::topology::CopySetInfo csInfo(1, 1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>({1, 2, 3}));

    ::curve::mds::topology::CopySetInfo csInfoWithCandidate(1, 1);
    csInfoWithCandidate.SetCopySetMembers(
        std::set<ChunkServerIdType>({1, 2, 3}));
    csInfoWithCandidate.SetCandidate(4);

    ::curve::mds::topology::ChunkServer
        chunkServer1(1, "", "", 1, "", 9000, "", ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer
        chunkServer2(2, "", "", 1, "", 9000, "", ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer
        chunkServer3(3, "", "", 1, "", 9000, "", ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer
        chunkServer4(4, "", "", 1, "", 9000, "", ChunkServerStatus::READWRITE);

    ::curve::mds::topology::Server server(1, "", "", 0, "", 0, 1, 1, "");

    EXPECT_CALL(*mockTopo_, GetCopySet(key, _))
        .WillOnce(Return(false))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(csInfoWithCandidate), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(csInfoWithCandidate), Return(true)));

    EXPECT_CALL(*mockTopo_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)))
        .WillOnce(Return(false))
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*mockTopo_, GetChunkServer(2, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkServer2), Return(true)));
    EXPECT_CALL(*mockTopo_, GetChunkServer(3, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkServer3), Return(true)));
    EXPECT_CALL(*mockTopo_, GetChunkServer(4, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer4), Return(true)))
        .WillOnce(Return(false));

    EXPECT_CALL(*mockTopo_, GetServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(server), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(server), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(server), Return(true)))
        .WillOnce(Return(false))
        .WillRepeatedly(DoAll(SetArgPointee<1>(server), Return(true)));

    CopySetInfo info;
    // can not get
    ASSERT_FALSE(topoAdapter_->GetCopySetInfo(key, &info));
    // get normal
    ASSERT_TRUE(topoAdapter_->GetCopySetInfo(key, &info));
    ASSERT_EQ(csInfo.GetId(), info.id.second);
    ASSERT_EQ(csInfo.GetLeader(), info.leader);
    ASSERT_EQ(csInfo.GetEpoch(), info.epoch);
    ASSERT_EQ(csInfo.GetCopySetMembers().size(), info.peers.size());
    // cannot get chunkServer
    ASSERT_FALSE(topoAdapter_->GetCopySetInfo(key, &info));
    // cannot get Server
    ASSERT_FALSE(topoAdapter_->GetCopySetInfo(key, &info));
    // has candidate,can get server
    ASSERT_TRUE(topoAdapter_->GetCopySetInfo(key, &info));
    ASSERT_EQ(csInfoWithCandidate.GetCandidate(), info.candidatePeerInfo.id);
    // has candidate, can not get server
    ASSERT_FALSE(topoAdapter_->GetCopySetInfo(key, &info));
}

TEST_F(TestTopoAdapterImpl, test_GetCopySetInfos) {
    auto key1 = std::pair<PoolIdType, CopySetIdType>(1, 1);
    auto key2 = std::pair<PoolIdType, CopySetIdType>(1, 2);

    std::vector<CopySetKey> keys({key1, key2});
    EXPECT_CALL(*mockTopo_, GetCopySetsInCluster()).WillOnce(Return(keys));

    ::curve::mds::topology::CopySetInfo csInfo(1, 1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>({1, 2, 3}));

    ::curve::mds::topology::ChunkServer
        chunkServer1(1, "", "", 1, "", 9000, "", ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer
        chunkServer2(2, "", "", 1, "", 9000, "", ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer
        chunkServer3(3, "", "", 1, "", 9000, "", ChunkServerStatus::READWRITE);
    ::curve::mds::topology::Server server(1, "", "", 0, "", 0, 1, 1, "");

    EXPECT_CALL(*mockTopo_, GetCopySet(key1, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo), Return(true)));
    EXPECT_CALL(*mockTopo_, GetCopySet(key2, _)).WillOnce(Return(false));

    EXPECT_CALL(*mockTopo_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*mockTopo_, GetChunkServer(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer2), Return(true)));
    EXPECT_CALL(*mockTopo_, GetChunkServer(3, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer3), Return(true)));

    EXPECT_CALL(*mockTopo_, GetServer(1, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(server), Return(true)));

    auto res = topoAdapter_->GetCopySetInfos();
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(1, res[0].id.second);
}

TEST_F(TestTopoAdapterImpl, test_GetChunkSeverInfo) {
    ::curve::mds::topology::ChunkServer
        chunkServer(1, "", "", 1, "", 9000, "", ChunkServerStatus::READWRITE);
    ::curve::mds::topology::Server server(1, "", "", 0, "", 0, 1, 1, "");

    EXPECT_CALL(*mockTopo_, GetChunkServer(1, _))
        .WillOnce(Return(false))
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkServer), Return(true)));

    EXPECT_CALL(*mockTopo_, GetServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(server), Return(true)))
        .WillOnce(Return(false));

    // do not have chunkServer
    ChunkServerInfo cs;
    ASSERT_FALSE(topoAdapter_->GetChunkServerInfo(1, &cs));
    // get chunkServer normal
    ASSERT_TRUE(topoAdapter_->GetChunkServerInfo(1, &cs));
    ASSERT_EQ(chunkServer.GetPort(), cs.info.port);
    // can not get Server
    ASSERT_FALSE(topoAdapter_->GetChunkServerInfo(1, &cs));
}

TEST_F(TestTopoAdapterImpl, test_GetChunkServerInfos) {
    EXPECT_CALL(*mockTopo_, GetChunkServerInCluster())
        .WillOnce(Return(std::list<ChunkServerIdType>({1, 2})));

    ::curve::mds::topology::ChunkServer
        chunkServer(1, "", "", 1, "", 9000, "", ChunkServerStatus::READWRITE);
    ::curve::mds::topology::Server server(1, "", "", 0, "", 0, 1, 1, "");

    EXPECT_CALL(*mockTopo_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer), Return(true)));
    EXPECT_CALL(*mockTopo_, GetChunkServer(2, _))
        .WillOnce(Return(false));

    EXPECT_CALL(*mockTopo_, GetServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(server), Return(true)));

    auto infos = topoAdapter_->GetChunkServerInfos();
    ASSERT_EQ(1, infos.size());
    ASSERT_EQ(1, infos[0].info.id);
}

TEST_F(TestTopoAdapterImpl, test_GetParam) {
    ::curve::mds::topology::LogicalPool::RedundanceAndPlaceMentPolicy policy;
    EXPECT_CALL(*mockTopo_, GetLogicalPool(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(GetLogicalPoolForTest()),
                        Return(true)))
        .WillOnce(Return(false))
        .WillOnce(DoAll(SetArgPointee<1>(GetLogicalPoolForTest()),
                        Return(true)))
        .WillOnce(Return(false));
    ASSERT_EQ(3, topoAdapter_->GetStandardReplicaNumInLogicalPool(1));
    ASSERT_EQ(0, topoAdapter_->GetStandardReplicaNumInLogicalPool(1));
    ASSERT_EQ(3, topoAdapter_->GetStandardZoneNumInLogicalPool(1));
    ASSERT_EQ(0, topoAdapter_->GetStandardZoneNumInLogicalPool(1));

    policy.appendFileRAP.replicaNum = 4;
    policy.appendFileRAP.zoneNum = 4;
    ::curve::mds::topology::LogicalPool::UserPolicy userPolicy;
    ::curve::mds::topology::LogicalPool logicalPool
        (1, "", 1, LogicalPoolType::APPENDFILE, policy, userPolicy, 1000, true);
    EXPECT_CALL(*mockTopo_, GetLogicalPool(_, _))
        .Times(2).WillRepeatedly(DoAll(SetArgPointee<1>(logicalPool),
                                       Return(true)));
    ASSERT_EQ(4, topoAdapter_->GetStandardReplicaNumInLogicalPool(1));
    ASSERT_EQ(4, topoAdapter_->GetStandardZoneNumInLogicalPool(1));

    policy.appendECFileRAP.zoneNum = 1;
    policy.appendECFileRAP.cSegmentNum = 2;
    policy.appendECFileRAP.dSegmentNum = 3;
    ::curve::mds::topology::LogicalPool logicalPool2
        (1, "", 1, LogicalPoolType::APPENDECFILE, policy, userPolicy, 1000,
         true);
    EXPECT_CALL(*mockTopo_, GetLogicalPool(_, _))
        .Times(2).WillRepeatedly(DoAll(SetArgPointee<1>(logicalPool2),
                                       Return(true)));
    ASSERT_EQ(0, topoAdapter_->GetStandardReplicaNumInLogicalPool(1));
    ASSERT_EQ(0, topoAdapter_->GetStandardZoneNumInLogicalPool(1));

    EXPECT_CALL(*mockTopoManager_, CreateCopysetAtChunkServer(_, _))
        .WillOnce(Return(true)).WillOnce(Return(false));
    ASSERT_TRUE(topoAdapter_->CreateCopySetAtChunkServer(
        std::pair<PoolIdType, ChunkServerIdType>(1, 1), 1));
    ASSERT_FALSE(topoAdapter_->CreateCopySetAtChunkServer(
        std::pair<PoolIdType, ChunkServerIdType>(1, 1), 1));
}

TEST(TestCopySetInfo, test_copySetInfo_function) {
    auto testcopySetInfo = GetCopySetInfoForTest();

    // 1. test copyConstructed
    // 1.1 with no configChangeInfo or CopySetStatistic
    CopySetInfo test1(testcopySetInfo);
    ASSERT_EQ(testcopySetInfo.id.first, test1.id.first);
    ASSERT_EQ(testcopySetInfo.id.second, test1.id.second);
    ASSERT_EQ(testcopySetInfo.epoch, test1.epoch);
    ASSERT_EQ(testcopySetInfo.peers.size(), test1.peers.size());
    ASSERT_FALSE(test1.configChangeInfo.IsInitialized());
    ASSERT_FALSE(test1.statisticsInfo.IsInitialized());

    // 1.2 with configChangeInfo
    testcopySetInfo.candidatePeerInfo = PeerInfo(1, 1, 1, "", 9000);
    testcopySetInfo.configChangeInfo.set_peer("192.168.10.1:9000");
    testcopySetInfo.configChangeInfo.set_finished(false);
    ASSERT_TRUE(testcopySetInfo.configChangeInfo.IsInitialized());
    CopySetInfo test2(testcopySetInfo);
    ASSERT_TRUE(test2.configChangeInfo.IsInitialized());
    ASSERT_EQ(testcopySetInfo.candidatePeerInfo.id,
              test2.candidatePeerInfo.id);
    ASSERT_EQ(testcopySetInfo.configChangeInfo.finished(),
              test2.configChangeInfo.finished());
    ASSERT_FALSE(test2.statisticsInfo.IsInitialized());

    // 1.3 with configChangeInfo(candidate error)
    auto errMsg = new std::string("test error");
    auto candidateErr = new CandidateError();
    candidateErr->set_errtype(1);
    candidateErr->set_allocated_errmsg(errMsg);
    testcopySetInfo.configChangeInfo.set_allocated_err(candidateErr);
    ASSERT_TRUE(testcopySetInfo.configChangeInfo.has_err());
    CopySetInfo test3(testcopySetInfo);
    ASSERT_TRUE(test3.configChangeInfo.has_err());
    ASSERT_EQ(testcopySetInfo.configChangeInfo.err().errmsg(),
              test3.configChangeInfo.err().errmsg());
    ASSERT_EQ(testcopySetInfo.configChangeInfo.err().errtype(),
              test3.configChangeInfo.err().errtype());

    // 1.4 with statisticInfo
    testcopySetInfo.statisticsInfo.set_writerate(1);
    testcopySetInfo.statisticsInfo.set_writeiops(2);
    testcopySetInfo.statisticsInfo.set_readrate(3);
    testcopySetInfo.statisticsInfo.set_readiops(4);
    ASSERT_TRUE(testcopySetInfo.statisticsInfo.IsInitialized());
    CopySetInfo test4(testcopySetInfo);
    ASSERT_TRUE(test4.statisticsInfo.IsInitialized());
    ASSERT_EQ(testcopySetInfo.statisticsInfo.writerate(),
              test4.statisticsInfo.writerate());
    ASSERT_EQ(testcopySetInfo.statisticsInfo.writeiops(),
              test4.statisticsInfo.writeiops());
    ASSERT_EQ(testcopySetInfo.statisticsInfo.readrate(),
              test4.statisticsInfo.readrate());
    ASSERT_EQ(testcopySetInfo.statisticsInfo.readiops(),
              test4.statisticsInfo.readiops());

    // 2. test containPeer
    ASSERT_FALSE(testcopySetInfo.ContainPeer(4));
    ASSERT_TRUE(testcopySetInfo.ContainPeer(1));
}

TEST(TestChunkServerInfo, test_onlineState) {
    ChunkServerInfo cs(PeerInfo(1, 1, 1, "", 9000),
                       OnlineState::ONLINE,
                       1,
                       2,
                       1,
                       ChunkServerStatisticInfo());
    ASSERT_FALSE(cs.IsOffline());
    cs.state = OnlineState::OFFLINE;
    ASSERT_TRUE(cs.IsOffline());
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
