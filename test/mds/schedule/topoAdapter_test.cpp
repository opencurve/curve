/*
 * Project: curve
 * Created Date: Wed Dec 26 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include "test/mds/mock/mock_topology.h"
#include "test/mds/schedule/mock_topology_service_manager.h"
#include "test/mds/schedule/common.h"

using ::curve::mds::topology::ChunkServerStatus;
using ::curve::mds::topology::ChunkServerStat;
using ::curve::mds::topology::MockTopologyStat;
using ::curve::mds::copyset::CopysetOption;
using ::curve::mds::topology::MockTopology;

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
        CopysetOption copysetOption;
        mockTopoManager_ = std::make_shared<MockTopologyServiceManager>(
            mockTopo_, std::make_shared<::curve::mds::copyset::CopysetManager>(
                copysetOption));
        mockTopoStat_ = std::make_shared<MockTopologyStat>();
        topoAdapter_ = std::make_shared<TopoAdapterImpl>(mockTopo_,
                                                        mockTopoManager_,
                                                        mockTopoStat_);
    }
    void TearDown() override {
        mockTopo_ = nullptr;
        mockTopoManager_ = nullptr;
        topoAdapter_ = nullptr;
        mockTopoStat_ = nullptr;
    }

 protected:
    std::shared_ptr<MockTopology> mockTopo_;
    std::shared_ptr<MockTopologyServiceManager> mockTopoManager_;
    std::shared_ptr<MockTopologyStat> mockTopoStat_;
    std::shared_ptr<TopoAdapterImpl> topoAdapter_;
};

TEST_F(TestTopoAdapterImpl, test_copysetInfo) {
    CopySetKey key;
    key.first = 1;
    key.second = 1;
    CopySetInfo info;
    auto testcopySetInfo = GetCopySetInfoForTest();
    auto testTopoCopySet = GetTopoCopySetInfoForTest();
    auto testTopoChunkServer = GetTopoChunkServerForTest();
    auto testTopoServer = GetServerForTest();
    ::curve::mds::topology::LogicalPool lpool;
    lpool.SetLogicalPoolAvaliableFlag(true);
    {
        // 1. test GetCopySetInfo cannot get CopySetInfo
        EXPECT_CALL(*mockTopo_, GetCopySet(_, _)).WillOnce(Return(false));
        ASSERT_FALSE(topoAdapter_->GetCopySetInfo(key, &info));
    }
    {
        // 2. test GetCopySetInfo get success
        EXPECT_CALL(*mockTopo_, GetCopySet(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoCopySet), Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[0]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[1]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[2]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(4, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[3]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[0]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[1]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[2]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(4, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[3]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetLogicalPool(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(lpool), Return(true)));
        ASSERT_TRUE(topoAdapter_->GetCopySetInfo(key, &info));
        ASSERT_EQ(testcopySetInfo.id.first, info.id.first);
        ASSERT_EQ(testcopySetInfo.id.second, info.id.second);
        ASSERT_EQ(testcopySetInfo.epoch, info.epoch);
        ASSERT_EQ(testcopySetInfo.leader, info.leader);
        ASSERT_EQ(testcopySetInfo.peers.size(), info.peers.size());
        ASSERT_EQ(testcopySetInfo.configChangeInfo.peer().address(),
            info.configChangeInfo.peer().address());
        ASSERT_TRUE(testcopySetInfo.logicalPoolWork);
    }
    {
        // 3. test GetCopySetInfo cannot get Chunkserver
        EXPECT_CALL(*mockTopo_, GetCopySet(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoCopySet), Return(true)));
        EXPECT_CALL(*mockTopo_, GetLogicalPool(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(lpool), Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(1, _))
            .WillOnce(Return(false));
        ASSERT_FALSE(topoAdapter_->GetCopySetInfo(key, &info));
    }
    {
        // 4. test GetCopySetInfo can not get server
        EXPECT_CALL(*mockTopo_, GetCopySet(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoCopySet), Return(true)));
        EXPECT_CALL(*mockTopo_, GetLogicalPool(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(lpool), Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[0]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(1, _))
            .WillOnce(Return(false));
        ASSERT_FALSE(topoAdapter_->GetCopySetInfo(key, &info));
    }
    {
        // 5. test GetCopySetInfos fail
        std::vector<CopySetKey> infos{testcopySetInfo.id};
        EXPECT_CALL(*mockTopo_, GetCopySetsInCluster(_))
            .WillOnce(Return(infos));
        EXPECT_CALL(*mockTopo_, GetCopySet(_, _)).WillOnce(Return(false));
        ASSERT_EQ(0, topoAdapter_->GetCopySetInfos().size());
    }
    {
        // 6. test GetCopySetInfos sucess
        testTopoCopySet.ClearCandidate();
        std::vector<CopySetKey> infos{testcopySetInfo.id};
        EXPECT_CALL(*mockTopo_, GetCopySetsInCluster(_))
            .WillOnce(Return(infos));
        EXPECT_CALL(*mockTopo_, GetCopySet(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoCopySet), Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[0]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[1]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[2]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[0]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[1]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[2]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetLogicalPool(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(lpool), Return(true)));
        auto out = topoAdapter_->GetCopySetInfos();
        ASSERT_EQ(1, out.size());
        ASSERT_EQ(testcopySetInfo.id.first, out[0].id.first);
        ASSERT_EQ(testcopySetInfo.id.second, out[0].id.second);
        ASSERT_EQ(testcopySetInfo.epoch, out[0].epoch);
        ASSERT_EQ(testcopySetInfo.leader, out[0].leader);
        ASSERT_EQ(testcopySetInfo.peers.size(), out[0].peers.size());
        ASSERT_FALSE(info.configChangeInfo.IsInitialized());
    }
    {
        // 7. test GetCopySetInfosInChunkServer error
        std::vector<CopySetKey> infos{testcopySetInfo.id};
        EXPECT_CALL(*mockTopo_, GetCopySetsInChunkServer(_, _))
            .WillOnce(Return(infos));
        EXPECT_CALL(*mockTopo_, GetCopySet(_, _)).WillOnce(Return(false));
        ASSERT_EQ(0, topoAdapter_->GetCopySetInfosInChunkServer(1).size());
    }
    {
        // 8. test GetCopySetInfosInChunkServer success
        std::vector<CopySetKey> infos{testcopySetInfo.id};
        EXPECT_CALL(*mockTopo_, GetCopySetsInChunkServer(_, _))
            .WillOnce(Return(infos));
        EXPECT_CALL(*mockTopo_, GetCopySet(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoCopySet), Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[0]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[1]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[2]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[0]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[1]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[2]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetLogicalPool(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(lpool), Return(true)));
        auto out = topoAdapter_->GetCopySetInfosInChunkServer(1);
        ASSERT_EQ(1, out.size());
        ASSERT_EQ(testcopySetInfo.id.first, out[0].id.first);
        ASSERT_EQ(testcopySetInfo.id.second, out[0].id.second);
        ASSERT_EQ(testcopySetInfo.epoch, out[0].epoch);
        ASSERT_EQ(testcopySetInfo.leader, out[0].leader);
        ASSERT_EQ(testcopySetInfo.peers.size(), out[0].peers.size());
        ASSERT_FALSE(info.configChangeInfo.IsInitialized());
    }
    {
        // 9. test GetCopySetInfo can not get logicalPoolId
        EXPECT_CALL(*mockTopo_, GetCopySet(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoCopySet), Return(true)));
        EXPECT_CALL(*mockTopo_, GetLogicalPool(1, _)).WillOnce(Return(false));
        ASSERT_FALSE(topoAdapter_->GetCopySetInfo(key, &info));
    }
    {
        // 10. test GetCopySetInfos logical pool unavailable
        std::vector<CopySetKey> infos{testcopySetInfo.id};
        EXPECT_CALL(*mockTopo_, GetCopySetsInCluster(_))
            .WillOnce(Return(infos));
        EXPECT_CALL(*mockTopo_, GetCopySet(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoCopySet), Return(true)));
        lpool.SetLogicalPoolAvaliableFlag(false);
        EXPECT_CALL(*mockTopo_, GetLogicalPool(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(lpool), Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[0]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[1]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[2]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[0]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[1]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[2]),
                            Return(true)));
        ASSERT_EQ(0, topoAdapter_->GetCopySetInfos().size());
    }
    {
        // 11. test GetCopySetInfosInChunkServer logical pool unavailable
        std::vector<CopySetKey> infos{testcopySetInfo.id};
        EXPECT_CALL(*mockTopo_, GetCopySetsInChunkServer(_, _))
            .WillOnce(Return(infos));
        EXPECT_CALL(*mockTopo_, GetCopySet(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoCopySet), Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[0]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[1]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[2]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[0]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[1]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[2]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetLogicalPool(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(lpool), Return(true)));
        ASSERT_TRUE(topoAdapter_->GetCopySetInfosInChunkServer(1).empty());
    }
}

TEST_F(TestTopoAdapterImpl, test_chunkserverInfo) {
    ChunkServerInfo info;
    auto testTopoServer = GetServerForTest();
    auto testTopoChunkServer = GetTopoChunkServerForTest();
    ChunkServerStat stat;
    stat.leaderCount = 10;
    {
        // 1. test GetChunkServerInfo fail
        EXPECT_CALL(*mockTopo_, GetChunkServer(_, _)).WillOnce(Return(false));
        ASSERT_FALSE(topoAdapter_->GetChunkServerInfo(1, &info));

         EXPECT_CALL(*mockTopo_, GetChunkServer(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[0]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[0]), Return(true)));
        EXPECT_CALL(*mockTopoStat_, GetChunkServerStat(_, _))
            .WillOnce(Return(false));
        ASSERT_TRUE(topoAdapter_->GetChunkServerInfo(1, &info));
        ASSERT_EQ(1, info.info.id);
        ASSERT_EQ(testTopoChunkServer[0].GetOnlineState(), info.state);
        ASSERT_EQ(testTopoChunkServer[0].GetChunkServerState().GetDiskState(),
            info.diskState);
        ASSERT_EQ(testTopoChunkServer[0].GetStatus(), info.status);
        ASSERT_EQ(0, info.leaderCount);
    }
    {
        // 2. test GetChunkServerInfo success
        EXPECT_CALL(*mockTopo_, GetChunkServer(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[0]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[0]), Return(true)));
        EXPECT_CALL(*mockTopoStat_, GetChunkServerStat(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(stat), Return(true)));
        ASSERT_TRUE(topoAdapter_->GetChunkServerInfo(1, &info));
        ASSERT_EQ(1, info.info.id);
        ASSERT_EQ(testTopoChunkServer[0].GetOnlineState(), info.state);
        ASSERT_EQ(testTopoChunkServer[0].GetChunkServerState().GetDiskState(),
            info.diskState);
        ASSERT_EQ(testTopoChunkServer[0].GetStatus(), info.status);
        ASSERT_EQ(stat.leaderCount, info.leaderCount);
    }
    {
        // 3. test GetChunkServerInfos fail
        EXPECT_CALL(*mockTopo_, GetChunkServerInCluster(_))
            .WillOnce(Return(std::vector<ChunkServerIdType>{1}));
        EXPECT_CALL(*mockTopo_, GetChunkServer(_, _)).WillOnce(Return(false));
        ASSERT_FALSE(topoAdapter_->GetChunkServerInfos().size());
    }
    {
        // 4. test GetChunkServerInfos success
        EXPECT_CALL(*mockTopo_, GetChunkServerInCluster(_))
            .WillOnce(Return(std::vector<ChunkServerIdType>{1}));
        EXPECT_CALL(*mockTopo_, GetChunkServer(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoChunkServer[0]),
                            Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[0]), Return(true)));
        EXPECT_CALL(*mockTopoStat_, GetChunkServerStat(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(stat), Return(true)));

        auto res = topoAdapter_->GetChunkServerInfos();
        ASSERT_EQ(1, res[0].info.id);
        ASSERT_EQ(testTopoChunkServer[0].GetOnlineState(), res[0].state);
        ASSERT_EQ(testTopoChunkServer[0].GetChunkServerState().GetDiskState(),
            res[0].diskState);
        ASSERT_EQ(testTopoChunkServer[0].GetStatus(), res[0].status);
    }
    {
        // 5. tests GetChunkServersInPhysicalPool topo get chunkserver error
        EXPECT_CALL(*mockTopo_, GetChunkServer(_, _)).WillOnce(Return(false));
        EXPECT_CALL(*mockTopo_, GetChunkServerInPhysicalPool(_, _))
            .WillOnce(Return(std::list<ChunkServerIdType>{1}));
        ASSERT_EQ(0, topoAdapter_->GetChunkServersInPhysicalPool(1).size());
    }
    {
        // 6. tests GetChunkServersInPhysicalPool success
        EXPECT_CALL(*mockTopo_, GetChunkServer(_, _))
            .WillOnce(DoAll(
                SetArgPointee<1>(testTopoChunkServer[0]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[0]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetChunkServerInPhysicalPool(_, _))
            .WillOnce(Return(std::list<ChunkServerIdType>{1}));
        EXPECT_CALL(*mockTopoStat_, GetChunkServerStat(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(stat), Return(true)));

        auto res = topoAdapter_->GetChunkServersInPhysicalPool(1);
        ASSERT_EQ(1, res[0].info.id);
        ASSERT_EQ(testTopoChunkServer[0].GetOnlineState(), res[0].state);
        ASSERT_EQ(testTopoChunkServer[0].GetChunkServerState().GetDiskState(),
            res[0].diskState);
        ASSERT_EQ(testTopoChunkServer[0].GetStatus(), res[0].status);
        ASSERT_EQ(stat.leaderCount, res[0].leaderCount);
    }
}

TEST_F(TestTopoAdapterImpl, test_other_functions) {
    auto testPageFileLogicalPool = GetPageFileLogicalPoolForTest();
    auto testAppendFileLogicalPool = GetAppendFileLogicalPoolForTest();
    auto testAppendECFileLogicalPool = GetAppendECFileLogicalPoolForTest();
    {
        // 1. test GetStandardZoneNumInLogicalPool
        EXPECT_CALL(*mockTopo_, GetLogicalPool(_, _)).WillOnce(Return(false));
        ASSERT_EQ(0, topoAdapter_->GetStandardZoneNumInLogicalPool(1));

        EXPECT_CALL(*mockTopo_, GetLogicalPool(_, _))
            .WillOnce(DoAll(
                SetArgPointee<1>(testPageFileLogicalPool), Return(true)));
        ASSERT_EQ(testPageFileLogicalPool.GetRedundanceAndPlaceMentPolicy().
            pageFileRAP.zoneNum,
            topoAdapter_->GetStandardZoneNumInLogicalPool(1));

        EXPECT_CALL(*mockTopo_, GetLogicalPool(_, _))
            .WillOnce(DoAll(
                SetArgPointee<1>(testAppendFileLogicalPool), Return(true)));
        ASSERT_EQ(testAppendFileLogicalPool.GetRedundanceAndPlaceMentPolicy().
            appendFileRAP.zoneNum,
            topoAdapter_->GetStandardZoneNumInLogicalPool(1));

        EXPECT_CALL(*mockTopo_, GetLogicalPool(_, _))
            .WillOnce(DoAll(
                SetArgPointee<1>(testAppendECFileLogicalPool), Return(true)));
        ASSERT_EQ(0, topoAdapter_->GetStandardZoneNumInLogicalPool(1));
    }
    {
        // 2. test GetStandardReplicaNumInLogicalPool
        EXPECT_CALL(*mockTopo_, GetLogicalPool(_, _)).WillOnce(Return(false));
        ASSERT_EQ(0, topoAdapter_->GetStandardReplicaNumInLogicalPool(1));

        EXPECT_CALL(*mockTopo_, GetLogicalPool(_, _))
            .WillOnce(DoAll(
                SetArgPointee<1>(testPageFileLogicalPool), Return(true)));
        ASSERT_EQ(testPageFileLogicalPool.GetRedundanceAndPlaceMentPolicy().
            pageFileRAP.replicaNum,
            topoAdapter_->GetStandardReplicaNumInLogicalPool(1));

        EXPECT_CALL(*mockTopo_, GetLogicalPool(_, _))
            .WillOnce(DoAll(
                SetArgPointee<1>(testAppendFileLogicalPool), Return(true)));
        ASSERT_EQ(testAppendFileLogicalPool.GetRedundanceAndPlaceMentPolicy().
            appendFileRAP.replicaNum,
            topoAdapter_->GetStandardReplicaNumInLogicalPool(1));

        EXPECT_CALL(*mockTopo_, GetLogicalPool(_, _))
            .WillOnce(DoAll(
                SetArgPointee<1>(testAppendECFileLogicalPool), Return(true)));
        ASSERT_EQ(0, topoAdapter_->GetStandardReplicaNumInLogicalPool(1));
    }
    {
        // 3. test CreateCopySetAtChunkServer
        CopySetKey key;
        EXPECT_CALL(*mockTopoManager_, CreateCopysetNodeOnChunkServer(_, _))
            .WillOnce(Return(false));
        ASSERT_FALSE(topoAdapter_->CreateCopySetAtChunkServer(key, 1));

        EXPECT_CALL(*mockTopoManager_, CreateCopysetNodeOnChunkServer(_, _))
            .WillOnce(Return(true));
        ASSERT_TRUE(topoAdapter_->CreateCopySetAtChunkServer(key, 1));
    }
    {
        // 4. test GetMinScatterWidthInLogicalPool
        EXPECT_CALL(*mockTopo_, GetLogicalPool(1, _))
            .WillOnce(Return(false));
        ASSERT_EQ(0, topoAdapter_->GetMinScatterWidthInLogicalPool(1));

        ::curve::mds::topology::LogicalPool lpool;
        lpool.SetScatterWidth(90);
        EXPECT_CALL(*mockTopo_, GetLogicalPool(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(lpool), Return(true)));
        ASSERT_EQ(90, topoAdapter_->GetMinScatterWidthInLogicalPool(1));
    }
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
    testcopySetInfo.candidatePeerInfo = PeerInfo(1, 1, 1, 1, "", 9000);
    auto replica = new ::curve::common::Peer();
    replica->set_address("192.168.10.1:9000");
    testcopySetInfo.configChangeInfo.set_allocated_peer(replica);
    testcopySetInfo.configChangeInfo.set_type(ConfigChangeType::ADD_PEER);
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
    ChunkServerInfo cs(PeerInfo(1, 1, 1, 1, "", 9000),
                       OnlineState::ONLINE,
                       DiskState::DISKNORMAL,
                       ChunkServerStatus::READWRITE,
                       2,
                       1,
                       2,
                       ChunkServerStatisticInfo());
    ASSERT_FALSE(cs.IsOffline());
    ASSERT_TRUE(cs.IsHealthy());
    cs.state = OnlineState::OFFLINE;
    ASSERT_TRUE(cs.IsOffline());
    ASSERT_FALSE(cs.IsHealthy());
    cs.state = OnlineState::ONLINE;
    cs.diskState = DiskState::DISKERROR;
    ASSERT_FALSE(cs.IsHealthy());
    ASSERT_FALSE(cs.IsPendding());
    cs.status = ChunkServerStatus::PENDDING;
    ASSERT_TRUE(cs.IsPendding());
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
