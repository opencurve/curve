/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * @Project: curve
 * @Date: 2021-11-15 11:01:48
 * @Author: chenwei
 */

#include <gtest/gtest.h>
#include "curvefs/test/mds/mock/mock_topology.h"
#include "curvefs/test/mds/schedule/common.h"

using ::curvefs::mds::topology::MetaserverClient;
using ::curvefs::mds::topology::MockTopology;
using ::curvefs::mds::topology::MockTopologyManager;
using ::curvefs::mds::topology::TopologyIdGenerator;
using ::curvefs::mds::topology::TopologyTokenGenerator;
using ::curvefs::mds::topology::TopologyStorage;
using ::curvefs::mds::topology::MetaServerSpace;

using ::testing::_;
using ::testing::Return;
using ::testing::AtLeast;
using ::testing::SetArgPointee;
using ::testing::DoAll;
using ::testing::InSequence;

namespace curvefs {
namespace mds {
namespace schedule {
class TestTopoAdapterImpl : public ::testing::Test {
 protected:
    TestTopoAdapterImpl() {}
    ~TestTopoAdapterImpl() {}

    void SetUp() override {
        std::shared_ptr<TopologyIdGenerator> idGenerator;
        std::shared_ptr<TopologyTokenGenerator> tokenGenerator;
        std::shared_ptr<TopologyStorage> storage;
        mockTopo_ = std::make_shared<MockTopology>(idGenerator, tokenGenerator,
                                                   storage);
        MetaserverOptions metaserverOptions;
        metaserverClient_ =
            std::make_shared<MetaserverClient>(metaserverOptions);
        mockTopoManager_ =
            std::make_shared<MockTopologyManager>(mockTopo_, metaserverClient_);
        topoAdapter_ =
            std::make_shared<TopoAdapterImpl>(mockTopo_, mockTopoManager_);
    }
    void TearDown() override {
        mockTopo_ = nullptr;
        mockTopoManager_ = nullptr;
        topoAdapter_ = nullptr;
    }

 protected:
    std::shared_ptr<MockTopology> mockTopo_;
    std::shared_ptr<MockTopologyManager> mockTopoManager_;
    std::shared_ptr<TopoAdapterImpl> topoAdapter_;
    std::shared_ptr<MetaserverClient> metaserverClient_;
};

TEST_F(TestTopoAdapterImpl, test_copysetInfo) {
    CopySetKey key;
    key.first = 1;
    key.second = 1;
    CopySetInfo info;
    auto testcopySetInfo = GetCopySetInfoForTest();
    auto testTopoCopySet = GetTopoCopySetInfoForTest();
    auto testTopoMetaServer = GetTopoMetaServerForTest();
    auto testTopoServer = GetServerForTest();
    {
        // 1. test GetCopySetInfo cannot get CopySetInfo
        EXPECT_CALL(*mockTopo_, GetCopySet(_, _)).WillOnce(Return(false));
        ASSERT_FALSE(topoAdapter_->GetCopySetInfo(key, &info));
    }
    {
        // 2. test GetCopySetInfo get success
        EXPECT_CALL(*mockTopo_, GetCopySet(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoCopySet), Return(true)));
        EXPECT_CALL(*mockTopo_, GetMetaServer(1, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(testTopoMetaServer[0]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetMetaServer(2, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(testTopoMetaServer[1]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetMetaServer(3, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(testTopoMetaServer[2]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetMetaServer(4, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(testTopoMetaServer[3]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[0]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[1]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[2]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(4, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[3]), Return(true)));
        ASSERT_TRUE(topoAdapter_->GetCopySetInfo(key, &info));
        ASSERT_EQ(testcopySetInfo.id.first, info.id.first);
        ASSERT_EQ(testcopySetInfo.id.second, info.id.second);
        ASSERT_EQ(testcopySetInfo.epoch, info.epoch);
        ASSERT_EQ(testcopySetInfo.leader, info.leader);
        ASSERT_EQ(testcopySetInfo.peers.size(), info.peers.size());
        ASSERT_EQ(testcopySetInfo.configChangeInfo.peer().address(),
                  info.configChangeInfo.peer().address());
    }
    {
        // 3. test GetCopySetInfo cannot get Metaserver
        EXPECT_CALL(*mockTopo_, GetCopySet(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoCopySet), Return(true)));
        EXPECT_CALL(*mockTopo_, GetMetaServer(1, _)).WillOnce(Return(false));
        ASSERT_FALSE(topoAdapter_->GetCopySetInfo(key, &info));
    }
    {
        // 4. test GetCopySetInfo can not get server
        EXPECT_CALL(*mockTopo_, GetCopySet(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoCopySet), Return(true)));
        EXPECT_CALL(*mockTopo_, GetMetaServer(1, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(testTopoMetaServer[0]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(1, _)).WillOnce(Return(false));
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
        EXPECT_CALL(*mockTopo_, GetMetaServer(1, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(testTopoMetaServer[0]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetMetaServer(2, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(testTopoMetaServer[1]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetMetaServer(3, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(testTopoMetaServer[2]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[0]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[1]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[2]), Return(true)));
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
        // 7. test GetCopySetInfosInMetaServer error
        std::vector<CopySetKey> infos{testcopySetInfo.id};
        EXPECT_CALL(*mockTopo_, GetCopySetsInMetaServer(_, _))
            .WillOnce(Return(infos));
        EXPECT_CALL(*mockTopo_, GetCopySet(_, _)).WillOnce(Return(false));
        ASSERT_EQ(0, topoAdapter_->GetCopySetInfosInMetaServer(1).size());
    }
    {
        // 8. test GetCopySetInfosInMetaServer success
        std::vector<CopySetKey> infos{testcopySetInfo.id};
        EXPECT_CALL(*mockTopo_, GetCopySetsInMetaServer(_, _))
            .WillOnce(Return(infos));
        EXPECT_CALL(*mockTopo_, GetCopySet(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoCopySet), Return(true)));
        EXPECT_CALL(*mockTopo_, GetMetaServer(1, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(testTopoMetaServer[0]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetMetaServer(2, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(testTopoMetaServer[1]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetMetaServer(3, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(testTopoMetaServer[2]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[0]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[1]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[2]), Return(true)));
        auto out = topoAdapter_->GetCopySetInfosInMetaServer(1);
        ASSERT_EQ(1, out.size());
        ASSERT_EQ(testcopySetInfo.id.first, out[0].id.first);
        ASSERT_EQ(testcopySetInfo.id.second, out[0].id.second);
        ASSERT_EQ(testcopySetInfo.epoch, out[0].epoch);
        ASSERT_EQ(testcopySetInfo.leader, out[0].leader);
        ASSERT_EQ(testcopySetInfo.peers.size(), out[0].peers.size());
        ASSERT_FALSE(info.configChangeInfo.IsInitialized());
    }
}

TEST_F(TestTopoAdapterImpl, test_metaserverInfo) {
    MetaServerInfo info;
    auto testTopoServer = GetServerForTest();
    auto testTopoMetaServer = GetTopoMetaServerForTest();
    {
        // 1. test GetMetaServerInfo fail
        EXPECT_CALL(*mockTopo_, GetMetaServer(_, _)).WillOnce(Return(false));
        ASSERT_FALSE(topoAdapter_->GetMetaServerInfo(1, &info));
    }
    {
        uint32_t leaderNum = 3;
        uint32_t copysetNum = 4;
        // 2. test GetMetaServerInfo success
        EXPECT_CALL(*mockTopo_, GetMetaServer(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(testTopoMetaServer[0]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[0]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetLeaderNumInMetaserver(_))
            .WillOnce(Return(leaderNum));
        EXPECT_CALL(*mockTopo_, GetCopysetNumInMetaserver(_))
            .WillOnce(Return(copysetNum));
        ASSERT_TRUE(topoAdapter_->GetMetaServerInfo(1, &info));
        ASSERT_EQ(1, info.info.id);
        ASSERT_EQ(testTopoMetaServer[0].GetOnlineState(), info.state);
        ASSERT_EQ(testTopoMetaServer[0].GetStartUpTime(), info.startUpTime);
        ASSERT_EQ(info.copysetNum, copysetNum);
        ASSERT_EQ(info.leaderNum, leaderNum);
    }
    {
        // 3. test GetMetaServerInfos fail
        EXPECT_CALL(*mockTopo_, GetMetaServerInCluster(_))
            .WillOnce(Return(std::vector<MetaServerIdType>{1}));
        EXPECT_CALL(*mockTopo_, GetMetaServer(_, _)).WillOnce(Return(false));
        ASSERT_FALSE(topoAdapter_->GetMetaServerInfos().size());
    }
    {
        // 4. test GetMetaServerInfos success
        EXPECT_CALL(*mockTopo_, GetMetaServerInCluster(_))
            .WillOnce(Return(std::vector<MetaServerIdType>{1}));
        EXPECT_CALL(*mockTopo_, GetMetaServer(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(testTopoMetaServer[0]), Return(true)));
        EXPECT_CALL(*mockTopo_, GetServer(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(testTopoServer[0]), Return(true)));
        uint32_t leaderNum = 3;
        uint32_t copysetNum = 4;
        EXPECT_CALL(*mockTopo_, GetLeaderNumInMetaserver(_))
            .WillOnce(Return(leaderNum));
        EXPECT_CALL(*mockTopo_, GetCopysetNumInMetaserver(_))
            .WillOnce(Return(copysetNum));
        auto res = topoAdapter_->GetMetaServerInfos();
        ASSERT_EQ(1, res[0].info.id);
        ASSERT_EQ(testTopoMetaServer[0].GetOnlineState(), res[0].state);
        ASSERT_EQ(res[0].copysetNum, copysetNum);
        ASSERT_EQ(res[0].leaderNum, leaderNum);
    }
}

TEST_F(TestTopoAdapterImpl, test_other_functions) {
    auto testPool = GetPoolForTest();
    {
        // 1. test GetStandardZoneNumInPool
        EXPECT_CALL(*mockTopo_, GetPool(1, _)).WillOnce(Return(false));
        ASSERT_EQ(0, topoAdapter_->GetStandardZoneNumInPool(1));

        EXPECT_CALL(*mockTopo_, GetPool(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(testPool), Return(true)));
        ASSERT_EQ(testPool.GetRedundanceAndPlaceMentPolicy().zoneNum,
                  topoAdapter_->GetStandardZoneNumInPool(1));
    }
    {
        // 2. test GetStandardReplicaNumInPool
        EXPECT_CALL(*mockTopo_, GetPool(1, _)).WillOnce(Return(false));
        ASSERT_EQ(0, topoAdapter_->GetStandardReplicaNumInPool(1));

        EXPECT_CALL(*mockTopo_, GetPool(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(testPool), Return(true)));
        ASSERT_EQ(testPool.GetRedundanceAndPlaceMentPolicy().replicaNum,
                  topoAdapter_->GetStandardReplicaNumInPool(1));
    }
    {
        // 3. test CreateCopySetAtMetaServer
        CopySetKey key;
        EXPECT_CALL(*mockTopoManager_, CreateCopysetNodeOnMetaServer(_, _, _))
            .WillOnce(Return(false));
        ASSERT_FALSE(topoAdapter_->CreateCopySetAtMetaServer(key, 1));

        EXPECT_CALL(*mockTopoManager_, CreateCopysetNodeOnMetaServer(_, _, _))
            .WillOnce(Return(true));
        ASSERT_TRUE(topoAdapter_->CreateCopySetAtMetaServer(key, 1));
    }
    {
        // 5. test Getpools
        EXPECT_CALL(*mockTopo_, GetPoolInCluster(_))
            .WillOnce(Return(std::vector<PoolIdType>({1})));
        ASSERT_EQ(1, topoAdapter_->Getpools()[0]);
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

    // 1.2 with configChangeInfo
    testcopySetInfo.candidatePeerInfo = PeerInfo(1, 1, 1, "", 9000);
    auto replica = new ::curvefs::common::Peer();
    replica->set_address("192.168.10.1:9000");
    testcopySetInfo.configChangeInfo.set_allocated_peer(replica);
    testcopySetInfo.configChangeInfo.set_type(ConfigChangeType::ADD_PEER);
    testcopySetInfo.configChangeInfo.set_finished(false);
    ASSERT_TRUE(testcopySetInfo.configChangeInfo.IsInitialized());
    CopySetInfo test2(testcopySetInfo);
    ASSERT_TRUE(test2.configChangeInfo.IsInitialized());
    ASSERT_EQ(testcopySetInfo.candidatePeerInfo.id, test2.candidatePeerInfo.id);
    ASSERT_EQ(testcopySetInfo.configChangeInfo.finished(),
              test2.configChangeInfo.finished());

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

    // 2. test containPeer
    ASSERT_FALSE(testcopySetInfo.ContainPeer(4));
    ASSERT_TRUE(testcopySetInfo.ContainPeer(1));
}

TEST(TestMetaServerInfo, test_onlineState) {
    MetaServerSpace space;
    MetaServerInfo ms(PeerInfo(1, 1, 1, "", 9000), OnlineState::ONLINE, space);
    ASSERT_TRUE(ms.IsOnline());
    ASSERT_FALSE(ms.IsOffline());
    ASSERT_TRUE(ms.IsHealthy());

    ms.state = OnlineState::OFFLINE;
    ASSERT_TRUE(ms.IsOffline());
    ASSERT_FALSE(ms.IsHealthy());

    ms.state = OnlineState::UNSTABLE;
    ASSERT_TRUE(ms.IsUnstable());
    ASSERT_FALSE(ms.IsHealthy());
}

TEST(TestMetaServerInfo, test_IsResourceOverload) {
    // disk overload
    {
        uint64_t diskThreshold = 10;
        uint64_t diskUsed = 20;
        uint64_t memoryUsed = 0;
        uint64_t memoryThreshold = 0;
        uint64_t memoryCopySetMinRequire = 0;
        uint64_t diskCopysetMinRequire = 0;
        MetaServerSpace space(diskThreshold, diskUsed, diskCopysetMinRequire,
                              memoryThreshold, memoryUsed,
                              memoryCopySetMinRequire);
        MetaServerInfo ms(PeerInfo(1, 1, 1, "", 9000), OnlineState::ONLINE,
                          space);
        ASSERT_TRUE(ms.IsResourceOverload());
    }

    // memory overload
    {
        uint64_t diskThreshold = 0;
        uint64_t diskUsed = 0;
        uint64_t memoryUsed = 20;
        uint64_t memoryThreshold = 10;
        uint64_t memoryCopySetMinRequire = 2;
        uint64_t diskCopysetMinRequire = 0;
        MetaServerSpace space(diskThreshold, diskUsed, diskCopysetMinRequire,
                              memoryThreshold, memoryUsed,
                              memoryCopySetMinRequire);
        MetaServerInfo ms(PeerInfo(1, 1, 1, "", 9000), OnlineState::ONLINE,
                          space);
        ASSERT_TRUE(ms.IsResourceOverload());
    }

    // both overload
    {
        uint64_t diskThreshold = 10;
        uint64_t diskUsed = 20;
        uint64_t memoryUsed = 20;
        uint64_t memoryThreshold = 10;
        uint64_t memoryCopySetMinRequire = 2;
        uint64_t diskCopysetMinRequire = 0;
        MetaServerSpace space(diskThreshold, diskUsed, diskCopysetMinRequire,
                              memoryThreshold, memoryUsed,
                              memoryCopySetMinRequire);
        MetaServerInfo ms(PeerInfo(1, 1, 1, "", 9000), OnlineState::ONLINE,
                          space);
        ASSERT_TRUE(ms.IsResourceOverload());
    }

    // not overload
    {
        uint64_t diskThreshold = 20;
        uint64_t diskUsed = 10;
        uint64_t memoryUsed = 10;
        uint64_t memoryThreshold = 20;
        uint64_t memoryCopySetMinRequire = 2;
        uint64_t diskCopysetMinRequire = 0;
        MetaServerSpace space(diskThreshold, diskUsed, diskCopysetMinRequire,
                              memoryThreshold, memoryUsed,
                              memoryCopySetMinRequire);
        MetaServerInfo ms(PeerInfo(1, 1, 1, "", 9000), OnlineState::ONLINE,
                          space);
        ASSERT_FALSE(ms.IsResourceOverload());
    }
}

TEST(TestMetaServerInfo, test_GetResourceUseRatioPercent) {
    // default is 0
    {
        MetaServerSpace space;
        PeerInfo info;
        MetaServerInfo ms(info, OnlineState::ONLINE, space);
        ASSERT_DOUBLE_EQ(ms.GetResourceUseRatioPercent(), 0);
    }

    // memoryCopySetMinRequireByte is 0, calc disk usage
    {
        uint64_t diskThreshold = 20;
        uint64_t diskUsed = 10;
        uint64_t memoryUsed = 10;
        uint64_t memoryThreshold = 40;
        uint64_t memoryCopySetMinRequire = 0;
        uint64_t diskCopysetMinRequire = 0;
        MetaServerSpace space(diskThreshold, diskUsed, diskCopysetMinRequire,
                              memoryThreshold, memoryUsed,
                              memoryCopySetMinRequire);
        MetaServerInfo ms(PeerInfo(1, 1, 1, "", 9000), OnlineState::ONLINE,
                          space);
        ASSERT_DOUBLE_EQ(ms.GetResourceUseRatioPercent(), 50);
    }

    // diskThreshold is 0
    {
        uint64_t diskThreshold = 0;
        uint64_t diskUsed = 10;
        uint64_t memoryUsed = 10;
        uint64_t memoryThreshold = 40;
        uint64_t memoryCopySetMinRequire = 0;
        uint64_t diskCopysetMinRequire = 0;
        MetaServerSpace space(diskThreshold, diskUsed, diskCopysetMinRequire,
                              memoryThreshold, memoryUsed,
                              memoryCopySetMinRequire);
        MetaServerInfo ms(PeerInfo(1, 1, 1, "", 9000), OnlineState::ONLINE,
                          space);
        ASSERT_DOUBLE_EQ(ms.GetResourceUseRatioPercent(), 25);
    }

    // diskThreshold and memoryThreshold not 0
    {
        uint64_t diskThreshold = 20;
        uint64_t diskUsed = 10;
        uint64_t memoryUsed = 10;
        uint64_t memoryThreshold = 40;
        uint64_t memoryCopySetMinRequire = 2;
        uint64_t diskCopysetMinRequire = 0;
        MetaServerSpace space(diskThreshold, diskUsed, diskCopysetMinRequire,
                              memoryThreshold, memoryUsed,
                              memoryCopySetMinRequire);
        MetaServerInfo ms(PeerInfo(1, 1, 1, "", 9000), OnlineState::ONLINE,
                          space);
        ASSERT_DOUBLE_EQ(ms.GetResourceUseRatioPercent(), 50);
    }

    // memory  threshold is 0
    {
        uint64_t diskThreshold = 20;
        uint64_t diskUsed = 10;
        uint64_t memoryUsed = 10;
        uint64_t memoryThreshold = 0;
        uint64_t memoryCopySetMinRequire = 2;
        uint64_t diskCopysetMinRequire = 0;
        MetaServerSpace space(diskThreshold, diskUsed, diskCopysetMinRequire,
                              memoryThreshold, memoryUsed,
                              memoryCopySetMinRequire);
        MetaServerInfo ms(PeerInfo(1, 1, 1, "", 9000), OnlineState::ONLINE,
                          space);
        ASSERT_DOUBLE_EQ(ms.GetResourceUseRatioPercent(), 50);
    }
}

TEST(TestMetaServerInfo, test_IsMetaserverResourceAvailable) {
    // metaserver not online
    {
        MetaServerSpace space;
        MetaServerInfo ms(PeerInfo(1, 1, 1, "", 9000), OnlineState::OFFLINE,
                          space);
        ASSERT_FALSE(ms.IsMetaserverResourceAvailable());
    }

    // disk not enough, momory enough
    {
        uint64_t diskThreshold = 200;
        uint64_t diskUsed = 190;
        uint64_t memoryUsed = 40;
        uint64_t memoryThreshold = 200;
        uint64_t memoryCopySetMinRequire = 20;
        uint64_t diskCopysetMinRequire = 20;
        MetaServerSpace space(diskThreshold, diskUsed, diskCopysetMinRequire,
                              memoryThreshold, memoryUsed,
                              memoryCopySetMinRequire);
        MetaServerInfo ms(PeerInfo(1, 1, 1, "", 9000), OnlineState::ONLINE,
                          space);
        ASSERT_FALSE(ms.IsMetaserverResourceAvailable());
    }

    // disk enough, momory not enough
    {
        uint64_t diskThreshold = 200;
        uint64_t diskUsed = 40;
        uint64_t memoryUsed = 190;
        uint64_t memoryThreshold = 200;
        uint64_t memoryCopySetMinRequire = 20;
        uint64_t diskCopysetMinRequire = 20;
        MetaServerSpace space(diskThreshold, diskUsed, diskCopysetMinRequire,
                              memoryThreshold, memoryUsed,
                              memoryCopySetMinRequire);
        MetaServerInfo ms(PeerInfo(1, 1, 1, "", 9000), OnlineState::ONLINE,
                          space);
        ASSERT_FALSE(ms.IsMetaserverResourceAvailable());
    }

    // disk enough, momory not enough, but momory require 0
    {
        uint64_t diskThreshold = 200;
        uint64_t diskUsed = 40;
        uint64_t memoryUsed = 210;
        uint64_t memoryThreshold = 200;
        uint64_t memoryCopySetMinRequire = 0;
        uint64_t diskCopysetMinRequire = 20;
        MetaServerSpace space(diskThreshold, diskUsed, diskCopysetMinRequire,
                              memoryThreshold, memoryUsed,
                              memoryCopySetMinRequire);
        MetaServerInfo ms(PeerInfo(1, 1, 1, "", 9000), OnlineState::ONLINE,
                          space);
        ASSERT_TRUE(ms.IsMetaserverResourceAvailable());
    }

    // disk, momory both eough
    {
        uint64_t diskThreshold = 200;
        uint64_t diskUsed = 40;
        uint64_t memoryUsed = 40;
        uint64_t memoryThreshold = 200;
        uint64_t memoryCopySetMinRequire = 20;
        uint64_t diskCopysetMinRequire = 20;
        MetaServerSpace space(diskThreshold, diskUsed, diskCopysetMinRequire,
                              memoryThreshold, memoryUsed,
                              memoryCopySetMinRequire);
        MetaServerInfo ms(PeerInfo(1, 1, 1, "", 9000), OnlineState::ONLINE,
                          space);
        ASSERT_TRUE(ms.IsMetaserverResourceAvailable());
    }

    // disk, momory both not eough
    {
        uint64_t diskThreshold = 200;
        uint64_t diskUsed = 190;
        uint64_t memoryUsed = 190;
        uint64_t memoryThreshold = 200;
        uint64_t memoryCopySetMinRequire = 20;
        uint64_t diskCopysetMinRequire = 20;
        MetaServerSpace space(diskThreshold, diskUsed, diskCopysetMinRequire,
                              memoryThreshold, memoryUsed,
                              memoryCopySetMinRequire);
        MetaServerInfo ms(PeerInfo(1, 1, 1, "", 9000), OnlineState::ONLINE,
                          space);
        ASSERT_FALSE(ms.IsMetaserverResourceAvailable());
    }
}
}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
