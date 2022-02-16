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
 * @Date: 2021-11-19 11:01:48
 * @Author: chenwei
 */

#include "curvefs/src/mds/schedule/scheduler.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "curvefs/src/mds/schedule/operatorController.h"
#include "curvefs/src/mds/schedule/scheduleMetrics.h"
#include "curvefs/src/mds/topology/topology_id_generator.h"
#include "curvefs/test/mds/mock/mock_topology.h"
#include "curvefs/test/mds/schedule/mock_topoAdapter.h"

using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;
using ::testing::_;

using ::curvefs::mds::topology::TopologyIdGenerator;
using ::curvefs::mds::topology::MockTopology;
using ::curvefs::mds::topology::MockIdGenerator;
using ::curvefs::mds::topology::MockTokenGenerator;
using ::curvefs::mds::topology::MockStorage;

namespace curvefs {
namespace mds {
namespace schedule {

class SchedulerTest : public ::testing::Test {
 protected:
    SchedulerTest() {
        topo_ = std::make_shared<MockTopology>(idGenerator_, tokenGenerator_,
                                               storage_);
        metric_ = std::make_shared<ScheduleMetrics>(topo_);
        opController_ = std::make_shared<OperatorController>(2, metric_);
        topoAdapter_ = std::make_shared<MockTopoAdapter>();
        ScheduleOption opt;
        opt.transferLeaderTimeLimitSec = 10;
        opt.removePeerTimeLimitSec = 100;
        opt.addPeerTimeLimitSec = 1000;
        opt.changePeerTimeLimitSec = 1000;
        opt.recoverSchedulerIntervalSec = 1;
        scheduler_ =
            std::make_shared<Scheduler>(opt, topoAdapter_, opController_);
    }
    ~SchedulerTest() {}

    void SetUp() override {}
    void TearDown() override {}

 public:
    std::shared_ptr<MockTopoAdapter> topoAdapter_;
    std::shared_ptr<OperatorController> opController_;
    std::shared_ptr<Scheduler> scheduler_;
    std::shared_ptr<ScheduleMetrics> metric_;
    std::shared_ptr<MockTopology> topo_;
    std::shared_ptr<MockIdGenerator> idGenerator_;
    std::shared_ptr<MockTokenGenerator> tokenGenerator_;
    std::shared_ptr<MockStorage> storage_;
};

// test1: can not get metaserver info of oldpeer
TEST_F(SchedulerTest, SelectBestPlacementMetaServer_test1) {
    CopySetInfo copySetInfo;
    MetaServerIdType oldPeer = 1;
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(oldPeer, _))
        .WillOnce(Return(false));
    MetaServerIdType metaserverId =
        scheduler_->SelectBestPlacementMetaServer(copySetInfo, oldPeer);

    ASSERT_EQ(metaserverId, UNINITIALIZE_ID);
}

// test2: get metaserver list empty
TEST_F(SchedulerTest, SelectBestPlacementMetaServer_test2) {
    CopySetInfo copySetInfo;
    PoolIdType poolId = 1;
    copySetInfo.id.first = poolId;
    MetaServerIdType oldPeer = 2;
    MetaServerInfo oldPeerInfo;
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(oldPeer, _))
        .WillOnce(DoAll(SetArgPointee<1>(oldPeerInfo), Return(true)));

    std::vector<MetaServerInfo> metaServers;
    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
        .WillOnce(Return(metaServers));

    MetaServerIdType metaserverId =
        scheduler_->SelectBestPlacementMetaServer(copySetInfo, oldPeer);
    ASSERT_EQ(metaserverId, UNINITIALIZE_ID);
}

// test3: the standard zone num in pool is 0
TEST_F(SchedulerTest, SelectBestPlacementMetaServer_test3) {
    CopySetInfo copySetInfo;
    PoolIdType poolId = 1;
    copySetInfo.id.first = poolId;
    PeerInfo peer1;
    PeerInfo peer2;
    PeerInfo peer3;
    peer1.id = 10;
    peer2.id = 11;
    peer3.id = 12;
    peer1.zoneId = 20;
    peer2.zoneId = 21;
    peer3.zoneId = 22;
    copySetInfo.peers.push_back(peer1);
    copySetInfo.peers.push_back(peer2);
    copySetInfo.peers.push_back(peer3);
    MetaServerIdType oldPeer = 2;
    MetaServerInfo oldPeerInfo;
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(oldPeer, _))
        .WillOnce(DoAll(SetArgPointee<1>(oldPeerInfo), Return(true)));

    std::vector<MetaServerInfo> metaServers;
    MetaServerInfo metaServer1;
    MetaServerInfo metaServer2;
    MetaServerInfo metaServer3;
    MetaServerInfo metaServer4;
    metaServers.push_back(metaServer1);
    metaServers.push_back(metaServer2);
    metaServers.push_back(metaServer3);
    metaServers.push_back(metaServer4);
    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
        .WillOnce(Return(metaServers));

    EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInPool(poolId))
        .WillOnce(Return(0));

    MetaServerIdType metaserverId =
        scheduler_->SelectBestPlacementMetaServer(copySetInfo, oldPeer);
    ASSERT_EQ(metaserverId, UNINITIALIZE_ID);
}

// test4: choose new metaserver fail
TEST_F(SchedulerTest, SelectBestPlacementMetaServer_test4) {
    CopySetInfo copySetInfo;
    PoolIdType poolId = 1;
    copySetInfo.id.first = poolId;
    PeerInfo peer1;
    PeerInfo peer2;
    PeerInfo peer3;
    peer1.id = 10;
    peer2.id = 11;
    peer3.id = 12;
    peer1.zoneId = 20;
    peer2.zoneId = 21;
    peer3.zoneId = 22;
    copySetInfo.peers.push_back(peer1);
    copySetInfo.peers.push_back(peer2);
    copySetInfo.peers.push_back(peer3);
    MetaServerIdType oldPeer = 2;
    MetaServerInfo oldPeerInfo;
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(oldPeer, _))
        .WillOnce(DoAll(SetArgPointee<1>(oldPeerInfo), Return(true)));

    std::vector<MetaServerInfo> metaServers;
    MetaServerInfo metaServer1;
    MetaServerInfo metaServer2;
    MetaServerInfo metaServer3;
    MetaServerInfo metaServer4;
    metaServers.push_back(metaServer1);
    metaServers.push_back(metaServer2);
    metaServers.push_back(metaServer3);
    metaServers.push_back(metaServer4);
    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
        .WillOnce(Return(metaServers));

    EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInPool(poolId))
        .WillOnce(Return(3));

    EXPECT_CALL(*topoAdapter_, ChooseNewMetaServerForCopyset(poolId, _, _, _))
        .WillOnce(Return(false));

    MetaServerIdType metaserverId =
        scheduler_->SelectBestPlacementMetaServer(copySetInfo, oldPeer);
    ASSERT_EQ(metaserverId, UNINITIALIZE_ID);
}

// test5: choose new metaserver success
TEST_F(SchedulerTest, SelectBestPlacementMetaServer_test5) {
    CopySetInfo copySetInfo;
    PoolIdType poolId = 1;
    copySetInfo.id.first = poolId;
    PeerInfo peer1;
    PeerInfo peer2;
    PeerInfo peer3;
    peer1.id = 10;
    peer2.id = 11;
    peer3.id = 12;
    peer1.zoneId = 20;
    peer2.zoneId = 21;
    peer3.zoneId = 22;
    copySetInfo.peers.push_back(peer1);
    copySetInfo.peers.push_back(peer2);
    copySetInfo.peers.push_back(peer3);
    MetaServerIdType oldPeer = 2;
    MetaServerInfo oldPeerInfo;
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(oldPeer, _))
        .WillOnce(DoAll(SetArgPointee<1>(oldPeerInfo), Return(true)));

    std::vector<MetaServerInfo> metaServers;
    MetaServerInfo metaServer1;
    MetaServerInfo metaServer2;
    MetaServerInfo metaServer3;
    MetaServerInfo metaServer4;
    metaServers.push_back(metaServer1);
    metaServers.push_back(metaServer2);
    metaServers.push_back(metaServer3);
    metaServers.push_back(metaServer4);
    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
        .WillOnce(Return(metaServers));

    EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInPool(poolId))
        .WillOnce(Return(3));

    MetaServerIdType newPeerId = 30;
    EXPECT_CALL(*topoAdapter_, ChooseNewMetaServerForCopyset(poolId, _, _, _))
        .WillOnce(DoAll(SetArgPointee<3>(newPeerId), Return(true)));

    MetaServerIdType metaserverId =
        scheduler_->SelectBestPlacementMetaServer(copySetInfo, oldPeer);
    ASSERT_EQ(metaserverId, newPeerId);
}

TEST_F(SchedulerTest, CopysetAllPeersOnline_Test) {
    CopySetInfo copySetInfo;
    PeerInfo peer1;
    PeerInfo peer2;
    PeerInfo peer3;
    peer1.id = 10;
    peer2.id = 11;
    peer3.id = 12;
    copySetInfo.peers.push_back(peer1);
    copySetInfo.peers.push_back(peer2);
    copySetInfo.peers.push_back(peer3);
    {
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(10, _))
            .WillOnce(Return(false));

        ASSERT_FALSE(scheduler_->CopysetAllPeersOnline(copySetInfo));
    }

    {
        MetaServerSpace space;
        MetaServerInfo info1(peer1, OnlineState::OFFLINE, space);
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(10, _))
            .WillOnce(DoAll(SetArgPointee<1>(info1), Return(true)));

        ASSERT_FALSE(scheduler_->CopysetAllPeersOnline(copySetInfo));
    }

    {
        MetaServerSpace space;
        MetaServerInfo info1(peer1, OnlineState::ONLINE, space);
        MetaServerInfo info2(peer2, OnlineState::OFFLINE, space);

        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(10, _))
            .WillOnce(DoAll(SetArgPointee<1>(info1), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(11, _))
            .WillOnce(DoAll(SetArgPointee<1>(info2), Return(true)));

        ASSERT_FALSE(scheduler_->CopysetAllPeersOnline(copySetInfo));
    }

    {
        MetaServerSpace space;
        MetaServerInfo info1(peer1, OnlineState::ONLINE, space);
        MetaServerInfo info2(peer2, OnlineState::ONLINE, space);
        MetaServerInfo info3(peer3, OnlineState::ONLINE, space);

        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(10, _))
            .WillOnce(DoAll(SetArgPointee<1>(info1), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(11, _))
            .WillOnce(DoAll(SetArgPointee<1>(info2), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(12, _))
            .WillOnce(DoAll(SetArgPointee<1>(info3), Return(true)));

        ASSERT_TRUE(scheduler_->CopysetAllPeersOnline(copySetInfo));
    }
}
}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
