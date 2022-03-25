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
 * @Date: 2022-02-28 10:36:31
 * @Author: chenwei
 */

#include <glog/logging.h>
#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/src/mds/schedule/operatorController.h"
#include "curvefs/src/mds/schedule/scheduleMetrics.h"
#include "curvefs/src/mds/schedule/scheduler.h"
#include "curvefs/test/mds/mock/mock_topology.h"
#include "curvefs/test/mds/schedule/common.h"
#include "curvefs/test/mds/schedule/mock_topoAdapter.h"

using ::testing::_;
using ::testing::Return;
using ::testing::AtLeast;
using ::testing::SetArgPointee;
using ::testing::DoAll;

using ::curvefs::mds::topology::MockTopology;
using ::curvefs::mds::topology::MockIdGenerator;
using ::curvefs::mds::topology::MockTokenGenerator;
using ::curvefs::mds::topology::MockStorage;

namespace curvefs {
namespace mds {
namespace schedule {
class TestCopysetSheduler : public ::testing::Test {
 protected:
    TestCopysetSheduler() {}
    ~TestCopysetSheduler() {}

    void SetUp() override {
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
        opt.copysetSchedulerIntervalSec = 1;
        opt.balanceRatioPercent = 30;
        copysetScheduler_ = std::make_shared<CopySetScheduler>(
            opt, topoAdapter_, opController_);
    }
    void TearDown() override {
        opController_ = nullptr;
        topoAdapter_ = nullptr;
        copysetScheduler_ = nullptr;
    }

 protected:
    std::shared_ptr<MockTopoAdapter> topoAdapter_;
    std::shared_ptr<OperatorController> opController_;
    std::shared_ptr<CopySetScheduler> copysetScheduler_;
    std::shared_ptr<MockIdGenerator> idGenerator_;
    std::shared_ptr<MockTokenGenerator> tokenGenerator_;
    std::shared_ptr<MockStorage> storage_;
    std::shared_ptr<MockTopology> topo_;
    std::shared_ptr<ScheduleMetrics> metric_;
};

TEST_F(TestCopysetSheduler, empty_cluster_test) {
    ASSERT_EQ(copysetScheduler_->GetRunningInterval(), 1);

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({})));
    ASSERT_EQ(copysetScheduler_->Schedule(), 0);
}

TEST_F(TestCopysetSheduler, empty_pool_test) {
    PoolIdType poolid = 1;
    std::vector<CopySetInfo> copysets;
    std::vector<MetaServerInfo> metaservers;
    std::list<ZoneIdType> zoneList;

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolid})));

    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolid))
        .WillOnce(Return(copysets));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolid))
        .WillOnce(Return(metaservers));

    EXPECT_CALL(*topoAdapter_, GetZoneInPool(poolid))
        .WillOnce(Return(zoneList));

    ASSERT_EQ(copysetScheduler_->Schedule(), 0);
}

TEST_F(TestCopysetSheduler, no_need_balance_test) {
    PoolIdType poolid = 1;
    CopySetInfo testCopySetInfo = GetCopySetInfoForTest();
    std::vector<CopySetInfo> copysets = {testCopySetInfo};
    MetaServerSpace space(100, 20);
    MetaServerInfo msInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                           space);
    MetaServerInfo msInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                           space);
    MetaServerInfo msInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                           space);
    std::vector<MetaServerInfo> metaservers = {msInfo1, msInfo2, msInfo3};
    std::list<ZoneIdType> zoneList;

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolid})));

    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolid))
        .WillOnce(Return(copysets));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolid))
        .WillOnce(Return(metaservers));

    EXPECT_CALL(*topoAdapter_, GetZoneInPool(poolid))
        .WillOnce(Return(zoneList));

    ASSERT_EQ(copysetScheduler_->Schedule(), 0);
}

TEST_F(TestCopysetSheduler, overload_get_zone_num_fail_test) {
    PoolIdType poolid = 1;
    CopySetInfo testCopySetInfo = GetCopySetInfoForTest();
    std::vector<CopySetInfo> copysets = {testCopySetInfo};
    MetaServerSpace space(100, 20);
    MetaServerSpace spaceOverload(100, 110);
    MetaServerInfo msInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                           spaceOverload);
    MetaServerInfo msInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                           space);
    MetaServerInfo msInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                           space);
    // peer4, peer5 has same zone with ms1
    PeerInfo peer4(4, 1, 1, "192.168.10.1", 9001);
    PeerInfo peer5(5, 1, 1, "192.168.10.1", 9002);
    MetaServerInfo msInfo4(peer4, OnlineState::OFFLINE, space);
    MetaServerInfo msInfo5(peer5, OnlineState::UNSTABLE, space);

    std::vector<MetaServerInfo> metaservers = {msInfo1, msInfo2, msInfo3,
                                               msInfo4, msInfo5};
    std::list<ZoneIdType> zoneList;

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolid})));

    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolid))
        .WillOnce(Return(copysets));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolid))
        .Times(1)
        .WillRepeatedly(Return(metaservers));

    EXPECT_CALL(*topoAdapter_, GetZoneInPool(poolid))
        .WillOnce(Return(zoneList));

    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolid))
        .WillOnce(Return(0));

    ASSERT_EQ(copysetScheduler_->Schedule(), 0);
}

TEST_F(TestCopysetSheduler, overload_copyset_get_metaserver_fail_test) {
    PoolIdType poolid = 1;
    CopySetInfo testCopySetInfo = GetCopySetInfoForTest();
    std::vector<CopySetInfo> copysets = {testCopySetInfo};
    MetaServerSpace space(100, 20);
    MetaServerSpace spaceOverload(100, 110);
    MetaServerInfo msInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                           spaceOverload);
    MetaServerInfo msInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                           space);
    MetaServerInfo msInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                           space);
    // peer4, peer5 has same zone with ms1
    PeerInfo peer4(4, 1, 1, "192.168.10.1", 9001);
    PeerInfo peer5(5, 1, 1, "192.168.10.1", 9002);
    MetaServerInfo msInfo4(peer4, OnlineState::OFFLINE, space);
    MetaServerInfo msInfo5(peer5, OnlineState::UNSTABLE, space);

    std::vector<MetaServerInfo> metaservers = {msInfo1, msInfo2, msInfo3,
                                               msInfo4, msInfo5};
    std::list<ZoneIdType> zoneList;

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolid})));

    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolid))
        .WillOnce(Return(copysets));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolid))
        .Times(1)
        .WillRepeatedly(Return(metaservers));

    EXPECT_CALL(*topoAdapter_, GetZoneInPool(poolid))
        .WillOnce(Return(zoneList));

    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolid))
        .WillOnce(Return(3));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo1), Return(true)));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(2, _)).WillOnce(Return(false));

    ASSERT_EQ(copysetScheduler_->Schedule(), 0);
}

TEST_F(TestCopysetSheduler, overload_copyset_has_peer_offline_test) {
    PoolIdType poolid = 1;
    CopySetInfo testCopySetInfo = GetCopySetInfoForTest();
    std::vector<CopySetInfo> copysets = {testCopySetInfo};
    MetaServerSpace space(100, 20);
    MetaServerSpace spaceOverload(100, 110);
    MetaServerInfo msInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                           spaceOverload);
    MetaServerInfo msInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                           space);
    MetaServerInfo msInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                           space);
    // peer4, peer5 has same zone with ms1
    PeerInfo peer4(4, 1, 1, "192.168.10.1", 9001);
    PeerInfo peer5(5, 1, 1, "192.168.10.1", 9002);
    MetaServerInfo msInfo4(peer4, OnlineState::OFFLINE, space);
    MetaServerInfo msInfo5(peer5, OnlineState::UNSTABLE, space);

    std::vector<MetaServerInfo> metaservers = {msInfo1, msInfo2, msInfo3,
                                               msInfo4, msInfo5};
    std::list<ZoneIdType> zoneList;

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolid})));

    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolid))
        .WillOnce(Return(copysets));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolid))
        .Times(1)
        .WillRepeatedly(Return(metaservers));

    EXPECT_CALL(*topoAdapter_, GetZoneInPool(poolid))
        .WillOnce(Return(zoneList));

    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolid))
        .WillOnce(Return(3));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo1), Return(true)));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(msInfo2), Return(true)));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(3, _))
        .WillOnce(DoAll(SetArgPointee<1>(msInfo4), Return(true)));

    ASSERT_EQ(copysetScheduler_->Schedule(), 0);
}

TEST_F(TestCopysetSheduler, overload_get_metasever_fail_test2) {
    PoolIdType poolid = 1;
    CopySetInfo testCopySetInfo = GetCopySetInfoForTest();
    std::vector<CopySetInfo> copysets = {testCopySetInfo};
    MetaServerSpace space(100, 20);
    MetaServerSpace spaceOverload(100, 110);
    MetaServerInfo msInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                           spaceOverload);
    MetaServerInfo msInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                           space);
    MetaServerInfo msInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                           space);
    // peer4, peer5 has same zone with ms1
    PeerInfo peer4(4, 1, 1, "192.168.10.1", 9001);
    PeerInfo peer5(5, 1, 1, "192.168.10.1", 9002);
    MetaServerInfo msInfo4(peer4, OnlineState::OFFLINE, space);
    MetaServerInfo msInfo5(peer5, OnlineState::UNSTABLE, space);

    std::vector<MetaServerInfo> metaservers = {msInfo1, msInfo2, msInfo3,
                                               msInfo4, msInfo5};
    std::list<ZoneIdType> zoneList;

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolid})));

    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolid))
        .WillOnce(Return(copysets));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolid))
        .Times(1)
        .WillRepeatedly(Return(metaservers));

    EXPECT_CALL(*topoAdapter_, GetZoneInPool(poolid))
        .WillOnce(Return(zoneList));

    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolid))
        .WillOnce(Return(3));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<1>(msInfo1), Return(true)))
        .WillOnce(Return(false));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(msInfo2), Return(true)));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(3, _))
        .WillOnce(DoAll(SetArgPointee<1>(msInfo3), Return(true)));

    ASSERT_EQ(copysetScheduler_->Schedule(), 0);
}

TEST_F(TestCopysetSheduler, overload_get_metasevers_fail_test2) {
    PoolIdType poolid = 1;
    CopySetInfo testCopySetInfo = GetCopySetInfoForTest();
    std::vector<CopySetInfo> copysets = {testCopySetInfo};
    MetaServerSpace space(100, 20);
    MetaServerSpace spaceOverload(100, 110);
    MetaServerInfo msInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                           spaceOverload);
    MetaServerInfo msInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                           space);
    MetaServerInfo msInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                           space);
    // peer4, peer5 has same zone with ms1
    PeerInfo peer4(4, 1, 1, "192.168.10.1", 9001);
    PeerInfo peer5(5, 1, 1, "192.168.10.1", 9002);
    MetaServerInfo msInfo4(peer4, OnlineState::OFFLINE, space);
    MetaServerInfo msInfo5(peer5, OnlineState::UNSTABLE, space);

    std::vector<MetaServerInfo> metaservers = {msInfo1, msInfo2, msInfo3,
                                               msInfo4, msInfo5};
    std::vector<MetaServerInfo> emptyMetaservers;
    std::list<ZoneIdType> zoneList;

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolid})));

    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolid))
        .WillOnce(Return(copysets));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolid))
        .Times(2)
        .WillOnce(Return(metaservers))
        .WillOnce(Return(emptyMetaservers));

    EXPECT_CALL(*topoAdapter_, GetZoneInPool(poolid))
        .WillOnce(Return(zoneList));

    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolid))
        .WillOnce(Return(3));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo1), Return(true)));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(msInfo2), Return(true)));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(3, _))
        .WillOnce(DoAll(SetArgPointee<1>(msInfo3), Return(true)));

    ASSERT_EQ(copysetScheduler_->Schedule(), 0);
}

TEST_F(TestCopysetSheduler, overload_get_zonenum_fail_test2) {
    PoolIdType poolid = 1;
    CopySetInfo testCopySetInfo = GetCopySetInfoForTest();
    std::vector<CopySetInfo> copysets = {testCopySetInfo};
    MetaServerSpace space(100, 20);
    MetaServerSpace spaceOverload(100, 110);
    MetaServerInfo msInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                           spaceOverload);
    MetaServerInfo msInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                           space);
    MetaServerInfo msInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                           space);
    // peer4, peer5 has same zone with ms1
    PeerInfo peer4(4, 1, 1, "192.168.10.1", 9001);
    PeerInfo peer5(5, 1, 1, "192.168.10.1", 9002);
    MetaServerInfo msInfo4(peer4, OnlineState::OFFLINE, space);
    MetaServerInfo msInfo5(peer5, OnlineState::UNSTABLE, space);

    std::vector<MetaServerInfo> metaservers = {msInfo1, msInfo2, msInfo3,
                                               msInfo4, msInfo5};
    std::vector<MetaServerInfo> emptyMetaservers;
    std::list<ZoneIdType> zoneList;

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolid})));

    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolid))
        .WillOnce(Return(copysets));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolid))
        .Times(2)
        .WillRepeatedly(Return(metaservers));

    EXPECT_CALL(*topoAdapter_, GetZoneInPool(poolid))
        .WillOnce(Return(zoneList));

    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolid))
        .WillOnce(Return(3));

    EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInPool(poolid))
        .WillOnce(Return(0));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo1), Return(true)));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(msInfo2), Return(true)));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(3, _))
        .WillOnce(DoAll(SetArgPointee<1>(msInfo3), Return(true)));

    ASSERT_EQ(copysetScheduler_->Schedule(), 0);
}

TEST_F(TestCopysetSheduler, overload_no_available_metaserver_test) {
    PoolIdType poolid = 1;
    CopySetInfo testCopySetInfo = GetCopySetInfoForTest();
    std::vector<CopySetInfo> copysets = {testCopySetInfo};
    MetaServerSpace space(100, 20);
    MetaServerSpace spaceOverload(100, 110);
    MetaServerInfo msInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                           spaceOverload);
    MetaServerInfo msInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                           space);
    MetaServerInfo msInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                           space);
    // peer4, peer5 has same zone with ms1
    PeerInfo peer4(4, 1, 1, "192.168.10.1", 9001);
    PeerInfo peer5(5, 1, 1, "192.168.10.1", 9002);
    MetaServerInfo msInfo4(peer4, OnlineState::OFFLINE, space);
    MetaServerInfo msInfo5(peer5, OnlineState::UNSTABLE, space);

    std::vector<MetaServerInfo> metaservers = {msInfo1, msInfo2, msInfo3,
                                               msInfo4, msInfo5};
    std::list<ZoneIdType> zoneList;

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolid})));

    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolid))
        .WillOnce(Return(copysets));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolid))
        .Times(2)
        .WillRepeatedly(Return(metaservers));

    EXPECT_CALL(*topoAdapter_, GetZoneInPool(poolid))
        .WillOnce(Return(zoneList));

    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolid))
        .WillOnce(Return(3));

    EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInPool(poolid))
        .WillOnce(Return(3));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo1), Return(true)));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(msInfo2), Return(true)));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(3, _))
        .WillOnce(DoAll(SetArgPointee<1>(msInfo3), Return(true)));

    EXPECT_CALL(*topoAdapter_, ChooseNewMetaServerForCopyset(poolid, _, _, _))
        .WillOnce(Return(false));

    ASSERT_EQ(copysetScheduler_->Schedule(), 0);
}

TEST_F(TestCopysetSheduler, overload_create_copyset_fail_test) {
    PoolIdType poolid = 1;
    CopySetInfo testCopySetInfo = GetCopySetInfoForTest();
    std::vector<CopySetInfo> copysets = {testCopySetInfo};
    MetaServerSpace space(100, 20);
    MetaServerSpace spaceOverload(100, 110);
    MetaServerInfo msInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                           spaceOverload);
    MetaServerInfo msInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                           space);
    MetaServerInfo msInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                           space);
    // peer4, peer5 has same zone with ms1
    PeerInfo peer4(4, 1, 1, "192.168.10.1", 9001);
    PeerInfo peer5(5, 1, 1, "192.168.10.1", 9002);
    MetaServerInfo msInfo4(peer4, OnlineState::ONLINE, space);
    MetaServerInfo msInfo5(peer5, OnlineState::UNSTABLE, space);

    std::vector<MetaServerInfo> metaservers = {msInfo1, msInfo2, msInfo3,
                                               msInfo4, msInfo5};
    std::list<ZoneIdType> zoneList;

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolid})));

    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolid))
        .WillOnce(Return(copysets));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolid))
        .Times(2)
        .WillRepeatedly(Return(metaservers));

    EXPECT_CALL(*topoAdapter_, GetZoneInPool(poolid))
        .WillOnce(Return(zoneList));

    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolid))
        .WillOnce(Return(3));

    EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInPool(poolid))
        .WillOnce(Return(3));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo1), Return(true)));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(msInfo2), Return(true)));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(3, _))
        .WillOnce(DoAll(SetArgPointee<1>(msInfo3), Return(true)));

    MetaServerIdType destId = 4;
    EXPECT_CALL(*topoAdapter_, ChooseNewMetaServerForCopyset(poolid, _, _, _))
        .WillOnce(DoAll(SetArgPointee<3>(destId), Return(true)));

    EXPECT_CALL(*topoAdapter_,
                CreateCopySetAtMetaServer(testCopySetInfo.id, destId))
        .WillOnce(Return(false));

    ASSERT_EQ(copysetScheduler_->Schedule(), 0);
}

TEST_F(TestCopysetSheduler, overload_balance_success_test) {
    PoolIdType poolid = 1;
    CopySetInfo testCopySetInfo = GetCopySetInfoForTest();
    std::vector<CopySetInfo> copysets = {testCopySetInfo};
    MetaServerSpace space(100, 20);
    MetaServerSpace spaceOverload(100, 110);
    MetaServerInfo msInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                           spaceOverload);
    MetaServerInfo msInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                           space);
    MetaServerInfo msInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                           space);
    // peer4, peer5 has same zone with ms1
    PeerInfo peer4(4, 1, 1, "192.168.10.1", 9001);
    PeerInfo peer5(5, 1, 1, "192.168.10.1", 9002);
    MetaServerInfo msInfo4(peer4, OnlineState::ONLINE, space);
    MetaServerInfo msInfo5(peer5, OnlineState::UNSTABLE, space);

    std::vector<MetaServerInfo> metaservers = {msInfo1, msInfo2, msInfo3,
                                               msInfo4, msInfo5};
    std::list<ZoneIdType> zoneList;

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolid})));

    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolid))
        .WillOnce(Return(copysets));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolid))
        .Times(2)
        .WillRepeatedly(Return(metaservers));

    // EXPECT_CALL(*topoAdapter_, GetZoneInPool(poolid))
    //     .WillOnce(Return(zoneList));

    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolid))
        .WillOnce(Return(3));

    EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInPool(poolid))
        .WillOnce(Return(3));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo1), Return(true)));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(msInfo2), Return(true)));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(3, _))
        .WillOnce(DoAll(SetArgPointee<1>(msInfo3), Return(true)));

    MetaServerIdType destId = 4;
    EXPECT_CALL(*topoAdapter_, ChooseNewMetaServerForCopyset(poolid, _, _, _))
        .WillOnce(DoAll(SetArgPointee<3>(destId), Return(true)));

    EXPECT_CALL(*topoAdapter_,
                CreateCopySetAtMetaServer(testCopySetInfo.id, destId))
        .WillOnce(Return(true));

    ASSERT_EQ(copysetScheduler_->Schedule(), 1);
}

TEST_F(TestCopysetSheduler, normal_no_dest_metaserver_test) {
    PoolIdType poolid = 1;
    CopySetInfo testCopySetInfo = GetCopySetInfoForTest();
    std::vector<CopySetInfo> copysets = {testCopySetInfo};
    MetaServerSpace space(100, 20);
    MetaServerSpace space2(100, 70);
    MetaServerSpace space3(100, 30);
    MetaServerInfo msInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                           space);
    MetaServerInfo msInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                           space);
    MetaServerInfo msInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                           space);
    // peer4, peer5 has same zone with ms1
    PeerInfo peer4(4, 1, 1, "192.168.10.1", 9001);
    PeerInfo peer5(5, 1, 1, "192.168.10.1", 9002);

    // peer6 has same zone with ms2
    PeerInfo peer6(6, 2, 2, "192.168.10.2", 9001);

    MetaServerInfo msInfo4(peer4, OnlineState::ONLINE, space);
    MetaServerInfo msInfo5(peer5, OnlineState::OFFLINE, space2);
    MetaServerInfo msInfo6(peer6, OnlineState::ONLINE, space);

    std::vector<MetaServerInfo> metaservers = {msInfo1, msInfo2, msInfo3,
                                               msInfo4, msInfo5, msInfo6};
    std::list<ZoneIdType> zoneList = {1, 2, 3};

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolid})));

    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolid))
        .WillOnce(Return(copysets));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolid))
        .Times(1)
        .WillRepeatedly(Return(metaservers));

    EXPECT_CALL(*topoAdapter_, GetZoneInPool(poolid))
        .WillOnce(Return(zoneList));

    std::vector<MetaServerInfo> metaseverZone1 = {msInfo1, msInfo4, msInfo5};

    std::vector<MetaServerInfo> metaseverZone2 = {msInfo2, msInfo6};

    std::vector<MetaServerInfo> metaseverZone3 = {msInfo3};

    EXPECT_CALL(*topoAdapter_, GetMetaServersInZone(1))
        .WillOnce(Return(metaseverZone1));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInZone(2))
        .WillOnce(Return(metaseverZone2));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInZone(3))
        .WillOnce(Return(metaseverZone3));

    ASSERT_EQ(copysetScheduler_->Schedule(), 0);
}

TEST_F(TestCopysetSheduler, normal_get_copyset_fail_test) {
    PoolIdType poolid = 1;
    CopySetInfo testCopySetInfo = GetCopySetInfoForTest();
    std::vector<CopySetInfo> copysets = {testCopySetInfo};
    MetaServerSpace space(100, 20);
    MetaServerSpace space2(100, 70);
    MetaServerSpace space3(100, 30);
    MetaServerInfo msInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                           space);
    MetaServerInfo msInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                           space);
    MetaServerInfo msInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                           space);
    // peer4, peer5 has same zone with ms1
    PeerInfo peer4(4, 1, 1, "192.168.10.1", 9001);
    PeerInfo peer5(5, 1, 1, "192.168.10.1", 9002);

    // peer6 has same zone with ms2
    PeerInfo peer6(6, 2, 2, "192.168.10.2", 9001);

    MetaServerInfo msInfo4(peer4, OnlineState::ONLINE, space);
    MetaServerInfo msInfo5(peer5, OnlineState::ONLINE, space2);
    MetaServerInfo msInfo6(peer6, OnlineState::ONLINE, space);

    std::vector<MetaServerInfo> metaservers = {msInfo1, msInfo2, msInfo3,
                                               msInfo4, msInfo5, msInfo6};
    std::list<ZoneIdType> zoneList = {1, 2, 3};

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolid})));

    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolid))
        .WillOnce(Return(copysets));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolid))
        .Times(1)
        .WillRepeatedly(Return(metaservers));

    EXPECT_CALL(*topoAdapter_, GetZoneInPool(poolid))
        .WillOnce(Return(zoneList));

    std::vector<MetaServerInfo> metaseverZone1 = {msInfo1, msInfo4, msInfo5};

    std::vector<MetaServerInfo> metaseverZone2 = {msInfo2, msInfo6};

    std::vector<MetaServerInfo> metaseverZone3 = {msInfo3};

    EXPECT_CALL(*topoAdapter_, GetMetaServersInZone(1))
        .WillOnce(Return(metaseverZone1));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInZone(2))
        .WillOnce(Return(metaseverZone2));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInZone(3))
        .WillOnce(Return(metaseverZone3));

    std::vector<CopySetInfo> copysetVector;
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInMetaServer(5))
        .WillOnce(Return(copysetVector));

    ASSERT_EQ(copysetScheduler_->Schedule(), 0);
}

TEST_F(TestCopysetSheduler, normal_balanca_success_test) {
    PoolIdType poolid = 1;
    CopySetInfo testCopySetInfo = GetCopySetInfoForTest();
    std::vector<CopySetInfo> copysets = {testCopySetInfo};
    MetaServerSpace space(100, 20);
    MetaServerSpace space2(100, 70);
    MetaServerSpace space3(100, 30);
    MetaServerInfo msInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                           space2);
    MetaServerInfo msInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                           space);
    MetaServerInfo msInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                           space);
    // peer4, peer5 has same zone with ms1
    PeerInfo peer4(4, 1, 1, "192.168.10.1", 9001);
    PeerInfo peer5(5, 1, 1, "192.168.10.1", 9002);

    // peer6 has same zone with ms2
    PeerInfo peer6(6, 2, 2, "192.168.10.2", 9001);

    MetaServerInfo msInfo4(peer4, OnlineState::ONLINE, space);
    MetaServerInfo msInfo5(peer5, OnlineState::ONLINE, space3);
    MetaServerInfo msInfo6(peer6, OnlineState::ONLINE, space);

    std::vector<MetaServerInfo> metaservers = {msInfo1, msInfo2, msInfo3,
                                               msInfo4, msInfo5, msInfo6};
    std::list<ZoneIdType> zoneList = {1, 2, 3};

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolid})));

    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolid))
        .WillOnce(Return(copysets));

    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolid))
        .Times(1)
        .WillRepeatedly(Return(metaservers));

    EXPECT_CALL(*topoAdapter_, GetZoneInPool(poolid))
        .WillOnce(Return(zoneList));

    std::vector<MetaServerInfo> metaseverZone1 = {msInfo1, msInfo4, msInfo5};

    std::vector<MetaServerInfo> metaseverZone2 = {msInfo2, msInfo6};

    std::vector<MetaServerInfo> metaseverZone3 = {msInfo3};

    EXPECT_CALL(*topoAdapter_, GetMetaServersInZone(1))
        .WillOnce(Return(metaseverZone1));

    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInMetaServer(1))
        .WillOnce(Return(copysets));

    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolid))
        .WillOnce(Return(3));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo1), Return(true)));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(msInfo2), Return(true)));

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(3, _))
        .WillOnce(DoAll(SetArgPointee<1>(msInfo3), Return(true)));

    EXPECT_CALL(*topoAdapter_,
                CreateCopySetAtMetaServer(testCopySetInfo.id, 4))
        .WillOnce(Return(true));

    ASSERT_EQ(copysetScheduler_->Schedule(), 1);
}

}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
