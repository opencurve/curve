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
 * @Date: 2022-04-08 16:32:16
 * @Author: chenwei
 */

#include <glog/logging.h>
#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/src/mds/schedule/operatorController.h"
#include "curvefs/src/mds/schedule/scheduleMetrics.h"
#include "curvefs/src/mds/schedule/scheduler.h"
#include "curvefs/src/mds/topology/topology_id_generator.h"
#include "curvefs/test/mds/mock/mock_topology.h"
#include "curvefs/test/mds/schedule/common.h"
#include "curvefs/test/mds/mock/mock_topoAdapter.h"

using ::curvefs::mds::topology::MockTopology;
using ::curvefs::mds::topology::MockIdGenerator;
using ::curvefs::mds::topology::MockTokenGenerator;
using ::curvefs::mds::topology::MockStorage;
using ::std::chrono::steady_clock;

using ::testing::_;
using ::testing::Return;
using ::testing::AtLeast;
using ::testing::SetArgPointee;
using ::testing::DoAll;

namespace curvefs {
namespace mds {
namespace schedule {
class TestLeaderSchedule : public ::testing::Test {
 protected:
    TestLeaderSchedule() {}
    ~TestLeaderSchedule() {}

    void SetUp() override {
        topo_ = std::make_shared<MockTopology>(idGenerator_, tokenGenerator_,
                                               storage_);
        metric_ = std::make_shared<ScheduleMetrics>(topo_);
        opController_ = std::make_shared<OperatorController>(2, metric_);
        topoAdapter_ = std::make_shared<MockTopoAdapter>();

        opt_.transferLeaderTimeLimitSec = 10;
        opt_.removePeerTimeLimitSec = 100;
        opt_.addPeerTimeLimitSec = 1000;
        opt_.changePeerTimeLimitSec = 1000;
        opt_.leaderSchedulerIntervalSec = 1;
        opt_.metaserverCoolingTimeSec = 10;
        leaderScheduler_ = std::make_shared<LeaderScheduler>(
            opt_, topoAdapter_, opController_);
    }

    void TearDown() override {
        topoAdapter_ = nullptr;
        opController_ = nullptr;
        leaderScheduler_ = nullptr;
        metric_ = nullptr;
        topo_ = nullptr;
        storage_ = nullptr;
        tokenGenerator_ = nullptr;
        idGenerator_ = nullptr;
    }

 protected:
    std::shared_ptr<MockTopoAdapter> topoAdapter_;
    std::shared_ptr<OperatorController> opController_;
    std::shared_ptr<LeaderScheduler> leaderScheduler_;
    std::shared_ptr<MockIdGenerator> idGenerator_;
    std::shared_ptr<MockTokenGenerator> tokenGenerator_;
    std::shared_ptr<MockStorage> storage_;
    std::shared_ptr<MockTopology> topo_;
    std::shared_ptr<ScheduleMetrics> metric_;
    ScheduleOption opt_;
};

TEST_F(TestLeaderSchedule, test_no_pool) {
    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({})));
    ASSERT_EQ(leaderScheduler_->Schedule(), 0);
    ASSERT_EQ(0, opController_->GetOperators().size());
}

TEST_F(TestLeaderSchedule, test_get_replica_num_fail) {
    PoolIdType poolId = 1;
    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolId})));
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolId))
        .WillOnce(Return(0));
    ASSERT_EQ(leaderScheduler_->Schedule(), 0);
    ASSERT_EQ(0, opController_->GetOperators().size());
}

TEST_F(TestLeaderSchedule, test_no_metaserverInfos) {
    PoolIdType poolId = 1;
    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolId})));
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolId))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
        .WillOnce(Return(std::vector<MetaServerInfo>()));
    ASSERT_EQ(leaderScheduler_->Schedule(), 0);
    ASSERT_EQ(0, opController_->GetOperators().size());
}

TEST_F(TestLeaderSchedule, test_has_metaServer_offline) {
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    MetaServerSpace space(100, 20);
    MetaServerInfo msInfo1(peer1, OnlineState::OFFLINE, space);
    MetaServerInfo msInfo2(peer2, OnlineState::ONLINE, space);
    MetaServerInfo msInfo3(peer3, OnlineState::ONLINE, space);
    std::vector<MetaServerInfo> msInfos({msInfo1, msInfo2, msInfo3});

    PoolIdType poolId = 1;
    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolId})));
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolId))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
        .WillOnce(Return(msInfos));
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo1), Return(true)));

    ASSERT_EQ(leaderScheduler_->Schedule(), 0);
    ASSERT_EQ(0, opController_->GetOperators().size());
}

TEST_F(TestLeaderSchedule, test_copySet_has_candidate) {
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    MetaServerSpace space(100, 20);
    MetaServerInfo msInfo1(peer1, OnlineState::ONLINE, space, 10, 2);
    MetaServerInfo msInfo2(peer2, OnlineState::ONLINE, space, 10, 5);
    MetaServerInfo msInfo3(peer3, OnlineState::ONLINE, space, 10, 3);
    std::vector<MetaServerInfo> msInfos({msInfo1, msInfo2, msInfo3});

    PoolIdType poolId = 1;
    CopySetIdType copysetId = 1;
    CopySetKey copySetKey;
    copySetKey.first = poolId;
    copySetKey.second = copysetId;
    EpochType epoch = 1;
    MetaServerIdType leader = 2;
    std::vector<PeerInfo> peers({peer1, peer2, peer3});
    CopySetInfo copySet1(copySetKey, epoch, leader, peers, ConfigChangeInfo{});
    copySet1.candidatePeerInfo = peer1;
    std::vector<CopySetInfo> copySetInfos({copySet1});

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolId})));
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolId))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
        .WillOnce(Return(msInfos));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolId))
        .WillRepeatedly(Return(copySetInfos));

    ASSERT_EQ(leaderScheduler_->Schedule(), 0);
    ASSERT_EQ(0, opController_->GetOperators().size());
}

TEST_F(TestLeaderSchedule, test_cannot_get_metaServerInfo) {
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    MetaServerSpace space(100, 20);
    MetaServerInfo msInfo1(peer1, OnlineState::ONLINE, space, 10, 2);
    MetaServerInfo msInfo2(peer2, OnlineState::ONLINE, space, 10, 5);
    MetaServerInfo msInfo3(peer3, OnlineState::ONLINE, space, 10, 3);
    std::vector<MetaServerInfo> msInfos({msInfo1, msInfo2, msInfo3});

    PoolIdType poolId = 1;
    CopySetIdType copysetId = 1;
    CopySetKey copySetKey;
    copySetKey.first = poolId;
    copySetKey.second = copysetId;
    EpochType epoch = 1;
    MetaServerIdType leader = 2;
    CopySetInfo copySet1(copySetKey, epoch, leader,
        std::vector<PeerInfo>({peer1, peer2, peer3}),
        ConfigChangeInfo{});
    std::vector<CopySetInfo> copySetInfos({copySet1});

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolId})));
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolId))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
        .WillOnce(Return(msInfos));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolId))
        .WillRepeatedly(Return(copySetInfos));
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
        .WillRepeatedly(Return(false));


    ASSERT_EQ(leaderScheduler_->Schedule(), 0);
    ASSERT_EQ(0, opController_->GetOperators().size());
}

TEST_F(TestLeaderSchedule, test_no_need_tranferLeaderOut) {
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    MetaServerSpace space(100, 20);
    MetaServerInfo msInfo1(peer1, OnlineState::ONLINE, space, 10, 5);
    MetaServerInfo msInfo2(peer2, OnlineState::ONLINE, space, 10, 3);
    MetaServerInfo msInfo3(peer3, OnlineState::ONLINE, space, 10, 3);
    msInfo3.startUpTime = 3;
    std::vector<MetaServerInfo> msInfos({msInfo1, msInfo2, msInfo3});

    PoolIdType poolId = 1;
    CopySetIdType copysetId = 1;
    CopySetKey copySetKey;
    copySetKey.first = poolId;
    copySetKey.second = copysetId;
    EpochType epoch = 1;
    MetaServerIdType leader = 2;
    std::vector<PeerInfo> peers({peer1, peer2, peer3});
    CopySetInfo copySet1(copySetKey, epoch, leader, peers, ConfigChangeInfo{});
    std::vector<CopySetInfo> copySetInfos({copySet1});

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolId})));
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolId))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolId))
        .WillRepeatedly(Return(copySetInfos));
    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
        .WillOnce(Return(msInfos));

    ASSERT_EQ(leaderScheduler_->Schedule(), 0);
    ASSERT_EQ(0, opController_->GetOperators().size());
}

TEST_F(TestLeaderSchedule, test_tranferLeaderout_normal) {
    //                ms1     ms2    ms3    ms4     ms5    ms6
    // leaderCount     3       5      2      3       3      3
    // copyset         10      10     10     10      10     10
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    PeerInfo peer4(4, 4, 4, "192.168.10.4", 9000);
    PeerInfo peer5(5, 5, 5, "192.168.10.5", 9000);
    PeerInfo peer6(6, 6, 6, "192.168.10.6", 9000);
    MetaServerSpace space(100, 20);
    MetaServerInfo msInfo1(peer1, OnlineState::ONLINE, space, 10, 3);
    MetaServerInfo msInfo2(peer2, OnlineState::ONLINE, space, 10, 5);
    MetaServerInfo msInfo3(peer3, OnlineState::ONLINE, space, 10, 2);
    MetaServerInfo msInfo4(peer4, OnlineState::ONLINE, space, 10, 3);
    MetaServerInfo msInfo5(peer5, OnlineState::ONLINE, space, 10, 3);
    MetaServerInfo msInfo6(peer5, OnlineState::ONLINE, space, 10, 3);
    struct timeval tm;
    gettimeofday(&tm, NULL);
    msInfo3.startUpTime = tm.tv_sec - opt_.metaserverCoolingTimeSec - 1;
    std::vector<MetaServerInfo> msInfos({msInfo1, msInfo2, msInfo3,
                                          msInfo4, msInfo5, msInfo6});

    PoolIdType poolId = 1;
    CopySetIdType copysetId = 1;
    CopySetKey copySetKey;
    copySetKey.first = poolId;
    copySetKey.second = copysetId;
    EpochType epoch = 1;
    MetaServerIdType leader = 2;
    CopySetInfo copySet1(copySetKey, epoch, leader,
        std::vector<PeerInfo>({peer1, peer2, peer3}),
        ConfigChangeInfo{});
    CopySetInfo copySet2(copySetKey, epoch, peer4.id,
        std::vector<PeerInfo>({peer4, peer5, peer6}),
        ConfigChangeInfo{});
    std::vector<CopySetInfo> copySetInfos({copySet1, copySet2});

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolId})));
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolId))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
        .WillOnce(Return(msInfos));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInMetaServer(leader))
        .WillOnce(Return(copySetInfos));
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo1), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(msInfo2), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(3, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo3), Return(true)));

    leaderScheduler_->Schedule();
    Operator op;
    ASSERT_TRUE(opController_->GetOperatorById(copySet1.id, &op));
    ASSERT_EQ(OperatorPriority::NormalPriority, op.priority);
    ASSERT_EQ(std::chrono::seconds(10), op.timeLimit);
    TransferLeader *res = dynamic_cast<TransferLeader *>(op.step.get());
    ASSERT_TRUE(res != nullptr);
    ASSERT_EQ(msInfo3.info.id, res->GetTargetPeer());
}

TEST_F(TestLeaderSchedule, test_tranferLeaderout_rapid) {
    //                ms1     ms2    ms3    ms4     ms5    ms6
    // leaderCount     3       5      2      3       3      3
    // copyset         10      10     10     10      10     10
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    PeerInfo peer4(4, 4, 4, "192.168.10.4", 9000);
    PeerInfo peer5(5, 5, 5, "192.168.10.5", 9000);
    PeerInfo peer6(6, 6, 6, "192.168.10.6", 9000);
    MetaServerSpace space(100, 20);
    MetaServerInfo msInfo1(peer1, OnlineState::ONLINE, space, 10, 3);
    MetaServerInfo msInfo2(peer2, OnlineState::ONLINE, space, 10, 5);
    MetaServerInfo msInfo3(peer3, OnlineState::ONLINE, space, 10, 2);
    MetaServerInfo msInfo4(peer4, OnlineState::ONLINE, space, 10, 3);
    MetaServerInfo msInfo5(peer5, OnlineState::ONLINE, space, 10, 3);
    MetaServerInfo msInfo6(peer6, OnlineState::ONLINE, space, 10, 3);
    MetaServerInfo msInfo7(peer2, OnlineState::ONLINE, space, 10, 3);
    MetaServerInfo msInfo8(peer3, OnlineState::ONLINE, space, 10, 3);
    std::vector<MetaServerInfo> msInfos({msInfo1, msInfo2, msInfo3,
                                          msInfo4, msInfo5, msInfo6});
    std::vector<MetaServerInfo> msInfos2({msInfo1, msInfo7, msInfo8,
                                          msInfo4, msInfo5, msInfo6});

    PoolIdType poolId = 1;
    CopySetIdType copysetId = 1;
    CopySetKey copySetKey;
    copySetKey.first = poolId;
    copySetKey.second = copysetId;
    EpochType epoch = 1;
    MetaServerIdType leader = 2;
    CopySetInfo copySet1(copySetKey, epoch, leader,
        std::vector<PeerInfo>({peer1, peer2, peer3}),
        ConfigChangeInfo{});
    CopySetInfo copySet2(copySetKey, epoch, peer4.id,
        std::vector<PeerInfo>({peer4, peer5, peer6}),
        ConfigChangeInfo{});
    std::vector<CopySetInfo> copySetInfos({copySet1, copySet2});

    // default FLAGS_enableRapidLeaderScheduler is false, can not select target
    {
        EXPECT_CALL(*topoAdapter_, Getpools())
            .WillOnce(Return(std::vector<PoolIdType>({poolId})));
        EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolId))
            .WillOnce(Return(3));
        EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
            .WillOnce(Return(msInfos));
        EXPECT_CALL(*topoAdapter_, GetCopySetInfosInMetaServer(leader))
            .WillOnce(Return(copySetInfos));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo1), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(msInfo2), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(3, _))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo3), Return(true)));

        ASSERT_FALSE(leaderScheduler_->GetRapidLeaderSchedulerFlag());
        ASSERT_EQ(0, leaderScheduler_->Schedule());
        ASSERT_FALSE(leaderScheduler_->GetRapidLeaderSchedulerFlag());
    }

    {
        // set FLAGS_enableRapidLeaderScheduler to true
        EXPECT_CALL(*topoAdapter_, Getpools())
            .WillOnce(Return(std::vector<PoolIdType>({poolId})));
        EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolId))
            .WillOnce(Return(3));
        EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
            .WillOnce(Return(msInfos));
        EXPECT_CALL(*topoAdapter_, GetCopySetInfosInMetaServer(leader))
            .WillOnce(Return(copySetInfos));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo1), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(msInfo2), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(3, _))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo3), Return(true)));

        leaderScheduler_->SetRapidLeaderSchedulerFlag(true);
        ASSERT_TRUE(leaderScheduler_->GetRapidLeaderSchedulerFlag());
        ASSERT_EQ(1, leaderScheduler_->Schedule());
        ASSERT_TRUE(leaderScheduler_->GetRapidLeaderSchedulerFlag());

        Operator op;
        ASSERT_TRUE(opController_->GetOperatorById(copySet1.id, &op));
        ASSERT_EQ(OperatorPriority::NormalPriority, op.priority);
        ASSERT_EQ(std::chrono::seconds(10), op.timeLimit);
        TransferLeader *res = dynamic_cast<TransferLeader *>(op.step.get());
        ASSERT_TRUE(res != nullptr);
        ASSERT_EQ(msInfo3.info.id, res->GetTargetPeer());
    }

    {
        // no schedule operator generate, FLAGS_enableRapidLeaderScheduler is
        // set to false automaticlly
        EXPECT_CALL(*topoAdapter_, Getpools())
            .WillOnce(Return(std::vector<PoolIdType>({poolId})));
        EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolId))
            .WillOnce(Return(3));
        EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
            .WillOnce(Return(msInfos2));
        ASSERT_TRUE(leaderScheduler_->GetRapidLeaderSchedulerFlag());
        ASSERT_EQ(0, leaderScheduler_->Schedule());
        ASSERT_FALSE(leaderScheduler_->GetRapidLeaderSchedulerFlag());
    }
}

TEST_F(TestLeaderSchedule, test_transferLeaderIn_normal) {
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    PeerInfo peer4(3, 4, 4, "192.168.10.4", 9000);
    MetaServerSpace space(100, 20);
    MetaServerInfo msInfo1(peer1, OnlineState::ONLINE, space, 10, 2);
    msInfo1.startUpTime = ::curve::common::TimeUtility::GetTimeofDaySec()
                                        - opt_.metaserverCoolingTimeSec - 1;
    MetaServerInfo msInfo2(peer2, OnlineState::ONLINE, space, 10, 3);
    MetaServerInfo msInfo3(peer3, OnlineState::ONLINE, space, 10, 4);
    MetaServerInfo msInfo4(peer4, OnlineState::ONLINE, space, 10, 3);
    std::vector<MetaServerInfo> msInfos({msInfo1, msInfo2, msInfo3, msInfo4});

    PoolIdType poolId = 1;
    CopySetIdType copysetId1 = 1;
    CopySetIdType copysetId2 = 2;
    EpochType epoch = 1;
    // has operator
    CopySetInfo copySet1(CopySetKey(poolId, copysetId1), epoch, 3,
        std::vector<PeerInfo>({peer1, peer2, peer3}),
        ConfigChangeInfo{});
    // not include metaserver1
    CopySetInfo copySet2(CopySetKey(poolId, copysetId2), epoch, 4,
        std::vector<PeerInfo>({peer2, peer3, peer4}),
        ConfigChangeInfo{});

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolId})));
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolId))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
        .WillOnce(Return(msInfos));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolId))
        .WillOnce(Return(std::vector<CopySetInfo>{copySet1, copySet2}));
     EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo1), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(2, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo2), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(3, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo3), Return(true)));

    ASSERT_EQ(1, leaderScheduler_->Schedule());
    Operator op;
    ASSERT_TRUE(opController_->GetOperatorById(copySet1.id, &op));
    ASSERT_EQ(OperatorPriority::NormalPriority, op.priority);
    ASSERT_EQ(std::chrono::seconds(10), op.timeLimit);
    TransferLeader *res = dynamic_cast<TransferLeader *>(op.step.get());
    ASSERT_TRUE(res != nullptr);
    ASSERT_EQ(1, res->GetTargetPeer());
}

TEST_F(TestLeaderSchedule, test_transferLeaderIn_rapid) {
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    PeerInfo peer4(3, 4, 4, "192.168.10.4", 9000);
    MetaServerSpace space(100, 20);
    MetaServerInfo msInfo1(peer1, OnlineState::ONLINE, space, 10, 2);
    msInfo1.startUpTime = ::curve::common::TimeUtility::GetTimeofDaySec();
    MetaServerInfo msInfo2(peer2, OnlineState::ONLINE, space, 10, 3);
    MetaServerInfo msInfo3(peer3, OnlineState::ONLINE, space, 10, 4);
    MetaServerInfo msInfo4(peer4, OnlineState::ONLINE, space, 10, 3);
    MetaServerInfo msInfo5(peer1, OnlineState::ONLINE, space, 10, 3);
    msInfo5.startUpTime = ::curve::common::TimeUtility::GetTimeofDaySec();
    std::vector<MetaServerInfo> msInfos({msInfo1, msInfo2, msInfo3, msInfo4});
    std::vector<MetaServerInfo> msInfos2({msInfo5, msInfo2, msInfo3, msInfo4});

    PoolIdType poolId = 1;
    CopySetIdType copysetId1 = 1;
    CopySetIdType copysetId2 = 2;
    EpochType epoch = 1;
    // has operator
    CopySetInfo copySet1(CopySetKey(poolId, copysetId1), epoch, 3,
        std::vector<PeerInfo>({peer1, peer2, peer3}),
        ConfigChangeInfo{});
    // not include metaserver1
    CopySetInfo copySet2(CopySetKey(poolId, copysetId2), epoch, 4,
        std::vector<PeerInfo>({peer2, peer3, peer4}),
        ConfigChangeInfo{});

    // default FLAGS_enableRapidLeaderScheduler is false, can not select target
    // to transfer leader in
    {
        EXPECT_CALL(*topoAdapter_, Getpools())
            .WillOnce(Return(std::vector<PoolIdType>({poolId})));
        EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolId))
            .WillOnce(Return(3));
        EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
            .WillOnce(Return(msInfos));

        ASSERT_FALSE(leaderScheduler_->GetRapidLeaderSchedulerFlag());
        ASSERT_EQ(0, leaderScheduler_->Schedule());
        ASSERT_FALSE(leaderScheduler_->GetRapidLeaderSchedulerFlag());
    }

    // set FLAGS_enableRapidLeaderScheduler to true
    {
        EXPECT_CALL(*topoAdapter_, Getpools())
            .WillOnce(Return(std::vector<PoolIdType>({poolId})));
        EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolId))
            .WillOnce(Return(3));
        EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
            .WillOnce(Return(msInfos));
        EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolId))
            .WillOnce(Return(std::vector<CopySetInfo>{copySet1, copySet2}));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
            .Times(1)
            .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo1), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(2, _))
            .Times(1)
            .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo2), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(3, _))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo3), Return(true)));

        leaderScheduler_->SetRapidLeaderSchedulerFlag(true);
        ASSERT_TRUE(leaderScheduler_->GetRapidLeaderSchedulerFlag());
        ASSERT_EQ(1, leaderScheduler_->Schedule());
        ASSERT_TRUE(leaderScheduler_->GetRapidLeaderSchedulerFlag());
        Operator op;
        ASSERT_TRUE(opController_->GetOperatorById(copySet1.id, &op));
        ASSERT_EQ(OperatorPriority::NormalPriority, op.priority);
        ASSERT_EQ(std::chrono::seconds(10), op.timeLimit);
        TransferLeader *res = dynamic_cast<TransferLeader *>(op.step.get());
        ASSERT_TRUE(res != nullptr);
        ASSERT_EQ(1, res->GetTargetPeer());
    }

    // no schedule operator generate, FLAGS_enableRapidLeaderScheduler is
    // set to false automaticlly
    {
        EXPECT_CALL(*topoAdapter_, Getpools())
            .WillOnce(Return(std::vector<PoolIdType>({poolId})));
        EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolId))
            .WillOnce(Return(3));
        EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
            .WillOnce(Return(msInfos2));

        ASSERT_TRUE(leaderScheduler_->GetRapidLeaderSchedulerFlag());
        ASSERT_EQ(0, leaderScheduler_->Schedule());
        ASSERT_FALSE(leaderScheduler_->GetRapidLeaderSchedulerFlag());
    }
}

TEST_F(TestLeaderSchedule, test_transferLeaderIn_add_operator_fail) {
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    PeerInfo peer4(3, 4, 4, "192.168.10.4", 9000);
    MetaServerSpace space(100, 20);
    MetaServerInfo msInfo1(peer1, OnlineState::ONLINE, space, 10, 2);
    uint64_t currentTime = ::curve::common::TimeUtility::GetTimeofDaySec();
    msInfo1.startUpTime = currentTime - opt_.metaserverCoolingTimeSec - 1;
    MetaServerInfo msInfo2(peer2, OnlineState::ONLINE, space, 10, 3);
    MetaServerInfo msInfo3(peer3, OnlineState::ONLINE, space, 10, 4);
    MetaServerInfo msInfo4(peer4, OnlineState::ONLINE, space, 10, 3);
    std::vector<MetaServerInfo> msInfos({msInfo1, msInfo2, msInfo3, msInfo4});

    PoolIdType poolId = 1;
    CopySetIdType copysetId1 = 1;
    CopySetIdType copysetId2 = 2;
    EpochType epoch = 1;
    // has operator
    CopySetInfo copySet1(CopySetKey(poolId, copysetId1), epoch, 3,
        std::vector<PeerInfo>({peer1, peer2, peer3}),
        ConfigChangeInfo{});
    // not include metaserver1
    CopySetInfo copySet2(CopySetKey(poolId, copysetId2), epoch, 4,
        std::vector<PeerInfo>({peer2, peer3, peer4}),
        ConfigChangeInfo{});

    Operator testOperator(1, CopySetKey(poolId, copysetId1),
                          OperatorPriority::NormalPriority,
                          steady_clock::now(), std::make_shared<AddPeer>(1));
    ASSERT_TRUE(opController_->AddOperator(testOperator));

    EXPECT_CALL(*topoAdapter_, Getpools())
        .WillOnce(Return(std::vector<PoolIdType>({poolId})));
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(poolId))
        .WillOnce(Return(3));
    EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(poolId))
        .WillOnce(Return(msInfos));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInPool(poolId))
        .WillOnce(Return(std::vector<CopySetInfo>{copySet1, copySet2}));
     EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo1), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(2, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo2), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(3, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(msInfo3), Return(true)));

    ASSERT_EQ(0, leaderScheduler_->Schedule());
}
}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
