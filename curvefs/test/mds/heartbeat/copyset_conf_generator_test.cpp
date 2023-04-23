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
 * @Date: 2021-11-12 16:36:02
 * @Author: chenwei
 */

#include "curvefs/src/mds/heartbeat/copyset_conf_generator.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/time.h>

#include "curvefs/test/mds/mock/mock_coordinator.h"
#include "curvefs/test/mds/mock/mock_topology.h"

using ::curvefs::mds::MockCoordinator;
using ::curvefs::mds::topology::CopySetIdType;
using ::curvefs::mds::topology::MockIdGenerator;
using ::curvefs::mds::topology::MockStorage;
using ::curvefs::mds::topology::MockTokenGenerator;
using ::curvefs::mds::topology::MockTopology;
using ::curvefs::mds::topology::TopoStatusCode;
using ::curvefs::mds::topology::UNINITIALIZE_ID;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArgPointee;

namespace curvefs {
namespace mds {
namespace heartbeat {
class TestCopysetConfGenerator : public ::testing::Test {
 protected:
    TestCopysetConfGenerator() {}
    ~TestCopysetConfGenerator() {}

    void SetUp() override {
        steady_clock::time_point mdsStartTime = steady_clock::now();
        uint64_t cleanFollowerAfterMs = 1000;
        topology_ = std::make_shared<MockTopology>(idGenerator_,
                                                   tokenGenerator_, storage_);
        coordinator_ = std::make_shared<MockCoordinator>();
        generator_ = std::make_shared<CopysetConfGenerator>(
            topology_, coordinator_, mdsStartTime, cleanFollowerAfterMs);
    }

    void TearDown() override {}

 protected:
    std::shared_ptr<MockIdGenerator> idGenerator_;
    std::shared_ptr<MockTokenGenerator> tokenGenerator_;
    std::shared_ptr<MockStorage> storage_;
    std::shared_ptr<MockTopology> topology_;
    std::shared_ptr<MockCoordinator> coordinator_;
    std::shared_ptr<CopysetConfGenerator> generator_;
};

TEST_F(TestCopysetConfGenerator, get_copyset_fail) {
    MetaServerIdType reportId = 1;
    PoolIdType poolId = 1;
    CopySetIdType copysetId = 2;
    ::curvefs::mds::topology::CopySetInfo reportCopySetInfo(poolId, copysetId);
    ::curvefs::mds::heartbeat::ConfigChangeInfo configChInfo;
    ::curvefs::mds::heartbeat::CopySetConf copysetConf;

    EXPECT_CALL(*topology_, GetCopySet(_, _)).WillOnce(Return(false));

    bool ret = generator_->GenCopysetConf(reportId, reportCopySetInfo,
                                          configChInfo, &copysetConf);
    ASSERT_TRUE(ret);
    ASSERT_EQ(copysetConf.poolid(), poolId);
    ASSERT_EQ(copysetConf.copysetid(), copysetId);
    ASSERT_EQ(copysetConf.epoch(), 0);
}

TEST_F(TestCopysetConfGenerator, get_report_copyset_leader_fail) {
    MetaServerIdType reportId = 1;
    ::curvefs::mds::topology::CopySetInfo reportCopySetInfo;
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo;
    ::curvefs::mds::heartbeat::ConfigChangeInfo configChInfo;
    ::curvefs::mds::heartbeat::CopySetConf copysetConf;

    reportCopySetInfo.SetEpoch(1);
    reportCopySetInfo.SetLeader(reportId);

    EXPECT_CALL(*coordinator_, CopySetHeartbeat(_, _, _))
        .WillOnce(Return(UNINITIALIZE_ID));

    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));


    bool ret = generator_->GenCopysetConf(reportId, reportCopySetInfo,
                                          configChInfo, &copysetConf);
    ASSERT_FALSE(ret);
}

TEST_F(TestCopysetConfGenerator, get_report_copyset_leader_updatecopyset_fail) {
    MetaServerIdType reportId = 1;
    ::curvefs::mds::topology::CopySetInfo reportCopySetInfo;
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo;
    ::curvefs::mds::heartbeat::ConfigChangeInfo configChInfo;
    ::curvefs::mds::heartbeat::CopySetConf copysetConf;

    reportCopySetInfo.SetEpoch(1);
    reportCopySetInfo.SetLeader(reportId);

    EXPECT_CALL(*coordinator_, CopySetHeartbeat(_, _, _))
        .WillOnce(Return(reportId));

    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    EXPECT_CALL(*topology_, UpdateCopySetTopo(_))
        .WillOnce(Return(TopoStatusCode::TOPO_INTERNAL_ERROR));

    bool ret = generator_->GenCopysetConf(reportId, reportCopySetInfo,
                                          configChInfo, &copysetConf);
    ASSERT_FALSE(ret);
}

TEST_F(TestCopysetConfGenerator, get_report_copyset_leader_updatecopyset_succ) {
    MetaServerIdType reportId = 1;
    ::curvefs::mds::topology::CopySetInfo reportCopySetInfo;
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo;
    ::curvefs::mds::heartbeat::ConfigChangeInfo configChInfo;
    ::curvefs::mds::heartbeat::CopySetConf copysetConf;

    reportCopySetInfo.SetEpoch(1);
    reportCopySetInfo.SetLeader(reportId);

    EXPECT_CALL(*coordinator_, CopySetHeartbeat(_, _, _))
        .WillOnce(Return(reportId));

    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    EXPECT_CALL(*topology_, UpdateCopySetTopo(_))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));

    bool ret = generator_->GenCopysetConf(reportId, reportCopySetInfo,
                                          configChInfo, &copysetConf);
    ASSERT_TRUE(ret);
}

// recordCopySetInfo epoch < reportCopySetInfo epoch
TEST_F(TestCopysetConfGenerator, get_report_copyset_follower1) {
    MetaServerIdType reportId = 1;
    ::curvefs::mds::topology::CopySetInfo reportCopySetInfo;
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo;
    ::curvefs::mds::heartbeat::ConfigChangeInfo configChInfo;
    ::curvefs::mds::heartbeat::CopySetConf copysetConf;

    reportCopySetInfo.SetEpoch(2);
    reportCopySetInfo.SetLeader(reportId + 1);


    recordCopySetInfo.SetEpoch(1);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    bool ret = generator_->GenCopysetConf(reportId, reportCopySetInfo,
                                          configChInfo, &copysetConf);
    ASSERT_FALSE(ret);
}

// recordCopySetInfo epoch >= reportCopySetInfo epoch,
// wait cleanFollowerAfterMs_
TEST_F(TestCopysetConfGenerator, get_report_copyset_follower2) {
    MetaServerIdType reportId = 1;
    ::curvefs::mds::topology::CopySetInfo reportCopySetInfo;
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo;
    ::curvefs::mds::heartbeat::ConfigChangeInfo configChInfo;
    ::curvefs::mds::heartbeat::CopySetConf copysetConf;

    reportCopySetInfo.SetEpoch(2);
    reportCopySetInfo.SetLeader(reportId + 1);

    // recordCopySetInfo epoch == reportCopySetInfo epoch
    recordCopySetInfo.SetEpoch(2);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    bool ret = generator_->GenCopysetConf(reportId, reportCopySetInfo,
                                          configChInfo, &copysetConf);
    ASSERT_FALSE(ret);

    // recordCopySetInfo epoch > reportCopySetInfo epoch
    recordCopySetInfo.SetEpoch(3);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));
    ret = generator_->GenCopysetConf(reportId, reportCopySetInfo, configChInfo,
                                     &copysetConf);
    ASSERT_FALSE(ret);
}

// recordCopySetInfo epoch >= reportCopySetInfo epoch,
// metaserver going to be added into the copyset
TEST_F(TestCopysetConfGenerator, get_report_copyset_follower3) {
    MetaServerIdType reportId = 1;
    ::curvefs::mds::topology::CopySetInfo reportCopySetInfo;
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo;
    ::curvefs::mds::heartbeat::ConfigChangeInfo configChInfo;
    ::curvefs::mds::heartbeat::CopySetConf copysetConf;

    reportCopySetInfo.SetEpoch(2);
    reportCopySetInfo.SetLeader(reportId + 1);

    // recordCopySetInfo epoch == reportCopySetInfo epoch
    recordCopySetInfo.SetEpoch(2);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    EXPECT_CALL(*coordinator_, MetaserverGoingToAdd(_, _))
        .WillOnce(Return(true));

    sleep(1);
    bool ret = generator_->GenCopysetConf(reportId, reportCopySetInfo,
                                          configChInfo, &copysetConf);
    ASSERT_FALSE(ret);
}

// recordCopySetInfo epoch >= reportCopySetInfo epoch,
// this metaserver is a candidate
TEST_F(TestCopysetConfGenerator, get_report_copyset_follower4) {
    MetaServerIdType reportId = 1;
    ::curvefs::mds::topology::CopySetInfo reportCopySetInfo;
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo;
    ::curvefs::mds::heartbeat::ConfigChangeInfo configChInfo;
    ::curvefs::mds::heartbeat::CopySetConf copysetConf;

    reportCopySetInfo.SetEpoch(2);
    reportCopySetInfo.SetLeader(reportId + 1);

    // recordCopySetInfo epoch == reportCopySetInfo epoch
    recordCopySetInfo.SetEpoch(2);
    recordCopySetInfo.SetCandidate(reportId);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));


    sleep(1);
    bool ret = generator_->GenCopysetConf(reportId, reportCopySetInfo,
                                          configChInfo, &copysetConf);
    ASSERT_FALSE(ret);
}

// recordCopySetInfo epoch >= reportCopySetInfo epoch,
// this metaserver is in the copyset it reported
TEST_F(TestCopysetConfGenerator, get_report_copyset_follower5) {
    MetaServerIdType reportId = 1;
    ::curvefs::mds::topology::CopySetInfo reportCopySetInfo;
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo;
    ::curvefs::mds::heartbeat::ConfigChangeInfo configChInfo;
    ::curvefs::mds::heartbeat::CopySetConf copysetConf;

    reportCopySetInfo.SetEpoch(2);
    reportCopySetInfo.SetLeader(reportId + 1);

    // recordCopySetInfo epoch == reportCopySetInfo epoch
    recordCopySetInfo.SetEpoch(2);
    recordCopySetInfo.SetCopySetMembers({reportId});
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    sleep(1);
    bool ret = generator_->GenCopysetConf(reportId, reportCopySetInfo,
                                          configChInfo, &copysetConf);
    ASSERT_FALSE(ret);
}

// recordCopySetInfo epoch >= reportCopySetInfo epoch,
// build CopySetConf
TEST_F(TestCopysetConfGenerator, get_report_copyset_follower6) {
    MetaServerIdType reportId = 1;
    PoolIdType poolId = 2;
    CopySetIdType copySetId = 3;
    ::curvefs::mds::topology::CopySetInfo reportCopySetInfo;
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo;
    ::curvefs::mds::heartbeat::ConfigChangeInfo configChInfo;
    ::curvefs::mds::heartbeat::CopySetConf copysetConf;

    reportCopySetInfo.SetEpoch(2);
    reportCopySetInfo.SetLeader(reportId + 1);

    recordCopySetInfo.SetEpoch(3);
    recordCopySetInfo.SetPoolId(poolId);
    recordCopySetInfo.SetCopySetId(copySetId);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    EXPECT_CALL(*coordinator_, MetaserverGoingToAdd(_, _))
        .WillOnce(Return(false));

    sleep(1);
    bool ret = generator_->GenCopysetConf(reportId, reportCopySetInfo,
                                          configChInfo, &copysetConf);
    ASSERT_TRUE(ret);
    ASSERT_EQ(copysetConf.poolid(), poolId);
    ASSERT_EQ(copysetConf.copysetid(), copySetId);
    ASSERT_EQ(copysetConf.epoch(), 3);
}

// recordCopySetInfo epoch >= reportCopySetInfo epoch,
// build CopySetConf, with candidata fail
TEST_F(TestCopysetConfGenerator, get_report_copyset_follower7) {
    MetaServerIdType reportId = 1;
    PoolIdType poolId = 2;
    CopySetIdType copySetId = 3;
    ::curvefs::mds::topology::CopySetInfo reportCopySetInfo;
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo;
    ::curvefs::mds::heartbeat::ConfigChangeInfo configChInfo;
    ::curvefs::mds::heartbeat::CopySetConf copysetConf;

    reportCopySetInfo.SetEpoch(2);
    reportCopySetInfo.SetLeader(reportId + 1);

    recordCopySetInfo.SetEpoch(3);
    recordCopySetInfo.SetPoolId(poolId);
    recordCopySetInfo.SetCopySetId(copySetId);
    recordCopySetInfo.SetCandidate(reportId + 1);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    EXPECT_CALL(*coordinator_, MetaserverGoingToAdd(_, _))
        .WillOnce(Return(false));

    EXPECT_CALL(*topology_, GetMetaServer(_, _)).WillOnce(Return(false));

    sleep(1);
    bool ret = generator_->GenCopysetConf(reportId, reportCopySetInfo,
                                          configChInfo, &copysetConf);
    ASSERT_FALSE(ret);
}

// recordCopySetInfo epoch >= reportCopySetInfo epoch,
// build CopySetConf, with candidata success
TEST_F(TestCopysetConfGenerator, get_report_copyset_follower8) {
    MetaServerIdType reportId = 1;
    PoolIdType poolId = 2;
    CopySetIdType copySetId = 3;
    ::curvefs::mds::topology::CopySetInfo reportCopySetInfo;
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo;
    ::curvefs::mds::heartbeat::ConfigChangeInfo configChInfo;
    ::curvefs::mds::heartbeat::CopySetConf copysetConf;

    reportCopySetInfo.SetEpoch(2);
    reportCopySetInfo.SetLeader(reportId + 1);

    recordCopySetInfo.SetEpoch(3);
    recordCopySetInfo.SetPoolId(poolId);
    recordCopySetInfo.SetCopySetId(copySetId);
    recordCopySetInfo.SetCandidate(reportId + 1);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    EXPECT_CALL(*coordinator_, MetaserverGoingToAdd(_, _))
        .WillOnce(Return(false));

    MetaServer metaServer;
    EXPECT_CALL(*topology_, GetMetaServer(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer), Return(true)));

    sleep(1);
    bool ret = generator_->GenCopysetConf(reportId, reportCopySetInfo,
                                          configChInfo, &copysetConf);
    ASSERT_TRUE(ret);
    ASSERT_EQ(copysetConf.poolid(), poolId);
    ASSERT_EQ(copysetConf.copysetid(), copySetId);
    ASSERT_EQ(copysetConf.epoch(), 3);
}

// recordCopySetInfo epoch >= reportCopySetInfo epoch,
// build CopySetConf, with copyset member
TEST_F(TestCopysetConfGenerator, get_report_copyset_follower9) {
    MetaServerIdType reportId = 1;
    PoolIdType poolId = 2;
    CopySetIdType copySetId = 3;
    ::curvefs::mds::topology::CopySetInfo reportCopySetInfo;
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo;
    ::curvefs::mds::heartbeat::ConfigChangeInfo configChInfo;
    ::curvefs::mds::heartbeat::CopySetConf copysetConf;

    reportCopySetInfo.SetEpoch(2);
    reportCopySetInfo.SetLeader(reportId + 1);

    recordCopySetInfo.SetEpoch(3);
    recordCopySetInfo.SetPoolId(poolId);
    recordCopySetInfo.SetCopySetId(copySetId);
    recordCopySetInfo.SetCopySetMembers({reportId + 1});
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    EXPECT_CALL(*coordinator_, MetaserverGoingToAdd(_, _))
        .WillOnce(Return(false));

    MetaServer metaServer;
    EXPECT_CALL(*topology_, GetMetaServer(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer), Return(true)));

    sleep(1);
    bool ret = generator_->GenCopysetConf(reportId, reportCopySetInfo,
                                          configChInfo, &copysetConf);
    ASSERT_TRUE(ret);
    ASSERT_EQ(copysetConf.poolid(), poolId);
    ASSERT_EQ(copysetConf.copysetid(), copySetId);
    ASSERT_EQ(copysetConf.epoch(), 3);
}

// recordCopySetInfo epoch >= reportCopySetInfo epoch,
// build CopySetConf, with copyset member
TEST_F(TestCopysetConfGenerator, get_report_copyset_follower10) {
    MetaServerIdType reportId = 1;
    PoolIdType poolId = 2;
    CopySetIdType copySetId = 3;
    ::curvefs::mds::topology::CopySetInfo reportCopySetInfo;
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo;
    ::curvefs::mds::heartbeat::ConfigChangeInfo configChInfo;
    ::curvefs::mds::heartbeat::CopySetConf copysetConf;

    reportCopySetInfo.SetEpoch(2);
    reportCopySetInfo.SetLeader(reportId + 1);

    recordCopySetInfo.SetEpoch(3);
    recordCopySetInfo.SetPoolId(poolId);
    recordCopySetInfo.SetCopySetId(copySetId);
    recordCopySetInfo.SetCopySetMembers({reportId + 1});
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    EXPECT_CALL(*coordinator_, MetaserverGoingToAdd(_, _))
        .WillOnce(Return(false));

    MetaServer metaServer;
    EXPECT_CALL(*topology_, GetMetaServer(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer), Return(false)));

    sleep(1);
    bool ret = generator_->GenCopysetConf(reportId, reportCopySetInfo,
                                          configChInfo, &copysetConf);
    ASSERT_FALSE(ret);
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curvefs
