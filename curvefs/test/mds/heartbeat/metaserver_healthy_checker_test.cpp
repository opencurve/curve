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
 * @Date: 2021-09-28 18:24:59
 * @Author: chenwei
 */

#include "curvefs/src/mds/heartbeat/metaserver_healthy_checker.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "curvefs/src/mds/topology/topology_item.h"
#include "curvefs/test/mds/mock/mock_topology.h"

using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;
using ::testing::_;
using ::curvefs::mds::topology::MockTopology;

using ::curvefs::mds::topology::MetaServer;
using ::curvefs::mds::topology::OnlineState;
using ::curvefs::mds::topology::CopySetKey;
using ::curvefs::mds::topology::MockTopology;
using ::curvefs::mds::topology::MockIdGenerator;
using ::curvefs::mds::topology::MockTokenGenerator;
using ::curvefs::mds::topology::MockStorage;
using ::curvefs::mds::topology::TopoStatusCode;

namespace curvefs {
namespace mds {
namespace heartbeat {

class TestMetaserverHealthyChecker : public ::testing::Test {
 protected:
    TestMetaserverHealthyChecker() {}
    ~TestMetaserverHealthyChecker() {}

    void SetUp() override {
        HeartbeatOption option;
        option.heartbeatIntervalMs = 1000;
        option.heartbeatMissTimeOutMs = 3000;
        option.offLineTimeOutMs = 5000;
        topology_ = std::make_shared<MockTopology>(idGenerator_,
                                                   tokenGenerator_, storage_);
        checker_ =
            std::make_shared<MetaserverHealthyChecker>(option, topology_);
    }

    void TearDown() override { topology_ = nullptr; }

 protected:
    std::shared_ptr<MockTopology> topology_;
    std::shared_ptr<MockIdGenerator> idGenerator_;
    std::shared_ptr<MockTokenGenerator> tokenGenerator_;
    std::shared_ptr<MockStorage> storage_;
    std::shared_ptr<MetaserverHealthyChecker> checker_;
};

TEST_F(TestMetaserverHealthyChecker, test_checkHeartBeat_interval1) {
    HeartbeatInfo info;

    // metaserver send heartbeat first time
    checker_->UpdateLastReceivedHeartbeatTime(1, steady_clock::now());
    checker_->UpdateLastReceivedHeartbeatTime(
        2, steady_clock::now() - std::chrono::milliseconds(4000));
    checker_->UpdateLastReceivedHeartbeatTime(
        3, steady_clock::now() - std::chrono::milliseconds(10000));
    checker_->UpdateLastReceivedHeartbeatTime(
        5, steady_clock::now() - std::chrono::milliseconds(10000));
    checker_->UpdateLastReceivedHeartbeatTime(
        6, steady_clock::now() - std::chrono::milliseconds(10000));
    checker_->UpdateLastReceivedHeartbeatTime(
        7, steady_clock::now() - std::chrono::milliseconds(10000));
    ASSERT_TRUE(checker_->GetHeartBeatInfo(1, &info));
    ASSERT_EQ(OnlineState::UNSTABLE, info.state);
    ASSERT_TRUE(checker_->GetHeartBeatInfo(2, &info));
    ASSERT_EQ(OnlineState::UNSTABLE, info.state);
    ASSERT_TRUE(checker_->GetHeartBeatInfo(3, &info));
    ASSERT_EQ(OnlineState::UNSTABLE, info.state);
    ASSERT_FALSE(checker_->GetHeartBeatInfo(4, &info));
    ASSERT_TRUE(checker_->GetHeartBeatInfo(5, &info));
    ASSERT_EQ(OnlineState::UNSTABLE, info.state);
    ASSERT_TRUE(checker_->GetHeartBeatInfo(6, &info));
    ASSERT_EQ(OnlineState::UNSTABLE, info.state);
    ASSERT_TRUE(checker_->GetHeartBeatInfo(7, &info));
    ASSERT_EQ(OnlineState::UNSTABLE, info.state);

    LOG(INFO) << "TEST CheckHeartBeatInterval";

    EXPECT_CALL(*topology_, UpdateMetaServerOnlineState(_, _))
        .Times(5)
        .WillRepeatedly(Return(TopoStatusCode::TOPO_OK));
    checker_->CheckHeartBeatInterval();
    ASSERT_TRUE(checker_->GetHeartBeatInfo(1, &info));
    ASSERT_EQ(OnlineState::ONLINE, info.state);
    ASSERT_TRUE(checker_->GetHeartBeatInfo(2, &info));
    ASSERT_EQ(OnlineState::UNSTABLE, info.state);
    ASSERT_TRUE(checker_->GetHeartBeatInfo(3, &info));
    ASSERT_EQ(OnlineState::OFFLINE, info.state);
    ASSERT_TRUE(checker_->GetHeartBeatInfo(5, &info));
    ASSERT_EQ(OnlineState::OFFLINE, info.state);
    ASSERT_TRUE(checker_->GetHeartBeatInfo(6, &info));
    ASSERT_EQ(OnlineState::OFFLINE, info.state);
    ASSERT_TRUE(checker_->GetHeartBeatInfo(7, &info));
    ASSERT_EQ(OnlineState::OFFLINE, info.state);

    // chunkserver-6， chunkserver-7 receive heartbeat
    EXPECT_CALL(*topology_, UpdateMetaServerOnlineState(_, _))
        .Times(2)
        .WillRepeatedly(Return(TopoStatusCode::TOPO_OK));
    checker_->UpdateLastReceivedHeartbeatTime(6, steady_clock::now());
    checker_->UpdateLastReceivedHeartbeatTime(7, steady_clock::now());
    checker_->CheckHeartBeatInterval();
    ASSERT_TRUE(checker_->GetHeartBeatInfo(6, &info));
    ASSERT_EQ(OnlineState::ONLINE, info.state);
    ASSERT_TRUE(checker_->GetHeartBeatInfo(7, &info));
    ASSERT_EQ(OnlineState::ONLINE, info.state);
}

}  // namespace heartbeat
}  // namespace mds
}  // namespace curvefs
