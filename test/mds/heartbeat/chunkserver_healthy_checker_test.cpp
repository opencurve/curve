/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: curve
 * Created Date: Mon Mar 25 2019
 * Author: lixiaocui
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "src/mds/heartbeat/chunkserver_healthy_checker.h"
#include "src/mds/topology/topology_item.h"
#include "test/mds/mock/mock_topology.h"

using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;
using ::testing::_;
using ::curve::mds::topology::MockTopology;

using ::curve::mds::topology::ChunkServer;
using ::curve::mds::topology::ChunkServerStatus;
using ::curve::mds::topology::OnlineState;
using ::curve::mds::topology::CopySetKey;
using ::curve::mds::topology::kTopoErrCodeSuccess;
using ::curve::mds::topology::kTopoErrCodeInternalError;

namespace curve {
namespace mds {
namespace heartbeat {
TEST(ChunkserverHealthyChecker, test_checkHeartBeat_interval) {
    HeartbeatOption option;
    option.heartbeatIntervalMs = 1000;
    option.heartbeatMissTimeOutMs = 3000;
    option.offLineTimeOutMs = 5000;
    std::shared_ptr<MockTopology> topology = std::make_shared<MockTopology>();
    std::shared_ptr<ChunkserverHealthyChecker> checker =
        std::make_shared<ChunkserverHealthyChecker>(option, topology);

    HeartbeatInfo info;
    {
        // Chunkserver updates heartbeatInfo for the first time
        checker->UpdateLastReceivedHeartbeatTime(1, steady_clock::now());
        checker->UpdateLastReceivedHeartbeatTime(
            2, steady_clock::now() - std::chrono::milliseconds(4000));
        checker->UpdateLastReceivedHeartbeatTime(
            3, steady_clock::now() - std::chrono::milliseconds(10000));
        checker->UpdateLastReceivedHeartbeatTime(
            5, steady_clock::now() - std::chrono::milliseconds(10000));
        checker->UpdateLastReceivedHeartbeatTime(
            6, steady_clock::now() - std::chrono::milliseconds(10000));
        checker->UpdateLastReceivedHeartbeatTime(
            7, steady_clock::now() - std::chrono::milliseconds(10000));
        checker->UpdateLastReceivedHeartbeatTime(
            8, steady_clock::now());
        checker->UpdateLastReceivedHeartbeatTime(
            9, steady_clock::now() - std::chrono::milliseconds(4000));
        checker->UpdateLastReceivedHeartbeatTime(
            10, steady_clock::now() - std::chrono::milliseconds(10000));
        ASSERT_TRUE(checker->GetHeartBeatInfo(1, &info));
        ASSERT_EQ(OnlineState::UNSTABLE, info.state);
        ASSERT_TRUE(checker->GetHeartBeatInfo(2, &info));
        ASSERT_EQ(OnlineState::UNSTABLE, info.state);
        ASSERT_TRUE(checker->GetHeartBeatInfo(3, &info));
        ASSERT_EQ(OnlineState::UNSTABLE, info.state);
        ASSERT_FALSE(checker->GetHeartBeatInfo(4, &info));
        ASSERT_EQ(OnlineState::UNSTABLE, info.state);
        ASSERT_TRUE(checker->GetHeartBeatInfo(5, &info));
        ASSERT_EQ(OnlineState::UNSTABLE, info.state);
        ASSERT_TRUE(checker->GetHeartBeatInfo(6, &info));
        ASSERT_EQ(OnlineState::UNSTABLE, info.state);
        ASSERT_TRUE(checker->GetHeartBeatInfo(7, &info));
        ASSERT_EQ(OnlineState::UNSTABLE, info.state);
        ASSERT_TRUE(checker->GetHeartBeatInfo(8, &info));
        ASSERT_EQ(OnlineState::UNSTABLE, info.state);
        ASSERT_TRUE(checker->GetHeartBeatInfo(9, &info));
        ASSERT_EQ(OnlineState::UNSTABLE, info.state);
        ASSERT_TRUE(checker->GetHeartBeatInfo(10, &info));
        ASSERT_EQ(OnlineState::UNSTABLE, info.state);
    }

    {
        // chunkserver-1 update to online
        // chunkserver-2 Heartbeat Miss, Keep Unstable
        // chunkserver-3, chunkserver-5, chunkserver-6 heartbeat offline,
        // The retried status of chunkserver-3 will be updated and removed from the heartbeat map
        // chunkserver-5 is already in a retired state and does not need to be updated
        // chunkserver-6 get info failed, status not successfully updated
        // chunkserver-7 update failed, status not successfully updated
        // chunkserver-8, pendding && online, updated to onLine
        // chunkserver-9, pendding && unstable, updated to retired
        // chunkserver-10, pendding && offline, updated to retired
        EXPECT_CALL(*topology, UpdateChunkServerOnlineState(_, _))
            .Times(7).WillRepeatedly(Return(kTopoErrCodeSuccess));
        ChunkServer cs2(2, "", "", 1, "", 0, "",
            ChunkServerStatus::READWRITE, OnlineState::UNSTABLE);
        ChunkServer cs3(3, "", "", 1, "", 0, "",
            ChunkServerStatus::READWRITE, OnlineState::UNSTABLE);
        ChunkServer cs5(5, "", "", 1, "", 0, "",
            ChunkServerStatus::RETIRED, OnlineState::UNSTABLE);
        ChunkServer cs7(7, "", "", 1, "", 0, "",
            ChunkServerStatus::READWRITE, OnlineState::UNSTABLE);
        ChunkServer cs9(9, "", "", 1, "", 0, "",
            ChunkServerStatus::PENDDING, OnlineState::UNSTABLE);
        ChunkServer cs10(10, "", "", 1, "", 0, "",
            ChunkServerStatus::PENDDING, OnlineState::UNSTABLE);
        EXPECT_CALL(*topology, GetChunkServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(cs2), Return(true)));
        EXPECT_CALL(*topology, GetChunkServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(cs3), Return(true)));
        EXPECT_CALL(*topology, GetCopySetsInChunkServer(3, _))
            .WillOnce(Return(std::vector<CopySetKey>{}));
        EXPECT_CALL(*topology, GetCopySetsInChunkServer(7, _))
            .WillOnce(Return(std::vector<CopySetKey>{}));
        EXPECT_CALL(*topology, GetChunkServer(5, _))
            .WillOnce(DoAll(SetArgPointee<1>(cs5), Return(true)));
        EXPECT_CALL(*topology, GetChunkServer(6, _))
            .WillOnce(Return(false));
        EXPECT_CALL(*topology, GetChunkServer(7, _))
            .WillOnce(DoAll(SetArgPointee<1>(cs7), Return(true)));
        EXPECT_CALL(*topology, GetChunkServer(9, _))
            .WillOnce(DoAll(SetArgPointee<1>(cs9), Return(true)));
        EXPECT_CALL(*topology, GetCopySetsInChunkServer(9, _))
            .WillOnce(Return(std::vector<CopySetKey>{}));
        EXPECT_CALL(*topology, GetChunkServer(10, _))
            .WillOnce(DoAll(SetArgPointee<1>(cs10), Return(true)));
        EXPECT_CALL(*topology, GetCopySetsInChunkServer(10, _))
            .WillOnce(Return(std::vector<CopySetKey>{}));
        EXPECT_CALL(*topology, UpdateChunkServerRwState(_, _))
            .Times(4)
            .WillOnce(Return(kTopoErrCodeSuccess))
            .WillOnce(Return(kTopoErrCodeInternalError))
            .WillOnce(Return(kTopoErrCodeSuccess))
            .WillOnce(Return(kTopoErrCodeSuccess));
        checker->CheckHeartBeatInterval();
        ASSERT_TRUE(checker->GetHeartBeatInfo(1, &info));
        ASSERT_EQ(OnlineState::ONLINE, info.state);
        ASSERT_TRUE(checker->GetHeartBeatInfo(2, &info));
        ASSERT_EQ(OnlineState::UNSTABLE, info.state);
        ASSERT_FALSE(checker->GetHeartBeatInfo(3, &info));
        ASSERT_FALSE(checker->GetHeartBeatInfo(5, &info));
        ASSERT_TRUE(checker->GetHeartBeatInfo(6, &info));
        ASSERT_EQ(OnlineState::OFFLINE, info.state);
        ASSERT_TRUE(checker->GetHeartBeatInfo(7, &info));
        ASSERT_EQ(OnlineState::OFFLINE, info.state);
        ASSERT_TRUE(checker->GetHeartBeatInfo(8, &info));
        ASSERT_EQ(OnlineState::ONLINE, info.state);
        ASSERT_FALSE(checker->GetHeartBeatInfo(9, &info));
        ASSERT_FALSE(checker->GetHeartBeatInfo(10, &info));
    }

    {
        // chunkserver 2, 6, 7 Heartbeat received
        checker->UpdateLastReceivedHeartbeatTime(
            2, steady_clock::now());
        checker->UpdateLastReceivedHeartbeatTime(
            6, steady_clock::now());
        checker->UpdateLastReceivedHeartbeatTime(
            7, steady_clock::now());
        EXPECT_CALL(*topology, UpdateChunkServerOnlineState(_, _))
            .Times(3).WillRepeatedly(Return(kTopoErrCodeSuccess));
        checker->CheckHeartBeatInterval();
        ASSERT_TRUE(checker->GetHeartBeatInfo(2, &info));
        ASSERT_EQ(OnlineState::ONLINE, info.state);
        ASSERT_TRUE(checker->GetHeartBeatInfo(6, &info));
        ASSERT_EQ(OnlineState::ONLINE, info.state);
        ASSERT_TRUE(checker->GetHeartBeatInfo(7, &info));
        ASSERT_EQ(OnlineState::ONLINE, info.state);
    }
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve
