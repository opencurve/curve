/*
 * Project: curve
 * Created Date: Mon Mar 25 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
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
        // chunkserver首次更新heartbeatInfo
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
        ASSERT_TRUE(checker->GetHeartBeatInfo(1, &info));
        ASSERT_TRUE(info.OnlineFlag);
        ASSERT_TRUE(checker->GetHeartBeatInfo(2, &info));
        ASSERT_TRUE(info.OnlineFlag);
        ASSERT_TRUE(checker->GetHeartBeatInfo(3, &info));
        ASSERT_TRUE(info.OnlineFlag);
        ASSERT_FALSE(checker->GetHeartBeatInfo(4, &info));
        ASSERT_TRUE(checker->GetHeartBeatInfo(5, &info));
        ASSERT_TRUE(info.OnlineFlag);
        ASSERT_TRUE(checker->GetHeartBeatInfo(6, &info));
        ASSERT_TRUE(info.OnlineFlag);
        ASSERT_TRUE(checker->GetHeartBeatInfo(7, &info));
        ASSERT_TRUE(info.OnlineFlag);
    }

    {
        // chunkserver-2 心跳miss，
        // chunkserver-3,chunkserver-5,chunkserver-6心跳offline,
        // chunkserver-3的retired状态会被更新, 从心跳map中移除
        // chunkserver-5已经是retired状态，无需更新
        // chunkserver-6 get info失败, 未成功更新状态
        // chunnkserver-7 update失败, 未成功更新状态
        EXPECT_CALL(*topology, UpdateOnlineState(_, _))
            .Times(6).WillRepeatedly(Return(kTopoErrCodeSuccess));
        ChunkServer cs3(3, "", "", 1, "", 0, "",
            ChunkServerStatus::READWRITE, OnlineState::ONLINE);
        ChunkServer cs5(5, "", "", 1, "", 0, "",
            ChunkServerStatus::RETIRED, OnlineState::ONLINE);
        ChunkServer cs7(7, "", "", 1, "", 0, "",
            ChunkServerStatus::READWRITE, OnlineState::ONLINE);
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
        EXPECT_CALL(*topology, UpdateChunkServer(_))
            .Times(2)
            .WillOnce(Return(kTopoErrCodeSuccess))
            .WillOnce(Return(kTopoErrCodeInternalError));
        checker->CheckHeartBeatInterval();
        ASSERT_TRUE(checker->GetHeartBeatInfo(1, &info));
        ASSERT_TRUE(info.OnlineFlag);
        ASSERT_TRUE(checker->GetHeartBeatInfo(2, &info));
        ASSERT_TRUE(info.OnlineFlag);
        ASSERT_FALSE(checker->GetHeartBeatInfo(3, &info));
        ASSERT_FALSE(checker->GetHeartBeatInfo(5, &info));
        ASSERT_TRUE(checker->GetHeartBeatInfo(6, &info));
        ASSERT_FALSE(info.OnlineFlag);
        ASSERT_TRUE(checker->GetHeartBeatInfo(7, &info));
        ASSERT_FALSE(info.OnlineFlag);
    }

    {
        // chunkserver-6， chunkserver-7 收到心跳
        checker->UpdateLastReceivedHeartbeatTime(
            6, steady_clock::now());
        checker->UpdateLastReceivedHeartbeatTime(
            7, steady_clock::now());
        checker->CheckHeartBeatInterval();
        ASSERT_TRUE(checker->GetHeartBeatInfo(6, &info));
        ASSERT_TRUE(info.OnlineFlag);
        ASSERT_TRUE(checker->GetHeartBeatInfo(7, &info));
        ASSERT_TRUE(info.OnlineFlag);
    }
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve
