/*
 * Project: curve
 * Created Date: Mon Mar 25 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "src/mds/heartbeat/chunkserver_healthy_checker.h"
#include "test/mds/heartbeat/mock_topology.h"

using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;
using ::testing::_;

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
    checker->UpdateLastReceivedHeartbeatTime(1, steady_clock::now());
    checker->UpdateLastReceivedHeartbeatTime(
        2, steady_clock::now() - std::chrono::milliseconds(4000));
    checker->UpdateLastReceivedHeartbeatTime(
        3, steady_clock::now() - std::chrono::milliseconds(10000));
    ASSERT_TRUE(checker->GetHeartBeatInfo(1, &info));
    ASSERT_TRUE(info.OnlineFlag);
    ASSERT_TRUE(checker->GetHeartBeatInfo(2, &info));
    ASSERT_TRUE(info.OnlineFlag);
    ASSERT_TRUE(checker->GetHeartBeatInfo(3, &info));
    ASSERT_TRUE(info.OnlineFlag);
    ASSERT_FALSE(checker->GetHeartBeatInfo(4, &info));

    ::curve::mds::topology::ChunkServer chunkServer(
        1, "", "", 1, "", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology, GetChunkServer(_, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkServer), Return(true)));
    EXPECT_CALL(*topology, UpdateChunkServerState(_, _))
        .Times(2).WillRepeatedly(Return(true));
    checker->CheckHeartBeatInterval();
    ASSERT_TRUE(checker->GetHeartBeatInfo(1, &info));
    ASSERT_TRUE(info.OnlineFlag);
    ASSERT_TRUE(checker->GetHeartBeatInfo(2, &info));
    ASSERT_TRUE(info.OnlineFlag);
    ASSERT_TRUE(checker->GetHeartBeatInfo(3, &info));
    ASSERT_FALSE(info.OnlineFlag);

    checker->UpdateLastReceivedHeartbeatTime(
        3, steady_clock::now());
    checker->CheckHeartBeatInterval();
    ASSERT_TRUE(checker->GetHeartBeatInfo(1, &info));
    ASSERT_TRUE(info.OnlineFlag);
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve
