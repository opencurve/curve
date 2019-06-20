/*
 * Project: curve
 * Created Date: Sat Jan 05 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include "src/mds/heartbeat/heartbeat_manager.h"
#include "src/mds/heartbeat/chunkserver_healthy_checker.h"
#include "test/mds/heartbeat/mock_coordinator.h"
#include "test/mds/mock/mock_topology.h"
#include "test/mds/heartbeat/mock_topoAdapter.h"
#include "test/mds/heartbeat/common.h"

using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;
using ::testing::_;
using ::curve::mds::topology::MockTopology;
using ::curve::mds::topology::MockTopologyStat;

namespace curve {
namespace mds {
namespace heartbeat {
class TestHeartbeatManager : public ::testing::Test {
 protected:
  TestHeartbeatManager() {}
  ~TestHeartbeatManager() {}

  void SetUp() override {
    HeartbeatOption option;
    option.cleanFollowerAfterMs = 0;
    option.mdsStartTime = steady_clock::now();
    topology_ = std::make_shared<MockTopology>();
    coordinator_ = std::make_shared<MockCoordinator>();
    topologyStat_ = std::make_shared<MockTopologyStat>();
    heartbeatManager_ = std::make_shared<HeartbeatManager>(
        option, topology_, topologyStat_, coordinator_);
  }

  void TearDown() override {}

 protected:
  std::shared_ptr<MockTopology> topology_;
  std::shared_ptr<MockTopologyStat> topologyStat_;
  std::shared_ptr<MockCoordinator> coordinator_;
  std::shared_ptr<HeartbeatManager> heartbeatManager_;
};

TEST_F(TestHeartbeatManager, test_checkReuqest_abnormal) {
    // 1. request not initialized
    ChunkServerHeartbeatRequest request;
    ChunkServerHeartbeatResponse response;
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());

    // 2. can not get chunkServer
    request = GetChunkServerHeartbeatRequestForTest();
    ASSERT_TRUE(request.IsInitialized());
    EXPECT_CALL(*topology_, GetChunkServer(_, _)).WillOnce(Return(false));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());

    // 3. ip not same
    ::curve::mds::topology::ChunkServer chunkServer(
        1, "hello", "", 1, "192.168.10.4", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServer(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer), Return(true)));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());

    // 4. port not same
    chunkServer.SetHostIp("192.168.10.1");
    chunkServer.SetPort(11000);
    EXPECT_CALL(*topology_, GetChunkServer(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer), Return(true)));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());

    // 5. token not same
    chunkServer.SetPort(9000);
    chunkServer.SetToken("hellocode");
    EXPECT_CALL(*topology_, GetChunkServer(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer), Return(true)));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());

    // 6. chunkserver retired
    auto req = GetChunkServerHeartbeatRequestForTest();
    ASSERT_TRUE(request.IsInitialized());
    ::curve::mds::topology::ChunkServer retiredCs(
        1, "hello", "", 1, "192.168.10.4", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::RETIRED);
    EXPECT_CALL(*topology_, GetChunkServer(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(retiredCs), Return(true)));
    heartbeatManager_->ChunkServerHeartbeat(req, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager, test_getChunkserverIdByPeerStr) {
    auto request = GetChunkServerHeartbeatRequestForTest();
    ChunkServerHeartbeatResponse response;

    // 1. test invalid form
    request.clear_copysetinfos();
    ::curve::mds::heartbeat::CopySetInfo info;
    info.set_logicalpoolid(1);
    info.set_copysetid(1);
    info.set_epoch(10);
    auto replica = info.add_peers();
    replica->set_address("192.168.10.1:9000");
    auto leader = new ::curve::common::Peer();
    leader->set_address("192.168.10.1:9000");
    info.set_allocated_leaderpeer(leader);
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;
    ::curve::mds::topology::ChunkServer chunkServer(
        1, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer), Return(true)));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());

    // 2. test can not get chunkServer
    request = GetChunkServerHeartbeatRequestForTest();
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.1", 9000, _))
        .WillOnce(Return(false));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager, test_heartbeatCopySetInfo_to_topologyOne) {
    auto request = GetChunkServerHeartbeatRequestForTest();
    ChunkServerHeartbeatResponse response;
    ::curve::mds::topology::ChunkServer chunkServer1(
        1, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer chunkServer2(
        2, "hello", "", 1, "192.168.10.2", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer chunkServer3(
        3, "hello", "", 1, "192.168.10.3", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);

    // 1. can not get peers
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(Return(false));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());

    // 2. can not get leader
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(false)));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());

    // 3. has candidate and cannot get candidate
    request.clear_copysetinfos();
    ::curve::mds::heartbeat::CopySetInfo info;
    info.set_logicalpoolid(1);
    info.set_copysetid(1);
    info.set_epoch(10);
    auto candidate = new ConfigChangeInfo;
    for (int i = 1; i <= 4; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        if (i == 1) {
            auto replica = new ::curve::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }

        if (i == 4) {
            auto replica = new ::curve::common::Peer();
            replica->set_address(ip);
            candidate->set_allocated_peer(replica);
            candidate->set_finished(true);
            candidate->set_type(ConfigChangeType::ADD_PEER);
            continue;
        }
        auto replica = info.add_peers();
        replica->set_address(ip);
    }
    info.set_allocated_configchangeinfo(candidate);
    auto addInfo = request.add_copysetinfos();
    *addInfo = info;
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)))
        .WillOnce(Return(false));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager, test_follower_reqEpoch_notSmallerThan_mdsRecord) {
    auto request = GetChunkServerHeartbeatRequestForTest();
    ChunkServerHeartbeatResponse response;

    request.clear_copysetinfos();
    ::curve::mds::heartbeat::CopySetInfo info;
    info.set_logicalpoolid(1);
    info.set_copysetid(1);
    info.set_epoch(2);
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto replica = info.add_peers();
        replica->set_address(ip);
        if (i == 2) {
            auto replica = new ::curve::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }
    }
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;
    ::curve::mds::topology::ChunkServer chunkServer1(
        1, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    ::curve::mds::topology::ChunkServer leaderChunkServer(
        2, "hello", "", 1, "192.168.10.2", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.2", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(leaderChunkServer), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.1", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)));
    ::curve::mds::topology::ChunkServer chunkServer3(
        3, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.3", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)));
    ::curve::mds::topology::CopySetInfo recordCopySetInfo(1, 1);
    recordCopySetInfo.SetEpoch(1);
    recordCopySetInfo.SetLeader(2);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager,
       test_follower_reqEpoch_smallerThan_mdsRecord_test1) {
    // test report chunkServer in copySet members
    auto request = GetChunkServerHeartbeatRequestForTest();
    ChunkServerHeartbeatResponse response;

    request.clear_copysetinfos();
    ::curve::mds::heartbeat::CopySetInfo info;
    info.set_logicalpoolid(1);
    info.set_copysetid(1);
    info.set_epoch(1);
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto replica = info.add_peers();
        replica->set_address(ip);
        if (i == 2) {
            auto replica = new ::curve::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }
    }
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;

    ::curve::mds::topology::ChunkServer chunkServer1(
        1, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    ::curve::mds::topology::ChunkServer leaderChunkServer(
        2, "hello", "", 1, "192.168.10.2", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.2", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(leaderChunkServer), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.1", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)));
    ::curve::mds::topology::ChunkServer chunkServer3(
        3, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.3", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)));
    ::curve::mds::topology::CopySetInfo recordCopySetInfo(1, 1);
    recordCopySetInfo.SetEpoch(2);
    recordCopySetInfo.SetLeader(2);
    std::set<ChunkServerIdType> peers{1, 2, 3};
    recordCopySetInfo.SetCopySetMembers(peers);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager,
       test_follower_reqEpoch_smallerThan_mdsRecord_test2) {
    // test report chunkServer not in copySet members, but is candidate
    auto request = GetChunkServerHeartbeatRequestForTest();
    ChunkServerHeartbeatResponse response;

    request.clear_copysetinfos();
    ::curve::mds::heartbeat::CopySetInfo info;
    info.set_logicalpoolid(1);
    info.set_copysetid(1);
    info.set_epoch(1);
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto replica = info.add_peers();
        replica->set_address(ip);
        if (i == 2) {
            auto replica = new ::curve::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }
    }
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;

    ::curve::mds::topology::ChunkServer chunkServer1(
        1, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    ::curve::mds::topology::ChunkServer leaderChunkServer(
        2, "hello", "", 1, "192.168.10.2", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.2", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(leaderChunkServer),
                                 Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.1", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)));
    ::curve::mds::topology::ChunkServer chunkServer3(
        3, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.3", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)));
    ::curve::mds::topology::CopySetInfo recordCopySetInfo(1, 1);
    recordCopySetInfo.SetEpoch(2);
    recordCopySetInfo.SetLeader(2);
    std::set<ChunkServerIdType> peers{2, 3, 4};
    recordCopySetInfo.SetCopySetMembers(peers);
    recordCopySetInfo.SetCandidate(1);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager,
       test_follower_reqEpoch_smallerThan_mdsRecord_test3) {
    // test report chunkServer is not in copySet members and not candidate
    // cannot get candidate chunkServer from topology
    auto request = GetChunkServerHeartbeatRequestForTest();
    ChunkServerHeartbeatResponse response;

    request.clear_copysetinfos();
    ::curve::mds::heartbeat::CopySetInfo info;
    info.set_logicalpoolid(1);
    info.set_copysetid(1);
    info.set_epoch(1);
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto replica = info.add_peers();
        replica->set_address(ip);
        if (i == 2) {
            auto replica = new ::curve::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }
    }
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;

    ::curve::mds::topology::ChunkServer chunkServer1(
        1, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    ::curve::mds::topology::ChunkServer leaderChunkServer(
        2, "hello", "", 1, "192.168.10.2", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.2", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(leaderChunkServer),
                                 Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.1", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)));
    ::curve::mds::topology::ChunkServer chunkServer3(
        3, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.3", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)));
    EXPECT_CALL(*coordinator_, ChunkserverGoingToAdd(_, _))
        .WillOnce(Return(false));
    ::curve::mds::topology::CopySetInfo recordCopySetInfo(1, 1);
    recordCopySetInfo.SetEpoch(2);
    recordCopySetInfo.SetLeader(2);
    std::set<ChunkServerIdType> peers{2, 3, 4};
    recordCopySetInfo.SetCopySetMembers(peers);
    recordCopySetInfo.SetCandidate(7);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServer(7, _)).WillOnce(Return(false));

    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager,
       test_follower_reqEpoch_smallerThan_mdsRecord_test4) {
    // test report chunkServer is not in copySet members and not candidate
    // get candidate chunkServer ok
    auto request = GetChunkServerHeartbeatRequestForTest();
    ChunkServerHeartbeatResponse response;

    request.clear_copysetinfos();
    ::curve::mds::heartbeat::CopySetInfo info;
    info.set_logicalpoolid(1);
    info.set_copysetid(1);
    info.set_epoch(1);
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto replica = info.add_peers();
        replica->set_address(ip);
        if (i == 2) {
            auto replica = new ::curve::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }
    }
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;

    ::curve::mds::topology::ChunkServer chunkServer1(
        1, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    ::curve::mds::topology::ChunkServer leaderChunkServer(
        2, "hello", "", 1, "192.168.10.2", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.2", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(leaderChunkServer),
                                 Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.1", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)));
    ::curve::mds::topology::ChunkServer chunkServer3(
        3, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.3", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)));
    ::curve::mds::topology::CopySetInfo recordCopySetInfo(1, 1);
    recordCopySetInfo.SetEpoch(2);
    recordCopySetInfo.SetLeader(2);
    std::set<ChunkServerIdType> peers{2, 3, 4};
    recordCopySetInfo.SetCopySetMembers(peers);
    recordCopySetInfo.SetCandidate(7);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));
    ::curve::mds::topology::ChunkServer chunkServer7(
        7, "hello", "", 1, "192.168.10.7", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServer(7, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer7), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServer(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(leaderChunkServer), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServer(3, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer3), Return(true)));
    ::curve::mds::topology::ChunkServer chunkServer4(
        4, "hello", "", 1, "192.168.10.4", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServer(4, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer4), Return(true)));

    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(1, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager,
       test_follower_reqEpoch_smallerThan_mdsRecord_test5) {
    // test report chunkServer is not in copySet members
    auto request = GetChunkServerHeartbeatRequestForTest();
    ChunkServerHeartbeatResponse response;

    request.clear_copysetinfos();
    ::curve::mds::heartbeat::CopySetInfo info;
    info.set_logicalpoolid(1);
    info.set_copysetid(1);
    info.set_epoch(1);
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto replica = info.add_peers();
        replica->set_address(ip);
        if (i == 2) {
            auto replica = new ::curve::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }
    }
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;

    ::curve::mds::topology::ChunkServer chunkServer1(
        1, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    ::curve::mds::topology::ChunkServer leaderChunkServer(
        2, "hello", "", 1, "192.168.10.2", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.2", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(leaderChunkServer), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.1", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)));
    ::curve::mds::topology::ChunkServer chunkServer3(
        3, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServerNotRetired("192.168.10.3", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)));
    ::curve::mds::topology::CopySetInfo recordCopySetInfo(1, 1);
    recordCopySetInfo.SetEpoch(2);
    recordCopySetInfo.SetLeader(2);
    std::set<ChunkServerIdType> peers{2, 3, 4};
    recordCopySetInfo.SetCopySetMembers(peers);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServer(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(leaderChunkServer), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServer(3, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer3), Return(true)));
    ::curve::mds::topology::ChunkServer chunkServer4(
        4, "hello", "", 1, "192.168.10.4", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServer(4, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer4), Return(true)));

    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(1, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager, test_chunkServer_heartbeat_get_copySetInfo_err) {
    auto request = GetChunkServerHeartbeatRequestForTest();
    ChunkServerHeartbeatResponse response;
    ::curve::mds::topology::ChunkServer chunkServer1(
        1, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer chunkServer2(
        2, "hello", "", 1, "192.168.10.2", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer chunkServer3(
        3, "hello", "", 1, "192.168.10.3", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)));
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .Times(2).WillRepeatedly(Return(false));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(1, response.needupdatecopysets_size());
    ASSERT_EQ(1, response.needupdatecopysets(0).logicalpoolid());
    ASSERT_EQ(1, response.needupdatecopysets(0).copysetid());
    ASSERT_EQ(0, response.needupdatecopysets(0).epoch());
}

TEST_F(TestHeartbeatManager,
       test_handle_copySetInfo_stale_epoch_update_err) {
    auto request = GetChunkServerHeartbeatRequestForTest();
    ChunkServerHeartbeatResponse response;
    ::curve::mds::topology::ChunkServer chunkServer1(
        1, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer chunkServer2(
        2, "hello", "", 1, "192.168.10.2", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer chunkServer3(
        3, "hello", "", 1, "192.168.10.3", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);

    // update fail
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)));
    ::curve::mds::topology::CopySetInfo copySetInfo;
    copySetInfo.SetEpoch(1);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(copySetInfo), Return(true)));
    EXPECT_CALL(*coordinator_, CopySetHeartbeat(_, _, _))
        .WillOnce(Return(::curve::mds::topology::UNINTIALIZE_ID));
    EXPECT_CALL(*topology_, UpdateCopySetTopo(_))
        .WillOnce(Return(::curve::mds::topology::kTopoErrCodeInternalError));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager, test_handle_copySetInfo_bigger_epoch) {
    auto request = GetChunkServerHeartbeatRequestForTest();
    ChunkServerHeartbeatResponse response;
    ::curve::mds::topology::ChunkServer chunkServer1(
        1, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer chunkServer2(
        2, "hello", "", 1, "192.168.10.2", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer chunkServer3(
        3, "hello", "", 1, "192.168.10.3", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);

    // update fail
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)));
    ::curve::mds::topology::CopySetInfo copySetInfo;
    copySetInfo.SetEpoch(19);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(copySetInfo), Return(true)));
    EXPECT_CALL(*coordinator_, CopySetHeartbeat(_, _, _))
        .WillOnce(Return(false));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager, test_handle_copysetInfo_equal_epoch) {
    auto request = GetChunkServerHeartbeatRequestForTest();
    ChunkServerHeartbeatResponse response;
    ::curve::mds::topology::ChunkServer chunkServer1(
        1, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer chunkServer2(
        2, "hello", "", 1, "192.168.10.2", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer chunkServer3(
        3, "hello", "", 1, "192.168.10.3", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);

    // 1. topo record and report copySet do not have candidate
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)));
    ::curve::mds::topology::CopySetInfo copySetInfo;
    copySetInfo.SetEpoch(10);
    copySetInfo.SetLeader(1);
    copySetInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(copySetInfo), Return(true)));
    EXPECT_CALL(*coordinator_, CopySetHeartbeat(_, _, _))
        .WillOnce(Return(false));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());

    // 2. topo record candidate but copySet do not have one, update fail
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)));
    copySetInfo.SetCandidate(4);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(copySetInfo), Return(true)));
    EXPECT_CALL(*coordinator_, CopySetHeartbeat(_, _, _))
        .WillOnce(Return(false));
    EXPECT_CALL(*topology_, UpdateCopySetTopo(_))
        .WillOnce(Return(::curve::mds::topology::kTopoErrCodeInternalError));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());
    copySetInfo.ClearCandidate();

    // 3. topo record copySet no candidate but report one
    ::curve::mds::topology::ChunkServer chunkServer4(
        4, "hello", "", 1, "192.168.10.4", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    request.clear_copysetinfos();
    ::curve::mds::heartbeat::CopySetInfo info;
    info.set_logicalpoolid(1);
    info.set_copysetid(1);
    info.set_epoch(10);
    auto candidate = new ConfigChangeInfo;
    for (int i = 1; i <= 4; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        if (i == 1) {
            auto replica = new ::curve::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }
        if (i == 4) {
            auto replica = new ::curve::common::Peer();
            replica->set_address(ip);
            candidate->set_allocated_peer(replica);
            candidate->set_finished(false);
            candidate->set_type(ConfigChangeType::ADD_PEER);
            continue;
        }
        auto replica = info.add_peers();
        replica->set_address(ip);
    }
    info.set_allocated_configchangeinfo(candidate);
    auto addInfo = request.add_copysetinfos();
    *addInfo = info;
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer4), Return(true)));
    EXPECT_CALL(*topology_, UpdateCopySetTopo(_))
        .WillOnce(Return(::curve::mds::topology::kTopoErrCodeInternalError));
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(copySetInfo), Return(true)));
    EXPECT_CALL(*coordinator_, CopySetHeartbeat(_, _, _))
        .WillOnce(Return(false));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());

    // 4. topo record copySet candidate same as report one
    copySetInfo.SetCandidate(4);
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer4), Return(true)));
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(copySetInfo), Return(true)));
    EXPECT_CALL(*coordinator_, CopySetHeartbeat(_, _, _))
        .WillOnce(Return(false));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());

    // 5. topo record copySet candidate not same as report one, update fail
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer4), Return(true)));
    copySetInfo.SetCandidate(5);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(copySetInfo), Return(true)));
    EXPECT_CALL(*coordinator_, CopySetHeartbeat(_, _, _))
        .WillOnce(Return(false));
    EXPECT_CALL(*topology_, UpdateCopySetTopo(_))
        .WillOnce(Return(::curve::mds::topology::kTopoErrCodeSuccess));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager, test_patrol_copySetInfo_no_order) {
    auto request = GetChunkServerHeartbeatRequestForTest();
    ChunkServerHeartbeatResponse response;
    ::curve::mds::topology::ChunkServer chunkServer1(
        1, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer chunkServer2(
        2, "hello", "", 1, "192.168.10.2", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer chunkServer3(
        3, "hello", "", 1, "192.168.10.3", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);

    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)));
    ::curve::mds::topology::CopySetInfo copySetInfo;
    copySetInfo.SetEpoch(10);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copySetInfo), Return(true)))
        .WillOnce(Return(false));
    EXPECT_CALL(*coordinator_, CopySetHeartbeat(_, _, _))
        .WillOnce(Return(false));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager, test_patrol_copySetInfo_return_order) {
    auto request = GetChunkServerHeartbeatRequestForTest();
    ChunkServerHeartbeatResponse response;
    ::curve::mds::topology::ChunkServer chunkServer1(
        1, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer chunkServer2(
        2, "hello", "", 1, "192.168.10.2", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    ::curve::mds::topology::ChunkServer chunkServer3(
        3, "hello", "", 1, "192.168.10.3", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);

    EXPECT_CALL(*topology_, GetChunkServer(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServerNotRetired(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)));
    ::curve::mds::topology::CopySetInfo copySetInfo;
    copySetInfo.SetEpoch(10);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copySetInfo), Return(true)))
        .WillOnce(Return(false));
    ::curve::mds::heartbeat::CopySetConf res;
    res.set_logicalpoolid(1);
    res.set_copysetid(1);
    res.set_epoch(10);
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto replica = res.add_peers();
        replica->set_address(ip);
        if (i == 2) {
            auto replica = new ::curve::common::Peer();
            replica->set_address(ip);
            res.set_allocated_configchangeitem(replica);
        }
    }
    res.set_type(TRANSFER_LEADER);
    EXPECT_CALL(*coordinator_, CopySetHeartbeat(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(res), Return(true)));
    EXPECT_CALL(*topology_, UpdateCopySetTopo(_))
        .WillOnce(Return(::curve::mds::topology::kTopoErrCodeSuccess));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(1, response.needupdatecopysets_size());
    ASSERT_EQ("192.168.10.2:9000:0",
              response.needupdatecopysets(0).configchangeitem().address());
    ASSERT_EQ(TRANSFER_LEADER, response.needupdatecopysets(0).type());
    ASSERT_EQ(3, response.needupdatecopysets(0).peers_size());
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve


