/*
 * Project: curve
 * Created Date: Sat Jan 05 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include "src/mds/heartbeat/heartbeat_manager.h"
#include "test/mds/heartbeat/mock_coordinator.h"
#include "test/mds/heartbeat/mock_topology.h"
#include "test/mds/heartbeat/mock_topoAdapter.h"
#include "test/mds/heartbeat/common.h"

using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;
using ::testing::_;

namespace curve {
namespace mds {
namespace heartbeat {
class TestHeartbeatManager : public ::testing::Test {
 protected:
  TestHeartbeatManager() {}
  ~TestHeartbeatManager() {}

  void SetUp() override {
      topology_ = std::make_shared<MockTopology>();
      topoAdapter_ = std::make_shared<MockTopoAdapter>();
      coordinator_ = std::make_shared<MockCoordinator>();
      heartbeatManager_ = std::make_shared<HeartbeatManager>(topology_,
                                                             coordinator_,
                                                             topoAdapter_);
  }

  void TearDown() override {
  }

 protected:
  std::shared_ptr<MockTopology> topology_;
  std::shared_ptr<MockCoordinator> coordinator_;
  std::shared_ptr<MockTopoAdapter> topoAdapter_;
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
}

TEST_F(TestHeartbeatManager, test_getPeerIdByPort) {
    auto request = GetChunkServerHeartbeatRequestForTest();
    ChunkServerHeartbeatResponse response;

    // 1. test invalid form
    request.clear_copysetinfos();
    CopysetInfo info;
    info.set_logicalpoolid(1);
    info.set_copysetid(1);
    info.set_epoch(10);
    info.set_leaderpeer("192.168.10.1:9000");
    info.add_peers("192.168.10.1:9000:0");
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
    EXPECT_CALL(*topology_, GetChunkServer("192.168.10.1", 9000, _))
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
    EXPECT_CALL(*topology_, GetChunkServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(Return(false));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());

    // 2. can not get leader
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)))
        .WillOnce(Return(false));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());

    // 3. has candidate
    request.clear_copysetinfos();
    CopysetInfo info;
    info.set_logicalpoolid(1);
    info.set_copysetid(1);
    info.set_epoch(10);
    info.set_leaderpeer("192.168.10.1:9000:0");
    info.add_peers("192.168.10.1:9000:0");
    info.add_peers("192.168.10.2:9000:0");
    info.add_peers("192.168.10.3:9000:0");
    auto candidate = new ConfigChangeInfo;
    candidate->set_peer("192.168.10.4:9000:0");
    candidate->set_finished(true);
    info.set_allocated_configchangeinfo(candidate);
    auto addInfo = request.add_copysetinfos();
    *addInfo = info;
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(Return(false));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager, test_chunkServer_heartbeat_not_leader) {
    auto request = GetChunkServerHeartbeatRequestForTest();
    ChunkServerHeartbeatResponse response;

    request.clear_copysetinfos();
    CopysetInfo info;
    info.set_logicalpoolid(1);
    info.set_copysetid(1);
    info.set_epoch(10);
    info.set_leaderpeer("192.168.10.2:9000:0");
    info.add_peers("192.168.10.1:9000:0");
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;
    ::curve::mds::topology::ChunkServer chunkServer(
        1, "hello", "", 1, "192.168.10.1", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer), Return(true)));
    ::curve::mds::topology::ChunkServer leaderChunkServer(
        2, "hello", "", 1, "192.168.10.2", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(leaderChunkServer), Return(true)));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());
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
    EXPECT_CALL(*topology_, GetChunkServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetCopySet(_, _)).WillOnce(Return(false));
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
    EXPECT_CALL(*topology_, GetChunkServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)));
    ::curve::mds::topology::CopySetInfo copySetInfo;
    copySetInfo.SetEpoch(1);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copySetInfo), Return(true)));
    EXPECT_CALL(*topology_, UpdateCopySet(_))
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
    EXPECT_CALL(*topology_, GetChunkServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)));
    ::curve::mds::topology::CopySetInfo copySetInfo;
    copySetInfo.SetEpoch(19);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copySetInfo), Return(true)));
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
    EXPECT_CALL(*topology_, GetChunkServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)));
    ::curve::mds::topology::CopySetInfo copySetInfo;
    copySetInfo.SetEpoch(10);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copySetInfo), Return(true)));
    EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
        .WillOnce(Return(false));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());

    // 2. topo record candidate but copySet do not have one, update fail
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)));
    copySetInfo.SetCandidate(4);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copySetInfo), Return(true)));
    EXPECT_CALL(*topology_, UpdateCopySet(_))
        .WillOnce(Return(::curve::mds::topology::kTopoErrCodeInternalError));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());
    copySetInfo.ClearCandidate();

    // 3. topo record copySet no candidate but report one
    ::curve::mds::topology::ChunkServer chunkServer4(
        4, "hello", "", 1, "192.168.10.4", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    request.clear_copysetinfos();
    CopysetInfo info;
    info.set_logicalpoolid(1);
    info.set_copysetid(1);
    info.set_epoch(10);
    info.set_leaderpeer("192.168.10.1:9000:0");
    info.add_peers("192.168.10.1:9000:0");
    info.add_peers("192.168.10.2:9000:0");
    info.add_peers("192.168.10.3:9000:0");
    auto candidate = new ConfigChangeInfo;
    candidate->set_peer("192.168.10.4:9000:0");
    candidate->set_finished(false);
    info.set_allocated_configchangeinfo(candidate);
    auto addInfo = request.add_copysetinfos();
    *addInfo = info;
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer4), Return(true)));
    EXPECT_CALL(*topology_, UpdateCopySet(_))
        .WillOnce(Return(::curve::mds::topology::kTopoErrCodeInternalError));
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copySetInfo), Return(true)));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());

    // 4. topo record copySet candidate same as report one
    copySetInfo.SetCandidate(4);
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer4), Return(true)));
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copySetInfo), Return(true)));
    EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
        .WillOnce(Return(false));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());

    // 5. topo record copySet candidate not same as report one, update fail
    EXPECT_CALL(*topology_, GetChunkServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer4), Return(true)));
    copySetInfo.SetCandidate(5);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copySetInfo), Return(true)));
    EXPECT_CALL(*topology_, UpdateCopySet(_))
        .WillOnce(Return(::curve::mds::topology::kTopoErrCodeInternalError));
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
    EXPECT_CALL(*topology_, GetChunkServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)));
    ::curve::mds::topology::CopySetInfo copySetInfo;
    copySetInfo.SetEpoch(10);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copySetInfo), Return(true)));
    EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*coordinator_, CopySetHeartbeat(_, _)).WillOnce(Return(false));
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
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer2), Return(true)));
    EXPECT_CALL(*topology_, GetChunkServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)));
    ::curve::mds::topology::CopySetInfo copySetInfo;
    copySetInfo.SetEpoch(10);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copySetInfo), Return(true)));
    EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
        .WillOnce(Return(true));
    ::curve::mds::schedule::CopySetConf res;
    res.id.first = 1;
    res.id.second = 1;
    res.epoch = 10;
    ::curve::mds::schedule::PeerInfo peerInfo1(1, 1, 1, "192.168.10.1", 9000);
    ::curve::mds::schedule::PeerInfo peerInfo2(2, 1, 1, "192.168.10.2", 9000);
    ::curve::mds::schedule::PeerInfo peerInfo3(3, 1, 1, "192.168.10.3", 9000);
    res.peers = std::vector<::curve::mds::schedule::PeerInfo>(
        {peerInfo1, peerInfo2, peerInfo3});
    res.type = TRANSFER_LEADER;
    res.configChangeItem = 2;
    EXPECT_CALL(*coordinator_, CopySetHeartbeat(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(res), Return(true)));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(1, response.needupdatecopysets_size());
    ASSERT_EQ("192.168.10.2:9000:0",
              response.needupdatecopysets(0).configchangeitem());
    ASSERT_EQ(TRANSFER_LEADER, response.needupdatecopysets(0).type());
}

TEST_F(TestHeartbeatManager,
       test_patrol_copySetInfo_return_order_cannot_get_candidate) {
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
        .WillOnce(DoAll(SetArgPointee<1>(chunkServer1), Return(true)))
        .WillOnce(Return(false));
    EXPECT_CALL(*topology_, GetChunkServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer3), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkServer1), Return(true)));
    ::curve::mds::topology::CopySetInfo copySetInfo;
    copySetInfo.SetEpoch(10);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copySetInfo), Return(true)));
    EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
        .WillOnce(Return(true));
    ::curve::mds::schedule::CopySetConf res;
    res.id.first = 1;
    res.id.second = 1;
    res.epoch = 10;
    ::curve::mds::schedule::PeerInfo peerInfo1(1, 1, 1, "192.168.10.1", 9000);
    ::curve::mds::schedule::PeerInfo peerInfo2(2, 1, 1, "192.168.10.2", 9000);
    ::curve::mds::schedule::PeerInfo peerInfo3(3, 1, 1, "192.168.10.3", 9000);
    res.peers = std::vector<::curve::mds::schedule::PeerInfo>(
        {peerInfo1, peerInfo2, peerInfo3});
    res.type = TRANSFER_LEADER;
    res.configChangeItem = 5;
    EXPECT_CALL(*coordinator_, CopySetHeartbeat(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(res), Return(true)));
    heartbeatManager_->ChunkServerHeartbeat(request, &response);
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager, test_checkHeartBeat_interval) {
    HeartbeatOption option;
    option.heartbeatInterval = 1;
    option.heartbeatMissTimeOut = 3;
    option.offLineTimeOut = 5;
    heartbeatManager_->Init(option);

    HeartbeatInfo info;
    heartbeatManager_->UpdateLastReceivedHeartbeatTime(1, steady_clock::now());
    heartbeatManager_->UpdateLastReceivedHeartbeatTime(
        2, steady_clock::now() - std::chrono::seconds(4));
    heartbeatManager_->UpdateLastReceivedHeartbeatTime(
        3, steady_clock::now() - std::chrono::seconds(10));
    ASSERT_TRUE(heartbeatManager_->GetHeartBeatInfo(1, &info));
    ASSERT_TRUE(info.OnlineFlag);
    ASSERT_TRUE(heartbeatManager_->GetHeartBeatInfo(2, &info));
    ASSERT_TRUE(info.OnlineFlag);
    ASSERT_TRUE(heartbeatManager_->GetHeartBeatInfo(3, &info));
    ASSERT_TRUE(info.OnlineFlag);
    ASSERT_FALSE(heartbeatManager_->GetHeartBeatInfo(4, &info));

    ::curve::mds::topology::ChunkServer chunkServer(
        1, "", "", 1, "", 9000, "",
        ::curve::mds::topology::ChunkServerStatus::READWRITE);
    EXPECT_CALL(*topology_, GetChunkServer(_, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkServer), Return(true)));
    EXPECT_CALL(*topology_, UpdateChunkServerState(_, _))
        .Times(2).WillRepeatedly(Return(true));
    heartbeatManager_->CheckHeartBeatInterval();
    ASSERT_TRUE(heartbeatManager_->GetHeartBeatInfo(1, &info));
    ASSERT_TRUE(info.OnlineFlag);
    ASSERT_TRUE(heartbeatManager_->GetHeartBeatInfo(2, &info));
    ASSERT_TRUE(info.OnlineFlag);
    ASSERT_TRUE(heartbeatManager_->GetHeartBeatInfo(3, &info));
    ASSERT_FALSE(info.OnlineFlag);

    heartbeatManager_->UpdateLastReceivedHeartbeatTime(
        3, steady_clock::now());
    heartbeatManager_->CheckHeartBeatInterval();
    ASSERT_TRUE(heartbeatManager_->GetHeartBeatInfo(1, &info));
    ASSERT_TRUE(info.OnlineFlag);
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve


