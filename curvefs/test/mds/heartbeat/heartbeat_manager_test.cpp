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
 * @Date: 2021-09-28 14:07:44
 * @Author: chenwei
 */

#include "curvefs/src/mds/heartbeat/heartbeat_manager.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/time.h>

#include "curvefs/src/mds/heartbeat/metaserver_healthy_checker.h"
#include "curvefs/test/mds/mock/mock_topology.h"
#include "curvefs/test/mds/mock/mock_coordinator.h"
#include "src/common/timeutility.h"

using ::curvefs::mds::topology::MockIdGenerator;
using ::curvefs::mds::topology::MockStorage;
using ::curvefs::mds::topology::MockTokenGenerator;
using ::curvefs::mds::topology::MockTopology;
using ::curvefs::mds::topology::TopoStatusCode;
using ::curve::mds::heartbeat::ConfigChangeType;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArgPointee;

namespace curvefs {
namespace mds {
namespace heartbeat {
class TestHeartbeatManager : public ::testing::Test {
 protected:
    TestHeartbeatManager() {}
    ~TestHeartbeatManager() {}

    void SetUp() override {
        HeartbeatOption option;
        option.heartbeatIntervalMs = 1000;
        option.heartbeatMissTimeOutMs = 10000;
        option.offLineTimeOutMs = 30000;
        topology_ = std::make_shared<MockTopology>(idGenerator_,
                                                   tokenGenerator_, storage_);
        coordinator_ = std::make_shared<MockCoordinator>();
        heartbeatManager_ =
            std::make_shared<HeartbeatManager>(option, topology_, coordinator_);
    }

    void TearDown() override {}

 protected:
    std::shared_ptr<MockIdGenerator> idGenerator_;
    std::shared_ptr<MockTokenGenerator> tokenGenerator_;
    std::shared_ptr<MockStorage> storage_;
    std::shared_ptr<MockTopology> topology_;
    std::shared_ptr<HeartbeatManager> heartbeatManager_;
    std::shared_ptr<Coordinator> coordinator_;
};

MetaServerHeartbeatRequest GetMetaServerHeartbeatRequestForTest() {
    MetaServerHeartbeatRequest request;
    request.set_metaserverid(1);
    request.set_token("hello");
    request.set_ip("192.168.10.1");
    request.set_port(9000);
    request.set_starttime(1000);
    request.set_leadercount(10);
    request.set_copysetcount(100);
    MetaSeverSpaceStatus status;
    status.set_diskcopysetminrequirebyte(0);
    status.set_diskthresholdbyte(0);
    status.set_diskusedbyte(0);
    status.set_memorythresholdbyte(0);
    status.set_memorycopysetminrequirebyte(0);
    status.set_memoryusedbyte(0);
    request.mutable_spacestatus()->CopyFrom(status);

    auto info = request.add_copysetinfos();
    info->set_poolid(1);
    info->set_copysetid(1);
    info->set_epoch(10);
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto peer = info->add_peers();
        peer->set_address(ip);
        if (i == 1) {
            auto peer = new ::curvefs::common::Peer();
            peer->set_address(ip);
            info->set_allocated_leaderpeer(peer);
        }
    }

    return request;
}

TEST_F(TestHeartbeatManager, test_stop_and_run) {
    heartbeatManager_->Run();
    heartbeatManager_->Stop();
}

TEST_F(TestHeartbeatManager, test_stop_and_run_repeat) {
    heartbeatManager_->Run();
    heartbeatManager_->Run();
    heartbeatManager_->Stop();
    heartbeatManager_->Stop();
}

TEST_F(TestHeartbeatManager, test_Init_empty) {
    std::vector<MetaServerIdType> metaserverList;
    EXPECT_CALL(*topology_, GetMetaServerInCluster(_))
        .WillOnce(Return(metaserverList));
    heartbeatManager_->Init();
}

TEST_F(TestHeartbeatManager, test_Init) {
    std::vector<MetaServerIdType> metaserverList = {1};
    EXPECT_CALL(*topology_, GetMetaServerInCluster(_))
        .WillOnce(Return(metaserverList));
    heartbeatManager_->Init();
}

TEST_F(TestHeartbeatManager, test_checkReuqest_abnormal) {
    MetaServerHeartbeatRequest request;
    MetaServerHeartbeatResponse response;

    // 2. can not get metaServer
    request = GetMetaServerHeartbeatRequestForTest();
    ASSERT_TRUE(request.IsInitialized());
    EXPECT_CALL(*topology_, GetMetaServer(_, _)).WillOnce(Return(false));
    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbMetaServerUnknown, response.statuscode());

    // 3. ip not same
    ::curvefs::mds::topology::MetaServer metaServer(
        1, "hostname", "hello", 1, "192.168.10.4", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer), Return(true)));
    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbMetaServerIpPortNotMatch,
              response.statuscode());

    // 4. port not same
    metaServer.SetInternalIp("192.168.10.1");
    metaServer.SetInternalPort(11000);
    EXPECT_CALL(*topology_, GetMetaServer(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer), Return(true)));
    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbMetaServerIpPortNotMatch,
              response.statuscode());

    // 5. token not same
    metaServer.SetInternalPort(9000);
    metaServer.SetToken("hellocode");
    EXPECT_CALL(*topology_, GetMetaServer(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer), Return(true)));
    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbMetaServerTokenNotMatch,
              response.statuscode());
}

TEST_F(TestHeartbeatManager, test_updatespace_ok) {
    ::curvefs::mds::topology::MetaServer metaServer(
        1, "hostname", "hello", 1, "192.168.10.1", 9000, "", 9000);
    ::curvefs::mds::topology::Server server;
    ::curvefs::mds::topology::Zone zone;

    auto request = GetMetaServerHeartbeatRequestForTest();
    MetaServerHeartbeatResponse response;
    request.clear_copysetinfos();
    EXPECT_CALL(*topology_, GetMetaServer(_, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(metaServer), Return(true)));
    EXPECT_CALL(*topology_, GetServer(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(server), Return(true)));

    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbOK, response.statuscode());
}

TEST_F(TestHeartbeatManager, test_updatespace_get_metaserver_fail1) {
    ::curvefs::mds::topology::MetaServer metaServer(
        1, "hostname", "hello", 1, "192.168.10.1", 9000, "", 9000);

    auto request = GetMetaServerHeartbeatRequestForTest();
    MetaServerHeartbeatResponse response;
    request.clear_copysetinfos();
    EXPECT_CALL(*topology_, GetMetaServer(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer), Return(false)));
    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbMetaServerUnknown, response.statuscode());
}

TEST_F(TestHeartbeatManager, test_updatespace_get_metaserver_fail2) {
    ::curvefs::mds::topology::MetaServer metaServer(
        1, "hostname", "hello", 1, "192.168.10.1", 9000, "", 9000);

    auto request = GetMetaServerHeartbeatRequestForTest();
    MetaServerHeartbeatResponse response;
    request.clear_copysetinfos();
    EXPECT_CALL(*topology_, GetMetaServer(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer), Return(false)));
    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbOK, response.statuscode());
}


TEST_F(TestHeartbeatManager, test_updatespace_get_server_fail) {
    ::curvefs::mds::topology::MetaServer metaServer(
        1, "hostname", "hello", 1, "192.168.10.1", 9000, "", 9000);
    ::curvefs::mds::topology::Server server;
    ::curvefs::mds::topology::Pool pool;

    auto request = GetMetaServerHeartbeatRequestForTest();
    MetaServerHeartbeatResponse response;
    request.clear_copysetinfos();
    EXPECT_CALL(*topology_, GetMetaServer(_, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(metaServer), Return(true)));
    EXPECT_CALL(*topology_, GetServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(server), Return(false)));
    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbOK, response.statuscode());
}

TEST_F(TestHeartbeatManager, test_updatespace_get_pool_fail) {
    ::curvefs::mds::topology::MetaServer metaServer(
        1, "hostname", "hello", 1, "192.168.10.1", 9000, "", 9000);
    ::curvefs::mds::topology::Server server;
    ::curvefs::mds::topology::Zone zone;

    auto request = GetMetaServerHeartbeatRequestForTest();
    MetaServerHeartbeatResponse response;
    request.clear_copysetinfos();
    EXPECT_CALL(*topology_, GetMetaServer(_, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(metaServer), Return(true)));
    EXPECT_CALL(*topology_, GetServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(server), Return(true)));

    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbOK, response.statuscode());
}

TEST_F(TestHeartbeatManager, test_getMetaserverIdByPeerStr) {
    auto request = GetMetaServerHeartbeatRequestForTest();
    MetaServerHeartbeatResponse response;

    // 1. test invalid form
    request.clear_copysetinfos();
    ::curvefs::mds::heartbeat::CopySetInfo info;
    info.set_poolid(1);
    info.set_copysetid(1);
    info.set_epoch(10);
    auto replica = info.add_peers();
    replica->set_address("192.168.10.1:9000");
    auto leader = new ::curvefs::common::Peer();
    leader->set_address("192.168.10.1:9000");
    info.set_allocated_leaderpeer(leader);
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;
    ::curvefs::mds::topology::MetaServer metaServer(
        1, "hostname", "hello", 1, "192.168.10.1", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer), Return(true)));
    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbAnalyseCopysetError,
              response.statuscode());

    // 2. test can not get metaServer
    request = GetMetaServerHeartbeatRequestForTest();
    EXPECT_CALL(*topology_, GetMetaServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer), Return(true)));
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.1", 9000, _))
        .WillOnce(Return(false));
    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbAnalyseCopysetError,
              response.statuscode());
}

TEST_F(TestHeartbeatManager, test_heartbeatCopySetInfo_to_topologyOne) {
    auto request = GetMetaServerHeartbeatRequestForTest();
    MetaServerHeartbeatResponse response;
    ::curvefs::mds::topology::MetaServer metaServer1(
        1, "hostname", "hello", 1, "192.168.10.1", 9000, "", 9000);
    ::curvefs::mds::topology::MetaServer metaServer2(
        2, "hostname", "hello", 1, "192.168.10.2", 9000, "", 9000);
    ::curvefs::mds::topology::MetaServer metaServer3(
        3, "hostname", "hello", 1, "192.168.10.3", 9000, "", 9000);

    // 1. can not get peers
    EXPECT_CALL(*topology_, GetMetaServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)));
    EXPECT_CALL(*topology_, GetMetaServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer1), Return(true)))
        .WillOnce(Return(false));
    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbAnalyseCopysetError,
              response.statuscode());

    // 2. can not get leader
    EXPECT_CALL(*topology_, GetMetaServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)));
    EXPECT_CALL(*topology_, GetMetaServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer1), Return(false)));
    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbAnalyseCopysetError,
              response.statuscode());

    // 3. has candidate and cannot get candidate
    request.clear_copysetinfos();
    ::curvefs::mds::heartbeat::CopySetInfo info;
    info.set_poolid(1);
    info.set_copysetid(1);
    info.set_epoch(10);
    auto candidate = new ConfigChangeInfo;
    for (int i = 1; i <= 4; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        if (i == 1) {
            auto replica = new ::curvefs::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }

        if (i == 4) {
            auto replica = new ::curvefs::common::Peer();
            replica->set_address(ip);
            candidate->set_allocated_peer(replica);
            candidate->set_finished(true);
            candidate->set_type(ConfigChangeType::ADD_PEER);
            continue;
        }
        auto replica = info.add_peers();
        replica->set_address(ip);
    }
    auto addInfo = request.add_copysetinfos();
    info.set_allocated_configchangeinfo(candidate);
    *addInfo = info;
    EXPECT_CALL(*topology_, GetMetaServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)));
    EXPECT_CALL(*topology_, GetMetaServer(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer2), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer3), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer3), Return(true)));

    EXPECT_CALL(*topology_, GetCopySet(_, _)).WillOnce(Return(false))
        .WillOnce(Return(false));

    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbOK, response.statuscode());
}

TEST_F(TestHeartbeatManager, test_not_leader) {
    auto request = GetMetaServerHeartbeatRequestForTest();
    MetaServerHeartbeatResponse response;

    request.clear_copysetinfos();
    ::curvefs::mds::heartbeat::CopySetInfo info;
    info.set_poolid(1);
    info.set_copysetid(1);
    info.set_epoch(2);
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto replica = info.add_peers();
        replica->set_address(ip);
        if (i == 2) {
            auto replica = new ::curvefs::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }
    }
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;
    ::curvefs::mds::topology::MetaServer metaServer1(
        1, "hostname", "hello", 1, "192.168.10.1", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer leaderMetaServer(
        2, "hostname", "hello", 1, "192.168.10.2", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.2", _, _))
//        .WillOnce(DoAll(SetArgPointee<2>(leaderMetaServer), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(leaderMetaServer), Return(true)));
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.1", _, _))
//        .WillOnce(DoAll(SetArgPointee<2>(metaServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer metaServer3(
        3, "hostname", "hello", 1, "192.168.10.3", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.3", _, _))
//        .WillOnce(DoAll(SetArgPointee<2>(metaServer3), Return(true)))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer3), Return(true)));

    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbOK, response.statuscode());
}

TEST_F(TestHeartbeatManager, test_reqEpoch_LargerThan_mdsRecord_UpdateSuccess) {
    auto request = GetMetaServerHeartbeatRequestForTest();
    MetaServerHeartbeatResponse response;

    request.clear_copysetinfos();
    ::curvefs::mds::heartbeat::CopySetInfo info;
    info.set_poolid(1);
    info.set_copysetid(1);
    info.set_epoch(2);
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto replica = info.add_peers();
        replica->set_address(ip);
        if (i == 1) {
            auto replica = new ::curvefs::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }
    }
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;
    ::curvefs::mds::topology::MetaServer metaServer1(
        1, "hostname", "hello", 1, "192.168.10.1", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer leaderMetaServer(
        2, "hostname", "hello", 1, "192.168.10.2", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.2", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(leaderMetaServer), Return(true)));
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.1", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer metaServer3(
        3, "hostname", "hello", 1, "192.168.10.3", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.3", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer3), Return(true)));
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo(1, 1);
    recordCopySetInfo.SetEpoch(1);
    recordCopySetInfo.SetLeader(2);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    EXPECT_CALL(*topology_, UpdateCopySetTopo(_))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));

    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbOK, response.statuscode());
}

TEST_F(TestHeartbeatManager, test_reqEpoch_LargerThan_mdsRecord_UpdateFail) {
    auto request = GetMetaServerHeartbeatRequestForTest();
    MetaServerHeartbeatResponse response;

    request.clear_copysetinfos();
    ::curvefs::mds::heartbeat::CopySetInfo info;
    info.set_poolid(1);
    info.set_copysetid(1);
    info.set_epoch(2);
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto replica = info.add_peers();
        replica->set_address(ip);
        if (i == 1) {
            auto replica = new ::curvefs::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }
    }
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;
    ::curvefs::mds::topology::MetaServer metaServer1(
        1, "hostname", "hello", 1, "192.168.10.1", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer leaderMetaServer(
        2, "hostname", "hello", 1, "192.168.10.2", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.2", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(leaderMetaServer), Return(true)));
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.1", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer metaServer3(
        3, "hostname", "hello", 1, "192.168.10.3", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.3", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer3), Return(true)));
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo(1, 1);
    recordCopySetInfo.SetEpoch(1);
    recordCopySetInfo.SetLeader(2);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    EXPECT_CALL(*topology_, UpdateCopySetTopo(_))
        .WillOnce(Return(TopoStatusCode::TOPO_INTERNAL_ERROR));

    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbOK, response.statuscode());
}

TEST_F(TestHeartbeatManager,
        test_reqEpoch_EqualTo_mdsRecord_copysetmember_not_same) {
    auto request = GetMetaServerHeartbeatRequestForTest();
    MetaServerHeartbeatResponse response;

    request.clear_copysetinfos();
    ::curvefs::mds::heartbeat::CopySetInfo info;
    info.set_poolid(1);
    info.set_copysetid(1);
    info.set_epoch(2);
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto replica = info.add_peers();
        replica->set_address(ip);
        if (i == 1) {
            auto replica = new ::curvefs::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }
    }
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;
    ::curvefs::mds::topology::MetaServer metaServer1(
        1, "hostname", "hello", 1, "192.168.10.1", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer leaderMetaServer(
        2, "hostname", "hello", 1, "192.168.10.2", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.2", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(leaderMetaServer), Return(true)));
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.1", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer metaServer3(
        3, "hostname", "hello", 1, "192.168.10.3", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.3", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer3), Return(true)));
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo(1, 1);
    recordCopySetInfo.SetEpoch(2);
    recordCopySetInfo.SetLeader(2);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbOK, response.statuscode());
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager, test_reqEpoch_EqualTo_mdsRecord_no_candidate) {
    auto request = GetMetaServerHeartbeatRequestForTest();
    MetaServerHeartbeatResponse response;

    request.clear_copysetinfos();
    ::curvefs::mds::heartbeat::CopySetInfo info;
    info.set_poolid(1);
    info.set_copysetid(1);
    info.set_epoch(2);
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto replica = info.add_peers();
        replica->set_address(ip);
        if (i == 1) {
            auto replica = new ::curvefs::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }
    }
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;
    ::curvefs::mds::topology::MetaServer metaServer1(
        1, "hostname", "hello", 1, "192.168.10.1", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer leaderMetaServer(
        2, "hostname", "hello", 1, "192.168.10.2", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.2", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(leaderMetaServer), Return(true)));
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.1", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer metaServer3(
        3, "hostname", "hello", 1, "192.168.10.3", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.3", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer3), Return(true)));
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo(1, 1);
    recordCopySetInfo.SetEpoch(2);
    recordCopySetInfo.SetLeader(2);
    std::set<MetaServerIdType> peers;
    for (int i = 1; i <= 3; i++) {
        peers.emplace(i);
    }
    recordCopySetInfo.SetCopySetMembers(peers);

    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbOK, response.statuscode());
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager, test_reqEpoch_EqualTo_mdsRecord_topo_candidate) {
    auto request = GetMetaServerHeartbeatRequestForTest();
    MetaServerHeartbeatResponse response;

    request.clear_copysetinfos();
    ::curvefs::mds::heartbeat::CopySetInfo info;
    info.set_poolid(1);
    info.set_copysetid(1);
    info.set_epoch(2);
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto replica = info.add_peers();
        replica->set_address(ip);
        if (i == 1) {
            auto replica = new ::curvefs::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }
    }
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;
    ::curvefs::mds::topology::MetaServer metaServer1(
        1, "hostname", "hello", 1, "192.168.10.1", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer leaderMetaServer(
        2, "hostname", "hello", 1, "192.168.10.2", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.2", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(leaderMetaServer), Return(true)));
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.1", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer metaServer3(
        3, "hostname", "hello", 1, "192.168.10.3", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.3", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer3), Return(true)));
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo(1, 1);
    recordCopySetInfo.SetEpoch(2);
    recordCopySetInfo.SetLeader(2);
    std::set<MetaServerIdType> peers;
    for (int i = 1; i <= 3; i++) {
        peers.emplace(i);
    }
    recordCopySetInfo.SetCopySetMembers(peers);
    recordCopySetInfo.SetCandidate(4);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbOK, response.statuscode());
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager, test_reqEpoch_EqualTo_mdsRecord_report_candidate) {
    auto request = GetMetaServerHeartbeatRequestForTest();
    MetaServerHeartbeatResponse response;

    request.clear_copysetinfos();
    ::curvefs::mds::heartbeat::CopySetInfo info;
    info.set_poolid(1);
    info.set_copysetid(1);
    info.set_epoch(2);
    auto candidate = new ConfigChangeInfo;
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto replica = info.add_peers();
        replica->set_address(ip);
        if (i == 1) {
            auto replica = new ::curvefs::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }

        if (i == 4) {
            auto replica = new ::curvefs::common::Peer();
            replica->set_address(ip);
            candidate->set_allocated_peer(replica);
            candidate->set_finished(false);
            candidate->set_type(ConfigChangeType::ADD_PEER);
            continue;
        }
    }
    info.set_allocated_configchangeinfo(candidate);
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;
    ::curvefs::mds::topology::MetaServer metaServer1(
        1, "hostname", "hello", 1, "192.168.10.1", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer leaderMetaServer(
        2, "hostname", "hello", 1, "192.168.10.2", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.2", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(leaderMetaServer), Return(true)));
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.1", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer metaServer3(
        3, "hostname", "hello", 1, "192.168.10.3", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.3", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer3), Return(true)));
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo(1, 1);
    recordCopySetInfo.SetEpoch(2);
    recordCopySetInfo.SetLeader(2);
    std::set<MetaServerIdType> peers;
    for (int i = 1; i <= 3; i++) {
        peers.emplace(i);
    }
    recordCopySetInfo.SetCopySetMembers(peers);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbOK, response.statuscode());
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager, test_reqEpoch_EqualTo_mdsRecord_has_candidate) {
    auto request = GetMetaServerHeartbeatRequestForTest();
    MetaServerHeartbeatResponse response;

    request.clear_copysetinfos();
    ::curvefs::mds::heartbeat::CopySetInfo info;
    info.set_poolid(1);
    info.set_copysetid(1);
    info.set_epoch(2);
    auto candidate = new ConfigChangeInfo;
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto replica = info.add_peers();
        replica->set_address(ip);
        if (i == 1) {
            auto replica = new ::curvefs::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }

        if (i == 4) {
            auto replica = new ::curvefs::common::Peer();
            replica->set_address(ip);
            candidate->set_allocated_peer(replica);
            candidate->set_finished(false);
            candidate->set_type(ConfigChangeType::ADD_PEER);
            continue;
        }
    }
    info.set_allocated_configchangeinfo(candidate);
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;
    ::curvefs::mds::topology::MetaServer metaServer1(
        1, "hostname", "hello", 1, "192.168.10.1", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer leaderMetaServer(
        2, "hostname", "hello", 1, "192.168.10.2", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.2", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(leaderMetaServer), Return(true)));
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.1", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer metaServer3(
        3, "hostname", "hello", 1, "192.168.10.3", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.3", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer3), Return(true)));
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo(1, 1);
    recordCopySetInfo.SetEpoch(2);
    recordCopySetInfo.SetLeader(2);
    std::set<MetaServerIdType> peers;
    for (int i = 1; i <= 3; i++) {
        peers.emplace(i);
    }
    recordCopySetInfo.SetCopySetMembers(peers);
    recordCopySetInfo.SetCandidate(4);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbOK, response.statuscode());
    ASSERT_EQ(0, response.needupdatecopysets_size());
}

TEST_F(TestHeartbeatManager, test_reqEpoch_SmallThan_mdsRecord) {
    auto request = GetMetaServerHeartbeatRequestForTest();
    MetaServerHeartbeatResponse response;

    request.clear_copysetinfos();
    ::curvefs::mds::heartbeat::CopySetInfo info;
    info.set_poolid(1);
    info.set_copysetid(1);
    info.set_epoch(2);
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto replica = info.add_peers();
        replica->set_address(ip);
        if (i == 1) {
            auto replica = new ::curvefs::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }
    }
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;
    ::curvefs::mds::topology::MetaServer metaServer1(
        1, "hostname", "hello", 1, "192.168.10.1", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer leaderMetaServer(
        2, "hostname", "hello", 1, "192.168.10.2", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.2", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(leaderMetaServer), Return(true)));
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.1", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer metaServer3(
        3, "hostname", "hello", 1, "192.168.10.3", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.3", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer3), Return(true)));
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo(1, 1);
    recordCopySetInfo.SetEpoch(100);
    recordCopySetInfo.SetLeader(2);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbOK, response.statuscode());
}

TEST_F(TestHeartbeatManager, test_update_partition) {
    auto request = GetMetaServerHeartbeatRequestForTest();
    MetaServerHeartbeatResponse response;

    request.clear_copysetinfos();
    ::curvefs::mds::heartbeat::CopySetInfo info;
    info.set_poolid(1);
    info.set_copysetid(1);
    info.set_epoch(2);
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto replica = info.add_peers();
        replica->set_address(ip);
        if (i == 1) {
            auto replica = new ::curvefs::common::Peer();
            replica->set_address(ip);
            info.set_allocated_leaderpeer(replica);
        }
    }
    auto partitioninfo = info.add_partitioninfolist();
    partitioninfo->set_fsid(1);
    partitioninfo->set_poolid(1);
    partitioninfo->set_copysetid(1);
    partitioninfo->set_partitionid(1);
    partitioninfo->set_start(1);
    partitioninfo->set_end(1);
    partitioninfo->set_txid(1);
    partitioninfo->set_status(PartitionStatus::READWRITE);
    partitioninfo->set_inodenum(1);
    auto addInfos = request.add_copysetinfos();
    *addInfos = info;
    ::curvefs::mds::topology::MetaServer metaServer1(
        1, "hostname", "hello", 1, "192.168.10.1", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer leaderMetaServer(
        2, "hostname", "hello", 1, "192.168.10.2", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.2", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(leaderMetaServer), Return(true)));
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.1", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer1), Return(true)));
    ::curvefs::mds::topology::MetaServer metaServer3(
        3, "hostname", "hello", 1, "192.168.10.3", 9000, "", 9000);
    EXPECT_CALL(*topology_, GetMetaServer("192.168.10.3", _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServer3), Return(true)));
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo(1, 1);
    recordCopySetInfo.SetEpoch(1);
    recordCopySetInfo.SetLeader(2);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)))
        .WillOnce(DoAll(SetArgPointee<1>(recordCopySetInfo), Return(true)));

    EXPECT_CALL(*topology_, UpdateCopySetTopo(_))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));

    std::list<::curvefs::mds::topology::Partition> topoPartitionList;
    ::curvefs::mds::topology::Partition tempPartition;
    tempPartition.SetPartitionId(1);
    topoPartitionList.push_back(tempPartition);
    EXPECT_CALL(*topology_, GetPartitionInfosInCopyset(_))
        .WillOnce(Return(topoPartitionList));

    ::curvefs::mds::topology::Partition partitionInTopo;
    EXPECT_CALL(*topology_, GetPartition(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(partitionInTopo), Return(true)));
    EXPECT_CALL(*topology_, UpdatePartitionStatistic(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));
    heartbeatManager_->MetaServerHeartbeat(request, &response);
    ASSERT_EQ(HeartbeatStatusCode::hbOK, response.statuscode());
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curvefs
