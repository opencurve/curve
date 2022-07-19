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
 * @Date: 2021-06-10 10:46:50
 * @Author: chenwei
 */
#include <butil/at_exit.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thread>  // NOLINT

#include "curvefs/src/metaserver/metaserver.h"
#include "curvefs/test/metaserver/mock_topology_service.h"
#include "curvefs/test/metaserver/mock_heartbeat_service.h"
#include "curvefs/test/metaserver/storage/utils.h"

using ::curvefs::mds::heartbeat::HeartbeatStatusCode;
using ::curvefs::mds::heartbeat::MetaServerHeartbeatRequest;
using ::curvefs::mds::heartbeat::MetaServerHeartbeatResponse;
using ::curvefs::mds::heartbeat::MockHeartbeatService;
using ::curvefs::mds::topology::MetaServerRegistRequest;
using ::curvefs::mds::topology::MetaServerRegistResponse;
using ::curvefs::mds::topology::MockTopologyService;
using ::curvefs::mds::topology::TopoStatusCode;
using ::curvefs::metaserver::storage::RandomStoragePath;
using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::StrEq;

butil::AtExitManager atExit;

namespace curvefs {
namespace metaserver {
class MetaserverTest : public ::testing::Test {
 protected:
    void SetUp() override {
        // run mds server
        Aws::InitAPI(aws_sdk_options_);
        dataDir_ = RandomStoragePath();
        metaPath_ = "./meta.dat";
        ASSERT_EQ(0, server_.AddService(&mockTopologyService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.AddService(&mockHeartbeatService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        uint16_t port = 56800;
        int ret = 0;
        while (port < 65535) {
            topologyServiceAddr_ = "127.0.0.1:" + std::to_string(port);
            ret = server_.Start(topologyServiceAddr_.c_str(), nullptr);
            if (ret >= 0) {
                LOG(INFO) << "topology service success, listen port = " << port;
                break;
            }

            ++port;
        }
        ASSERT_EQ(0, ret);

        metaserverIp_ = "127.0.0.1";
        metaserverPort_ = "56702";
        metaserverExternalPort_ = "56703";
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
        auto output = execShell("rm -rf " + dataDir_);
        ASSERT_EQ(output.size(), 0);
        Aws::ShutdownAPI(aws_sdk_options_);
        return;
    }

    std::string execShell(const string& cmd) {
        std::array<char, 128> buffer;
        std::string result;
        std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"),
                                                      pclose);
        if (!pipe) {
            throw std::runtime_error("popen() failed!");
        }
        while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
            result += buffer.data();
        }
        return result;
    }

    std::string dataDir_;
    std::string metaserverIp_;
    std::string metaserverPort_;
    std::string metaserverExternalPort_;
    std::string topologyServiceAddr_;
    std::string metaPath_;
    MockTopologyService mockTopologyService_;
    MockHeartbeatService mockHeartbeatService_;

    brpc::Server server_;
    Aws::SDKOptions aws_sdk_options_;
};

template <typename RpcRequestType, typename RpcResponseType,
          bool RpcFailed = false>
void RpcService(google::protobuf::RpcController *cntl_base,
                const RpcRequestType *request, RpcResponseType *response,
                google::protobuf::Closure *done) {
    if (RpcFailed) {
        brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);
        cntl->SetFailed(112, "Not connected to");
    }
    done->Run();
}

TEST_F(MetaserverTest, register_to_mds_success) {
    curvefs::metaserver::Metaserver metaserver;
    auto conf = std::make_shared<Configuration>();
    conf->SetConfigPath("curvefs/conf/metaserver.conf");
    ASSERT_TRUE(conf->LoadConfig());
    conf->SetStringValue("mds.listen.addr", topologyServiceAddr_);
    conf->SetStringValue("global.ip", metaserverIp_);
    conf->SetStringValue("global.port", metaserverPort_);
    conf->SetStringValue("metaserver.meta_file_path", metaPath_);
    conf->SetStringValue("storage.data_dir", dataDir_);

    // initialize MDS options
    metaserver.InitOptions(conf);

    // mock RegistMetaServer
    MetaServerRegistResponse response;
    response.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(mockTopologyService_, RegistMetaServer(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke(RpcService<MetaServerRegistRequest,
                                          MetaServerRegistResponse>)));

    // mock MetaServerHeartbeat
    MetaServerHeartbeatResponse response1;
    response1.set_statuscode(HeartbeatStatusCode::hbOK);
    EXPECT_CALL(mockHeartbeatService_, MetaServerHeartbeat(_, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(response1),
                              Invoke(RpcService<MetaServerHeartbeatRequest,
                                                MetaServerHeartbeatResponse>)));

    std::string cmd = "rm -rf " +  metaPath_;
    system(cmd.c_str());

    // Initialize other modules after winning election
    metaserver.Init();

    // start metaserver server
    std::thread metaserverThread(&Metaserver::Run, &metaserver);

    // sleep 2s
    sleep(2);

    // stop server and background threads
    metaserver.Stop();

    brpc::AskToQuit();
    metaserverThread.join();
}

TEST_F(MetaserverTest, register_to_mds_enable_external_success) {
    curvefs::metaserver::Metaserver metaserver;
    auto conf = std::make_shared<Configuration>();
    conf->SetConfigPath("curvefs/conf/metaserver.conf");
    ASSERT_TRUE(conf->LoadConfig());
    conf->SetStringValue("mds.listen.addr", topologyServiceAddr_);
    conf->SetStringValue("global.ip", metaserverIp_);
    conf->SetStringValue("global.port", metaserverPort_);
    conf->SetStringValue("metaserver.meta_file_path", metaPath_);
    conf->SetStringValue("global.enable_external_server", "true");
    conf->SetStringValue("global.external_port", metaserverExternalPort_);
    conf->SetStringValue("global.external_ip", metaserverIp_);
    conf->SetStringValue("storage.data_dir", dataDir_);

    // initialize MDS options
    metaserver.InitOptions(conf);

    // mock RegistMetaServer
    MetaServerRegistResponse response;
    response.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(mockTopologyService_, RegistMetaServer(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke(RpcService<MetaServerRegistRequest,
                                          MetaServerRegistResponse>)));

    // mock MetaServerHeartbeat
    MetaServerHeartbeatResponse response1;
    response1.set_statuscode(HeartbeatStatusCode::hbOK);
    EXPECT_CALL(mockHeartbeatService_, MetaServerHeartbeat(_, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(response1),
                              Invoke(RpcService<MetaServerHeartbeatRequest,
                                                MetaServerHeartbeatResponse>)));

    std::string cmd = "rm -rf " +  metaPath_;
    system(cmd.c_str());

    // Initialize other modules after winning election
    metaserver.Init();

    // start metaserver server
    std::thread metaserverThread(&Metaserver::Run, &metaserver);

    // sleep 2s
    sleep(2);

    // stop server and background threads
    metaserver.Stop();

    brpc::AskToQuit();
    metaserverThread.join();
}

TEST_F(MetaserverTest, test2) {
    curvefs::metaserver::Metaserver metaserver;
    auto conf = std::make_shared<Configuration>();
    conf->SetConfigPath("curvefs/conf/metaserver.conf");
    ASSERT_TRUE(conf->LoadConfig());

    conf->SetStringValue("mds.listen.addr", topologyServiceAddr_);
    conf->SetStringValue("global.ip", metaserverIp_);
    conf->SetStringValue("global.port", metaserverPort_);
    conf->SetStringValue("metaserver.meta_file_path", metaPath_);
    conf->SetStringValue("storage.data_dir", dataDir_);

    // mock RegistMetaServer
    MetaServerRegistResponse response;
    response.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(mockTopologyService_, RegistMetaServer(_, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                              Invoke(RpcService<MetaServerRegistRequest,
                                                MetaServerRegistResponse>)));

    // mock MetaServerHeartbeat
    MetaServerHeartbeatResponse response1;
    response1.set_statuscode(HeartbeatStatusCode::hbOK);
    EXPECT_CALL(mockHeartbeatService_, MetaServerHeartbeat(_, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(response1),
                              Invoke(RpcService<MetaServerHeartbeatRequest,
                                                MetaServerHeartbeatResponse>)));

    std::string cmd = "rm -rf " +  metaPath_;
    system(cmd.c_str());

    // initialize Metaserver options
    metaserver.InitOptions(conf);

    // not init, run
    metaserver.Run();
    metaserver.Run();

    // not start, stop
    metaserver.Stop();
    metaserver.Stop();

    // Initialize other modules after winning election
    metaserver.Init();

    // start metaserver server
    std::thread metaserverThread(&Metaserver::Run, &metaserver);

    // sleep 2s
    sleep(2);

    // stop server and background threads
    metaserver.Stop();
    brpc::AskToQuit();
    metaserverThread.join();
}

TEST_F(MetaserverTest, load_from_local) {
    curvefs::metaserver::Metaserver metaserver;
    auto conf = std::make_shared<Configuration>();
    conf->SetConfigPath("curvefs/conf/metaserver.conf");
    ASSERT_TRUE(conf->LoadConfig());
    conf->SetStringValue("mds.listen.addr", topologyServiceAddr_);
    conf->SetStringValue("global.ip", metaserverIp_);
    conf->SetStringValue("global.port", metaserverPort_);
    conf->SetStringValue("metaserver.meta_file_path", metaPath_);
    conf->SetStringValue("storage.data_dir", dataDir_);

    // initialize MDS options
    metaserver.InitOptions(conf);

    // mock MetaServerHeartbeat
    MetaServerHeartbeatResponse response1;
    response1.set_statuscode(HeartbeatStatusCode::hbOK);
    EXPECT_CALL(mockHeartbeatService_, MetaServerHeartbeat(_, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(response1),
                        Invoke(RpcService<MetaServerHeartbeatRequest,
                                          MetaServerHeartbeatResponse>)));

    // Initialize other modules after winning election
    metaserver.Init();

    // start metaserver server
    std::thread metaserverThread(&Metaserver::Run, &metaserver);

    // sleep 2s
    sleep(2);

    // stop server and background threads
    metaserver.Stop();

    std::string cmd = "rm -rf " +  metaPath_;
    system(cmd.c_str());

    brpc::AskToQuit();
    metaserverThread.join();
}
}  // namespace metaserver
}  // namespace curvefs
