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
 * Created Date: 2020-09-09
 * Author: wanghai1
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <brpc/controller.h>
#include <brpc/channel.h>
#include <json/json.h>
#include <stdio.h>

#include <fstream>
#include <sstream>

#include "include/client/libcurve.h"
#include "src/client/inflight_controller.h"
#include "src/chunkserver/cli2.h"
#include "src/common/timeutility.h"
#include "test/integration/common/chunkservice_op.h"
#include "test/integration/client/common/file_operation.h"
#include "test/integration/cluster_common/cluster.h"
#include "test/util/config_generator.h"
#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_item.h"
#include "src/mds/common/mds_define.h"

using curve::CurveCluster;
using curve::common::CountDownEvent;
using std::string;

using ::curve::mds::topology::ChunkServer;
using ::curve::mds::topology::ChunkServerStatus;
using ::curve::mds::topology::OnlineState;
using ::curve::mds::topology::CopySetKey;
using ::curve::mds::topology::kTopoErrCodeSuccess;
using ::curve::mds::topology::kTopoErrCodeInternalError;
using ::curve::mds::topology::TopologyService_Stub;
using curve::mds::topology::GetCopySetsInChunkServerRequest;
using curve::mds::topology::GetCopySetsInChunkServerResponse;

const char* kEtcdClientIpPort = "127.0.0.1:9600";
const char* kEtcdPeerIpPort = "127.0.0.1:9601";
const char* kMdsIpPort = "127.0.0.1:9602";
const char* kChunkServerIpPort1 = "127.0.0.1:9604";
const char* kChunkServerIpPort2 = "127.0.0.1:9605";
const char* kChunkServerIpPort3 = "127.0.0.1:9606";
const char* kChunkServerIpPort4 = "127.0.0.1:9607";

const int kMdsDummyPort = 9603;
const string kClientDummyPort = "9608";

const string kPhysicalPoolName = "pool0";
const string kMdsDbName = "CSOfflineTest_DB";
const string kEtcdName = "CSOfflineTest_ETCD";

const string kCSOfflineBaseDir = "./runlog/CSOfflineTest";
const string kLogPath = kCSOfflineBaseDir + "/Log";
const string mds0ConfPath = kCSOfflineBaseDir + "/mds0.conf";
const string chunkserverConfPath = kCSOfflineBaseDir + "/chunkserver.conf";
const string clientConfPath = kCSOfflineBaseDir + "/client.conf";  // NOLINT

const string chunkserver1BaseDir = kCSOfflineBaseDir + "/cs1";
const string chunkserver2BaseDir = kCSOfflineBaseDir + "/cs2";
const string chunkserver3BaseDir = kCSOfflineBaseDir + "/cs3";
const string chunkserver4BaseDir = kCSOfflineBaseDir + "/cs4";

const std::vector<string> mdsConf0{
    { " --confPath=" + mds0ConfPath },
    { " --log_dir=" + kLogPath },
    { " --mdsDbName=" + kMdsDbName },
    { " --sessionInterSec=20" },
};

const std::vector<string> mdsFileConf0{
    string("mds.listen.addr=") + kMdsIpPort,
    string("mds.dummy.listen.port=") + std::to_string(kMdsDummyPort),
    string("global.port=9602"),
    string("mds.etcd.endpoint=") + kEtcdClientIpPort,
    string("mds.heartbeat.offlinetimeoutMs=60000"), // 设置chunkserver offline 时间为1min（默认30min）// NOLINT
    string("mds.chunkserver.failure.tolerance=2"),
};

const std::vector<string> clientConf{
    string("mds.listen.addr=") + kMdsIpPort,
    string("global.logPath=" + kCSOfflineBaseDir),
    string("chunkserver.rpcTimeoutMS=1000"),
    string("chunkserver.opMaxRetry=10"),
    string("global.metricDummyServerStartPort=" + kClientDummyPort),
};

const std::vector<string> csCommonConf{
    string("mds.listen.addr=") + kMdsIpPort,
    string("curve.config_path=" + clientConfPath),
};

const std::vector<string> chunkserverConf1{
    { " -chunkServerStoreUri=local://" + chunkserver1BaseDir },
    { " -chunkServerMetaUri=local://" + chunkserver1BaseDir +
      "/chunkserver.dat" },
    { " -copySetUri=local://" + chunkserver1BaseDir + "/copysets" },
    { " -raftSnapshotUri=curve://" + chunkserver1BaseDir + "/copysets" },
    { " -recycleUri=local://" + chunkserver1BaseDir + "/recycler" },
    { " -chunkFilePoolDir=" + chunkserver1BaseDir + "/filepool" },
    { " -chunkFilePoolMetaPath=" + chunkserver1BaseDir +
      "/chunkfilepool.meta" },
    { " -conf=" + chunkserverConfPath },
    { " -raft_sync_segments=true" },
    { " --log_dir=" + kLogPath },
    { " --graceful_quit_on_sigterm" },
    { " -chunkServerIp=127.0.0.1" },
    { " -chunkServerPort=9604" },
    { " -enableChunkfilepool=false" }
};

const std::vector<string> chunkserverConf2{
    { " -chunkServerStoreUri=local://" + chunkserver2BaseDir },
    { " -chunkServerMetaUri=local://" + chunkserver2BaseDir +
      "/chunkserver.dat" },
    { " -copySetUri=local://" + chunkserver2BaseDir + "/copysets" },
    { " -raftSnapshotUri=curve://" + chunkserver2BaseDir + "/copysets" },
    { " -recycleUri=local://" + chunkserver2BaseDir + "/recycler" },
    { " -chunkFilePoolDir=" + chunkserver2BaseDir + "/filepool" },
    { " -chunkFilePoolMetaPath=" + chunkserver2BaseDir +
      "/chunkfilepool.meta" },
    { " -conf=" + chunkserverConfPath },
    { " -raft_sync_segments=true" },
    { " --log_dir=" + kLogPath },
    { " --graceful_quit_on_sigterm" },
    { " -chunkServerIp=127.0.0.1" },
    { " -chunkServerPort=9605" },
    { " -enableChunkfilepool=false" }
};

const std::vector<string> chunkserverConf3{
    { " -chunkServerStoreUri=local://" + chunkserver3BaseDir },
    { " -chunkServerMetaUri=local://" + chunkserver3BaseDir +
      "/chunkserver.dat" },
    { " -copySetUri=local://" + chunkserver3BaseDir + "/copysets" },
    { " -raftSnapshotUri=curve://" + chunkserver3BaseDir + "/copysets" },
    { " -recycleUri=local://" + chunkserver3BaseDir + "/recycler" },
    { " -chunkFilePoolDir=" + chunkserver3BaseDir + "/filepool" },
    { " -chunkFilePoolMetaPath=" + chunkserver3BaseDir +
      "/chunkfilepool.meta" },
    { " -conf=" + chunkserverConfPath },
    { " -raft_sync_segments=true" },
    { " --log_dir=" + kLogPath },
    { " --graceful_quit_on_sigterm" },
    { " -chunkServerIp=127.0.0.1" },
    { " -chunkServerPort=9606" },
    { " -enableChunkfilepool=false" }
};

const std::vector<string> chunkserverConf4{
    { " -chunkServerStoreUri=local://" + chunkserver4BaseDir },
    { " -chunkServerMetaUri=local://" + chunkserver4BaseDir +
      "/chunkserver.dat" },
    { " -copySetUri=local://" + chunkserver4BaseDir + "/copysets" },
    { " -raftSnapshotUri=curve://" + chunkserver4BaseDir + "/copysets" },
    { " -recycleUri=local://" + chunkserver4BaseDir + "/recycler" },
    { " -chunkFilePoolDir=" + chunkserver4BaseDir + "/filepool" },
    { " -chunkFilePoolMetaPath=" + chunkserver4BaseDir +
      "/chunkfilepool.meta" },
    { " -conf=" + chunkserverConfPath },
    { " -raft_sync_segments=true" },
    { " --log_dir=" + kLogPath },
    { " --graceful_quit_on_sigterm" },
    { " -chunkServerIp=127.0.0.1" },
    { " -chunkServerPort=9607" },
    { " -enableChunkfilepool=false" }
};

namespace curve {
namespace chunkserver {
class CSOfflineTest : public ::testing::Test {
 public:
    CSOfflineTest() {}

    void SetUp() {
        //初始化使用的目录
        InitCSOfflineTestDir();

        cluster_ = new CurveCluster();
        ASSERT_NE(nullptr, cluster_);

        //先读取模板配置文件再添加修改项到文件, 1 路径； 2 修改的配置项
        cluster_->PrepareConfig<curve::ClientConfigGenerator>(clientConfPath,
                                                              clientConf);
        cluster_->PrepareConfig<curve::MDSConfigGenerator>(mds0ConfPath,
                                                           mdsFileConf0);
        cluster_->PrepareConfig<curve::CSConfigGenerator>(chunkserverConfPath,
                                                          csCommonConf);

        // 1. 启动etcd
        LOG(INFO) << "begin to start etcd";
        pid_t pid = cluster_->StartSingleEtcd(
            1, kEtcdClientIpPort, kEtcdPeerIpPort,
            std::vector<string>{ " --name " + kEtcdName });
        LOG(INFO) << "etcd 1 started on " << kEtcdClientIpPort
                  << ", pid = " << pid;
        ASSERT_GT(pid, 0);
        ASSERT_TRUE(cluster_->WaitForEtcdClusterAvalible(5));

        // 2. 启动mds，让其成为leader
        pid = cluster_->StartSingleMDS(0, kMdsIpPort, kMdsDummyPort,
                                         mdsConf0, true);
        LOG(INFO) << "mds 0 started on " << kMdsIpPort
                  << ", pid = " << pid;
        ASSERT_GT(pid, 0);
        std::this_thread::sleep_for(std::chrono::seconds(8));

        // 生成topo.json
        Json::Value topo;
        Json::Value servers;
        std::string chunkServerIpPort[] = {kChunkServerIpPort1,
                                           kChunkServerIpPort2,
                                           kChunkServerIpPort3,
                                           kChunkServerIpPort4 };
        for (int i = 0; i < 4; ++i) {
            Json::Value server;
            std::vector<std::string> ipPort;
            curve::common::SplitString(chunkServerIpPort[i], ":", &ipPort);
            std::string ip = ipPort[0];
            uint64_t port;
            ASSERT_TRUE(curve::common::StringToUll(ipPort[1], &port));
            server["externalip"] = ipPort[0];
            server["externalport"] = port;
            server["internalip"] = ipPort[0];
            server["internalport"] = port;
            server["name"] = std::string("server") + std::to_string(i+1);
            server["physicalpool"] = kPhysicalPoolName;
        if (i < 3) {
            server["zone"] = std::string("zone") + std::to_string(i);
        } else {
            server["zone"] = std::string("zone") + std::to_string(i-1);
        }
            servers.append(server);
        }
        topo["servers"] = servers;
        Json::Value logicalPools;
        Json::Value logicalPool;
        logicalPool["copysetnum"] = 4;
        logicalPool["name"] = "defaultLogicalPool";
        logicalPool["physicalpool"] = kPhysicalPoolName;
        logicalPool["replicasnum"] = 3;
        logicalPool["scatterwidth"] = 0;
        logicalPool["type"] = 0;
        logicalPool["zonenum"] = 3;
        logicalPools.append(logicalPool);
        topo["logicalpools"] = logicalPools;
        std::ofstream topoConf(kCSOfflineBaseDir + "/topo.json");
        topoConf << topo.toStyledString();
        topoConf.close();

        // 3. 创建物理池
        string createPPCmd =
            string("./bazel-bin/tools/curvefsTool") +
            string(" -cluster_map=" + kCSOfflineBaseDir + "/topo.json") +
            string(" -mds_addr=") + kMdsIpPort +
            string(" -op=create_physicalpool") + string(" -stderrthreshold=0") +
            string(" -rpcTimeOutMs=10000") + string(" -minloglevel=0");

        int ret = 0;
        int retry = 0;
        while (retry < 5) {
            LOG(INFO) << "exec createPPCmd: " << createPPCmd;
            ret = system(createPPCmd.c_str());
            if (ret == 0)
                break;
            retry++;
        }
        ASSERT_EQ(ret, 0);

        // 4. 创建chunkserver
        pid = cluster_->StartSingleChunkServer(1, kChunkServerIpPort1,
                                               chunkserverConf1);
        LOG(INFO) << "chunkserver 1 started on " << kChunkServerIpPort1
                  << ", pid = " << pid;
        ASSERT_GT(pid, 0);
        pid = cluster_->StartSingleChunkServer(2, kChunkServerIpPort2,
                                               chunkserverConf2);
        LOG(INFO) << "chunkserver 2 started on " << kChunkServerIpPort2
                  << ", pid = " << pid;
        ASSERT_GT(pid, 0);
        pid = cluster_->StartSingleChunkServer(3, kChunkServerIpPort3,
                                               chunkserverConf3);
        LOG(INFO) << "chunkserver 3 started on " << kChunkServerIpPort3
                  << ", pid = " << pid;
        ASSERT_GT(pid, 0);
        pid = cluster_->StartSingleChunkServer(4, kChunkServerIpPort4,
                                               chunkserverConf4);
        LOG(INFO) << "chunkserver 4 started on " << kChunkServerIpPort4
                  << ", pid = " << pid;
        ASSERT_GT(pid, 0);

        std::this_thread::sleep_for(std::chrono::seconds(5));

        // 5. 创建逻辑池, 并睡眠一段时间让底层copyset先选主
        string createLPCmd =
            string("./bazel-bin/tools/curvefsTool") +
            string(" -cluster_map=" + kCSOfflineBaseDir + "/topo.json") +
            string(" -mds_addr=") + kMdsIpPort +
            string(" -op=create_logicalpool") +
            string(" -stderrthreshold=0 -minloglevel=0");
        ret = 0;
        retry = 0;
        while (retry < 5) {
            LOG(INFO) << "exec createLPCmd: " << createLPCmd;
            ret = system(createLPCmd.c_str());
            if (ret == 0)
                break;
            retry++;
        }
        ASSERT_EQ(ret, 0);
        std::this_thread::sleep_for(std::chrono::seconds(5));

        // 6. 初始化client配置
        LOG(INFO) << "init globalclient";
        ret = Init(clientConfPath.c_str());
        ASSERT_EQ(ret, 0);

        // 7. 先睡眠5s，让chunkserver选出leader
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    void TearDown() {
        UnInit();
        ASSERT_EQ(0, cluster_->StopCluster());
        delete cluster_;
    }

    void InitCSOfflineTestDir() {
        ASSERT_EQ(0, system(("rm -rf " + kEtcdName + ".etcd").c_str()));
        ASSERT_EQ(0, system(("rm -rf " + chunkserver1BaseDir).c_str()));
        ASSERT_EQ(0, system(("rm -rf " + chunkserver2BaseDir).c_str()));
        ASSERT_EQ(0, system(("rm -rf " + chunkserver3BaseDir).c_str()));
        ASSERT_EQ(0, system(("rm -rf " + chunkserver4BaseDir).c_str()));

        ASSERT_EQ(0, system(("mkdir -p " + kLogPath).c_str()));
        ASSERT_EQ(0, system(("mkdir -p " + chunkserver1BaseDir).c_str()));
        ASSERT_EQ(0, system(("mkdir -p " + chunkserver2BaseDir).c_str()));
        ASSERT_EQ(0, system(("mkdir -p " + chunkserver3BaseDir).c_str()));
        ASSERT_EQ(0, system(("mkdir -p " + chunkserver4BaseDir).c_str()));
        ASSERT_EQ(
          0, system(("mkdir " + chunkserver1BaseDir + "/copysets").c_str()));
        ASSERT_EQ(
          0, system(("mkdir " + chunkserver2BaseDir + "/copysets").c_str()));
        ASSERT_EQ(
          0, system(("mkdir " + chunkserver3BaseDir + "/copysets").c_str()));
        ASSERT_EQ(
          0, system(("mkdir " + chunkserver4BaseDir + "/copysets").c_str()));
        ASSERT_EQ(
          0, system(("mkdir " + chunkserver1BaseDir + "/recycler").c_str()));
        ASSERT_EQ(
          0, system(("mkdir " + chunkserver2BaseDir + "/recycler").c_str()));
        ASSERT_EQ(
          0, system(("mkdir " + chunkserver3BaseDir + "/recycler").c_str()));
        ASSERT_EQ(
          0, system(("mkdir " + chunkserver4BaseDir + "/recycler").c_str()));
        ASSERT_EQ(
          0, system(("mkdir " + chunkserver1BaseDir + "/filepool").c_str()));
        ASSERT_EQ(
          0, system(("mkdir " + chunkserver2BaseDir + "/filepool").c_str()));
        ASSERT_EQ(
          0, system(("mkdir " + chunkserver3BaseDir + "/filepool").c_str()));
        ASSERT_EQ(
          0, system(("mkdir " + chunkserver4BaseDir + "/filepool").c_str()));
    }

    CurveCluster* cluster_;
};

// 场景一：ChunkServer Offline测试
TEST_F(CSOfflineTest, ChunkServerOffline) {
    LOG(INFO) << "current case: ChunkServerOffline";

    // 操作一：kill掉一个chunkserver
    // 预期：该chunkserver上的copyset会全部迁移到其他chunkserver上
    ASSERT_EQ(0, cluster_->StopChunkServer(4));
    LOG(INFO) << "1. kill the chunkserver which id is 4";
    // 休眠一段时间，让mds检测到有chunkserver offlien并进行处理
    std::this_thread::sleep_for(std::chrono::seconds(100));

    brpc::Channel channel;
    if (channel.Init(kMdsIpPort, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }
    TopologyService_Stub stub(&channel);
    brpc::Controller cntl;
    GetCopySetsInChunkServerRequest request;
    GetCopySetsInChunkServerResponse response;
    request.set_chunkserverid(3);
    stub.GetCopySetsInChunkServer(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }
    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_EQ(4, response.copysetinfos_size());
    LOG(INFO) << "1. the copysets on chunkserver4 move to others";

    // 操作二：chunkserver recover完成之后，重新拉起该chunkserver
    // 预期：chunkserver下copyset的数据被清理到recycle下面
    pid_t pid = cluster_->StartSingleChunkServer(4, kChunkServerIpPort4,
                          chunkserverConf4);
    ASSERT_GT(pid, 0);
    LOG(INFO) << "2. chunkserver 4 recovered on " << kChunkServerIpPort4
              << ", pid = " << pid;

    std::this_thread::sleep_for(std::chrono::seconds(60));
    const string cmd = "ls " + chunkserver4BaseDir + "/recycler";
    FILE* fp = popen(cmd.c_str(), "r");
    ASSERT_TRUE(fp != nullptr);
    char buf[1024];
    memset(buf, 0, 1024 * sizeof(char));
    fread(buf, sizeof(char), sizeof(buf), fp);
    pclose(fp);
    std::string result(buf);
    LOG(INFO) << "chunkserver4 recyclye: " << result;
    EXPECT_NE(result, "");
    LOG(INFO) << "2. the copysets move to recycle after chunkserver4 recovered";

    // 操作三：kill掉该chunkserver, 删除dat文件，启动chunkserver
    // 预期：chunkserver注册成功；一段时间后有copyset迁移到该chunkserver上；原有的copyset会被清理到recycle目录下 // NOLINT
    ASSERT_EQ(0, cluster_->StopChunkServer(4));
    LOG(INFO) << "3. kill the chunkserver which id is 4";
    ASSERT_EQ(0, system(("rm -f " + chunkserver4BaseDir + "/*.dat").c_str()));
    LOG(INFO) << "3. delete the chunserver4.dat file";
    pid = cluster_->StartSingleChunkServer(4, kChunkServerIpPort4,
                          chunkserverConf4);
    LOG(INFO) << "3. chunkserver4 start after delete chunkserver.dat";

    std::this_thread::sleep_for(std::chrono::seconds(60));
    request.set_chunkserverid(4);
    stub.GetCopySetsInChunkServer(&cntl, &request, &response, nullptr);
    cntl.Reset();
    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }
    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_GT(response.copysetinfos_size(), 0);
    LOG(INFO) << "3. " << response.copysetinfos_size()
    << " copysets move to chunkserver4 when restart after delete .dat file";

    fp = popen(cmd.c_str(), "r");
    ASSERT_TRUE(fp != nullptr);
    memset(buf, 0, 1024 * sizeof(char));
    fread(buf, sizeof(char), sizeof(buf), fp);
    pclose(fp);
    result = buf;
    EXPECT_NE(result, "");
    LOG(INFO) << "3. the copysets move to recycle after chunkserver4 recovered";
}

// 场景二：chunkserver offline过程中mds重启
TEST_F(CSOfflineTest, ChunkServerOfflineAndMDSRestart) {
    LOG(INFO) << "current case: ChunkServerOfflineAndMDSRestart";

    // 操作一：kill掉一个chunkserver；一段时间内后重启mds
    // 预期： 配置变更正常完成，不会导致数据误删除
    ASSERT_EQ(0, cluster_->StopChunkServer(4));
    LOG(INFO) << "1. kill the chunkserver which id is 4";
    std::this_thread::sleep_for(std::chrono::seconds(80));
    ASSERT_EQ(0, cluster_->StopMDS(0));
    pid_t pid = cluster_->StartSingleMDS(0, kMdsIpPort, kMdsDummyPort,
                    mdsConf0, true);
    ASSERT_GT(pid, 0);
    LOG(INFO) << "1. mds 0 restarted on " << kMdsIpPort
              << ", pid = " << pid;

    std::this_thread::sleep_for(std::chrono::seconds(10));
    brpc::Channel channel;
    if (channel.Init(kMdsIpPort, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }
    TopologyService_Stub stub(&channel);
    brpc::Controller cntl;
    GetCopySetsInChunkServerRequest request;
    GetCopySetsInChunkServerResponse response;
    request.set_chunkserverid(3);
    stub.GetCopySetsInChunkServer(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }
    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_EQ(4, response.copysetinfos_size());

    // 操作二：恢复完成之后清理chunkserver上的所有数据，重新拉起该chunkserver
    // 预期： chunkserver注册成功；一段时间后有copyset迁移到该chunkserver上
    ASSERT_EQ(0, system(("rm -rf " + chunkserver4BaseDir + "/*.dat " +
                            chunkserver4BaseDir + "/copysets/* " +
                            chunkserver4BaseDir + "/recycler/* " +
                            chunkserver4BaseDir + "/filepool/*").c_str()));
    LOG(INFO) << "2. delete all file from chunkserver4";
    pid = cluster_->StartSingleChunkServer(4, kChunkServerIpPort4,
                          chunkserverConf4);
    ASSERT_GT(pid, 0);
    LOG(INFO) << "2. chunkserver4 start after all file deleted";

    std::this_thread::sleep_for(std::chrono::seconds(100));
    request.set_chunkserverid(4);
    stub.GetCopySetsInChunkServer(&cntl, &request, &response, nullptr);
    cntl.Reset();
    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }
    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_GT(response.copysetinfos_size(), 0);
    LOG(INFO) << "2. " << response.copysetinfos_size()
    << " copysets move to chunkserver4 when restart after delete all file";

    // 操作三：迁移过程中，kill掉目的chunkserver
    // 预期： 部分copyset的迁移会失败，但最终会成功；集群中copyset的数量均衡；目的chunkserver上的scatterwidth不超过上下限 // NOLINT
    ASSERT_EQ(0, cluster_->StopChunkServer(4));
    LOG(INFO) << "3. kill the chunkserver4 when copysets move to it";
    std::this_thread::sleep_for(std::chrono::seconds(10));
    pid = cluster_->StartSingleChunkServer(4, kChunkServerIpPort4,
                          chunkserverConf4);
    ASSERT_GT(pid, 0);
    LOG(INFO) << "3. chunkserver4 start after some copysets move to it failed";
    std::this_thread::sleep_for(std::chrono::seconds(80));

    request.set_chunkserverid(4);
    stub.GetCopySetsInChunkServer(&cntl, &request, &response, nullptr);
    cntl.Reset();
    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }
    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_EQ(2, response.copysetinfos_size());
}

// 场景三：多个chunkserver offline
TEST_F(CSOfflineTest, MultiChunkServerOffline) {
    LOG(INFO) << "current case: MultiChunkServerOffline";

    // 操作一: kill掉一台server上的多个chunkserver
    // 预期：mds报警，但不做恢复
    ASSERT_EQ(0, cluster_->StopChunkServer(3));
    ASSERT_EQ(0, cluster_->StopChunkServer(4));
    LOG(INFO) << "1. kill chunkservers which id is 3 and 4";
    std::this_thread::sleep_for(std::chrono::seconds(80));
}

}  // namespace chunkserver
}  // namespace curve
