/*
 * Project: curve
 * Created Date: 2020-01-08
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#include <fiu-control.h>
#include <gtest/gtest.h>
#include <brpc/channel.h>
#include <json/json.h>
#include <string>
#include <vector>

#include "src/mds/server/mds.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/timeutility.h"
#include "src/common/string_util.h"

using ::curve::common::Thread;

DECLARE_string(mdsAddr);
DECLARE_string(etcdAddr);

namespace curve {
namespace mds {
class MDSTest : public ::testing::Test {
 protected:
    void SetUp() {
        system("rm -fr testMds.etcd");

        etcdPid = ::fork();
        if (0 > etcdPid) {
            ASSERT_TRUE(false);
        } else if (0 == etcdPid) {
            std::string runEtcd =
                std::string("etcd --listen-client-urls") +
                std::string(" 'http://localhost:10002'") +
                std::string(" --advertise-client-urls") +
                std::string(" 'http://localhost:10002'") +
                std::string(" --listen-peer-urls 'http://localhost:10003'") +
                std::string(" --name testMds");
            ASSERT_EQ(0, execl("/bin/sh", "sh", "-c", runEtcd.c_str(), NULL));
            exit(0);
        }
        // 一定时间内尝试init直到etcd完全起来
        auto client = std::make_shared<EtcdClientImp>();
        EtcdConf conf = {kEtcdAddr, strlen(kEtcdAddr), 1000};
        uint64_t now = ::curve::common::TimeUtility::GetTimeofDaySec();
        bool initSuccess = false;
        while (::curve::common::TimeUtility::GetTimeofDaySec() - now <= 5) {
            if (0 == client->Init(conf, 0, 3)) {
                initSuccess = true;
                break;
            }
        }
        ASSERT_TRUE(initSuccess);
        ASSERT_EQ(
            EtcdErrCode::DeadlineExceeded, client->Put("05", "hello word"));
        ASSERT_EQ(EtcdErrCode::DeadlineExceeded,
            client->CompareAndSwap("04", "10", "110"));
        client->CloseClient();
        fiu_init(0);
        fiu_enable("src/mds/leaderElection/observeLeader", 1, nullptr, 0);
    }

    void TearDown() {
        system(("kill " + std::to_string(etcdPid)).c_str());
        std::this_thread::sleep_for(std::chrono::seconds(2));
        fiu_disable("src/mds/leaderElection/observeLeader");
    }

    brpc::Channel channel_;
    pid_t etcdPid;
    const std::string kMdsAddr = "127.0.0.1:10001";
    char kEtcdAddr[20] = {"127.0.0.1:10002"};
    const int kDummyPort = 10004;
};

TEST_F(MDSTest, common) {
    // 加载配置
    std::string confPath = "./conf/mds.conf";
    auto conf = std::make_shared<Configuration>();
    conf->SetConfigPath(confPath);
    LOG_IF(FATAL, !conf->LoadConfig())
        << "load mds configuration fail, conf path = " << confPath;

    conf->SetStringValue("mds.listen.addr", kMdsAddr);
    conf->SetStringValue("mds.etcd.endpoint", kEtcdAddr);
    conf->SetIntValue("mds.dummy.listen.port", kDummyPort);
    MDS mds;
    mds.InitMdsOptions(conf);
    mds.StartDummy();

    // 从dummy server获取version和mds监听端口
    brpc::Channel httpChannel;
    brpc::Controller cntl;
    brpc::ChannelOptions options;
        options.protocol = brpc::PROTOCOL_HTTP;
    std::string dummyAddr = "127.0.0.1:" + std::to_string(kDummyPort);
    ASSERT_EQ(0, httpChannel.Init(dummyAddr.c_str(), &options));

    // 测试获取version
    cntl.http_request().uri() = dummyAddr + "/vars/curve_version";
    httpChannel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    ASSERT_FALSE(cntl.Failed());

    // 测试获取mds监听端口
    cntl.Reset();
    cntl.http_request().uri() = dummyAddr + "/vars/mds_config_mds_listen_addr";
    httpChannel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    ASSERT_FALSE(cntl.Failed());
    Json::Reader reader;
    Json::Value value;
    std::string attachment = cntl.response_attachment().to_string();
    auto pos = attachment.find(":");
    ASSERT_NE(std::string::npos, pos);
    std::string jsonString = attachment.substr(pos + 2);
    // 去除两端引号
    jsonString = jsonString.substr(1, jsonString.size() - 2);
    reader.parse(jsonString, value);
    std::string mdsAddr = value["conf_value"].asString();
    ASSERT_EQ(kMdsAddr, mdsAddr);

    // 获取leader状态，此时mds_status应为follower
    cntl.Reset();
    cntl.http_request().uri() = dummyAddr + "/vars/mds_status";
    httpChannel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    ASSERT_FALSE(cntl.Failed());
    ASSERT_NE(std::string::npos,
        cntl.response_attachment().to_string().find("follower"));

    mds.StartCompaginLeader();

    // 此时isLeader应为true
    cntl.Reset();
    cntl.http_request().uri() = dummyAddr + "/vars/is_leader";
    ASSERT_FALSE(cntl.Failed());
    ASSERT_EQ(std::string::npos,
        cntl.response_attachment().to_string().find("leader"));

    mds.Init();
    // 启动mds
    Thread mdsThread(&MDS::Run, &mds);
    // sleep 5s
    sleep(5);

    // 1、init channel
    ASSERT_EQ(0, channel_.Init(kMdsAddr.c_str(), nullptr));

    // 2、测试hearbeat接口
    cntl.Reset();
    heartbeat::ChunkServerHeartbeatRequest request1;
    heartbeat::ChunkServerHeartbeatResponse response1;
    request1.set_chunkserverid(1);
    request1.set_token("123");
    request1.set_ip("127.0.0.1");
    request1.set_port(8888);
    heartbeat::DiskState* diskState = new heartbeat::DiskState();
    diskState->set_errtype(0);
    diskState->set_errmsg("");
    request1.set_allocated_diskstate(diskState);
    request1.set_diskcapacity(2 * 1024 * 1024 * 1024);
    request1.set_diskused(1 * 1024 * 1024 * 1024);
    request1.set_leadercount(10);
    request1.set_copysetcount(10);

    heartbeat::HeartbeatService_Stub stub1(&channel_);
    stub1.ChunkServerHeartbeat(&cntl, &request1, &response1, nullptr);
    ASSERT_FALSE(cntl.Failed());

    // 3、测试namespaceService接口
    cntl.Reset();
    GetFileInfoRequest request2;
    GetFileInfoResponse response2;
    request2.set_filename("/");
    uint64_t date = curve::common::TimeUtility::GetTimeofDayUs();
    request2.set_date(date);
    request2.set_owner("root");
    CurveFSService_Stub stub2(&channel_);
    stub2.GetFileInfo(&cntl, &request2, &response2, nullptr);
    ASSERT_FALSE(cntl.Failed());

    // 4、测试topology接口
    cntl.Reset();
    topology::ListPhysicalPoolRequest request3;
    topology::ListPhysicalPoolResponse response3;
    topology::TopologyService_Stub stub3(&channel_);
    stub3.ListPhysicalPool(&cntl, &request3, &response3, nullptr);
    ASSERT_FALSE(cntl.Failed());

    // 5、停掉mds
    uint64_t startTime = curve::common::TimeUtility::GetTimeofDayMs();
    mds.Stop();
    mdsThread.join();
    uint64_t stopTime = curve::common::TimeUtility::GetTimeofDayMs();
    ASSERT_LE(stopTime - startTime, 100);
}

}  // namespace mds
}  // namespace curve

