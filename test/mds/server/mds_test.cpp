/*
 * Project: curve
 * Created Date: 2020-01-08
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#include <fiu-control.h>
#include <gtest/gtest.h>
#include <brpc/channel.h>

#include "src/mds/server/mds.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/timeutility.h"

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

    MDS mds_;
    brpc::Channel channel_;
    pid_t etcdPid;
    char kMdsAddr[20] = {"127.0.0.1:10001"};
    char kEtcdAddr[20] = {"127.0.0.1:10002"};
};

TEST_F(MDSTest, common) {
    // 加载配置
    FLAGS_mdsAddr = kMdsAddr;
    FLAGS_etcdAddr = kEtcdAddr;
    std::string confPath = "./conf/mds.conf";
    auto conf = std::make_shared<Configuration>();
    conf->SetConfigPath(confPath);
    LOG_IF(FATAL, !conf->LoadConfig())
        << "load mds configuration fail, conf path = " << confPath;
    mds_.Init(conf);
    // 启动mds
    Thread mdsThread(&MDS::Run, &mds_);
    // sleep 5s
    sleep(5);

    // 1、init channel
    ASSERT_EQ(0, channel_.Init(kMdsAddr, nullptr));
    brpc::Controller cntl;

    // 2、测试hearbeat接口
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
    mds_.Stop();
    mdsThread.join();
    uint64_t stopTime = curve::common::TimeUtility::GetTimeofDayMs();
    ASSERT_LE(stopTime - startTime, 100);
}

}  // namespace mds
}  // namespace curve

