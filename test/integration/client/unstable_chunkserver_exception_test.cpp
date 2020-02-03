/*
 * Project: curve
 * File Created: 2019-10-24 19:28
 * Author: wuhanqing
 * Copyright (c)￼ 2019 netease
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <map>
#include <cmath>
#include <mutex>    // NOLINT
#include <thread>    // NOLINT
#include <atomic>
#include <unordered_map>
#include <memory>
#include <string>
#include <numeric>
#include <algorithm>
#include <condition_variable>    // NOLINT

#include "include/client/libcurve.h"
#include "src/common/timeutility.h"
#include "src/client/client_metric.h"
#include "src/client/inflight_controller.h"
#include "test/integration/client/common/file_operation.h"
#include "test/integration/cluster_common/cluster.h"
#include "test/util/config_generator.h"

using curve::CurveCluster;

const char* kMdsConfPath = "./test/integration/unstable_test_mds.conf";
const char* kCSConfPath = "./test/integration/unstable_test_cs.conf";
const char* kClientConfPath = "./test/integration/unstable_test_client.conf";
const char* kDbName = "unstable_chunkserver";

const char* kEtcdClientIpPort = "127.0.0.1:30000";
const char* kEtcdPeerIpPort = "127.0.0.1:29999";
const char* kMdsIpPort = "127.0.0.1:30010";
const char* kClientInflightNum = "6";
const char* kLogPath = "./runlog/";

curve::client::PerSecondMetric iops("test", "iops");

std::atomic<bool> running{false};

const std::vector<std::string> chunkserverConfigOpts{
    "chunkfilepool.enable_get_chunk_from_pool=false"
};

const std::vector<std::string> mdsConfigOpts{
    std::string("mds.DbName=") + std::string(kDbName)
};

const std::vector<std::string> clientConfigOpts{
    std::string("mds.listen.addr=") + kMdsIpPort,
    std::string("maxInFlightRPCNum=") + kClientInflightNum,
    std::string("global.logPath=") + kLogPath,
    std::string("isolation.taskQueueCapacity=5000"),
    std::string("schedule.queueCapacity=5000"),
};

const std::vector<std::string> mdsConf{
    std::string(" --confPath=") + kMdsConfPath,
    std::string(" --mdsAddr=") + kMdsIpPort,
    std::string(" --etcdAddr=") + kEtcdClientIpPort,
    {" --log_dir=./runlog/mds"},
    {" --stderrthreshold=3"}
};

const std::vector<std::string> chunkserverConfTemplate{
    {" -raft_sync_segments=true"},
    std::string(" -conf=") + kCSConfPath,
    {" -chunkServerPort=%d"},
    {" -chunkServerStoreUri=local://./ttt/%d/"},
    {" -chunkServerMetaUri=local://./ttt/%d/chunkserver.dat"},
    {" -copySetUri=local://./ttt/%d/copysets"},
    {" -recycleUri=local://./ttt/%d/recycler"},
    {" -chunkFilePoolDir=./ttt/%d/"},
    {" -chunkFilePoolMetaPath=./ttt/%d/chunkfilepool.meta"},
    {" -mdsListenAddr=127.0.0.1:30010,127.0.0.1:30011,127.0.0.1:30012"},
    {" -log_dir=./runlog/cs_%d"},
    {" --stderrthreshold=3"}
};

const std::vector<int> chunkserverPorts{
    31000, 31001,
    31010, 31011,
    31020, 31021,
};

std::vector<std::string> GenChunkserverConf(int port) {
    std::vector<std::string> conf(chunkserverConfTemplate);
    char temp[NAME_MAX_SIZE];

    auto formatter = [&](const std::string& format, int port) {
        snprintf(temp,
                 sizeof(temp),
                 format.c_str(),
                 port);
        return temp;
    };

    conf[2] = formatter(chunkserverConfTemplate[2], port);
    conf[3] = formatter(chunkserverConfTemplate[3], port);
    conf[4] = formatter(chunkserverConfTemplate[4], port);
    conf[5] = formatter(chunkserverConfTemplate[5], port);
    conf[6] = formatter(chunkserverConfTemplate[6], port);
    conf[7] = formatter(chunkserverConfTemplate[7], port);
    conf[8] = formatter(chunkserverConfTemplate[8], port);
    conf[10] = formatter(chunkserverConfTemplate[10], port);

    std::string rmcmd = "rm -rf ./runlog/cs_" + std::to_string(port);
    std::string mkcmd = "mkdir -p ./runlog/cs_" + std::to_string(port);
    system(rmcmd.c_str());
    system(mkcmd.c_str());

    return conf;
}

off_t RandomWriteOffset() {
    return rand() % 32 * (16 * 1024 * 1024);
}

size_t RandomWriteLength() {
    return rand() % 32 * 4096;
}

static char buffer[1024 * 4096];

struct ChunkserverParam {
    int id;
    int port;
    std::string addr{"127.0.0.1:"};
    std::vector<std::string> conf;

    ChunkserverParam(int id, int port) {
        this->id = id;
        this->port = port;
        this->addr.append(std::to_string(port));
        this->conf = GenChunkserverConf(port);
    }
};

class UnstableCSModuleException : public ::testing::Test {
 public:
    void SetUp() {
        // 清理文件夹
        system("rm -rf module_exception_curve_unstable_cs.etcd");
        system("rm -rf ttt");
        system("mkdir -p ttt");
        system("mkdir -p runlog");
        system("mkdir -p runlog/mds");

        cluster.reset(new CurveCluster());
        ASSERT_NE(nullptr, cluster.get());

        // 生成配置文件
        cluster->PrepareConfig<curve::MDSConfigGenerator>(
            kMdsConfPath,
            mdsConfigOpts);
        cluster->PrepareConfig<curve::CSConfigGenerator>(
            kCSConfPath,
            chunkserverConfigOpts);
        cluster->PrepareConfig<curve::ClientConfigGenerator>(
            kClientConfPath,
            clientConfigOpts);

        // 0. 初始化db
        cluster->InitDB(kDbName);
        cluster->mdsRepo_->dropDataBase();

        // 1. 启动etcd
        cluster->StartSingleEtcd(
            1,
            kEtcdClientIpPort,
            kEtcdPeerIpPort,
            std::vector<std::string>{
                " --name module_exception_curve_unstable_cs"});

        // 2. 启动一个mds
        cluster->StartSingleMDS(1, kMdsIpPort, 30013, mdsConf, true);
        std::this_thread::sleep_for(std::chrono::seconds(2));

        // 3. 创建物理池
        cluster->PreparePhysicalPool(
            1, "./test/integration/client/config/unstable/topo_unstable.txt");

        // 4. 创建chunkserver
        StartAllChunkserver();
        std::this_thread::sleep_for(std::chrono::seconds(5));

        // 5. 创建逻辑池，并睡眠一段时间让底层copyset先选主
        cluster->PrepareLogicalPool(
            1,
            "test/integration/client/config/unstable/topo_unstable.txt",
            300,
            "pool1");
        std::this_thread::sleep_for(std::chrono::seconds(10));

        // 6. 初始化client配置
        int ret = Init(kClientConfPath);
        ASSERT_EQ(ret, 0);

        // 7. 先睡眠10s，让chunkserver选出leader
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }

    void TearDown() {
        UnInit();
        cluster->mdsRepo_->dropDataBase();
        cluster->StopCluster();
        // 清理文件夹
        system("rm -rf module_exception_curve_unstable_cs");
    }

    void StartAllChunkserver() {
        int id = 1;
        for (auto port : chunkserverPorts) {
            ChunkserverParam param(id, port);
            chunkServers.emplace(id, param);

            cluster->StartSingleChunkServer(id, param.addr, param.conf);
            std::this_thread::sleep_for(std::chrono::seconds(1));
            ++id;
        }
    }

    static void OpenAndWrite(const std::string& filename) {
        int fd = curve::test::FileCommonOperation::Open(filename, "curve");
        ASSERT_NE(-1, fd);

        std::vector<std::thread> writeThs;
        for (int i = 0; i < 5; ++i) {
            writeThs.emplace_back(AioWriteFunc, fd);
            LOG(INFO) << "write " << filename
                << ", thread " << (i+1) << " started";
        }

        for (auto& th : writeThs) {
            th.join();
        }

        LOG(INFO) << "stop all write thread, filename " << filename;
    }

    static void AioWriteFunc(int fd) {
        auto cb = [](CurveAioContext* ctx) {
            iops.count << 1;
            delete ctx;
        };

        while (running) {
            CurveAioContext* context = new CurveAioContext;
            context->op = LIBCURVE_OP::LIBCURVE_OP_WRITE;
            context->cb = cb;
            context->offset = RandomWriteOffset();
            context->length = RandomWriteLength();
            context->buf = buffer;

            AioWrite(fd, context);
        }
    }

    // std::vector<int> fds;
    int fd;
    std::unique_ptr<CurveCluster> cluster;
    std::unordered_map<int, ChunkserverParam> chunkServers;
};

// 集群拓扑结构
//     1个client
//     1个etcd
//     1个mds
//     3个zone，每个里面2个chunkserver
TEST_F(UnstableCSModuleException, HangOneZone) {
    srand(time(nullptr));

    // 开启多个线程写文件
    LOG(INFO) << "starting write...";
    running = true;
    std::vector<std::thread> openAndWriteThreads;
    for (int i = 0; i < 2; ++i) {
        openAndWriteThreads.emplace_back(
            &UnstableCSModuleException::OpenAndWrite,
            "/test" + std::to_string(i));
    }

    // 正常写入60s, 并记录后30秒的iops
    std::vector<uint64_t> beforeRecords;
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (int i = 1; i <= 30; ++i) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        beforeRecords.push_back(iops.value.get_value(1));
    }
    auto beforeAvgIOps = std::accumulate(
        beforeRecords.begin(), beforeRecords.end(), 0) / beforeRecords.size();
    LOG(INFO) << "iops before hang: " << beforeAvgIOps;

    // hang一个zone的chunkserver
    LOG(INFO) << "hang one zone";
    cluster->HangChunkServer(1);
    cluster->HangChunkServer(2);

    std::vector<uint64_t> afterRecords;
    // 打印每一秒的iops情况
    for (int i = 1; i <= 10; ++i) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        auto tmp = iops.value.get_value(1);
        LOG(INFO) << "after " << i <<  "s, iops: " << tmp;
        // 记录后5s的iops值
        if (i >= 5) {
            afterRecords.push_back(tmp);
        }
    }

    uint64_t afterAvgIOps = std::accumulate(
        afterRecords.begin(), afterRecords.end(), 0) / afterRecords.size();
    LOG(INFO) << "before iops: " << beforeAvgIOps;
    LOG(INFO) << "after iops: " << afterAvgIOps;
    ASSERT_GT(afterAvgIOps, static_cast<uint64_t>(beforeAvgIOps * 0.7));

    cluster->RecoverHangChunkServer(1);
    cluster->RecoverHangChunkServer(2);

    running = false;
    for (auto& th : openAndWriteThreads) {
        th.join();
    }
    LOG(INFO) << "all write thread stoped";
}
