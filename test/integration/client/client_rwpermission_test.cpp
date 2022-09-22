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
 * File Created: 2022-08-06
 * Author: YangFan (fansehep)
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <chrono>
#include <map>
#include <cmath>
#include <mutex>   // NOLINT
#include <thread>  // NOLINT
#include <atomic>
#include <unordered_map>
#include <memory>
#include <string>
#include <numeric>
#include <algorithm>
#include <condition_variable>  // NOLINT

#include "include/client/libcurve.h"
#include "include/client/libcurve_define.h"
#include "src/common/timeutility.h"
#include "src/client/client_metric.h"
#include "test/integration/client/common/file_operation.h"
#include "test/integration/cluster_common/cluster.h"
#include "test/util/config_generator.h"


using curve::CurveCluster;

namespace {

const char* kMdsConfPath = "./test/integration/client_permission_test_mds.conf";
const char* kCSConfPath = "./test/integration/client_permission_test_cs.conf";
const char* kClientConfPath =
"./test/integration/client_permission_test_client.conf";

const char* kEtcdClientIpPort = "127.0.0.1:32000";
const char* kEtcdPeerIpPort = "127.0.0.1:32001";
const char* kMdsIpPort = "127.0.0.1:32002";
const char* kClientInflightNum = "6";
const char* kLogPath = "./runlog/rw-perm";

std::atomic<bool> running{ false };

const std::vector<std::string> chunkserverConfigOpts{
    "chunkfilepool.enable_get_chunk_from_pool=false",
    "walfilepool.enable_get_segment_from_pool=false"
};

const std::vector<std::string> mdsConfigOpts{
    std::string("mds.etcd.endpoint=") + std::string(kEtcdClientIpPort)
};

const std::vector<std::string> clientConfigOpts{
    std::string("mds.listen.addr=") + kMdsIpPort,
    std::string("maxInFlightRPCNum=") + kClientInflightNum,
    std::string("global.logPath=") + kLogPath,
    std::string("isolation.taskQueueCapacity=128"),
    std::string("schedule.queueCapacity=128"),
};

const std::vector<std::string> mdsConf{
    std::string(" --confPath=") + kMdsConfPath,
    std::string(" --mdsAddr=") + kMdsIpPort,
    std::string(" --etcdAddr=") + kEtcdClientIpPort,
    { " --log_dir=./runlog/rw-perm/mds" },
    { " --stderrthreshold=3" }
};

const std::vector<std::string> chunkserverConfTemplate{
    { " -raft_sync_segments=true" },
    std::string(" -conf=") + kCSConfPath,
    { " -chunkServerPort=%d" },
    { " -chunkServerStoreUri=local://./rw-perm/%d/" },
    { " -chunkServerMetaUri=local://./rw-perm/%d/chunkserver.dat" },
    { " -copySetUri=local://./rw-perm/%d/copysets" },
    { " -raftSnapshotUri=curve://./rw-perm/%d/copysets" },
    { " -raftLogUri=curve://./rw-perm/%d/copysets" },
    { " -recycleUri=local://./rw-perm/%d/recycler" },
    { " -chunkFilePoolDir=./rw-perm/%d/chunkfilepool/" },
    { " -chunkFilePoolMetaPath=./rw-perm/%d/chunkfilepool.meta" },
    { " -walFilePoolDir=./rw-perm/%d/walfilepool/" },
    { " -walFilePoolMetaPath=./rw-perm/%d/walfilepool.meta" },
    { " -mdsListenAddr=127.0.0.1:32002" },
    { " -log_dir=./runlog/rw-perm/cs_%d" },
    { " --stderrthreshold=3" }
};

const std::vector<int> chunkserverPorts{
    32010, 32020, 32030,
};

std::vector<std::string> GenChunkserverConf(int port) {
    std::vector<std::string> conf(chunkserverConfTemplate);
    char temp[NAME_MAX_SIZE];

    auto formatter = [&](const std::string& format, int port) {
        snprintf(temp, sizeof(temp), format.c_str(), port);
        return temp;
    };

    conf[2] = formatter(chunkserverConfTemplate[2], port);
    conf[3] = formatter(chunkserverConfTemplate[3], port);
    conf[4] = formatter(chunkserverConfTemplate[4], port);
    conf[5] = formatter(chunkserverConfTemplate[5], port);
    conf[6] = formatter(chunkserverConfTemplate[6], port);
    conf[7] = formatter(chunkserverConfTemplate[7], port);
    conf[8] = formatter(chunkserverConfTemplate[8], port);
    conf[9] = formatter(chunkserverConfTemplate[9], port);
    conf[10] = formatter(chunkserverConfTemplate[10], port);
    conf[11] = formatter(chunkserverConfTemplate[11], port);
    conf[12] = formatter(chunkserverConfTemplate[12], port);
    conf[14] = formatter(chunkserverConfTemplate[14], port);

    std::string rmcmd = "rm -rf ./runlog/rw-perm/cs_" + std::to_string(port);
    std::string mkcmd = "mkdir -p ./runlog/rw-perm/cs_" + std::to_string(port);
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
    std::string addr{ "127.0.0.1:" };
    std::vector<std::string> conf;

    ChunkserverParam(int id, int port) {
        this->id = id;
        this->port = port;
        this->addr.append(std::to_string(port));
        this->conf = GenChunkserverConf(port);
    }
};

}  // namespace

class MDSPermissionException : public ::testing::Test {
 protected:
    static void SetUpTestCase() {
        // clean folder
        system("rm -rf module_exception_curve_permission_cs_1.etcd");
        system("rm -rf rw-perm");
        system("mkdir -p rw-perm");
        system("mkdir -p runlog/rw-perm/mds");

        cluster.reset(new CurveCluster());
        ASSERT_NE(nullptr, cluster.get());

        // 1. generate configuration files
        cluster->PrepareConfig<curve::MDSConfigGenerator>(kMdsConfPath,
                                                          mdsConfigOpts);
        cluster->PrepareConfig<curve::CSConfigGenerator>(kCSConfPath,
                                                         chunkserverConfigOpts);
        cluster->PrepareConfig<curve::ClientConfigGenerator>(kClientConfPath,
                                                             clientConfigOpts);

        // 2. enable etcd
        pid_t pid = cluster->StartSingleEtcd(
            1, kEtcdClientIpPort, kEtcdPeerIpPort,
            std::vector<std::string>{
                " --name module_exception_curve_permission_cs_1" });
        LOG(INFO) << "etcd 1 started on " << kEtcdClientIpPort << ":"
                  << kEtcdPeerIpPort << ", pid = " << pid;
        ASSERT_GT(pid, 0);

        // 3. enable a MDS server
        pid = cluster->StartSingleMDS(1, kMdsIpPort, 32003, mdsConf, true);
        LOG(INFO) << "mds 1 started on " << kMdsIpPort << ", pid = " << pid;
        ASSERT_GT(pid, 0);
        std::this_thread::sleep_for(std::chrono::seconds(2));

        // 4. create pyhsicalpool
        ASSERT_EQ(
            0,
            cluster->PreparePhysicalPool(
                1,
                "./test/integration/client/config/unstable/"
                "topo_permission.json"));

        // create chunkservers
        StartAllChunkserver();
        std::this_thread::sleep_for(std::chrono::seconds(5));

        // 5. create logicalpool and sleep a while for copyset elections
        ASSERT_EQ(0, cluster->PrepareLogicalPool(
            1, "test/integration/client/config/unstable/topo_permission.json"));
        std::this_thread::sleep_for(std::chrono::seconds(10));

        // 6. init client config
        int ret = Init(kClientConfPath);
        ASSERT_EQ(ret, 0);

        // 7. sleep 10 seconds, let chunkservers election a leader
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }

    static void TearDownTestCase() {
        UnInit();
        ASSERT_EQ(0, cluster->StopCluster());
        // clean folder
        system("rm -rf module_exception_curve_unstable_cs_1.etcd");
        system("rm -rf module_exception_curve_unstable_cs_1");
        // system("rm -rf rw-perm");
    }

    static void StartAllChunkserver() {
        int id = 1;
        for (auto port : chunkserverPorts) {
            ChunkserverParam param(id, port);
            chunkServers.emplace(id, param);

            pid_t pid =
                cluster->StartSingleChunkServer(id, param.addr, param.conf);
            LOG(INFO) << "chunkserver " << id << " started on " << param.addr
                      << ", pid = " << pid;
            ASSERT_GT(pid, 0);
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
            LOG(INFO) << "write " << filename << ", thread " << (i + 1)
                      << " started";
        }

        for (auto& th : writeThs) {
            th.join();
        }

        curve::test::FileCommonOperation::Close(fd);
        LOG(INFO) << "stop all write thread, filename " << filename;
    }

    static void AioWriteFunc(int fd) {
        auto cb = [](CurveAioContext* ctx) {
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

    void SetUp() override {}

    void TearDown() override {}

    static int fd;
    static std::unique_ptr<CurveCluster> cluster;
    static std::unordered_map<int, ChunkserverParam> chunkServers;
};

int MDSPermissionException::fd = 0;
std::unique_ptr<CurveCluster> MDSPermissionException::cluster;
std::unordered_map<int, ChunkserverParam> MDSPermissionException::chunkServers; // NOLINT

// one client had be writer, others will be reader
TEST_F(MDSPermissionException, TestPreWriter) {
    const std::string filename = "/TestPreWriter";
    constexpr size_t length = 4ull * 1024 * 1024;
    constexpr off_t offset = 4ull * 1024 * 1024;
    std::unique_ptr<char[]> readBuff(new char[length]);

    C_UserInfo_t info;
    snprintf(info.owner, sizeof(info.owner), "curve");
    ::Create(filename.c_str(), &info, 10ull * 1024 * 1024 * 1024);
    auto fd = ::Open2(filename.c_str(), &info, CURVE_SHARED | CURVE_RDWR);
    ASSERT_GE(fd, 0);
    LOG(INFO) << " filename =  " << filename << " fd = " << fd;

    auto ret = ::Read(fd, readBuff.get(), offset, length);
    ASSERT_GE(ret, 0);
    ret = ::Write(fd, readBuff.get(), offset, length);
    ASSERT_GE(ret, 0);
    // now curclient has be writer
    // 5 clients at the same time open2 the file
    // and all want to be reader
    std::vector<std::thread> workers;
    for (auto i = 0; i < 5; i++) {
        workers.emplace_back([&](){
            std::unique_ptr<char[]> t_readBuff(new char[length]);
            auto tempinfo = info;
            auto fd = ::Open2(filename.c_str(), &tempinfo,
                CURVE_SHARED | CURVE_RDONLY);
            ASSERT_GE(ret, 0);
            LOG(INFO) << " filename =  " << filename << " fd = " << fd;
            auto err = ::Write(fd, t_readBuff.get(), offset, length);
            ASSERT_EQ(err, LIBCURVE_ERROR::PERMISSION_DENY);
            ::Close(fd);
        });
    }
    for (auto& thrd : workers) {
        if (thrd.joinable()) {
            thrd.join();
        }
    }
    ret = ::Write(fd, readBuff.get(), offset, length);
    ASSERT_GT(ret, 0);
    ::Close(fd);
}

// 5 clients concurrency open2 the file
// only the first lock success one can write
TEST_F(MDSPermissionException, TestConcurrencyopen2) {
    const std::string filename = "/TestConcurrencyopen";
    constexpr size_t length = 4ull * 1024 * 1024;
    constexpr off_t offset = 4ull * 1024 * 1024;
    std::unique_ptr<char[]> readBuff(new char[length]);
    C_UserInfo_t info;
    snprintf(info.owner, sizeof(info.owner), "curve");
    ::Create(filename.c_str(), &info, 10ull * 1024 * 1024 * 1024);
    auto fd = ::Open2(filename.c_str(), &info,
        CURVE_SHARED | CURVE_FORCE_WRITE);
    ASSERT_GE(fd, 0);
    auto ret = ::Read(fd, readBuff.get(), offset, length);
    ASSERT_GE(ret, 0);
    ret = ::Write(fd, readBuff.get(), offset, length);
    ASSERT_GT(ret, 0);
    std::vector<std::thread> workers;
    int writeresult[5] = {0};
    for (auto i = 0; i < 5; i++) {
        workers.emplace_back([&, i](){
            std::unique_ptr<char[]> t_readBuff(new char[length]);
            auto tempinfo = info;
            writeresult[i] = ::Open2(filename.c_str(), &tempinfo,
                CURVE_SHARED | CURVE_RDWR);
        });
    }

    for (auto& thrd : workers) {
        if (thrd.joinable()) {
            thrd.join();
        }
    }
    int writeerr = 0;
    for (auto i = 0; i < 5; i++) {
        if (writeresult[i] == -LIBCURVE_ERROR::PERMISSION_DENY) {
            writeerr++;
        } else {
            ASSERT_GE(writeresult[i], 0);
        }
    }
    ASSERT_EQ(writeerr, 4);
}

// curfilewriter close the file
// so other clients can be writer
TEST_F(MDSPermissionException, TestUnlockWriter) {
    const std::string filename = "/TestUnlockWriter";
    constexpr size_t length = 4ull * 1024 * 1024;
    constexpr off_t offset = 4ull * 1024 * 1024;
    std::unique_ptr<char[]> readBuff(new char[length]);

    C_UserInfo_t info;
    snprintf(info.owner, sizeof(info.owner), "curve");
    ::Create(filename.c_str(), &info, 10ull * 1024 * 1024 * 1024);
    // curclient has be writer
    auto fd = ::Open2(filename.c_str(), &info,
        CURVE_SHARED | CURVE_RDWR);
    ASSERT_GE(fd, 0);
    auto ret = ::Read(fd, readBuff.get(), offset, length);
    ret = ::Write(fd, readBuff.get(), offset, length);
    ASSERT_GT(ret, 0);
    // unlock the file
    ::Close(fd);
    // now curclient has be writer
    // 5 clients at the same time open2 the file
    // and all want to be reader
    int writerresults[5] = {0};
    std::vector<std::thread> workers;
    for (auto i = 0; i < 5; i++) {
        workers.emplace_back([&, i](){
            std::unique_ptr<char[]> t_readBuff(new char[length]);
            auto tempinfo = info;
            writerresults[i] = ::Open2(filename.c_str(), &tempinfo,
                CURVE_SHARED | CURVE_RDWR);
            LOG(INFO) << " filename =  " << filename << " fd = "
                << writerresults[i];
        });
    }
    for (auto& thrd : workers) {
        if (thrd.joinable()) {
            thrd.join();
        }
    }
    int readernums = 0;
    for (auto i = 0; i < 5; i++) {
        if (writerresults[i] == -LIBCURVE_ERROR::PERMISSION_DENY) {
            readernums++;
        } else {
            ASSERT_GE(writerresults[i], 0);
        }
    }
    ASSERT_EQ(readernums, 4);
}

// test the curfile writer refreshsession to the writerlock
TEST_F(MDSPermissionException, TestRefreshSessionWriter) {
    const std::string filename = "/TestRefreshSessionWriter";
    constexpr size_t length = 4ull * 1024 * 1024;
    constexpr off_t offset = 4ull * 1024 * 1024;
    std::unique_ptr<char[]> readBuff(new char[length]);

    C_UserInfo_t info;
    snprintf(info.owner, sizeof(info.owner), "curve");
    ::Create(filename.c_str(), &info, 10ull * 1024 * 1024 * 1024);
    // curclient has be writer
    auto fd = ::Open2(filename.c_str(), &info,
        CURVE_SHARED | CURVE_RDWR);
    ASSERT_GE(fd, 0);
    auto ret = ::Read(fd, readBuff.get(), offset, length);
    ret = ::Write(fd, readBuff.get(), offset, length);
    ASSERT_GT(ret, 0);
    std::vector<std::thread> workers;
    // 10s 5 clients want to lock the file
    for (auto i = 0; i < 5; i++) {
        workers.emplace_back([&, i]() {
            std::unique_ptr<char[]> t_readBuff(new char[length]);
            auto tempinfo = info;
            std::this_thread::sleep_for(std::chrono::seconds(10));
            auto t_fd = ::Open2(filename.c_str(), &tempinfo,
                CURVE_SHARED | CURVE_RDWR);
            ASSERT_EQ(t_fd, -LIBCURVE_ERROR::PERMISSION_DENY);
        });
    }
    for (auto& thrd : workers) {
        if (thrd.joinable()) {
            thrd.join();
        }
    }
    ::Close(fd);
}
