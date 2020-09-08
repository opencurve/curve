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
 * File Created: Monday, 2nd September 2019 1:36:34 pm
 * Author: tongguangxun
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <map>
#include <cmath>
#include <mutex>   // NOLINT
#include <thread>  // NOLINT
#include <string>
#include <numeric>
#include <algorithm>
#include <condition_variable>  // NOLINT

#include "src/common/timeutility.h"
#include "include/client/libcurve.h"
#include "src/client/inflight_controller.h"
#include "test/integration/client/common/file_operation.h"
#include "test/integration/cluster_common/cluster.h"
#include "test/util/config_generator.h"

bool resumeFlag = false;
uint64_t ioFailedCount = 0;
std::mutex resumeMtx;
std::condition_variable resumeCV;
curve::client::InflightControl inflightContl;

using curve::CurveCluster;
const std::vector<std::string> mdsConf{
    { " --confPath=./conf/mds.conf" },
    { " --log_dir=./runlog/ChunkserverException" },
    { " --mdsDbName=module_exception_curve_chunkserver" },
    { " --sessionInterSec=20" },
    { " --etcdAddr=127.0.0.1:22233" },
    { " --updateToRepoSec=5" },
};

const std::vector<std::string> chunkserverConf4{
    { " -chunkServerStoreUri=local://./moduleException4/" },
    { " -chunkServerMetaUri=local://./moduleException4/chunkserver.dat" },
    { " -copySetUri=local://./moduleException4/copysets" },
    { " -raftSnapshotUri=curve://./moduleException4/copysets" },
    { " -raftLogUri=curve://./moduleException4/copysets" },
    { " -recycleUri=local://./moduleException4/recycler" },
    { " -chunkFilePoolDir=./moduleException4/chunkfilepool/" },
    { " -chunkFilePoolMetaPath=./moduleException4/chunkfilepool.meta" },
    { " -conf=./conf/chunkserver.conf.example" },
    { " -raft_sync_segments=true" },
    { " --log_dir=./runlog/ChunkserverException" },
    { " -chunkServerIp=127.0.0.1" },
    { " -chunkServerPort=22125" },
    { " -enableChunkfilepool=false" },
    { " -mdsListenAddr=127.0.0.1:22122,127.0.0.1:22123,127.0.0.1:22124" },
    { " -enableWalfilepool=false" },
    { " -walFilePoolDir=./moduleException4/walfilepool/" },
    { " -walFilePoolMetaPath=./moduleException4/walfilepool.meta" }
};

const std::vector<std::string> chunkserverConf5{
    { " -chunkServerStoreUri=local://./moduleException5/" },
    { " -chunkServerMetaUri=local://./moduleException5/chunkserver.dat" },
    { " -copySetUri=local://./moduleException5/copysets" },
    { " -raftSnapshotUri=curve://./moduleException5/copysets" },
    { " -raftLogUri=curve://./moduleException5/copysets" },
    { " -recycleUri=local://./moduleException5/recycler" },
    { " -chunkFilePoolDir=./moduleException5/chunkfilepool/" },
    { " -chunkFilePoolMetaPath=./moduleException5/chunkfilepool.meta" },
    { " -conf=./conf/chunkserver.conf.example" },
    { " -raft_sync_segments=true" },
    { " --log_dir=./runlog/ChunkserverException" },
    { " -chunkServerIp=127.0.0.1" },
    { " -chunkServerPort=22126" },
    { " -enableChunkfilepool=false" },
    { " -mdsListenAddr=127.0.0.1:22122,127.0.0.1:22123,127.0.0.1:22124" },
    { " -enableWalfilepool=false" },
    { " -walFilePoolDir=./moduleException5/walfilepool/" },
    { " -walFilePoolMetaPath=./moduleException5/walfilepool.meta" }
};

const std::vector<std::string> chunkserverConf6{
    { " -chunkServerStoreUri=local://./moduleException6/" },
    { " -chunkServerMetaUri=local://./moduleException6/chunkserver.dat" },
    { " -copySetUri=local://./moduleException6/copysets" },
    { " -raftSnapshotUri=curve://./moduleException6/copysets" },
    { " -raftLogUri=curve://./moduleException6/copysets" },
    { " -recycleUri=local://./moduleException6/recycler" },
    { " -chunkFilePoolDir=./moduleException6/chunkfilepool/" },
    { " -chunkFilePoolMetaPath=./moduleException6/chunkfilepool.meta" },
    { " -conf=./conf/chunkserver.conf.example" },
    { " -raft_sync_segments=true" },
    { " --log_dir=./runlog/ChunkserverException" },
    { " -chunkServerIp=127.0.0.1" },
    { " -chunkServerPort=22127" },
    { " -enableChunkfilepool=false" },
    { " -mdsListenAddr=127.0.0.1:22122,127.0.0.1:22123,127.0.0.1:22124" },
    { " -enableWalfilepool=false" },
    { " -walFilePoolDir=./moduleException6/walfilepool/" },
    { " -walFilePoolMetaPath=./moduleException6/walfilepool.meta" }
};

std::string mdsaddr =  // NOLINT
    "127.0.0.1:22122,127.0.0.1:22123,127.0.0.1:22124";
std::string logpath = "./runlog/ChunkserverException";  // NOLINT

const std::vector<std::string> clientConf{
    std::string("mds.listen.addr=") + mdsaddr,
    std::string("global.logPath=") + logpath,
    std::string("chunkserver.rpcTimeoutMS=1000"),
    std::string("chunkserver.opMaxRetry=10"),
};
class CSModuleException : public ::testing::Test {
 public:
    void SetUp() {
        std::string confPath = "./test/integration/client/config/client.conf.1";
        system("mkdir ./runlog/ChunkserverException");
        system("rm -rf module_exception_test_chunkserver.etcd");
        system("rm -rf moduleException4 moduleException5 moduleException6");

        cluster = new CurveCluster();
        ASSERT_NE(nullptr, cluster);

        cluster->PrepareConfig<curve::ClientConfigGenerator>(confPath,
                                                             clientConf);

        // 1. 启动etcd
        pid_t pid = cluster->StartSingleEtcd(
            1, "127.0.0.1:22233", "127.0.0.1:22234",
            std::vector<std::string>{
                " --name module_exception_test_chunkserver" });
        LOG(INFO) << "etcd 1 started on 127.0.0.1:22233:22234, pid = " << pid;
        ASSERT_GT(pid, 0);

        // 2. 先启动一个mds，让其成为leader，然后再启动另外两个mds节点
        pid =
            cluster->StartSingleMDS(1, "127.0.0.1:22122", 22128, mdsConf, true);
        LOG(INFO) << "mds 1 started on 127.0.0.1:22122, pid = " << pid;
        ASSERT_GT(pid, 0);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        pid = cluster->StartSingleMDS(2, "127.0.0.1:22123", 22129, mdsConf,
                                      false);
        LOG(INFO) << "mds 2 started on 127.0.0.1:22123, pid = " << pid;
        ASSERT_GT(pid, 0);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        pid = cluster->StartSingleMDS(3, "127.0.0.1:22124", 22130, mdsConf,
                                      false);
        LOG(INFO) << "mds 3 started on 127.0.0.1:22124, pid = " << pid;
        ASSERT_GT(pid, 0);
        std::this_thread::sleep_for(std::chrono::seconds(8));

        // 3. 创建物理池
        std::string createPPCmd = std::string("./bazel-bin/tools/curvefsTool") +
                                  std::string(
                                      " -cluster_map=./test/integration/client/"
                                      "config/topo_example_1.json") +
                                  std::string(
                                      " -mds_addr=127.0.0.1:22122,127.0.0.1:"
                                      "22123,127.0.0.1:22124") +
                                  std::string(" -op=create_physicalpool") +
                                  std::string(" -stderrthreshold=0") +
                                  std::string(" -minloglevel=0") +
                                  std::string(" -rpcTimeOutMs=10000");

        LOG(INFO) << "exec cmd: " << createPPCmd;
        int ret = 0;
        int retry = 0;
        while (retry < 5) {
            ret = system(createPPCmd.c_str());
            if (ret == 0) break;
            retry++;
        }

        // 4. 创建chunkserver
        pid = cluster->StartSingleChunkServer(1, "127.0.0.1:22125",
                                              chunkserverConf4);
        LOG(INFO) << "chunkserver 1 started on 127.0.0.1:22125, pid = " << pid;
        ASSERT_GT(pid, 0);

        pid = cluster->StartSingleChunkServer(2, "127.0.0.1:22126",
                                              chunkserverConf5);
        LOG(INFO) << "chunkserver 2 started on 127.0.0.1:22126, pid = " << pid;
        ASSERT_GT(pid, 0);

        pid = cluster->StartSingleChunkServer(3, "127.0.0.1:22127",
                                              chunkserverConf6);
        LOG(INFO) << "chunkserver 3 started on 127.0.0.1:22127, pid = " << pid;
        ASSERT_GT(pid, 0);

        std::this_thread::sleep_for(std::chrono::seconds(5));
        // 5. 创建逻辑池, 并睡眠一段时间让底层copyset先选主
        std::string createLPCmd =
            std::string("./bazel-bin/tools/curvefsTool") +
            std::string(
                " -cluster_map=./test/integration/client/"
                "config/topo_example_1.json") +
            std::string(
                " -mds_addr=127.0.0.1:22122,127.0.0.1:"
                "22123,127.0.0.1:22124") +
            std::string(" -op=create_logicalpool") +
            std::string(" -stderrthreshold=0 -minloglevel=0");

        ret = 0;
        retry = 0;
        while (retry < 5) {
            ret = system(createLPCmd.c_str());
            if (ret == 0) break;
            retry++;
        }
        ASSERT_EQ(ret, 0);

        // 6. 初始化client配置
        ret = Init(confPath.c_str());
        ASSERT_EQ(ret, 0);

        // 7. 创建一个文件
        fd = curve::test::FileCommonOperation::Open("/test1", "curve");
        ASSERT_NE(fd, -1);

        // 8. 先睡眠10s，让chunkserver选出leader
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    void TearDown() {
        ::Close(fd);
        UnInit();
        ASSERT_EQ(0, cluster->StopCluster());
        delete cluster;
        system("rm -rf moduleException6 moduleException4 moduleException5");
    }

    void CreateOpenFileBackend() {
        createDone = false;
        createOrOpenFailed = false;
        auto func = [&]() {
            for (int i = 0; i < 20; i++) {
                std::string filename = "/" + std::to_string(i);
                int ret =
                    curve::test::FileCommonOperation::Open(filename, "curve");
                ret == -1 ? createOrOpenFailed = true : 0;

                if (ret != -1) {
                    ::Close(ret);
                } else {
                    break;
                }
            }

            std::unique_lock<std::mutex> lk(createMtx);
            createDone = true;
            createCV.notify_all();
        };

        std::thread t(func);
        t.detach();
    }

    void WaitBackendCreateDone() {
        std::unique_lock<std::mutex> lk(createMtx);
        createCV.wait(lk, [&]() { return createDone; });
    }

    /**
     * 监测client io能否在预期时间内正常下发
     * @param: off是当前需要下发IO的偏移
     * @param: size是下发io的大小
     * @param: predictTimeS是预期在多少秒内IO可以恢复
     * @param[out]: failCount为当前io下发中错误返回的数量
     * @return: 如果io在预期时间内能够正常下发，则返true，否则返回false
     */
    bool MonitorResume(uint64_t off, uint64_t size, uint64_t predictTimeS,
                       uint64_t* failCount = nullptr) {
        inflightContl.SetMaxInflightNum(16);
        resumeFlag = false;
        ioFailedCount = 0;

        auto wcb = [](CurveAioContext* context) {
            inflightContl.DecremInflightNum();
            if (context->ret == context->length) {
                std::unique_lock<std::mutex> lk(resumeMtx);
                resumeFlag = true;
                resumeCV.notify_all();
            } else {
                ioFailedCount++;
            }
            delete context;
        };

        char* writebuf = new char[size];
        memset(writebuf, 'a', size);
        auto iofunc = [&]() {
            std::this_thread::sleep_for(std::chrono::seconds(predictTimeS));
            inflightContl.WaitInflightComeBack();

            CurveAioContext* context = new CurveAioContext;
            context->op = LIBCURVE_OP::LIBCURVE_OP_WRITE;
            context->offset = off;
            context->length = size;
            context->buf = writebuf;
            context->cb = wcb;

            inflightContl.IncremInflightNum();
            AioWrite(fd, context);
        };

        std::thread iothread(iofunc);

        bool ret = false;
        {
            std::unique_lock<std::mutex> lk(resumeMtx);
            resumeCV.wait_for(lk, std::chrono::seconds(predictTimeS + 10));
            ret = resumeFlag;
        }

        failCount == nullptr ? 0 : (*failCount = ioFailedCount);

        // 唤醒io线程
        iothread.join();
        inflightContl.WaitInflightAllComeBack();

        delete[] writebuf;
        return ret;
    }

    int fd;

    // 是否出现挂卸载失败
    bool createOrOpenFailed;
    bool createDone;
    std::mutex createMtx;
    std::condition_variable createCV;

    CurveCluster* cluster;
};

// 测试环境拓扑：在单节点上启动一个client、三个chunkserver、三个mds、一个etcd

TEST_F(CSModuleException, ChunkserverException) {
    LOG(INFO) << "current case: KillOneChunkserverThenRestartTheChunkserver";
    /********* KillOneChunkserverThenRestartTheChunkserver **********/
    // 1. 测试重启一个chunkserver
    // 2.预期：
    //    a. 集群状态正常时：client读写请求可以正常下发
    //    b. kill一台chunkserver：client 读写请求最多卡顿
    //       election_timeout*2s可以正常读写
    //    c. 恢复chunkserver：client 读写请求无影响
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. kill掉一个chunkserver
    ASSERT_EQ(0, cluster->StopChunkServer(1));

    // 3. kill掉一个chunkserver之后，client的io预期最多会在2*electtime后恢复
    ASSERT_TRUE(MonitorResume(0, 4096, 2));

    // 4. 拉起刚才被kill的chunkserver
    pid_t pid =
        cluster->StartSingleChunkServer(1, "127.0.0.1:22125", chunkserverConf4);
    LOG(INFO) << "chunkserver 1 started on 127.0.0.1:22125, pid = " << pid;
    ASSERT_GT(pid, 0);

    // 5. 重新拉起对client IO没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: HangOneChunkserverThenResumeTheChunkserver";
    /********* HangOneChunkserverThenResumeTheChunkserver ***********/
    // 1. hang一台chunkserver，然后恢复hang的chunkserver
    // 2.预期
    //    a. 集群状态正常时：client读写请求可以正常下发
    //    b. hang一台chunkserver：client
    //       读写请求最多卡顿election_timeout*2s可以正常读写
    //    c. 恢复chunkserver：client 读写请求无影响
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. hang一个chunkserver
    ASSERT_EQ(0, cluster->HangChunkServer(1));

    // 3. hang一个chunkserver之后，client的io预期最多会在2*electtime后恢复
    ASSERT_TRUE(MonitorResume(0, 4096, 2));

    // 4. 拉起刚才被hang的chunkserver
    ASSERT_EQ(0, cluster->RecoverHangChunkServer(1));

    // 5. 重新拉起对client IO没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: KillTwoChunkserverThenRestartTheChunkserver";
    /******** KillTwoChunkserverThenRestartTheChunkserver *********/
    // 1. 测试重启两个chunkserver
    // 2.预期：
    //    a. 集群状态正常时：client读写请求可以正常下发
    //    b. kill两台chunkserver：预期client IO持续hang，新写IO和覆盖写都hang
    //       拉起被kill中的一台chunkserver：client IO预期在最多在
    //      （chunkserver启动回放数据+2*election_timeout）时间内恢复读写
    //    c. 拉起另外一台kill的chunkserver：client IO无影响
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. kill掉两个chunkserver
    ASSERT_EQ(0, cluster->StopChunkServer(1));
    ASSERT_EQ(0, cluster->StopChunkServer(2));

    // 3. kill掉两个chunkserver, io无法正常下发
    ASSERT_FALSE(MonitorResume(0, 4096, 30));

    // 4. 拉起刚才被kill的chunkserver的第一个
    pid =
        cluster->StartSingleChunkServer(1, "127.0.0.1:22125", chunkserverConf4);
    LOG(INFO) << "chunkserver 1 started on 127.0.0.1:22125, pid = " << pid;
    ASSERT_GT(pid, 0);

    // 5. 拉起刚才被kill的chunkserver的第一个，
    //    client的io预期最多会在2*electtime后恢复
    // 如果配置了慢启动，则需要等待
    // (copysetNum / load_concurrency) * election_timeout
    ASSERT_TRUE(MonitorResume(0, 4096, 80));

    // 6. 拉起刚才被kill的chunkserver的第二个
    pid =
        cluster->StartSingleChunkServer(2, "127.0.0.1:22126", chunkserverConf5);
    LOG(INFO) << "chunkserver 2 started on 127.0.0.1:22126, pid = " << pid;
    ASSERT_GT(pid, 0);

    // 7. 集群io不影响，正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: HangTwoChunkserverThenResumeTheChunkserver";
    /******* HangTwoChunkserverThenResumeTheChunkserver **********/
    // 1. hang两台chunkserver，然后恢复hang的chunkserver
    // 2.预期
    //    a. 集群状态正常时：client读写请求可以正常下发
    //    b. hang两台chunkserver：client IO持续hang，新写IO和覆盖写都hang
    //    c. 恢复其中的一台chunkserver：client IO 恢复读写，
    //       从恢复chunkserver到client IO恢复时间在election_timeout*2
    //    d. 恢复另外一台hang的chunkserver：client IO无影响
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. hang掉两个个chunkserver
    ASSERT_EQ(0, cluster->HangChunkServer(1));
    ASSERT_EQ(0, cluster->HangChunkServer(2));

    // 3. hang两个chunkserver, io无法正常下发
    ASSERT_FALSE(MonitorResume(0, 4096, 2));

    // 4. 拉起刚才被hang的chunkserver的第一个
    ASSERT_EQ(0, cluster->RecoverHangChunkServer(1));

    // 5. 拉起刚才被hang的chunkserver的第一个，
    //    client的io预期最多会在2*electtime后恢复
    // 如果配置了慢启动，则需要等待
    // (copysetNum / load_concurrency) * election_timeout
    ASSERT_TRUE(MonitorResume(0, 4096, 80));

    // 6. 拉起刚才被hang的chunkserver的第二个
    ASSERT_EQ(0, cluster->RecoverHangChunkServer(2));

    // 7. 集群io不影响，正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: KillThreeChunkserverThenRestartTheChunkserver";
    /******** KillThreeChunkserverThenRestartTheChunkserver ******/
    // 1. 测试重启三个chunkserver
    // 2.预期：
    //    a. 集群状态正常时：client读写请求可以正常下发
    //    b. 关闭三台chunkserver：client IO hang
    //    c. 重启一台chunkserver：client IO hang
    //    d. 重启第二台chunkserver：client IO hang，
    //       直到chunkserver完全恢复，IO恢复。
    //       恢复时间约等于（chunkserver启动回放数据+2*election_timeout）
    //    e. 重启第三台chunkserver：client IO无影响
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. kill掉三个chunkserver
    ASSERT_EQ(0, cluster->StopChunkServer(1));
    ASSERT_EQ(0, cluster->StopChunkServer(2));
    ASSERT_EQ(0, cluster->StopChunkServer(3));

    // 3. kill掉三个chunkserver, io无法正常下发
    ASSERT_FALSE(MonitorResume(0, 4096, 2));

    // 4. 拉起刚才被kill的chunkserver的第一个
    pid =
        cluster->StartSingleChunkServer(1, "127.0.0.1:22125", chunkserverConf4);
    LOG(INFO) << "chunkserver 1 started on 127.0.0.1:22125, pid = " << pid;
    ASSERT_GT(pid, 0);

    // 5. 只有一个chunkserver工作, io无法正常下发
    ASSERT_FALSE(MonitorResume(0, 4096, 80));

    // 6. 拉起刚才被kill的chunkserver的第二个
    pid =
        cluster->StartSingleChunkServer(2, "127.0.0.1:22126", chunkserverConf5);
    LOG(INFO) << "chunkserver 2 started on 127.0.0.1:22126, pid = " << pid;
    ASSERT_GT(pid, 0);

    // 7. client的io恢复
    ASSERT_TRUE(MonitorResume(0, 4096, 80));

    // 8. 拉起其他被kil的chunkserver
    pid =
        cluster->StartSingleChunkServer(3, "127.0.0.1:22127", chunkserverConf6);
    LOG(INFO) << "chunkserver 3 started on 127.0.0.1:22127, pid = " << pid;
    ASSERT_GT(pid, 0);

    LOG(INFO) << "current case: HangThreeChunkserverThenResumeTheChunkserver";
    /******** HangThreeChunkserverThenResumeTheChunkserver **********/
    // 1. hang三台chunkserver，然后恢复hang的chunkserver
    // 2.预期
    //    a. 集群状态正常时：client读写请求可以正常下发
    //    b. hang三台chunkserver：client IO hang
    //    c. 恢复一台chunkserver：client IO hang
    //    d. 再恢复一台chunkserver：预期在
    //       election_timeout*2左右的时间，client IO恢复
    //    e. 恢复最后一台chunkserver：预期client IO无影响
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. hang掉三个chunkserver
    ASSERT_EQ(0, cluster->HangChunkServer(1));
    ASSERT_EQ(0, cluster->HangChunkServer(2));
    ASSERT_EQ(0, cluster->HangChunkServer(3));

    // 3. hang三个chunkserver, io无法正常下发
    ASSERT_FALSE(MonitorResume(0, 4096, 30));

    // 4. 拉起刚才被hang的chunkserver的第一个
    ASSERT_EQ(0, cluster->RecoverHangChunkServer(1));

    // 5. 只有一个chunkserver工作, io无法正常下发
    ASSERT_FALSE(MonitorResume(0, 4096, 80));

    // 6. 拉起刚才被hang的chunkserver的第二个
    ASSERT_EQ(0, cluster->RecoverHangChunkServer(2));
    ASSERT_EQ(0, cluster->RecoverHangChunkServer(3));

    // 7. client的io预期最多会在2*electtime s内恢复
    // 如果配置了慢启动，则需要等待
    // (copysetNum / load_concurrency) * election_timeout
    ASSERT_TRUE(MonitorResume(0, 4096, 80));
}
