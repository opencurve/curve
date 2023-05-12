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
bool writeIOReturnFlag = false;
uint64_t ioFailedCount = 0;
std::mutex resumeMtx;
std::condition_variable resumeCV;
curve::client::InflightControl inflightContl;
bool testIOWrite = false;
bool testIORead = false;

using curve::CurveCluster;
const std::vector<std::string> mdsConf{
    { "--confPath=./conf/mds.conf" },
    { "--log_dir=./runlog/MDSExceptionTest" },
    { "--mdsDbName=module_exception_curve_mds" },
    { "--sessionInterSec=20" },
    { "--etcdAddr=127.0.0.1:22230" },
};

const std::vector<std::string> chunkserverConf1{
    { "-chunkServerStoreUri=local://./moduleException1/" },
    { "-chunkServerMetaUri=local://./moduleException1/chunkserver.dat" },
    { "-copySetUri=local://./moduleException1/copysets" },
    { "-raftSnapshotUri=curve://./moduleException1/copysets" },
    { "-raftLogUri=curve://./moduleException1/copysets" },
    { "-recycleUri=local://./moduleException1/recycler" },
    { "-chunkFilePoolDir=./moduleException1/chunkfilepool/" },
    { "-chunkFilePoolMetaPath=./moduleException1/chunkfilepool.meta" },
    { "-conf=./conf/chunkserver.conf.example" },
    { "-raft_sync_segments=true" },
    { "--log_dir=./runlog/MDSExceptionTest" },
    { "--graceful_quit_on_sigterm" },
    { "-chunkServerIp=127.0.0.1" },
    { "-chunkServerPort=22225" },
    { "-enableChunkfilepool=false" },
    { "-mdsListenAddr=127.0.0.1:22222,127.0.0.1:22223,127.0.0.1:22224" },
    { "-enableWalfilepool=false" },
    { "-walFilePoolDir=./moduleException1/walfilepool/" },
    { "-walFilePoolMetaPath=./moduleException1/walfilepool.meta" }
};

const std::vector<std::string> chunkserverConf2{
    { "-chunkServerStoreUri=local://./moduleException2/" },
    { "-chunkServerMetaUri=local://./moduleException2/chunkserver.dat" },
    { "-copySetUri=local://./moduleException2/copysets" },
    { "-raftSnapshotUri=curve://./moduleException2/copysets" },
    { "-raftLogUri=curve://./moduleException2/copysets" },
    { "-recycleUri=local://./moduleException2/recycler" },
    { "-chunkFilePoolDir=./moduleException2/chunkfilepool/" },
    { "-chunkFilePoolMetaPath=./moduleException2/chunkfilepool.meta" },
    { "-conf=./conf/chunkserver.conf.example" },
    { "-raft_sync_segments=true" },
    { "--log_dir=./runlog/MDSExceptionTest" },
    { "--graceful_quit_on_sigterm" },
    { "-chunkServerIp=127.0.0.1" },
    { "-chunkServerPort=22226" },
    { "-enableChunkfilepool=false" },
    { "-mdsListenAddr=127.0.0.1:22222,127.0.0.1:22223,127.0.0.1:22224" },
    { "-enableWalfilepool=false" },
    { "-walFilePoolDir=./moduleException2/walfilepool/" },
    { "-walFilePoolMetaPath=./moduleException2/walfilepool.meta" }
};

const std::vector<std::string> chunkserverConf3{
    { "-chunkServerStoreUri=local://./moduleException3/" },
    { "-chunkServerMetaUri=local://./moduleException3/chunkserver.dat" },
    { "-copySetUri=local://./moduleException3/copysets" },
    { "-raftSnapshotUri=curve://./moduleException3/copysets" },
    { "-raftLogUri=curve://./moduleException3/copysets" },
    { "-recycleUri=local://./moduleException3/recycler" },
    { "-chunkFilePoolDir=./moduleException3/chunkfilepool/" },
    { "-chunkFilePoolMetaPath=./moduleException3/chunkfilepool.meta" },
    { "-conf=./conf/chunkserver.conf.example" },
    { "-raft_sync_segments=true" },
    { "--log_dir=./runlog/MDSExceptionTest" },
    { "--graceful_quit_on_sigterm" },
    { "-chunkServerIp=127.0.0.1" },
    { "-chunkServerPort=22227" },
    { "-enableChunkfilepool=false" },
    { "-mdsListenAddr=127.0.0.1:22222,127.0.0.1:22223,127.0.0.1:22224" },
    { "-enableWalfilepool=false" },
    { "-walFilePoolDir=./moduleException3/walfilepool/" },
    { "-walFilePoolMetaPath=./moduleException3/walfilepool.meta" }
};

std::string mdsaddr =                                   // NOLINT
    "127.0.0.1:22222,127.0.0.1:22223,127.0.0.1:22224";  // NOLINT
std::string logpath = "./runlog/MDSExceptionTest";      // NOLINT

const std::vector<std::string> clientConf{
    std::string("mds.listen.addr=") + mdsaddr,
    std::string("global.logPath=") + logpath,
    std::string("chunkserver.rpcTimeoutMS=1000"),
    std::string("chunkserver.opMaxRetry=10"),
};

class MDSModuleException : public ::testing::Test {
 public:
    void SetUp() {
        std::string confPath = "./test/integration/client/config/client.conf";
        system("mkdir ./runlog/MDSExceptionTest");
        system("rm -rf module_exception_test_mds.etcd");
        system("rm -rf moduleException1 moduleException2 moduleException3");

        cluster = new CurveCluster();
        ASSERT_NE(nullptr, cluster);

        cluster->PrepareConfig<curve::ClientConfigGenerator>(confPath,
                                                             clientConf);

        // 1. 启动etcd
        pid_t pid = cluster->StartSingleEtcd(
            1, "127.0.0.1:22230", "127.0.0.1:22231",
            std::vector<std::string>{"--name=module_exception_test_mds"});
        LOG(INFO) << "etcd 1 started on 127.0.0.1:22230:22231, pid = " << pid;
        ASSERT_GT(pid, 0);

        // 2. 先启动一个mds，让其成为leader，然后再启动另外两个mds节点
        pid =
            cluster->StartSingleMDS(0, "127.0.0.1:22222", 22240, mdsConf, true);
        LOG(INFO) << "mds 0 started on 127.0.0.1:22222, pid = " << pid;
        ASSERT_GT(pid, 0);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        pid = cluster->StartSingleMDS(1, "127.0.0.1:22223", 22241, mdsConf,
                                      false);
        LOG(INFO) << "mds 1 started on 127.0.0.1:22223, pid = " << pid;
        ASSERT_GT(pid, 0);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        pid = cluster->StartSingleMDS(2, "127.0.0.1:22224", 22242, mdsConf,
                                      false);
        LOG(INFO) << "mds 2 started on 127.0.0.1:22224, pid = " << pid;
        ASSERT_GT(pid, 0);
        std::this_thread::sleep_for(std::chrono::seconds(8));

        // 3. 创建物理池
        std::string createPPCmd = std::string("./bazel-bin/tools/curvefsTool") +
                                  std::string(
                                      " -cluster_map=./test/integration/client/"
                                      "config/topo_example.json") +
                                  std::string(
                                      " -mds_addr=127.0.0.1:22222,127.0.0.1:"
                                      "22223,127.0.0.1:22224") +
                                  std::string(" -op=create_physicalpool") +
                                  std::string(" -stderrthreshold=0") +
                                  std::string(" -rpcTimeOutMs=10000") +
                                  std::string(" -minloglevel=0");

        LOG(INFO) << "exec cmd: " << createPPCmd;
        int ret = 0;
        int retry = 0;
        while (retry < 5) {
            ret = system(createPPCmd.c_str());
            if (ret == 0) break;
            retry++;
        }
        ASSERT_EQ(ret, 0);

        // 4. 创建chunkserver
        pid = cluster->StartSingleChunkServer(1, "127.0.0.1:22225",
                                              chunkserverConf1);
        LOG(INFO) << "chunkserver 1 started on 127.0.0.1:22225, pid = " << pid;
        ASSERT_GT(pid, 0);
        pid = cluster->StartSingleChunkServer(2, "127.0.0.1:22226",
                                              chunkserverConf2);
        LOG(INFO) << "chunkserver 2 started on 127.0.0.1:22226, pid = " << pid;
        ASSERT_GT(pid, 0);
        pid = cluster->StartSingleChunkServer(3, "127.0.0.1:22227",
                                              chunkserverConf3);
        LOG(INFO) << "chunkserver 3 started on 127.0.0.1:22227, pid = " << pid;
        ASSERT_GT(pid, 0);

        std::this_thread::sleep_for(std::chrono::seconds(5));

        // 5. 创建逻辑池, 并睡眠一段时间让底层copyset先选主
        std::string createLPCmd =
            std::string("./bazel-bin/tools/curvefsTool") +
            std::string(
                " -cluster_map=./test/integration/client/"
                "config/topo_example.json") +
            std::string(
                " -mds_addr=127.0.0.1:22222,127.0.0.1:"
                "22223,127.0.0.1:22224") +
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

        ipmap[0] = "127.0.0.1:22222";
        ipmap[1] = "127.0.0.1:22223";
        ipmap[2] = "127.0.0.1:22224";

        configmap[0] = mdsConf;
        configmap[1] = mdsConf;
        configmap[2] = mdsConf;
    }

    void TearDown() {
        ::Close(fd);
        UnInit();

        ASSERT_EQ(0, cluster->StopCluster());
        delete cluster;
        system("rm -rf module_exception_test_mds.etcd");
        system("rm -rf moduleException1 moduleException2 moduleException3");
    }

    void CreateOpenFileBackend() {
        createDone = false;
        createOrOpenFailed = false;
        auto func = [&]() {
            static int num = 0;
            for (int i = num; i < num + 20; i++) {
                std::string filename = "/" + std::to_string(i);
                LOG(INFO) << "now create file: " << filename;
                int ret =
                    curve::test::FileCommonOperation::Open(filename, "curve");
                ret == -1 ? createOrOpenFailed = true : 0;

                if (ret != -1) {
                    ::Close(ret);
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                } else {
                    break;
                }
            }
            num += 20;

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
     * @return: 如果io在预期时间内嫩够正常下发，则返true，否则返回false
     */
    bool MonitorResume(uint64_t off, uint64_t size, uint64_t predictTimeS) {
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
            LOG(INFO) << "end aiowrite with ret = " << context->ret;
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
            LOG(INFO) << "start aiowrite";

            AioWrite(fd, context);
        };

        std::thread iothread(iofunc);

        bool ret = false;
        {
            std::unique_lock<std::mutex> lk(resumeMtx);
            resumeCV.wait_for(lk, std::chrono::seconds(2 * predictTimeS + 30));
            ret = resumeFlag;
        }

        // 唤醒io线程
        iothread.join();
        inflightContl.WaitInflightAllComeBack();

        delete[] writebuf;
        return ret;
    }

    /**下发一个写请求
     * @param: offset是当前需要下发IO的偏移
     * @param: size是下发IO的大小
     * @return: IO是否下发成功
     */
    bool SendAioWriteRequest(uint64_t offset, uint64_t size) {
        writeIOReturnFlag = false;

        auto writeCallBack = [](CurveAioContext* context) {
            // 无论IO是否成功，只要返回，就置为true
            writeIOReturnFlag = true;
            char* buffer = reinterpret_cast<char*>(context->buf);
            delete[] buffer;
            delete context;
        };

        char* buffer = new char[size];
        memset(buffer, 'a', size);
        CurveAioContext* context = new CurveAioContext();
        context->op = LIBCURVE_OP::LIBCURVE_OP_WRITE;
        context->offset = offset;
        context->length = size;
        context->buf = buffer;
        context->cb = writeCallBack;

        return AioWrite(fd, context) == 0;
    }

    /** 下发一个写请求并读取进行数据验证
     *  @param: fd 卷fd
     *  @param: 当前需要下发io的偏移
     *  @param：下发io的大小
     *  @return: 数据是否一致
    */
    void VerifyDataConsistency(int fd, uint64_t offset, uint64_t size) {
        char* writebuf = new char[size];
        char* readbuf = new char[size];
        unsigned int i;

        LOG(INFO) << "VerifyDataConsistency(): offset " <<
                                offset << ", size " << size;
        for (i = 0; i < size; i++) {
            writebuf[i] = ('a' + std::rand() % 26);
        }

        // 开始写
        auto wcb = [](CurveAioContext* context) {
            if (context->ret == context->length) {
                testIOWrite = true;
            }
            std::unique_lock<std::mutex> lk(resumeMtx);
            resumeCV.notify_all();
            delete context;
        };

        auto writefunc = [&]() {
            CurveAioContext* context = new CurveAioContext;;
            context->op = LIBCURVE_OP::LIBCURVE_OP_WRITE;
            context->offset = offset;
            context->length = size;
            context->buf = writebuf;
            context->cb = wcb;
            ASSERT_EQ(LIBCURVE_ERROR::OK, AioWrite(fd, context));
        };

        std::thread writeThread(writefunc);
        {
            std::unique_lock<std::mutex> lk(resumeMtx);
            resumeCV.wait_for(lk, std::chrono::seconds(300));
        }

        writeThread.join();
        ASSERT_TRUE(testIOWrite);

        // 开始读
        auto rcb = [](CurveAioContext* context) {
            if (context->ret == context->length) {
                testIORead = true;
            }
            std::unique_lock<std::mutex> lk(resumeMtx);
            resumeCV.notify_all();
            delete context;
        };

        auto readfunc = [&]() {
            CurveAioContext* context = new CurveAioContext;;
            context->op = LIBCURVE_OP::LIBCURVE_OP_READ;
            context->offset = offset;
            context->length = size;
            context->buf = readbuf;
            context->cb = rcb;
            ASSERT_EQ(LIBCURVE_ERROR::OK, AioRead(fd, context));
        };

        std::thread readThread(readfunc);
        {
            std::unique_lock<std::mutex> lk(resumeMtx);
            resumeCV.wait_for(lk, std::chrono::seconds(300));
        }

        readThread.join();
        ASSERT_TRUE(testIORead);
        ASSERT_EQ(strcmp(writebuf, readbuf), 0);

        delete[] writebuf;
        delete[] readbuf;
        return;
    }

    int fd;

    // 是否出现挂卸载失败
    bool createOrOpenFailed;
    bool createDone;
    std::mutex createMtx;
    std::condition_variable createCV;

    CurveCluster* cluster;

    std::map<int, std::string> ipmap;
    std::map<int, std::vector<std::string>> configmap;
};

#define segment_size 1 * 1024 * 1024 * 1024ul
// 测试环境拓扑：在单节点上启动一个client、三个chunkserver、三个mds、一个etcd

TEST_F(MDSModuleException, MDSExceptionTest) {
    LOG(INFO) << "current case: KillOneInserviceMDSThenRestartTheMDS";
    /********** KillOneInserviceMDSThenRestartTheMDS *************/
    // 1. 重启一台正在服务的mds
    // 2.预期
    //    a. 集群状态正常时：client读写请求可以正常下发
    //    b. 关闭一台mds，在mds服务切换到另一台mds之前，
    //       client 新写IO会hang，挂卸载服务会异常
    //    c. mds服务切换后，预期client IO无影响，挂卸载服务正常
    //    d. 重新拉起mds，client IO无影响
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. kill一台正在服务的mds，在启动的时候第一台mds当选leader
    int serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    ASSERT_EQ(0, cluster->StopMDS(serviceMDSID));

    // 3. 启动后台挂卸载线程，预期挂卸载会出现失败
    CreateOpenFileBackend();

    // 4. 启动后台io监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    follower mds在session过期后重新续约后集群正常服务（20s续约）
    ASSERT_TRUE(MonitorResume(segment_size, 4096, 25));

    // 5. 等待后台挂卸载监测结束
    WaitBackendCreateDone();

    // 6. 判断当前挂卸载情况
    ASSERT_TRUE(createOrOpenFailed);

    // 7. 拉起被kill的进程
    pid_t pid = cluster->StartSingleMDS(serviceMDSID, ipmap[serviceMDSID],
                                        22240 + serviceMDSID,
                                        configmap[serviceMDSID], false);
    LOG(INFO) << "mds " << serviceMDSID << " started on " << ipmap[serviceMDSID]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);

    // 8. 再拉起被kill的mds，对集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: KillOneNotInserviceMDSThenRestartTheMDS";
    /*********** KillOneNotInserviceMDSThenRestartTheMDS *******/
    // 1. 重启一台不在服务的mds
    // 2.预期
    //    a. 集群状态正常时：client读写请求可以正常下发
    //    b. 关闭一台不在服务的mds，预期client IO无影响，挂卸载服务正常
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. kill一台不在服务的mds，在启动的时候第一台mds当选leader, kill第二台
    serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    int killid = (serviceMDSID + 1) % 3;
    ASSERT_EQ(0, cluster->StopMDS(killid));

    // 3. 启动后台挂卸载线程，预期挂卸载服务不会受影响
    CreateOpenFileBackend();

    // 4. 启动后台io监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    follower mds在session过期后重新续约后集群正常服务（20s续约）
    ASSERT_TRUE(MonitorResume(2 * segment_size, 4096, 25));

    // 5. 等待挂卸载监测结束
    WaitBackendCreateDone();

    // 6. 挂卸载服务正常
    ASSERT_FALSE(createOrOpenFailed);

    // 7. 拉起被kill的进程
    pid = cluster->StartSingleMDS(killid, ipmap[killid], 22240 + killid,
                                  configmap[killid], false);
    LOG(INFO) << "mds " << killid << " started on " << ipmap[killid]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);

    // 8. 再拉起被kill的mds，对集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: hangOneInserviceMDSThenResumeTheMDS";
    /************ hangOneInserviceMDSThenResumeTheMDS ********/
    // 1. hang一台正在服务的mds
    // 2.预期
    //    a. 集群状态正常时：client读写请求可以正常下发
    //    b. mds hang期间且在与etcd续约超时之前，这时候新写IO会失败，
    //       因为新写触发getorallocate，这个RPC发到mds会出现一直超时，然后重试
    //       最后重试失败。
    //    c. client session续约时长总比mds与etcd之间续约时长大，所以在
    //       session续约失败之前mds预期可以完成切换，所以client的session
    //       不会过期，覆盖写不会出现异常。
    //    d. 恢复被hang的mds，预期对client io无影响
    // 0. 先睡眠一段时间等待mds集群选出leader
    std::this_thread::sleep_for(std::chrono::seconds(10));

    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. hang一台正在服务的mds，在启动的时候第一台mds当选leader
    serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    ASSERT_EQ(0, cluster->HangMDS(serviceMDSID));

    // 3. 启动后台挂卸载线程，预期挂卸载会出现失败
    CreateOpenFileBackend();

    // 4. 启动后台io监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    follower mds在session过期后重新续约后集群正常服务（20s续约）
    auto ret = MonitorResume(3 * segment_size, 4096, 25);
    if (!ret) {
        ASSERT_EQ(0, cluster->RecoverHangMDS(serviceMDSID));
        ASSERT_TRUE(false);
    }

    // 5. 等待后台挂卸载监测结束
    WaitBackendCreateDone();

    // 6. 判断当前挂卸载情况
    ASSERT_EQ(0, cluster->RecoverHangMDS(serviceMDSID));
    ASSERT_EQ(0, cluster->StopMDS(serviceMDSID));
    pid = cluster->StartSingleMDS(serviceMDSID, ipmap[serviceMDSID],
                                  22240 + serviceMDSID, configmap[serviceMDSID],
                                  false);
    LOG(INFO) << "mds " << serviceMDSID << " started on " << ipmap[serviceMDSID]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);
    ASSERT_TRUE(createOrOpenFailed);

    // 7. 再拉起被kill的mds，对集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: hangOneNotInserviceMDSThenResumeTheMDS";
    /********** hangOneNotInserviceMDSThenResumeTheMDS ***********/
    // 1. hang一台不在服务的mds
    // 2.预期
    //    a. 集群状态正常时：client读写请求可以正常下发
    //    b. hang一台不在服务的mds，预期client IO无影响，挂卸载服务正常
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. hang一台不在服务的mds，在启动的时候第一台mds当选leader, hang第二台
    serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    int hangid = (serviceMDSID + 1) % 3;
    ASSERT_EQ(0, cluster->HangMDS(hangid));

    // 3. 启动后台挂卸载线程，预期挂卸载服务不会受影响
    CreateOpenFileBackend();

    // 4. 启动后台iops监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    follower mds在session过期后重新续约后集群正常服务（20s续约)
    ret = MonitorResume(4 * segment_size, 4096, 25);
    if (!ret) {
        ASSERT_EQ(0, cluster->RecoverHangMDS(hangid));
        ASSERT_TRUE(false);
    }

    // 5. 等待挂卸载监测结束
    WaitBackendCreateDone();

    // 6. 挂卸载服务正常
    ASSERT_EQ(0, cluster->RecoverHangMDS(hangid));
    ASSERT_EQ(0, cluster->StopMDS(hangid));
    pid = cluster->StartSingleMDS(hangid, ipmap[hangid], 22240 + hangid,
                                  configmap[hangid], false);
    LOG(INFO) << "mds " << hangid << " started on " << ipmap[hangid]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);

    ASSERT_FALSE(createOrOpenFailed);

    // 7. 集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: KillTwoInserviceMDSThenRestartTheMDS";
    /************* KillTwoInserviceMDSThenRestartTheMDS ***********/
    // 1. 重启两台mds，其中一台正在服务的mds
    // 2.预期
    //    a. 集群状态正常时：client读写请求可以正常下发
    //    b. 关闭两台mds，在mds服务切换到另一台mds之前，
    //       client 新写IO会出现失败，挂卸载服务会异常
    //    c. mds服务切换后，预期client IO恢复，挂卸载服务正常
    //    d. 重新拉起mds，client IO无影响
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. kill两台mds，在启动的时候第一台mds当选leader, kill前二台
    serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    int secondid = (serviceMDSID + 1) % 3;
    ASSERT_EQ(0, cluster->StopMDS(serviceMDSID));
    ASSERT_EQ(0, cluster->StopMDS(secondid));

    // 3. 启动后台挂卸载线程，预期挂卸载服务会受影响
    CreateOpenFileBackend();

    // 4. 启动后台io监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    follower mds在session过期后重新续约后集群正常服务（20s续约）
    ASSERT_TRUE(MonitorResume(5 * segment_size, 4096, 25));

    // 5. 等待后台挂卸载监测结束
    WaitBackendCreateDone();

    // 6. 判断当前挂卸载情况
    ASSERT_TRUE(createOrOpenFailed);

    // 7. 拉起被kill的进程
    pid = cluster->StartSingleMDS(serviceMDSID, ipmap[serviceMDSID],
                                  22240 + serviceMDSID, configmap[serviceMDSID],
                                  false);
    LOG(INFO) << "mds " << serviceMDSID << " started on " << ipmap[serviceMDSID]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);

    // 8. 再拉起被kill的mds，对集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 9. 拉起被kill的其他mds
    pid = cluster->StartSingleMDS(secondid, ipmap[secondid], 22240 + secondid,
                                  configmap[secondid], false);
    LOG(INFO) << "mds " << secondid << " started on " << ipmap[secondid]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);

    LOG(INFO) << "current case: KillTwoNotInserviceMDSThenRestartTheMDS";
    /******** KillTwoNotInserviceMDSThenRestartTheMDS ***********/
    // 1. 重启两台mds，其中两台都不在服务
    // 2.预期
    //    a. 集群状态正常时：client读写请求可以正常下发
    //    b. 关闭两台mds，预期client IO无影响，挂卸载服务正常
    //    c. 重启这两台mds，client IO无影响
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. 启动后台挂卸载线程，预期挂卸载服务会受影响
    CreateOpenFileBackend();

    // 3. kill两台mds，在启动的时候第一台mds当选leader, kill后二台
    serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    int tempid_1 = (serviceMDSID + 1) % 3;
    int tempid_2 = (serviceMDSID + 2) % 3;
    ASSERT_EQ(0, cluster->StopMDS(tempid_1));
    ASSERT_EQ(0, cluster->StopMDS(tempid_2));

    // 4. 启动后台io监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    不在服务的mds被kill对集群没有影响
    ASSERT_TRUE(MonitorResume(6 * segment_size, 4096, 10));

    // 5. 等待挂卸载监测结束
    WaitBackendCreateDone();

    // 6. 挂卸载服务正常
    ASSERT_FALSE(createOrOpenFailed);

    // 7. 拉起被kill的进程
    pid = cluster->StartSingleMDS(tempid_1, ipmap[tempid_1], 22240 + tempid_1,
                                  configmap[tempid_1], false);
    LOG(INFO) << "mds " << tempid_1 << " started on " << ipmap[tempid_1]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);

    // 8. 集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 9. 拉起其他mds，使集群恢复正常
    pid = cluster->StartSingleMDS(tempid_2, ipmap[tempid_2], 22240 + tempid_2,
                                  configmap[tempid_2], false);
    LOG(INFO) << "mds " << tempid_2 << " started on " << ipmap[tempid_2]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);

    LOG(INFO) << "current case: hangTwoInserviceMDSThenResumeTheMDS";
    /******** hangTwoInserviceMDSThenResumeTheMDS ************/
    // 1. hang两台mds，其中包含一台正在服务的mds，然后恢复
    // 2.预期
    //    a. 集群状态正常时：client读写请求可以正常下发
    //    b. mds hang期间且在与etcd续约超时之前，这时候新写IO会失败，
    //       因为新写触发getorallocate，这个RPC发到mds会出现一直超时，然后重试
    //       最后重试失败。
    //    c. client session续约时长总比mds与etcd之间续约时长大，所以在
    //       session续约失败之前mds预期可以完成切换，所以client的session
    //       不会过期，覆盖写不会出现异常。
    //    d. 恢复被hang的mds，预期对client io无影响
    // 1. hang两台mds，在启动的时候第一台mds当选leader, hang前二台
    serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    tempid_1 = serviceMDSID;
    tempid_2 = (serviceMDSID + 1) % 3;
    ASSERT_EQ(0, cluster->HangMDS(tempid_1));
    ASSERT_EQ(0, cluster->HangMDS(tempid_2));

    // 2. 启动后台挂卸载线程，预期挂卸载服务会受影响
    CreateOpenFileBackend();

    LOG(INFO) << "monitor resume start!";
    // 4. 启动后台io监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    follower mds在session过期后重新续约后集群正常服务（20s续约）
    ret = MonitorResume(7 * segment_size, 4096, 25);
    if (!ret) {
        ASSERT_EQ(0, cluster->RecoverHangMDS(tempid_1));
        ASSERT_EQ(0, cluster->RecoverHangMDS(tempid_2));
        ASSERT_TRUE(false);
    }

    LOG(INFO) << "monitor resume done!";
    // 5. 等待后台挂卸载监测结束
    WaitBackendCreateDone();
    LOG(INFO) << "wait backend create thread done!";

    // 6. 判断当前挂卸载情况
    ASSERT_EQ(0, cluster->RecoverHangMDS(tempid_1));
    ASSERT_EQ(0, cluster->RecoverHangMDS(tempid_2));
    ASSERT_EQ(0, cluster->StopMDS(tempid_1));
    ASSERT_EQ(0, cluster->StopMDS(tempid_2));
    pid = cluster->StartSingleMDS(tempid_1, ipmap[tempid_1], 22240 + tempid_1,
                                  configmap[tempid_1], false);
    LOG(INFO) << "mds " << tempid_1 << " started on " << ipmap[tempid_1]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);

    pid = cluster->StartSingleMDS(tempid_2, ipmap[tempid_2], 22240 + tempid_2,
                                  configmap[tempid_2], false);
    LOG(INFO) << "mds " << tempid_2 << " started on " << ipmap[tempid_2]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);
    ASSERT_TRUE(createOrOpenFailed);

    // 7. 再拉起被hang的mds，对集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: hangTwoNotInserviceMDSThenResumeTheMDS";
    /********** hangTwoNotInserviceMDSThenResumeTheMDS ********/
    // 1. hang两台mds，其中不包含正在服务的mds，然后恢复
    // 2.预期
    //    a. 集群状态正常时：client读写请求可以正常下发
    //    b. hang一台不在服务的mds，预期client IO无影响，挂卸载服务正常
    //    c. 恢复这两台mds，client IO无影响
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. hang两台mds，在启动的时候第一台mds当选leader, kill后二台
    serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    tempid_1 = (serviceMDSID + 1) % 3;
    tempid_2 = (serviceMDSID + 2) % 3;
    ASSERT_EQ(0, cluster->HangMDS(tempid_1));
    ASSERT_EQ(0, cluster->HangMDS(tempid_2));

    // 3. 启动后台挂卸载线程，预期挂卸载服务会受影响
    CreateOpenFileBackend();

    // 4. 启动后台io监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    不在服务的mds被kill对集群没有影响
    ret = MonitorResume(8 * segment_size, 4096, 10);
    if (!ret) {
        ASSERT_EQ(0, cluster->RecoverHangMDS(tempid_1));
        ASSERT_EQ(0, cluster->RecoverHangMDS(tempid_2));
        ASSERT_TRUE(false);
    }

    // 5. 等待挂卸载监测结束
    WaitBackendCreateDone();

    // 6. 挂卸载服务正常
    ASSERT_EQ(0, cluster->RecoverHangMDS(tempid_1));
    ASSERT_EQ(0, cluster->RecoverHangMDS(tempid_2));
    ASSERT_EQ(0, cluster->StopMDS(tempid_1));
    ASSERT_EQ(0, cluster->StopMDS(tempid_2));
    pid = cluster->StartSingleMDS(tempid_1, ipmap[tempid_1], 22240 + tempid_1,
                                  configmap[tempid_1], false);
    LOG(INFO) << "mds " << tempid_1 << " started on " << ipmap[tempid_1]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);

    pid = cluster->StartSingleMDS(tempid_2, ipmap[tempid_2], 22240 + tempid_2,
                                  configmap[tempid_2], false);
    LOG(INFO) << "mds " << tempid_2 << " started on " << ipmap[tempid_2]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);
    ASSERT_FALSE(createOrOpenFailed);

    // 7. 集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: KillThreeMDSThenRestartTheMDS";
    /********* KillThreeMDSThenRestartTheMDS *********/
    // 1. 重启三台mds
    // 2.预期
    //    a. 集群状态正常：client读写请求可以正常下发
    //    b. kill三台mds：client 在session过期之后出现IO 失败
    //    c. client session过期之前这段时间的新写会失败，覆盖写不影响
    //    d. 恢复其中hang的一台mds：client session重新续约成功，io恢复正常
    //    e. 恢复另外两台hang的mds，client io无影响

    // 1. kill三台mds
    ASSERT_EQ(0, cluster->StopAllMDS());
    // 确保mds确实退出了
    std::this_thread::sleep_for(std::chrono::seconds(10));

    // 2. 启动后台挂卸载线程，预期挂卸载服务会受影响
    CreateOpenFileBackend();

    // 3. 下发一个io，sleep一段时间后判断是否返回
    //    由于从下一个segment开始写，使其触发getorallocate逻辑
    //    MDS全部不在服务，写请求一直hang，无法返回
    ASSERT_TRUE(SendAioWriteRequest(9 * segment_size, 4096));
    std::this_thread::sleep_for(std::chrono::seconds(30));
    ASSERT_FALSE(writeIOReturnFlag);

    // 4. 等待后台挂卸载监测结束
    WaitBackendCreateDone();

    // 5. 判断当前挂卸载情况
    ASSERT_TRUE(createOrOpenFailed);

    // 6. 拉起被kill的进程
    pid = -1;
    while (pid < 0) {
        pid =
            cluster->StartSingleMDS(0, "127.0.0.1:22222", 22240, mdsConf, true);
        LOG(INFO) << "mds 0 started on 127.0.0.1:22222, pid = " << pid;
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }

    // 7. 检测上次IO是否返回
    std::this_thread::sleep_for(std::chrono::seconds(20));
    ASSERT_TRUE(writeIOReturnFlag);

    // 8. 新的mds开始提供服务
    ASSERT_TRUE(MonitorResume(segment_size, 4096, 10));

    // 9. 再拉起被kill的进程
    pid = cluster->StartSingleMDS(1, "127.0.0.1:22223", 22229, mdsConf, false);
    LOG(INFO) << "mds 1 started on 127.0.0.1:22223, pid = " << pid;
    ASSERT_GT(pid, 0);

    // 10. 对集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 11. 拉起其他被kill的mds
    pid = cluster->StartSingleMDS(2, "127.0.0.1:22224", 22232, mdsConf, false);
    LOG(INFO) << "mds 2 started on 127.0.0.1:22224, pid = " << pid;
    ASSERT_GT(pid, 0);

    LOG(INFO) << "current case: hangThreeMDSThenResumeTheMDS";
    /********** hangThreeMDSThenResumeTheMDS **************/
    // 1. hang三台mds，然后恢复
    // 2.预期
    //    a. 集群状态正常：client读写请求可以正常下发
    //    b. hang三台mds：client 在session过期之后出现IO hang
    //    c. client session过期之前这段时间的新写会一直hang，覆盖写不影响
    //    e. 恢复其中hang的一台mds：client session重新续约成功，io恢复正常
    //    f. 恢复另外两台hang的mds，client io无影响
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. hang三台mds
    ASSERT_EQ(0, cluster->HangMDS(0));
    ASSERT_EQ(0, cluster->HangMDS(1));
    ASSERT_EQ(0, cluster->HangMDS(2));

    // 3. 启动后台挂卸载线程，预期挂卸载服务会受影响
    CreateOpenFileBackend();

    // 4. 下发一个io，sleep一段时间后判断是否返回
    //    由于从下一个segment开始写，使其触发getorallocate逻辑
    //    MDS全部不在服务，写请求一直hang，无法返回
    ASSERT_TRUE(SendAioWriteRequest(10 * segment_size, 4096));
    std::this_thread::sleep_for(std::chrono::seconds(3));
    ret = writeIOReturnFlag;
    if (ret) {
        ASSERT_EQ(0, cluster->RecoverHangMDS(2));
        ASSERT_EQ(0, cluster->RecoverHangMDS(1));
        ASSERT_EQ(0, cluster->RecoverHangMDS(0));
        ASSERT_TRUE(false);
    }

    // 5. 等待监测结束
    WaitBackendCreateDone();

    // 6. 判断当前挂卸载情况
    if (!createOrOpenFailed) {
        ASSERT_EQ(0, cluster->RecoverHangMDS(2));
        ASSERT_EQ(0, cluster->RecoverHangMDS(1));
        ASSERT_EQ(0, cluster->RecoverHangMDS(0));
        ASSERT_TRUE(false);
    }

    // 7. 拉起被hang的进程, 有可能hang的进程因为长时间未与etcd握手，
    //    导致其被拉起后就退出了，所以这里在recover之后再启动该mds，
    //    这样保证集群中至少有一个mds在提供服务
    ASSERT_EQ(0, cluster->RecoverHangMDS(1));
    ASSERT_EQ(0, cluster->StopMDS(1));

    pid = -1;
    while (pid < 0) {
        pid =
            cluster->StartSingleMDS(1, "127.0.0.1:22223", 22229, mdsConf, true);
        LOG(INFO) << "mds 1 started on 127.0.0.1:22223, pid = " << pid;
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }

    // 检测上次IO是否返回
    std::this_thread::sleep_for(std::chrono::seconds(20));
    ASSERT_TRUE(writeIOReturnFlag);

    // 8. 新的mds开始提供服务
    ret = MonitorResume(segment_size, 4096, 1);
    if (!ret) {
        ASSERT_EQ(0, cluster->RecoverHangMDS(2));
        ASSERT_EQ(0, cluster->RecoverHangMDS(0));
        ASSERT_TRUE(false);
    }

    // 9. 再拉起被hang的进程
    ASSERT_EQ(0, cluster->RecoverHangMDS(2));
    ASSERT_EQ(0, cluster->RecoverHangMDS(0));

    // 10. 对集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));
}

TEST_F(MDSModuleException, StripeMDSExceptionTest) {
    LOG(INFO) << "current case: StripeMDSExceptionTest";
    // 1. 创建一个条带的卷
    int stripefd = curve::test::FileCommonOperation::Open("/test2",
                                       "curve", 1024 * 1024, 8);
    ASSERT_NE(stripefd, -1);
    uint64_t offset = std::rand() % 5 * segment_size;

    // 2. 进行数据的读写校验
    VerifyDataConsistency(stripefd, offset, 128 *1024 *1024);
    std::this_thread::sleep_for(std::chrono::seconds(60));
    // 3. kill 一台当前为leader的mds
    LOG(INFO) << "stop mds.";
    int serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    ASSERT_EQ(0, cluster->StopMDS(serviceMDSID));
    // 4. 启动后台挂卸载线程
    CreateOpenFileBackend();

    // 5. 继续随机写数据进行校验
    offset = std::rand() % 5 * segment_size;
    LOG(INFO) << "when stop mds, write and read data.";
    VerifyDataConsistency(stripefd, offset, 128 *1024 *1024);

    // 6. 等待挂卸载检测结果
    WaitBackendCreateDone();

    // 7. 挂卸载服务正常
    ASSERT_TRUE(createOrOpenFailed);

    LOG(INFO) <<"start mds.";
    pid_t pid = cluster->StartSingleMDS(serviceMDSID, ipmap[serviceMDSID],
                                        22240 + serviceMDSID,
                                        configmap[serviceMDSID], false);
    LOG(INFO) << "mds " << serviceMDSID << " started on " << ipmap[serviceMDSID]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);


    LOG(INFO) << "start mds, write and read data.";
    offset = std::rand() % 5 * segment_size;
    VerifyDataConsistency(stripefd, offset, 128 *1024 *1024);

    ::Close(stripefd);
}
