/*
 * Project: curve
 * File Created: Monday, 2nd September 2019 1:36:34 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2019 netease
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <map>
#include <cmath>
#include <mutex>    // NOLINT
#include <thread>    // NOLINT
#include <string>
#include <numeric>
#include <algorithm>
#include <condition_variable>    // NOLINT

#include "src/common/timeutility.h"
#include "include/client/libcurve.h"
#include "src/client/inflight_controller.h"
#include "test/integration/client/common/file_operation.h"
#include "test/integration/cluster_common/cluster.h"

bool resumeFlag = false;
uint64_t ioFailedCount = 0;
std::mutex resumeMtx;
std::condition_variable resumeCV;
curve::client::InflightControl inflightContl;

using curve::CurveCluster;
const std::vector<std::string> mdsConf1{
    {" --graceful_quit_on_sigterm"},
    {" --confPath=./test/integration/client/config/mds.conf.0"},
};

const std::vector<std::string> mdsConf2{
    {" --graceful_quit_on_sigterm"},
    {" --confPath=./test/integration/client/config/mds.conf.1"},
};

const std::vector<std::string> mdsConf3{
    {" --graceful_quit_on_sigterm"},
    {" --confPath=./test/integration/client/config/mds.conf.2"},
};

const std::vector<std::string> chunkserverConf1{
    {" --graceful_quit_on_sigterm"},
    {" -chunkServerStoreUri=local://./moduleException1/"},
    {" -chunkServerMetaUri=local://./moduleException1/chunkserver.dat"},
    {" -copySetUri=local://./moduleException1/copysets"},
    {" -recycleUri=local://./moduleException1/recycler"},
    {" -chunkFilePoolDir=./moduleException1/chunkfilepool/"},
    {" -chunkFilePoolMetaPath=./moduleException1/chunkfilepool.meta"},
    {" -conf=./test/integration/client/config/chunkserver.conf.0"},
    {" -raft_sync_segments=true"},
};

const std::vector<std::string> chunkserverConf2{
    {" --graceful_quit_on_sigterm"},
    {" -chunkServerStoreUri=local://./moduleException2/"},
    {" -chunkServerMetaUri=local://./moduleException2/chunkserver.dat"},
    {" -copySetUri=local://./moduleException2/copysets"},
    {" -recycleUri=local://./moduleException2/recycler"},
    {" -chunkFilePoolDir=./moduleException2/chunkfilepool/"},
    {" -chunkFilePoolMetaPath=./moduleException2/chunkfilepool.meta"},
    {" -conf=./test/integration/client/config/chunkserver.conf.1"},
    {" -raft_sync_segments=true"},
};

const std::vector<std::string> chunkserverConf3{
    {" --graceful_quit_on_sigterm"},
    {" -chunkServerStoreUri=local://./moduleException3/"},
    {" -chunkServerMetaUri=local://./moduleException3/chunkserver.dat"},
    {" -copySetUri=local://./moduleException3/copysets"},
    {" -recycleUri=local://./moduleException3/recycler"},
    {" -chunkFilePoolDir=./moduleException3/chunkfilepool/"},
    {" -chunkFilePoolMetaPath=./moduleException3/chunkfilepool.meta"},
    {" -conf=./test/integration/client/config/chunkserver.conf.2"},
    {" -raft_sync_segments=true"},
};

class MDSModuleException : public ::testing::Test {
 public:
    void SetUp() {
        // sleep一段时间，防止timewait导致的server启动不了
        std::this_thread::sleep_for(std::chrono::seconds(40));

        system("mkdir /var/log/mds_1 /var/log/mds_2 /var/log/mds_3");
        system("mkdir /var/log/chunkserver_1 /var/log/chunkserver_2 /var/log/chunkserver_3");   // NOLINT

        cluster = new CurveCluster();
        ASSERT_NE(nullptr, cluster);

        // 0. 初始化db
        cluster->InitDB("module_exception_curve_mds");

        // 1. 启动etcd
        cluster->StarSingleEtcd(1, "127.0.0.1:22230", "127.0.0.1:22231",
        std::vector<std::string>{" --name module_exception_test_mds > /var/log/etcd.log 2>&1"});    // NOLINT

        // 2. 先启动一个mds，让其成为leader，然后再启动另外两个mds节点
        cluster->StartSingleMDS(1, "127.0.0.1:22222", mdsConf1, false);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        cluster->StartSingleMDS(2, "127.0.0.1:22223", mdsConf2, false);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        cluster->StartSingleMDS(3, "127.0.0.1:22224", mdsConf3, false);
        std::this_thread::sleep_for(std::chrono::seconds(2));

        // 3. 创建物理池
        std::string createPPCmd = std::string("./bazel-bin/tools/curvefsTool")
        + std::string(" -cluster_map=./test/integration/client/config/topo_example.txt")    //  NOLINT
        + std::string(" -mds_addr=127.0.0.1:22222,127.0.0.1:22223,127.0.0.1:22224")     //  NOLINT
        + std::string(" -op=create_physicalpool")
        + std::string(" -stderrthreshold=0")
        + std::string(" -minloglevel=0");

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
        cluster->StartSingleChunkServer(1, "127.0.0.1:22225", chunkserverConf1);
        cluster->StartSingleChunkServer(2, "127.0.0.1:22226", chunkserverConf2);
        cluster->StartSingleChunkServer(3, "127.0.0.1:22227", chunkserverConf3);

        std::this_thread::sleep_for(std::chrono::seconds(5));

        // 5. 创建逻辑池, 并睡眠一段时间让底层copyset先选主
        std::string createLPCmd = std::string("./bazel-bin/tools/curvefsTool")
        + std::string(" -cluster_map=./test/integration/client/config/topo_example.txt")    //  NOLINT
        + std::string(" -mds_addr=127.0.0.1:22222,127.0.0.1:22223,127.0.0.1:22224")     //  NOLINT
        + std::string(" -copyset_num=300")
        + std::string(" -op=create_logicalpool")
        + std::string(" -physicalpool_name=pool1")
        + std::string(" -stderrthreshold=0 -minloglevel=0");

        ret = 0;
        retry = 0;
        while (retry < 5) {
            ret = system(createLPCmd.c_str());
            if (ret == 0) break;
            retry++;
        }
        ASSERT_EQ(ret, 0);

        // 6. 初始化client配置
        ret = Init("./test/integration/client/config/client.conf");
        ASSERT_EQ(ret, 0);

        // 7. 创建一个文件
        fd = curve::test::FileCommonOperation::Open("/test1", "curve");
        ASSERT_NE(fd, -1);

        // 8. 先睡眠10s，让chunkserver选出leader
        std::this_thread::sleep_for(std::chrono::seconds(5));

        ipmap[1] = "127.0.0.1:22222";
        ipmap[2] = "127.0.0.1:22223";
        ipmap[3] = "127.0.0.1:22224";

        configmap[1] = mdsConf1;
        configmap[2] = mdsConf2;
        configmap[3] = mdsConf3;
    }

    void TearDown() {
        ::Close(fd);
        UnInit();

        system("rm -rf module_exception_test_mds.etcd");
        system("rm -rf moduleException1 moduleException2 moduleException3");

        cluster->mdsRepo_->dropDataBase();
        cluster->StopEtcd(1);
        cluster->StopCluster();
        delete cluster;
    }

    void CreateOpenFileBackend() {
        createDone = false;
        createOrOpenFailed = false;
        auto func = [&]() {
            for (int i = 0; i < 20; i++) {
                std::string filename = "/" + std::to_string(i);
                int ret = curve::test::FileCommonOperation::Open(filename, "curve");    //  NOLINT
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
        createCV.wait(lk, [&](){ return createDone;});
    }

    /**
     * 监测client io能否在预期时间内正常下发
     * @param: off是当前需要下发IO的偏移
     * @param: size是下发io的大小
     * @param: predictTimeS是预期在多少秒内IO可以恢复
     * @param[out]: failCount为当前io下发中错误返回的数量
     * @return: 如果io在预期时间内嫩够正常下发，则返true，否则返回false
     */
    bool MonitorResume(uint64_t off,
                       uint64_t size,
                       uint64_t predictTimeS) {
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
            std::this_thread::sleep_for(
            std::chrono::seconds(predictTimeS));
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
            resumeCV.wait_for(lk, std::chrono::seconds(2*predictTimeS + 10));
            ret = resumeFlag;
        }

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

    std::map<int, std::string> ipmap;
    std::map<int, std::vector<std::string>> configmap;
};

#define segment_size 1*1024*1024*1024ul
// 测试环境拓扑：在单节点上启动一个client、三个chunkserver、三个mds、一个etcd

// 1. 重启一台正在服务的mds
// 2.预期
//    a. 集群状态正常时：client读写请求可以正常下发
//    b. 关闭一台mds，在mds服务切换到另一台mds之前，
//       client 新写IO会hang，挂卸载服务会异常
//    c. mds服务切换后，预期client IO无影响，挂卸载服务正常
//    d. 重新拉起mds，client IO无影响
TEST_F(MDSModuleException, KillOneInserviceMDSThenRestartTheMDS) {
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. 启动后台挂卸载线程，预期挂卸载会出现失败
    CreateOpenFileBackend();

    // 3. kill一台正在服务的mds，在启动的时候第一台mds当选leader
    int serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    cluster->StopMDS(serviceMDSID);

    LOG(INFO) << "start monitor resume!";
    // 4. 启动后台io监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    follower mds在session过期后重新续约后集群正常服务（20s续约）
    ASSERT_TRUE(MonitorResume(segment_size, 4096, 20));

    LOG(INFO) << "monitor resume done!";

    // 5. 等待后台挂卸载监测结束
    WaitBackendCreateDone();

    // 6. 判断当前挂卸载情况
    ASSERT_TRUE(createOrOpenFailed);

    LOG(INFO) << "start single mds!";
    // 7. 拉起被kill的进程
    cluster->StartSingleMDS(serviceMDSID, ipmap[serviceMDSID],
                            configmap[serviceMDSID], false);
    LOG(INFO) << "start single mds done!";

    // 8. 再拉起被kill的mds，对集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));
    LOG(INFO) << "monitor resume 1 seconds done!";
}

// 1. 重启一台不在服务的mds
// 2.预期
//    a. 集群状态正常时：client读写请求可以正常下发
//    b. 关闭一台不在服务的mds，预期client IO无影响，挂卸载服务正常
TEST_F(MDSModuleException, KillOneNotInserviceMDSThenRestartTheMDS) {
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. 启动后台挂卸载线程，预期挂卸载服务不会受影响
    CreateOpenFileBackend();

    // 3. kill一台不在服务的mds，在启动的时候第一台mds当选leader, kill第二台
    int serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    int killid = serviceMDSID%3 + 1;
    cluster->StopMDS(killid);

    // 4. 启动后台io监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    follower mds在session过期后重新续约后集群正常服务（20s续约）
    ASSERT_TRUE(MonitorResume(segment_size, 4096, 20));

    // 5. 等待挂卸载监测结束
    WaitBackendCreateDone();

    // 6. 挂卸载服务正常
    ASSERT_FALSE(createOrOpenFailed);

    // 7. 拉起被kill的进程
    cluster->StartSingleMDS(killid, ipmap[killid],
                            configmap[killid], false);

    // 8. 再拉起被kill的mds，对集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));
}

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
TEST_F(MDSModuleException, hangOneInserviceMDSThenResumeTheMDS) {
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. 启动后台挂卸载线程，预期挂卸载会出现失败
    CreateOpenFileBackend();

    // 3. hang一台正在服务的mds，在启动的时候第一台mds当选leader
    int serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    cluster->HangMDS(serviceMDSID);

    // 4. 启动后台io监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    follower mds在session过期后重新续约后集群正常服务（20s续约）
    auto ret = MonitorResume(segment_size, 4096, 25);
    if (!ret) {
        cluster->RecoverHangMDS(serviceMDSID);
        ASSERT_TRUE(false);
    }

    // 5. 等待后台挂卸载监测结束
    WaitBackendCreateDone();

    // 6. 判断当前挂卸载情况
    cluster->RecoverHangMDS(serviceMDSID);
    ASSERT_TRUE(createOrOpenFailed);

    // 7. 再拉起被kill的mds，对集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));
}

// 1. hang一台不在服务的mds
// 2.预期
//    a. 集群状态正常时：client读写请求可以正常下发
//    b. hang一台不在服务的mds，预期client IO无影响，挂卸载服务正常
TEST_F(MDSModuleException, hangOneNotInserviceMDSThenResumeTheMDS) {
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. 启动后台挂卸载线程，预期挂卸载服务不会受影响
    CreateOpenFileBackend();

    // 3. hang一台不在服务的mds，在启动的时候第一台mds当选leader, hang第二台
    int serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    int hangid = serviceMDSID%3 + 1;
    cluster->HangMDS(hangid);

    // 4. 启动后台iops监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    follower mds在session过期后重新续约后集群正常服务（20s续约)
    auto ret = MonitorResume(segment_size, 4096, 10);
    if (!ret) {
        cluster->RecoverHangMDS(hangid);
        ASSERT_TRUE(false);
    }

    // 5. 等待挂卸载监测结束
    WaitBackendCreateDone();

    // 6. 挂卸载服务正常
    cluster->RecoverHangMDS(hangid);
    ASSERT_FALSE(createOrOpenFailed);

    // 7. 集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));
}

// 1. 重启两台mds，其中一台正在服务的mds
// 2.预期
//    a. 集群状态正常时：client读写请求可以正常下发
//    b. 关闭两台mds，在mds服务切换到另一台mds之前，
//       client 新写IO会出现失败，挂卸载服务会异常
//    c. mds服务切换后，预期client IO恢复，挂卸载服务正常
//    d. 重新拉起mds，client IO无影响
TEST_F(MDSModuleException, KillTwoInserviceMDSThenRestartTheMDS) {
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. 启动后台挂卸载线程，预期挂卸载服务会受影响
    CreateOpenFileBackend();

    // 3. kill两台mds，在启动的时候第一台mds当选leader, kill前二台
    int serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    ASSERT_EQ(1, serviceMDSID);
    cluster->StopMDS(serviceMDSID);
    cluster->StopMDS(serviceMDSID+1);

    // 4. 启动后台io监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    follower mds在session过期后重新续约后集群正常服务（20s续约）
    ASSERT_TRUE(MonitorResume(segment_size, 4096, 20));

    // 5. 等待后台挂卸载监测结束
    WaitBackendCreateDone();

    // 6. 判断当前挂卸载情况
    ASSERT_TRUE(createOrOpenFailed);

    // 7. 拉起被kill的进程
    cluster->StartSingleMDS(1, "127.0.0.1:22222", mdsConf1, false);

    // 8. 再拉起被kill的mds，对集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));
}

// 1. 重启两台mds，其中两台都不在服务
// 2.预期
//    a. 集群状态正常时：client读写请求可以正常下发
//    b. 关闭两台mds，预期client IO无影响，挂卸载服务正常
//    c. 重启这两台mds，client IO无影响
TEST_F(MDSModuleException, KillTwoNotInserviceMDSThenRestartTheMDS) {
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. 启动后台挂卸载线程，预期挂卸载服务会受影响
    CreateOpenFileBackend();

    // 3. kill两台mds，在启动的时候第一台mds当选leader, kill后二台
    int serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    ASSERT_EQ(1, serviceMDSID);
    cluster->StopMDS(2);
    cluster->StopMDS(3);

    // 4. 启动后台io监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    不在服务的mds被kill对集群没有影响
    ASSERT_TRUE(MonitorResume(segment_size, 4096, 10));

    // 5. 等待挂卸载监测结束
    WaitBackendCreateDone();

    // 6. 挂卸载服务正常
    ASSERT_FALSE(createOrOpenFailed);

    // 7. 拉起被kill的进程
    cluster->StartSingleMDS(2, "127.0.0.1:22223", mdsConf2, false);

    // 8. 集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));
}

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
TEST_F(MDSModuleException, hangTwoInserviceMDSThenResumeTheMDS) {
    // 1. 启动后台挂卸载线程，预期挂卸载服务会受影响
    CreateOpenFileBackend();

    // 2. hang两台mds，在启动的时候第一台mds当选leader, hang前二台
    int serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    ASSERT_EQ(1, serviceMDSID);
    cluster->HangMDS(serviceMDSID);
    cluster->HangMDS(serviceMDSID+1);

    LOG(INFO) << "monitor resume start!";
    // 4. 启动后台io监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    follower mds在session过期后重新续约后集群正常服务（20s续约）
    auto ret = MonitorResume(segment_size, 4096, 25);
    if (!ret) {
        cluster->RecoverHangMDS(1);
        cluster->RecoverHangMDS(2);
        ASSERT_TRUE(false);
    }

    LOG(INFO) << "monitor resume done!";
    // 5. 等待后台挂卸载监测结束
    WaitBackendCreateDone();
    LOG(INFO) << "wait backend create thread done!";

    // 6. 判断当前挂卸载情况
    cluster->RecoverHangMDS(1);
    cluster->RecoverHangMDS(2);
    ASSERT_TRUE(createOrOpenFailed);

    // 7. 再拉起被hang的mds，对集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));
}

// 1. hang两台mds，其中不包含正在服务的mds，然后恢复
// 2.预期
//    a. 集群状态正常时：client读写请求可以正常下发
//    b. hang一台不在服务的mds，预期client IO无影响，挂卸载服务正常
//    c. 恢复这两台mds，client IO无影响
TEST_F(MDSModuleException, hangTwoNotInserviceMDSThenResumeTheMDS) {
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. 启动后台挂卸载线程，预期挂卸载服务会受影响
    CreateOpenFileBackend();

    // 2. hang两台mds，在启动的时候第一台mds当选leader, kill后二台
    int serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    ASSERT_EQ(1, serviceMDSID);
    cluster->HangMDS(2);
    cluster->HangMDS(3);

    // 4. 启动后台io监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    不在服务的mds被kill对集群没有影响
    auto ret = MonitorResume(segment_size, 4096, 10);
    if (!ret) {
        cluster->RecoverHangMDS(1);
        cluster->RecoverHangMDS(2);
        ASSERT_TRUE(false);
    }

    // 5. 等待挂卸载监测结束
    WaitBackendCreateDone();

    // 6. 挂卸载服务正常
    cluster->RecoverHangMDS(2);
    cluster->RecoverHangMDS(3);
    ASSERT_FALSE(createOrOpenFailed);

    // 7. 集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));
}

// 1. 重启三台mds
// 2.预期
//    a. 集群状态正常：client读写请求可以正常下发
//    b. kill三台mds：client 在session过期之后出现IO 失败
//    c. client session过期之前这段时间的新写会失败，覆盖写不影响
//    d. 恢复其中hang的一台mds：client session重新续约成功，io恢复正常
//    e. 恢复另外两台hang的mds，client io无影响
TEST_F(MDSModuleException, KillThreeMDSThenRestartTheMDS) {
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. 启动后台挂卸载线程，预期挂卸载服务会受影响
    CreateOpenFileBackend();

    // 3. kill三台mds
    cluster->StopMDS(1);
    cluster->StopMDS(2);
    cluster->StopMDS(3);

    // 4. 启动后台io监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    MDS全部不在服务，读写一直异常
    ASSERT_FALSE(MonitorResume(segment_size, 4096, 5));

    // 5. 等待后台挂卸载监测结束
    WaitBackendCreateDone();

    // 6. 判断当前挂卸载情况
    ASSERT_TRUE(createOrOpenFailed);

    // 7. 拉起被kill的进程
    cluster->StartSingleMDS(1, "127.0.0.1:22222", mdsConf1, true);

    // 9. 新的mds开始提供服务
    ASSERT_TRUE(MonitorResume(segment_size, 4096, 10));

    // 10. 再拉起被kill的进程
    cluster->StartSingleMDS(2, "127.0.0.1:22223", mdsConf1, false);

    // 11. 对集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));
}

// 1. hang三台mds，然后恢复
// 2.预期
//    a. 集群状态正常：client读写请求可以正常下发
//    b. hang三台mds：client 在session过期之后出现IO hang
//    c. client session过期之前这段时间的新写会一直hang，覆盖写不影响
//    e. 恢复其中hang的一台mds：client session重新续约成功，io恢复正常
//    f. 恢复另外两台hang的mds，client io无影响
TEST_F(MDSModuleException, hangThreeMDSThenResumeTheMDS) {
    // 1. 集群最初状态，io正常下发
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. 启动后台挂卸载线程，预期挂卸载服务会受影响
    CreateOpenFileBackend();

    // 3. hang三台mds
    cluster->HangMDS(1);
    cluster->HangMDS(2);
    cluster->HangMDS(3);

    // 4. 启动后台io监测, 从下一个segment开始写，使其触发getorallocate逻辑
    //    不在服务的mds被kill对集群没有影响
    auto ret = MonitorResume(segment_size, 4096, 10);
    if (ret) {
        cluster->RecoverHangMDS(3);
        cluster->RecoverHangMDS(2);
        cluster->RecoverHangMDS(1);
        ASSERT_TRUE(false);
    }

    // 5. 等待监测结束
    WaitBackendCreateDone();

    // 6. 判断IO状态，预期抖一段时间
    if (!createOrOpenFailed) {
        cluster->RecoverHangMDS(3);
        cluster->RecoverHangMDS(2);
        cluster->RecoverHangMDS(1);
        ASSERT_TRUE(false);
    }

    // 7. 拉起被hang的进程
    cluster->RecoverHangMDS(2);

    std::this_thread::sleep_for(std::chrono::seconds(10));

    // 8. 新的mds开始提供服务
    ret = MonitorResume(segment_size, 4096, 1);
    if (!ret) {
        cluster->RecoverHangMDS(3);
        cluster->RecoverHangMDS(1);
        ASSERT_TRUE(false);
    }

    // 9. 再拉起被hang的进程
    cluster->RecoverHangMDS(3);
    cluster->RecoverHangMDS(1);

    // 10. 对集群没有影响
    ASSERT_TRUE(MonitorResume(0, 4096, 1));
}
