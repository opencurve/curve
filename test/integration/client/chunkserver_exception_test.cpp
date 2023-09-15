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
    { "--confPath=./conf/mds.conf" },
    { "--log_dir=./runlog/ChunkserverException" },
    { "--mdsDbName=module_exception_curve_chunkserver" },
    { "--sessionInterSec=20" },
    { "--etcdAddr=127.0.0.1:22233" },
    { "--updateToRepoSec=5" },
};

const std::vector<std::string> chunkserverConf4{
    { "-chunkServerStoreUri=local://./moduleException4/" },
    { "-chunkServerMetaUri=local://./moduleException4/chunkserver.dat" },
    { "-copySetUri=local://./moduleException4/copysets" },
    { "-raftSnapshotUri=curve://./moduleException4/copysets" },
    { "-raftLogUri=curve://./moduleException4/copysets" },
    { "-recycleUri=local://./moduleException4/recycler" },
    { "-chunkFilePoolDir=./moduleException4/chunkfilepool/" },
    { "-chunkFilePoolMetaPath=./moduleException4/chunkfilepool.meta" },
    { "-conf=./conf/chunkserver.conf.example" },
    { "-raft_sync_segments=true" },
    { "--log_dir=./runlog/ChunkserverException" },
    { "-chunkServerIp=127.0.0.1" },
    { "-chunkServerPort=22125" },
    { "-enableChunkfilepool=false" },
    { "-mdsListenAddr=127.0.0.1:22122,127.0.0.1:22123,127.0.0.1:22124" },
    { "-enableWalfilepool=false" },
    { "-walFilePoolDir=./moduleException4/walfilepool/" },
    { "-walFilePoolMetaPath=./moduleException4/walfilepool.meta" }
};

const std::vector<std::string> chunkserverConf5{
    { "-chunkServerStoreUri=local://./moduleException5/" },
    { "-chunkServerMetaUri=local://./moduleException5/chunkserver.dat" },
    { "-copySetUri=local://./moduleException5/copysets" },
    { "-raftSnapshotUri=curve://./moduleException5/copysets" },
    { "-raftLogUri=curve://./moduleException5/copysets" },
    { "-recycleUri=local://./moduleException5/recycler" },
    { "-chunkFilePoolDir=./moduleException5/chunkfilepool/" },
    { "-chunkFilePoolMetaPath=./moduleException5/chunkfilepool.meta" },
    { "-conf=./conf/chunkserver.conf.example" },
    { "-raft_sync_segments=true" },
    { "--log_dir=./runlog/ChunkserverException" },
    { "-chunkServerIp=127.0.0.1" },
    { "-chunkServerPort=22126" },
    { "-enableChunkfilepool=false" },
    { "-mdsListenAddr=127.0.0.1:22122,127.0.0.1:22123,127.0.0.1:22124" },
    { "-enableWalfilepool=false" },
    { "-walFilePoolDir=./moduleException5/walfilepool/" },
    { "-walFilePoolMetaPath=./moduleException5/walfilepool.meta" }
};

const std::vector<std::string> chunkserverConf6{
    { "-chunkServerStoreUri=local://./moduleException6/" },
    { "-chunkServerMetaUri=local://./moduleException6/chunkserver.dat" },
    { "-copySetUri=local://./moduleException6/copysets" },
    { "-raftSnapshotUri=curve://./moduleException6/copysets" },
    { "-raftLogUri=curve://./moduleException6/copysets" },
    { "-recycleUri=local://./moduleException6/recycler" },
    { "-chunkFilePoolDir=./moduleException6/chunkfilepool/" },
    { "-chunkFilePoolMetaPath=./moduleException6/chunkfilepool.meta" },
    { "-conf=./conf/chunkserver.conf.example" },
    { "-raft_sync_segments=true" },
    { "--log_dir=./runlog/ChunkserverException" },
    { "-chunkServerIp=127.0.0.1" },
    { "-chunkServerPort=22127" },
    { "-enableChunkfilepool=false" },
    { "-mdsListenAddr=127.0.0.1:22122,127.0.0.1:22123,127.0.0.1:22124" },
    { "-enableWalfilepool=false" },
    { "-walFilePoolDir=./moduleException6/walfilepool/" },
    { "-walFilePoolMetaPath=./moduleException6/walfilepool.meta" }
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

        // 1. Start etcd
        pid_t pid = cluster->StartSingleEtcd(
            1, "127.0.0.1:22233", "127.0.0.1:22234",
            std::vector<std::string>{
                "--name=module_exception_test_chunkserver" });
        LOG(INFO) << "etcd 1 started on 127.0.0.1:22233:22234, pid = " << pid;
        ASSERT_GT(pid, 0);

        // 2. Start one mds first, make it a leader, and then start the other two mds nodes
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

        // 3. Creating a physical pool
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

        // 4. Create chunkserver
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
        // 5. Create a logical pool and sleep for a period of time to let the underlying copyset select the primary first
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

        // 6. Initialize client configuration
        ret = Init(confPath.c_str());
        ASSERT_EQ(ret, 0);

        // 7. Create a file
        fd = curve::test::FileCommonOperation::Open("/test1", "curve");
        ASSERT_NE(fd, -1);

        // 8. Sleep for 10 seconds first and let chunkserver select the leader
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }

    void TearDown() {
        ::Close(fd);
        UnInit();
        ASSERT_EQ(0, cluster->StopCluster());
        delete cluster;
        system(
            "rm -rf moduleException6 moduleException4 moduleException5 "
            "module_exception_test_chunkserver.etcd");
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
     * Monitor whether client io can be issued normally within the expected time
     * @param: off is the offset that currently requires issuing IO
     * @param: size is the size of the distributed io
     * @param: predictTimeS is the expected number of seconds in which IO can be restored
     * @param[out]: failCount is the number of error returns in the current io distribution
     * @return: If io can be issued normally within the expected time, return true; otherwise, return false
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

        // Wake up IO thread
        iothread.join();
        inflightContl.WaitInflightAllComeBack();

        delete[] writebuf;
        return ret;
    }

    int fd;

    // Is there a failure to hang and uninstall
    bool createOrOpenFailed;
    bool createDone;
    std::mutex createMtx;
    std::condition_variable createCV;

    CurveCluster* cluster;
};

//Test environment topology: Start one client, three chunkservers, three mds, and one etcd on a single node

TEST_F(CSModuleException, ChunkserverException) {
    LOG(INFO) << "current case: KillOneChunkserverThenRestartTheChunkserver";
    /********* KillOneChunkserverThenRestartTheChunkserver **********/
    // 1.. Test restarting a chunkserver
    // 2.. Expected:
    //    a. When the cluster status is normal: client read and write requests can be issued normally
    //    b. Kill a chunkserver: The client's read and write requests are stuck at most
    //       election_timeout*2s can read and write normally
    //    c. Restoring chunkserver: Client read and write requests have no impact
    // 1. The initial state of the cluster, IO is issued normally
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. Kill a chunkserver
    ASSERT_EQ(0, cluster->StopChunkServer(1));

    // 3. After killing a chunkserver, the client's IO is expected to recover at most 2 * electtime
    ASSERT_TRUE(MonitorResume(0, 4096, 2));

    // 4. Pull up the chunkserver that was just killed
    pid_t pid =
        cluster->StartSingleChunkServer(1, "127.0.0.1:22125", chunkserverConf4);
    LOG(INFO) << "chunkserver 1 started on 127.0.0.1:22125, pid = " << pid;
    ASSERT_GT(pid, 0);

    // 5. Pulling back has no impact on client IO
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: HangOneChunkserverThenResumeTheChunkserver";
    /********* HangOneChunkserverThenResumeTheChunkserver ***********/
    // 1. Hang a chunk server, and then restore the hang's chunk server
    // 2.. Expectations
    //      a. When the cluster status is normal: client read and write requests can be issued normally
    //      b. Hang a chunkserver: client
    //         Read and write requests may experience a maximum delay of selection_timeout*2s for normal read and write operations
    //      c. Restoring chunkserver: Client read and write requests have no impact
    // 1. The initial state of the cluster, IO is issued normally
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. Hang a chunkserver
    ASSERT_EQ(0, cluster->HangChunkServer(1));

    // 3. After hanging a chunkserver, the client's IO is expected to recover at most 2 * electtime
    ASSERT_TRUE(MonitorResume(0, 4096, 2));

    // 4. Pull up the chunkserver that was just hung
    ASSERT_EQ(0, cluster->RecoverHangChunkServer(1));

    // 5. Pulling back has no impact on client IO
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: KillTwoChunkserverThenRestartTheChunkserver";
    /******** KillTwoChunkserverThenRestartTheChunkserver *********/
    // 1. Test restarting two chunkservers
    // 2.. Expected:
    //      a. When the cluster status is normal: client read and write requests can be issued normally
    //      b. Kill two chunkservers: expected client IO to continue to hang, new write IO and overwrite write both hang
    //         Pulling up a chunkserver in the kill: client IO is expected to be at most
    //         Restore read and write within (chunkserver starts playback of data+2 * selection_timeout) time
    //      c. Pulling up another kill chunkserver: client IO has no impact
    // 1. The initial state of the cluster, IO is issued normally
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. Kill two chunkservers
    ASSERT_EQ(0, cluster->StopChunkServer(1));
    ASSERT_EQ(0, cluster->StopChunkServer(2));

    // 3. Kill two chunkservers, IO cannot be issued normally
    ASSERT_FALSE(MonitorResume(0, 4096, 30));

    // 4. Pull up the first chunk server that was just killed
    pid =
        cluster->StartSingleChunkServer(1, "127.0.0.1:22125", chunkserverConf4);
    LOG(INFO) << "chunkserver 1 started on 127.0.0.1:22125, pid = " << pid;
    ASSERT_GT(pid, 0);

    // 5. Pull up the first chunk server that was just killed,
    //    The client's IO is expected to recover at most 2 * electtime
    //If slow start is configured, wait
    // (copysetNum / load_concurrency) * election_timeout
    ASSERT_TRUE(MonitorResume(0, 4096, 80));

    // 6. Pull up the second chunk server that was just killed
    pid =
        cluster->StartSingleChunkServer(2, "127.0.0.1:22126", chunkserverConf5);
    LOG(INFO) << "chunkserver 2 started on 127.0.0.1:22126, pid = " << pid;
    ASSERT_GT(pid, 0);

    // 7. Cluster IO is not affected and is distributed normally
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: HangTwoChunkserverThenResumeTheChunkserver";
    /******* HangTwoChunkserverThenResumeTheChunkserver **********/
    // 1. Hang two chunkservers, and then restore Hang's chunkservers
    // 2.. Expectations
    //      a. When the cluster status is normal: client read and write requests can be issued normally
    //      b. Hang two chunkservers: client IO continues to hang, while new write IO and overwrite write both hang
    //      c. Restore one of the chunkservers: client IO restores read and write,
    //          Recovery time from chunkserver to client IO during election_ Timeout * 2
    //      d. Restoring another hang's chunkserver: client IO has no impact
    // 1. The initial state of the cluster, IO is issued normally
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. Hang off two chunkservers
    ASSERT_EQ(0, cluster->HangChunkServer(1));
    ASSERT_EQ(0, cluster->HangChunkServer(2));

    // 3. Hang two chunkservers, IO cannot be issued normally
    ASSERT_FALSE(MonitorResume(0, 4096, 2));

    // 4. Pull up the first chunkserver that was just hung
    ASSERT_EQ(0, cluster->RecoverHangChunkServer(1));

    // 5. Pull up the first chunkserver that was just hung,
    //The client's IO is expected to recover at most 2 * electtime
    //If slow start is configured, wait
    // (copysetNum / load_concurrency) * election_timeout
    ASSERT_TRUE(MonitorResume(0, 4096, 80));

    // 6. Pull up the second chunk server that was just hung
    ASSERT_EQ(0, cluster->RecoverHangChunkServer(2));

    // 7. Cluster IO is not affected and is distributed normally
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: KillThreeChunkserverThenRestartTheChunkserver";
    /******** KillThreeChunkserverThenRestartTheChunkserver ******/
    // 1. Test restarting three chunkservers
    // 2. Expected:
    //      a. When the cluster status is normal: client read and write requests can be issued normally
    //      b. Close three chunkservers: client IO hang
    //      c. Restart a chunkserver: client IO hang
    //      d. Restart the second chunkserver: client IO hang,
    //         Until the chunkserver is fully restored and IO is restored.
    //         The recovery time is approximately equal to (chunkserver starts playback data+2 * election_timeout)
    //      e. Restarting the third chunkserver: No impact on client IO
    // 1. The initial state of the cluster, IO is issued normally
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. Kill three chunkservers
    ASSERT_EQ(0, cluster->StopChunkServer(1));
    ASSERT_EQ(0, cluster->StopChunkServer(2));
    ASSERT_EQ(0, cluster->StopChunkServer(3));

    // 3. Kill three chunkservers, IO cannot be issued normally
    ASSERT_FALSE(MonitorResume(0, 4096, 2));

    // 4. Pull up the first chunk server that was just killed
    pid =
        cluster->StartSingleChunkServer(1, "127.0.0.1:22125", chunkserverConf4);
    LOG(INFO) << "chunkserver 1 started on 127.0.0.1:22125, pid = " << pid;
    ASSERT_GT(pid, 0);

    // 5. Only one chunkserver is working, IO cannot be issued normally
    ASSERT_FALSE(MonitorResume(0, 4096, 80));

    // 6. Pull up the second chunk server that was just killed
    pid =
        cluster->StartSingleChunkServer(2, "127.0.0.1:22126", chunkserverConf5);
    LOG(INFO) << "chunkserver 2 started on 127.0.0.1:22126, pid = " << pid;
    ASSERT_GT(pid, 0);

    // 7. Client's IO recovery
    ASSERT_TRUE(MonitorResume(0, 4096, 80));

    // 8. Pull up other chunkservers that have been killed
    pid =
        cluster->StartSingleChunkServer(3, "127.0.0.1:22127", chunkserverConf6);
    LOG(INFO) << "chunkserver 3 started on 127.0.0.1:22127, pid = " << pid;
    ASSERT_GT(pid, 0);

    LOG(INFO) << "current case: HangThreeChunkserverThenResumeTheChunkserver";
    /******** HangThreeChunkserverThenResumeTheChunkserver **********/
    // 1. Hang three chunkservers, and then restore Hang's chunkservers
    // 2. Expectations
    //      a. When the cluster status is normal: client read and write requests can be issued normally
    //      b. Hang three chunkservers: client IO hang
    //      c. Restore a chunkserver: client IO hang
    //      d. Restore another chunkserver: expected to be
    //          election_ About timeout * 2, client IO recovery
    //      e. Restore the last chunkserver: Expected no impact on client IO
    // 1. The initial state of the cluster, IO is issued normally
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. Hang down three chunkservers
    ASSERT_EQ(0, cluster->HangChunkServer(1));
    ASSERT_EQ(0, cluster->HangChunkServer(2));
    ASSERT_EQ(0, cluster->HangChunkServer(3));

    // 3. Hang three chunkservers, IO cannot be distributed normally
    ASSERT_FALSE(MonitorResume(0, 4096, 30));

    // 4. Pull up the first chunk server that was just hung
    ASSERT_EQ(0, cluster->RecoverHangChunkServer(1));

    // 5. Only one chunkserver is working, IO cannot be issued normally
    ASSERT_FALSE(MonitorResume(0, 4096, 80));

    // 6. Pull up the second chunk server that was just hung
    ASSERT_EQ(0, cluster->RecoverHangChunkServer(2));
    ASSERT_EQ(0, cluster->RecoverHangChunkServer(3));

    // 7. The client's IO is expected to recover within a maximum of 2 * electtime seconds
    // If slow start is configured, wait
    // (copysetNum / load_concurrency) * election_timeout
    ASSERT_TRUE(MonitorResume(0, 4096, 80));
}
