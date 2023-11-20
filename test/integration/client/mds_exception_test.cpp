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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <condition_variable> // NOLINT
#include <map>
#include <mutex> // NOLINT
#include <numeric>
#include <string>
#include <thread> // NOLINT

#include "include/client/libcurve.h"
#include "src/client/inflight_controller.h"
#include "src/common/timeutility.h"
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
    {"--confPath=./conf/mds.conf"},
    {"--log_dir=./runlog/MDSExceptionTest"},
    {"--mdsDbName=module_exception_curve_mds"},
    {"--sessionInterSec=20"},
    {"--etcdAddr=127.0.0.1:22230"},
};

const std::vector<std::string> chunkserverConf1{
    {"-chunkServerStoreUri=local://./moduleException1/"},
    {"-chunkServerMetaUri=local://./moduleException1/chunkserver.dat"},
    {"-copySetUri=local://./moduleException1/copysets"},
    {"-raftSnapshotUri=curve://./moduleException1/copysets"},
    {"-raftLogUri=curve://./moduleException1/copysets"},
    {"-recycleUri=local://./moduleException1/recycler"},
    {"-chunkFilePoolDir=./moduleException1/chunkfilepool/"},
    {"-chunkFilePoolAllocatedPercent=0"},
    {"-chunkFilePoolMetaPath=./moduleException1/chunkfilepool.meta"},
    {"-conf=./conf/chunkserver.conf.example"},
    {"-raft_sync_segments=true"},
    {"--log_dir=./runlog/MDSExceptionTest"},
    {"--graceful_quit_on_sigterm"},
    {"-chunkServerIp=127.0.0.1"},
    {"-chunkServerPort=22225"},
    {"-enableChunkfilepool=false"},
    {"-mdsListenAddr=127.0.0.1:22222,127.0.0.1:22223,127.0.0.1:22224"},
    {"-enableWalfilepool=false"},
    {"-walFilePoolDir=./moduleException1/walfilepool/"},
    {"-walFilePoolMetaPath=./moduleException1/walfilepool.meta"}};

const std::vector<std::string> chunkserverConf2{
    {"-chunkServerStoreUri=local://./moduleException2/"},
    {"-chunkServerMetaUri=local://./moduleException2/chunkserver.dat"},
    {"-copySetUri=local://./moduleException2/copysets"},
    {"-raftSnapshotUri=curve://./moduleException2/copysets"},
    {"-raftLogUri=curve://./moduleException2/copysets"},
    {"-recycleUri=local://./moduleException2/recycler"},
    {"-chunkFilePoolDir=./moduleException2/chunkfilepool/"},
    {"-chunkFilePoolAllocatedPercent=0"},
    {"-chunkFilePoolMetaPath=./moduleException2/chunkfilepool.meta"},
    {"-conf=./conf/chunkserver.conf.example"},
    {"-raft_sync_segments=true"},
    {"--log_dir=./runlog/MDSExceptionTest"},
    {"--graceful_quit_on_sigterm"},
    {"-chunkServerIp=127.0.0.1"},
    {"-chunkServerPort=22226"},
    {"-enableChunkfilepool=false"},
    {"-mdsListenAddr=127.0.0.1:22222,127.0.0.1:22223,127.0.0.1:22224"},
    {"-enableWalfilepool=false"},
    {"-walFilePoolDir=./moduleException2/walfilepool/"},
    {"-walFilePoolMetaPath=./moduleException2/walfilepool.meta"}};

const std::vector<std::string> chunkserverConf3{
    {"-chunkServerStoreUri=local://./moduleException3/"},
    {"-chunkServerMetaUri=local://./moduleException3/chunkserver.dat"},
    {"-copySetUri=local://./moduleException3/copysets"},
    {"-raftSnapshotUri=curve://./moduleException3/copysets"},
    {"-raftLogUri=curve://./moduleException3/copysets"},
    {"-recycleUri=local://./moduleException3/recycler"},
    {"-chunkFilePoolDir=./moduleException3/chunkfilepool/"},
    {"-chunkFilePoolAllocatedPercent=0"},
    {"-chunkFilePoolMetaPath=./moduleException3/chunkfilepool.meta"},
    {"-conf=./conf/chunkserver.conf.example"},
    {"-raft_sync_segments=true"},
    {"--log_dir=./runlog/MDSExceptionTest"},
    {"--graceful_quit_on_sigterm"},
    {"-chunkServerIp=127.0.0.1"},
    {"-chunkServerPort=22227"},
    {"-enableChunkfilepool=false"},
    {"-mdsListenAddr=127.0.0.1:22222,127.0.0.1:22223,127.0.0.1:22224"},
    {"-enableWalfilepool=false"},
    {"-walFilePoolDir=./moduleException3/walfilepool/"},
    {"-walFilePoolMetaPath=./moduleException3/walfilepool.meta"}};

std::string mdsaddr =                                  // NOLINT
    "127.0.0.1:22222,127.0.0.1:22223,127.0.0.1:22224"; // NOLINT
std::string logpath = "./runlog/MDSExceptionTest";     // NOLINT

const std::vector<std::string> clientConf{
    std::string("mds.listen.addr=") + mdsaddr,
    std::string("global.logPath=") + logpath,
    std::string("chunkserver.rpcTimeoutMS=1000"),
    std::string("chunkserver.opMaxRetry=10"),
};

class MDSModuleException : public ::testing::Test
{
public:
    void SetUp()
    {
        std::string confPath = "./test/integration/client/config/client.conf";
        system("mkdir ./runlog/MDSExceptionTest");
        system("rm -rf module_exception_test_mds.etcd");
        system("rm -rf moduleException1 moduleException2 moduleException3");

        cluster = new CurveCluster();
        ASSERT_NE(nullptr, cluster);

        cluster->PrepareConfig<curve::ClientConfigGenerator>(confPath,
                                                             clientConf);

        // 1. Start etcd
        pid_t pid = cluster->StartSingleEtcd(
            1, "127.0.0.1:22230", "127.0.0.1:22231",
            std::vector<std::string>{"--name=module_exception_test_mds"});
        LOG(INFO) << "etcd 1 started on 127.0.0.1:22230:22231, pid = " << pid;
        ASSERT_GT(pid, 0);

        // 2. Start one mds first, make it a leader, and then start the other
        // two mds nodes
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

        // 3. Creating a physical pool
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
        while (retry < 5)
        {
            ret = system(createPPCmd.c_str());
            if (ret == 0)
                break;
            retry++;
        }
        ASSERT_EQ(ret, 0);

        // 4. Create chunkserver
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

        // 5. Create a logical pool and sleep for a period of time to let the
        // underlying copyset select the primary first
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
        while (retry < 5)
        {
            ret = system(createLPCmd.c_str());
            if (ret == 0)
                break;
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
        std::this_thread::sleep_for(std::chrono::seconds(5));

        ipmap[0] = "127.0.0.1:22222";
        ipmap[1] = "127.0.0.1:22223";
        ipmap[2] = "127.0.0.1:22224";

        configmap[0] = mdsConf;
        configmap[1] = mdsConf;
        configmap[2] = mdsConf;
    }

    void TearDown()
    {
        ::Close(fd);
        UnInit();

        ASSERT_EQ(0, cluster->StopCluster());
        delete cluster;
        system("rm -rf module_exception_test_mds.etcd");
        system("rm -rf moduleException1 moduleException2 moduleException3");
    }

    void CreateOpenFileBackend()
    {
        createDone = false;
        createOrOpenFailed = false;
        auto func = [&]()
        {
            static int num = 0;
            for (int i = num; i < num + 20; i++)
            {
                std::string filename = "/" + std::to_string(i);
                LOG(INFO) << "now create file: " << filename;
                int ret =
                    curve::test::FileCommonOperation::Open(filename, "curve");
                ret == -1 ? createOrOpenFailed = true : 0;

                if (ret != -1)
                {
                    ::Close(ret);
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                }
                else
                {
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

    void WaitBackendCreateDone()
    {
        std::unique_lock<std::mutex> lk(createMtx);
        createCV.wait(lk, [&]()
                      { return createDone; });
    }

    /**
     * Monitor whether client io can be issued normally within the expected time
     * @param: off is the offset that currently requires issuing IO
     * @param: size is the size of the distributed io
     * @param: predictTimeS is the expected number of seconds in which IO can be
     * restored
     * @param[out]: failCount is the number of error returns in the current io
     * distribution
     * @return: If the io is issued normally within the expected time, return
     * true; otherwise, return false
     */
    bool MonitorResume(uint64_t off, uint64_t size, uint64_t predictTimeS)
    {
        inflightContl.SetMaxInflightNum(16);
        resumeFlag = false;
        ioFailedCount = 0;

        auto wcb = [](CurveAioContext *context)
        {
            inflightContl.DecremInflightNum();
            if (context->ret == context->length)
            {
                std::unique_lock<std::mutex> lk(resumeMtx);
                resumeFlag = true;
                resumeCV.notify_all();
            }
            else
            {
                ioFailedCount++;
            }
            LOG(INFO) << "end aiowrite with ret = " << context->ret;
            delete context;
        };

        char *writebuf = new char[size];
        memset(writebuf, 'a', size);
        auto iofunc = [&]()
        {
            std::this_thread::sleep_for(std::chrono::seconds(predictTimeS));
            inflightContl.WaitInflightComeBack();

            CurveAioContext *context = new CurveAioContext;
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

        // Wake up IO thread
        iothread.join();
        inflightContl.WaitInflightAllComeBack();

        delete[] writebuf;
        return ret;
    }

    /** Send a write request
     * @param: offset is the offset that currently requires issuing IO
     * @param: size is the size of the issued IO
     * @return: Whether the IO was successfully issued
     */
    bool SendAioWriteRequest(uint64_t offset, uint64_t size)
    {
        writeIOReturnFlag = false;

        auto writeCallBack = [](CurveAioContext *context)
        {
            // Regardless of whether IO is successful or not, as long as it
            // returns, it is set to true
            writeIOReturnFlag = true;
            char *buffer = reinterpret_cast<char *>(context->buf);
            delete[] buffer;
            delete context;
        };

        char *buffer = new char[size];
        memset(buffer, 'a', size);
        CurveAioContext *context = new CurveAioContext();
        context->op = LIBCURVE_OP::LIBCURVE_OP_WRITE;
        context->offset = offset;
        context->length = size;
        context->buf = buffer;
        context->cb = writeCallBack;

        return AioWrite(fd, context) == 0;
    }

    /** Send a write request and read for data validation
     * @param: fd volume fd
     * @param: The offset that currently needs to be issued for IO
     * @param: The size of the distributed IO
     * @return: Whether the data is consistent
     */
    void VerifyDataConsistency(int fd, uint64_t offset, uint64_t size)
    {
        char *writebuf = new char[size];
        char *readbuf = new char[size];
        unsigned int i;

        LOG(INFO) << "VerifyDataConsistency(): offset " << offset << ", size "
                  << size;
        for (i = 0; i < size; i++)
        {
            writebuf[i] = ('a' + std::rand() % 26);
        }

        // Start writing
        auto wcb = [](CurveAioContext *context)
        {
            if (context->ret == context->length)
            {
                testIOWrite = true;
            }
            std::unique_lock<std::mutex> lk(resumeMtx);
            resumeCV.notify_all();
            delete context;
        };

        auto writefunc = [&]()
        {
            CurveAioContext *context = new CurveAioContext;
            ;
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

        // Start reading
        auto rcb = [](CurveAioContext *context)
        {
            if (context->ret == context->length)
            {
                testIORead = true;
            }
            std::unique_lock<std::mutex> lk(resumeMtx);
            resumeCV.notify_all();
            delete context;
        };

        auto readfunc = [&]()
        {
            CurveAioContext *context = new CurveAioContext;
            ;
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

    // Whether mounting or unmounting fails.
    bool createOrOpenFailed;
    bool createDone;
    std::mutex createMtx;
    std::condition_variable createCV;

    CurveCluster *cluster;

    std::map<int, std::string> ipmap;
    std::map<int, std::vector<std::string>> configmap;
};

#define segment_size 1 * 1024 * 1024 * 1024ul
// Test environment topology: Start one client, three chunkservers, three mds,
// and one etcd on a single node

TEST_F(MDSModuleException, MDSExceptionTest)
{
    LOG(INFO) << "current case: KillOneInserviceMDSThenRestartTheMDS";
    /********** KillOneInserviceMDSThenRestartTheMDS *************/
    // 1. Restarting a currently serving MDS.
    // 2. Expectations:
    //    a. When the cluster is in a normal state, client read and write
    //    requests can be issued normally. b. When shutting down an MDS, before
    //    the MDS service switches to another MDS,
    //       new write IO from clients will hang, and mount/unmount services
    //       will behave abnormally.
    //    c. After the MDS service switches, it is expected that client IO will
    //    be unaffected, and mount/unmount services will be normal. d. When
    //    bringing the MDS back up, client IO will be unaffected.
    // 1. In the initial state of the cluster, IO can be issued normally.
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. Kill an MDS that is currently in service, and when it is started, the
    // first MDS is selected as the leader
    int serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    ASSERT_EQ(0, cluster->StopMDS(serviceMDSID));

    // 3. Start the background suspend and unload thread, and expect the suspend
    // and unload to fail
    CreateOpenFileBackend();

    // 4. Start background IO monitoring and start writing from the next segment
    // to trigger the getorallocate logic
    //    follower mds cluster normal service after renewing session expiration
    //    (20s renewal)
    ASSERT_TRUE(MonitorResume(segment_size, 4096, 25));

    // 5. Waiting for the end of backend suspension and uninstallation
    // monitoring
    WaitBackendCreateDone();

    // 6. Determine the current suspension and uninstallation situation
    ASSERT_TRUE(createOrOpenFailed);

    // 7. Pulling up the process of being killed
    pid_t pid = cluster->StartSingleMDS(serviceMDSID, ipmap[serviceMDSID],
                                        22240 + serviceMDSID,
                                        configmap[serviceMDSID], false);
    LOG(INFO) << "mds " << serviceMDSID << " started on " << ipmap[serviceMDSID]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);

    // 8. Pulling up the killed mds again has no impact on the cluster
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: KillOneNotInserviceMDSThenRestartTheMDS";
    /*********** KillOneNotInserviceMDSThenRestartTheMDS *******/
    // 1. Restart an MDS that is not in service
    // 2. Expectations
    //      a. When the cluster status is normal: client read and write requests
    //      can be issued normally b. Turn off an MDS that is not in service,
    //      expect no impact on client IO, and suspend and uninstall the service
    //      normally
    // 1. The initial state of the cluster, IO is issued normally
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. Kill an MDS that is not in service. When starting, the first MDS is
    // selected as the leader, and kill the second MDS
    serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    int killid = (serviceMDSID + 1) % 3;
    ASSERT_EQ(0, cluster->StopMDS(killid));

    // 3. Start the backend suspend and uninstall thread, and it is expected
    // that the suspend and uninstall service will not be affected
    CreateOpenFileBackend();

    // 4. Start background IO monitoring and start writing from the next segment
    // to trigger the getorallocate logic
    //    follower mds cluster normal service after renewing session expiration
    //    (20s renewal)
    ASSERT_TRUE(MonitorResume(2 * segment_size, 4096, 25));

    // 5. Waiting for the end of suspend/unload monitoring
    WaitBackendCreateDone();

    // 6. Hanging and uninstalling service is normal
    ASSERT_FALSE(createOrOpenFailed);

    // 7. Pulling up the process of being killed
    pid = cluster->StartSingleMDS(killid, ipmap[killid], 22240 + killid,
                                  configmap[killid], false);
    LOG(INFO) << "mds " << killid << " started on " << ipmap[killid]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);

    // 8. Pulling up the killed mds again has no impact on the cluster
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: hangOneInserviceMDSThenResumeTheMDS";
    /************ hangOneInserviceMDSThenResumeTheMDS ********/
    // 1. Hang one of the currently serving MDS.
    // 2. Expectations:
    //    a. When the cluster is in a normal state, client read and write
    //    requests can be issued normally. b. During the MDS hang period and
    //    before the lease renewal with etcd times out, new write IO will fail.
    //       This is because a new write triggers getorallocate, and the RPC
    //       sent to the MDS will keep timing out, leading to retries that
    //       eventually fail.
    //    c. The client session renewal duration is longer than the lease
    //    renewal duration between MDS and etcd.
    //       So, MDS is expected to complete the switch before session renewal
    //       failure occurs. Therefore, the client's session will not expire,
    //       and overwrite writes will not result in exceptions.
    //    d. When the hung MDS is restored, it is expected to have no impact on
    //    client IO.
    // 0. First, sleep for a period of time to allow the MDS cluster to elect a
    // leader.
    std::this_thread::sleep_for(std::chrono::seconds(10));

    // 1. The initial state of the cluster, IO is issued normally
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. Hang an MDS that is currently in service, and when it is started, the
    // first MDS is selected as the leader
    serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    ASSERT_EQ(0, cluster->HangMDS(serviceMDSID));

    // 3. Start the background suspend and unload thread, and expect the suspend
    // and unload to fail
    CreateOpenFileBackend();

    // 4. Start background IO monitoring and start writing from the next segment
    // to trigger the getorallocate logic
    //    follower mds cluster normal service after renewing session expiration
    //    (20s renewal)
    auto ret = MonitorResume(3 * segment_size, 4096, 25);
    if (!ret)
    {
        ASSERT_EQ(0, cluster->RecoverHangMDS(serviceMDSID));
        ASSERT_TRUE(false);
    }

    // 5. Waiting for the end of backend suspension and uninstallation
    // monitoring
    WaitBackendCreateDone();

    // 6. Determine the current suspension and uninstallation situation
    ASSERT_EQ(0, cluster->RecoverHangMDS(serviceMDSID));
    ASSERT_EQ(0, cluster->StopMDS(serviceMDSID));
    pid = cluster->StartSingleMDS(serviceMDSID, ipmap[serviceMDSID],
                                  22240 + serviceMDSID, configmap[serviceMDSID],
                                  false);
    LOG(INFO) << "mds " << serviceMDSID << " started on " << ipmap[serviceMDSID]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);
    ASSERT_TRUE(createOrOpenFailed);

    // 7. Pulling up the killed mds again has no impact on the cluster
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: hangOneNotInserviceMDSThenResumeTheMDS";
    /********** hangOneNotInserviceMDSThenResumeTheMDS ***********/
    // 1. Hang an out of service MDS
    // 2. Expectations
    //      a. When the cluster status is normal: client read and write requests
    //      can be issued normally b. Hang an MDS that is not in service,
    //      expecting no impact on client IO, and suspending and uninstalling
    //      the service is normal
    // 1. The initial state of the cluster, IO is issued normally
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. Hang an MDS that is not in service. When starting, the first MDS is
    // selected as the leader, and hang the second MDS
    serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    int hangid = (serviceMDSID + 1) % 3;
    ASSERT_EQ(0, cluster->HangMDS(hangid));

    // 3. Start the backend suspend and uninstall thread, and it is expected
    // that the suspend and uninstall service will not be affected
    CreateOpenFileBackend();

    // 4. Start backend iops monitoring and start writing from the next segment
    // to trigger getorallocate logic
    //    follower mds cluster normal service after renewing session expiration
    //    (20s renewal)
    ret = MonitorResume(4 * segment_size, 4096, 25);
    if (!ret)
    {
        ASSERT_EQ(0, cluster->RecoverHangMDS(hangid));
        ASSERT_TRUE(false);
    }

    // 5. Waiting for the end of suspend/unload monitoring
    WaitBackendCreateDone();

    // 6. Hanging and uninstalling service is normal
    ASSERT_EQ(0, cluster->RecoverHangMDS(hangid));
    ASSERT_EQ(0, cluster->StopMDS(hangid));
    pid = cluster->StartSingleMDS(hangid, ipmap[hangid], 22240 + hangid,
                                  configmap[hangid], false);
    LOG(INFO) << "mds " << hangid << " started on " << ipmap[hangid]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);

    ASSERT_FALSE(createOrOpenFailed);

    // 7. Cluster has no impact
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: KillTwoInserviceMDSThenRestartTheMDS";
    /************* KillTwoInserviceMDSThenRestartTheMDS ***********/
    // 1. Restart two MDS nodes, one of which is currently serving.
    // 2. Expectations:
    //    a. When the cluster is in a normal state, client read and write
    //    requests can be issued normally. b. When shutting down two MDS nodes,
    //    before the MDS service switches to another MDS,
    //       new write IO from clients will fail, and mount/unmount services
    //       will behave abnormally.
    //    c. After the MDS service switches, it is expected that client IO will
    //    recover, and mount/unmount services will be normal. d. When bringing
    //    the MDS nodes back up, client IO will be unaffected.
    // 1. In the initial state of the cluster, IO can be issued normally.
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. Kill two MDSs. When starting, the first MDS is selected as the leader,
    // and kill the first two MDSs
    serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    int secondid = (serviceMDSID + 1) % 3;
    ASSERT_EQ(0, cluster->StopMDS(serviceMDSID));
    ASSERT_EQ(0, cluster->StopMDS(secondid));

    // 3. Starting the backend suspend and uninstall thread, it is expected that
    // the suspend and uninstall service will be affected
    CreateOpenFileBackend();

    // 4. Start background IO monitoring and start writing from the next segment
    // to trigger the getorallocate logic
    //    follower mds cluster normal service after renewing session expiration
    //    (20s renewal)
    ASSERT_TRUE(MonitorResume(5 * segment_size, 4096, 25));

    // 5. Waiting for the end of backend suspension and uninstallation
    // monitoring
    WaitBackendCreateDone();

    // 6. Determine the current suspension and uninstallation situation
    ASSERT_TRUE(createOrOpenFailed);

    // 7. Pulling up the process of being killed
    pid = cluster->StartSingleMDS(serviceMDSID, ipmap[serviceMDSID],
                                  22240 + serviceMDSID, configmap[serviceMDSID],
                                  false);
    LOG(INFO) << "mds " << serviceMDSID << " started on " << ipmap[serviceMDSID]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);

    // 8. Pulling up the killed mds again has no impact on the cluster
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 9. Pull up other mds killed
    pid = cluster->StartSingleMDS(secondid, ipmap[secondid], 22240 + secondid,
                                  configmap[secondid], false);
    LOG(INFO) << "mds " << secondid << " started on " << ipmap[secondid]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);

    LOG(INFO) << "current case: KillTwoNotInserviceMDSThenRestartTheMDS";
    /******** KillTwoNotInserviceMDSThenRestartTheMDS ***********/
    // 1. Restart two MDS nodes, with both nodes not currently serving.
    // 2. Expectations:
    //    a. When the cluster is in a normal state, client read and write
    //    requests can be issued normally. b. When shutting down two MDS nodes,
    //    it is expected that client IO will be unaffected, and mount/unmount
    //    services will be normal. c. When restarting these two MDS nodes, it is
    //    expected that client IO will be unaffected.
    // 1. In the initial state of the cluster, IO can be issued normally.
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. Starting the backend suspend and uninstall thread, it is expected that
    // the suspend and uninstall service will be affected
    CreateOpenFileBackend();

    // 3. Kill two MDSs. When starting, the first MDS is selected as the leader,
    // and kill the second two MDSs
    serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    int tempid_1 = (serviceMDSID + 1) % 3;
    int tempid_2 = (serviceMDSID + 2) % 3;
    ASSERT_EQ(0, cluster->StopMDS(tempid_1));
    ASSERT_EQ(0, cluster->StopMDS(tempid_2));

    // 4. Start background IO monitoring and start writing from the next segment
    // to trigger the getorallocate logic
    // Killing mds that are not in service has no impact on the cluster
    ASSERT_TRUE(MonitorResume(6 * segment_size, 4096, 10));

    // 5. Waiting for the end of suspend/unload monitoring
    WaitBackendCreateDone();

    // 6. Hanging and uninstalling service is normal
    ASSERT_FALSE(createOrOpenFailed);

    // 7. Pulling up the process of being killed
    pid = cluster->StartSingleMDS(tempid_1, ipmap[tempid_1], 22240 + tempid_1,
                                  configmap[tempid_1], false);
    LOG(INFO) << "mds " << tempid_1 << " started on " << ipmap[tempid_1]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);

    // 8. Cluster has no impact
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 9. Pull up other mds to restore the cluster to normal
    pid = cluster->StartSingleMDS(tempid_2, ipmap[tempid_2], 22240 + tempid_2,
                                  configmap[tempid_2], false);
    LOG(INFO) << "mds " << tempid_2 << " started on " << ipmap[tempid_2]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);

    LOG(INFO) << "current case: hangTwoInserviceMDSThenResumeTheMDS";
    /******** hangTwoInserviceMDSThenResumeTheMDS ************/
    // 1. Hang two MDS nodes, one of which is currently serving, and then
    // recover them.
    // 2. Expectations:
    //    a. When the cluster is in a normal state, client read and write
    //    requests can be issued normally. b. During the MDS hang period and
    //    before the lease renewal with etcd times out, new write IO will fail.
    //       This is because a new write triggers getorallocate, and the RPC
    //       sent to the MDS will keep timing out, leading to retries that
    //       eventually fail.
    //    c. The client session renewal duration is longer than the lease
    //    renewal duration between MDS and etcd.
    //       So, MDS is expected to complete the switch before session renewal
    //       failure occurs. Therefore, the client's session will not expire,
    //       and overwrite writes will not result in exceptions.
    //    d. When the hung MDS nodes are recovered, it is expected to have no
    //    impact on client IO.
    // 1. Hang two MDS nodes, with the first MDS being elected as leader during
    // startup, and both being hung before the process.
    serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    tempid_1 = serviceMDSID;
    tempid_2 = (serviceMDSID + 1) % 3;
    ASSERT_EQ(0, cluster->HangMDS(tempid_1));
    ASSERT_EQ(0, cluster->HangMDS(tempid_2));

    // 2. Starting the backend suspend and uninstall thread, it is expected that
    // the suspend and uninstall service will be affected
    CreateOpenFileBackend();

    LOG(INFO) << "monitor resume start!";
    // 4. Start background IO monitoring and start writing from the next segment
    // to trigger the getorallocate logic
    //    follower mds cluster normal service after renewing session expiration
    //    (20s renewal)
    ret = MonitorResume(7 * segment_size, 4096, 25);
    if (!ret)
    {
        ASSERT_EQ(0, cluster->RecoverHangMDS(tempid_1));
        ASSERT_EQ(0, cluster->RecoverHangMDS(tempid_2));
        ASSERT_TRUE(false);
    }

    LOG(INFO) << "monitor resume done!";
    // 5. Waiting for the end of backend suspension and uninstallation
    // monitoring
    WaitBackendCreateDone();
    LOG(INFO) << "wait backend create thread done!";

    // 6. Determine the current suspension and uninstallation situation
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

    // 7. Pulling up the hung mds again has no impact on the cluster
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: hangTwoNotInserviceMDSThenResumeTheMDS";
    /********** hangTwoNotInserviceMDSThenResumeTheMDS ********/
    // 1. Hang two MDS nodes, neither of which is currently serving, and then
    // recover them.
    // 2. Expectations:
    //    a. When the cluster is in a normal state, client read and write
    //    requests can be issued normally. b. Hang one MDS node that is not
    //    currently serving. It is expected that client IO will be unaffected,
    //    and mount/unmount services will behave normally. c. When these two MDS
    //    nodes are recovered, client IO is expected to be unaffected.
    // 1. In the initial state of the cluster, IO can be issued normally.
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. Hang two mds, the first mds is selected as the leader when starting,
    // and kill the second two mds
    serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    tempid_1 = (serviceMDSID + 1) % 3;
    tempid_2 = (serviceMDSID + 2) % 3;
    ASSERT_EQ(0, cluster->HangMDS(tempid_1));
    ASSERT_EQ(0, cluster->HangMDS(tempid_2));

    // 3. Starting the backend suspend and uninstall thread, it is expected that
    // the suspend and uninstall service will be affected
    CreateOpenFileBackend();

    // 4. Start background IO monitoring and start writing from the next segment
    // to trigger the getorallocate logic
    // Killing mds that are not in service has no impact on the cluster
    ret = MonitorResume(8 * segment_size, 4096, 10);
    if (!ret)
    {
        ASSERT_EQ(0, cluster->RecoverHangMDS(tempid_1));
        ASSERT_EQ(0, cluster->RecoverHangMDS(tempid_2));
        ASSERT_TRUE(false);
    }

    // 5. Waiting for the end of suspend/unload monitoring
    WaitBackendCreateDone();

    // 6. Hanging and uninstalling service is normal
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

    // 7. Cluster has no impact
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    LOG(INFO) << "current case: KillThreeMDSThenRestartTheMDS";
    /********* KillThreeMDSThenRestartTheMDS *********/
    // 1. Restart three MDS nodes.
    // 2. Expectations:
    //    a. When the cluster is in a normal state, client read and write
    //    requests can be issued normally. b. Kill all three MDS nodes: Client
    //    IO failures occur after session expiration. c. During the period
    //    before the client session expires, new writes will fail, but overwrite
    //    writes will not be affected. d. Recover one of the hung MDS nodes:
    //    Client session renewal succeeds, and IO returns to normal. e. Recover
    //    the other two hung MDS nodes: Client IO remains unaffected.

    // 1. Kill three MDSs
    ASSERT_EQ(0, cluster->StopAllMDS());
    // Ensure that the mds has indeed exited
    std::this_thread::sleep_for(std::chrono::seconds(10));

    // 2. Starting the backend suspend and uninstall thread, it is expected that
    // the suspend and uninstall service will be affected
    CreateOpenFileBackend();

    // 3. Send an IO and sleep for a period of time to determine whether to
    // return
    //    Due to writing from the next segment, it triggers the getorallocate
    //    logic MDS is no longer in service, write requests are constantly
    //    hanging, unable to return
    ASSERT_TRUE(SendAioWriteRequest(9 * segment_size, 4096));
    std::this_thread::sleep_for(std::chrono::seconds(30));
    ASSERT_FALSE(writeIOReturnFlag);

    // 4. Waiting for the end of backend suspension and uninstallation
    // monitoring
    WaitBackendCreateDone();

    // 5. Determine the current suspension and uninstallation situation
    ASSERT_TRUE(createOrOpenFailed);

    // 6. Pulling up the process of being killed
    pid = -1;
    while (pid < 0)
    {
        pid =
            cluster->StartSingleMDS(0, "127.0.0.1:22222", 22240, mdsConf, true);
        LOG(INFO) << "mds 0 started on 127.0.0.1:22222, pid = " << pid;
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }

    // 7. Check if the last IO returned
    std::this_thread::sleep_for(std::chrono::seconds(20));
    ASSERT_TRUE(writeIOReturnFlag);

    // 8. New mds starts offering services
    ASSERT_TRUE(MonitorResume(segment_size, 4096, 10));

    // 9. Pull up the process of being killed again
    pid = cluster->StartSingleMDS(1, "127.0.0.1:22223", 22229, mdsConf, false);
    LOG(INFO) << "mds 1 started on 127.0.0.1:22223, pid = " << pid;
    ASSERT_GT(pid, 0);

    // 10. No impact on the cluster
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 11. Pull up other killed mds
    pid = cluster->StartSingleMDS(2, "127.0.0.1:22224", 22232, mdsConf, false);
    LOG(INFO) << "mds 2 started on 127.0.0.1:22224, pid = " << pid;
    ASSERT_GT(pid, 0);

    LOG(INFO) << "current case: hangThreeMDSThenResumeTheMDS";
    /********** hangThreeMDSThenResumeTheMDS **************/
    // 1. Hang three MDS nodes and then recover them.
    // 2. Expectations:
    //    a. When the cluster is in a normal state, client read and write
    //    requests can be issued normally. b. Hang three MDS nodes: Client IO
    //    hangs after the session expires. c. During the period before the
    //    client session expires, new writes will hang continuously, but
    //    overwrite writes will not be affected. e. Recover one of the hung MDS
    //    nodes: Client session renewal succeeds, and IO returns to normal. f.
    //    Recover the other two hung MDS nodes: Client IO remains unaffected.
    // 1. In the initial state of the cluster, IO can be issued normally.
    ASSERT_TRUE(MonitorResume(0, 4096, 1));

    // 2. Hang Three MDSs
    ASSERT_EQ(0, cluster->HangMDS(0));
    ASSERT_EQ(0, cluster->HangMDS(1));
    ASSERT_EQ(0, cluster->HangMDS(2));

    // 3. Starting the backend suspend and uninstall thread, it is expected that
    // the suspend and uninstall service will be affected
    CreateOpenFileBackend();

    // 4. Send an IO and sleep for a period of time to determine whether to
    // return
    //      Due to writing from the next segment, it triggers the getorallocate
    //      logic MDS is no longer in service, write requests are constantly
    //      hanging, unable to return
    ASSERT_TRUE(SendAioWriteRequest(10 * segment_size, 4096));
    std::this_thread::sleep_for(std::chrono::seconds(3));
    ret = writeIOReturnFlag;
    if (ret)
    {
        ASSERT_EQ(0, cluster->RecoverHangMDS(2));
        ASSERT_EQ(0, cluster->RecoverHangMDS(1));
        ASSERT_EQ(0, cluster->RecoverHangMDS(0));
        ASSERT_TRUE(false);
    }

    // 5. Waiting for monitoring to end
    WaitBackendCreateDone();

    // 6. Determine the current suspension and uninstallation situation
    if (!createOrOpenFailed)
    {
        ASSERT_EQ(0, cluster->RecoverHangMDS(2));
        ASSERT_EQ(0, cluster->RecoverHangMDS(1));
        ASSERT_EQ(0, cluster->RecoverHangMDS(0));
        ASSERT_TRUE(false);
    }

    // 7. Pulling up the process being hung may result in the process not
    // shaking hands with ETCD for a long time,
    //    After it was pulled up, it exited, so the mds was restarted after
    //    recover, This ensures that at least one mds in the cluster is
    //    providing services
    ASSERT_EQ(0, cluster->RecoverHangMDS(1));
    ASSERT_EQ(0, cluster->StopMDS(1));

    pid = -1;
    while (pid < 0)
    {
        pid =
            cluster->StartSingleMDS(1, "127.0.0.1:22223", 22229, mdsConf, true);
        LOG(INFO) << "mds 1 started on 127.0.0.1:22223, pid = " << pid;
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }

    // Check if the last IO returned
    std::this_thread::sleep_for(std::chrono::seconds(20));
    ASSERT_TRUE(writeIOReturnFlag);

    // 8. New mds starts offering services
    ret = MonitorResume(segment_size, 4096, 1);
    if (!ret)
    {
        ASSERT_EQ(0, cluster->RecoverHangMDS(2));
        ASSERT_EQ(0, cluster->RecoverHangMDS(0));
        ASSERT_TRUE(false);
    }

    // 9. Pull up the process of being hung again
    ASSERT_EQ(0, cluster->RecoverHangMDS(2));
    ASSERT_EQ(0, cluster->RecoverHangMDS(0));

    // 10. No impact on the cluster
    ASSERT_TRUE(MonitorResume(0, 4096, 1));
}

TEST_F(MDSModuleException, StripeMDSExceptionTest)
{
    LOG(INFO) << "current case: StripeMDSExceptionTest";
    // 1. Create a striped volume
    int stripefd = curve::test::FileCommonOperation::Open("/test2", "curve",
                                                          1024 * 1024, 8);
    ASSERT_NE(stripefd, -1);
    uint64_t offset = std::rand() % 5 * segment_size;

    // 2. Perform data read and write verification
    VerifyDataConsistency(stripefd, offset, 128 * 1024 * 1024);
    std::this_thread::sleep_for(std::chrono::seconds(60));
    // 3. Kill an MDS that is currently the leader
    LOG(INFO) << "stop mds.";
    int serviceMDSID = 0;
    cluster->CurrentServiceMDS(&serviceMDSID);
    ASSERT_EQ(0, cluster->StopMDS(serviceMDSID));
    // 4. Start the background suspend and unload thread
    CreateOpenFileBackend();

    // 5. Continue to randomly write data for verification
    offset = std::rand() % 5 * segment_size;
    LOG(INFO) << "when stop mds, write and read data.";
    VerifyDataConsistency(stripefd, offset, 128 * 1024 * 1024);

    // 6. Waiting for the results of pending uninstallation detection
    WaitBackendCreateDone();

    // 7. Hanging and uninstalling service is normal
    ASSERT_TRUE(createOrOpenFailed);

    LOG(INFO) << "start mds.";
    pid_t pid = cluster->StartSingleMDS(serviceMDSID, ipmap[serviceMDSID],
                                        22240 + serviceMDSID,
                                        configmap[serviceMDSID], false);
    LOG(INFO) << "mds " << serviceMDSID << " started on " << ipmap[serviceMDSID]
              << ", pid = " << pid;
    ASSERT_GT(pid, 0);

    LOG(INFO) << "start mds, write and read data.";
    offset = std::rand() % 5 * segment_size;
    VerifyDataConsistency(stripefd, offset, 128 * 1024 * 1024);

    ::Close(stripefd);
}
