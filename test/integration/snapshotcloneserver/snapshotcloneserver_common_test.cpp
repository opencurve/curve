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
 * Created Date: Sun Sep 29 2019
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <json/json.h>

#include "test/integration/cluster_common/cluster.h"
#include "src/client/libcurve_file.h"
#include "src/snapshotcloneserver/snapshot/snapshot_service_manager.h"
#include "src/snapshotcloneserver/clone/clone_service_manager.h"
#include "test/integration/snapshotcloneserver/test_snapshotcloneserver_helpler.h"
#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/common/snapshotclone_meta_store.h"
#include "src/client/source_reader.h"

using curve::CurveCluster;
using curve::client::FileClient;
using curve::client::UserInfo_t;
using curve::client::SourceReader;

const std::string kTestPrefix = "SCSTest";  // NOLINT

const uint64_t chunkSize = 16ULL * 1024 * 1024;
const uint64_t segmentSize = 32ULL * 1024 * 1024;
const uint64_t chunkGap = 1;

const char* kEtcdClientIpPort = "127.0.0.1:10001";
const char* kEtcdPeerIpPort = "127.0.0.1:10002";
const char* kMdsIpPort = "127.0.0.1:10003";
const char* kChunkServerIpPort1 = "127.0.0.1:10004";
const char* kChunkServerIpPort2 = "127.0.0.1:10005";
const char* kChunkServerIpPort3 = "127.0.0.1:10006";
const char* kSnapshotCloneServerIpPort = "127.0.0.1:10007";
const int kMdsDummyPort = 10008;

const char* kSnapshotCloneServerDummyServerPort = "12000";
const char* kLeaderCampaginPrefix = "snapshotcloneserverleaderlock3";

static const char* kDefaultPoolset = "default";

const std::string kLogPath = "./runlog/" + kTestPrefix + "Log";  // NOLINT
const std::string kMdsDbName = kTestPrefix + "DB";               // NOLINT
const std::string kMdsConfigPath =                               // NOLINT
    "./test/integration/snapshotcloneserver/config/" + kTestPrefix +
    "_mds.conf";

const std::string kCSConfigPath =                                // NOLINT
    "./test/integration/snapshotcloneserver/config/" + kTestPrefix +
    "_chunkserver.conf";

const std::string kCsClientConfigPath =                          // NOLINT
    "./test/integration/snapshotcloneserver/config/" + kTestPrefix +
    "_cs_client.conf";

const std::string kSnapClientConfigPath =                        // NOLINT
    "./test/integration/snapshotcloneserver/config/" + kTestPrefix +
    "_snap_client.conf";

const std::string kS3ConfigPath =                                // NOLINT
    "./test/integration/snapshotcloneserver/config/" + kTestPrefix +
    "_s3.conf";

const std::string kSCSConfigPath =                               // NOLINT
    "./test/integration/snapshotcloneserver/config/" + kTestPrefix +
    "_scs.conf";

const std::string kClientConfigPath =                            // NOLINT
    "./test/integration/snapshotcloneserver/config/" + kTestPrefix +
    "_client.conf";

const std::vector<std::string> mdsConfigOptions{
    std::string("mds.listen.addr=") + kMdsIpPort,
    std::string("mds.etcd.endpoint=") + kEtcdClientIpPort,
    std::string("mds.DbName=") + kMdsDbName,
    std::string("mds.file.expiredTimeUs=50000"),
    std::string("mds.file.expiredTimeUs=10000"),
    std::string("mds.snapshotcloneclient.addr=") + kSnapshotCloneServerIpPort,
};

const std::vector<std::string> mdsConf1{
    { "--graceful_quit_on_sigterm" },
    std::string("--confPath=") + kMdsConfigPath,
    std::string("--log_dir=") + kLogPath,
    std::string("--segmentSize=") + std::to_string(segmentSize),
    { "--stderrthreshold=3" },
};

const std::vector<std::string> chunkserverConfigOptions{
    std::string("mds.listen.addr=") + kMdsIpPort,
    std::string("curve.config_path=") + kCsClientConfigPath,
    std::string("s3.config_path=") + kS3ConfigPath,
    "walfilepool.use_chunk_file_pool=false",
    "walfilepool.enable_get_segment_from_pool=false",
    "global.block_size=4096",
    "global.meta_page_size=4096",
};

const std::vector<std::string> csClientConfigOptions{
    std::string("mds.listen.addr=") + kMdsIpPort,
};

const std::vector<std::string> snapClientConfigOptions{
    std::string("mds.listen.addr=") + kMdsIpPort,
};

const std::vector<std::string> s3ConfigOptions{};

const std::vector<std::string> chunkserverConf1{
    { "--graceful_quit_on_sigterm" },
    { "-chunkServerStoreUri=local://./" + kTestPrefix + "1/" },
    { "-chunkServerMetaUri=local://./" + kTestPrefix +
      "1/chunkserver.dat" },  // NOLINT
    { "-copySetUri=local://./" + kTestPrefix + "1/copysets" },
    { "-raftSnapshotUri=curve://./" + kTestPrefix + "1/copysets" },
    { "-recycleUri=local://./" + kTestPrefix + "1/recycler" },
    { "-chunkFilePoolDir=./" + kTestPrefix + "1/chunkfilepool/" },
    { "-chunkFilePoolMetaPath=./" + kTestPrefix +
      "1/chunkfilepool.meta" },  // NOLINT
    std::string("-conf=") + kCSConfigPath,
    { "-raft_sync_segments=true" },
    std::string("--log_dir=") + kLogPath,
    { "--stderrthreshold=3" },
    { "-raftLogUri=curve://./" + kTestPrefix + "1/copysets" },
    { "-walFilePoolDir=./" + kTestPrefix + "1/walfilepool/" },
    { "-walFilePoolMetaPath=./" + kTestPrefix +
        "1/walfilepool.meta" },
};

const std::vector<std::string> chunkserverConf2{
    { "--graceful_quit_on_sigterm" },
    { "-chunkServerStoreUri=local://./" + kTestPrefix + "2/" },
    { "-chunkServerMetaUri=local://./" + kTestPrefix +
      "2/chunkserver.dat" },  // NOLINT
    { "-copySetUri=local://./" + kTestPrefix + "2/copysets" },
    { "-raftSnapshotUri=curve://./" + kTestPrefix + "2/copysets" },
    { "-recycleUri=local://./" + kTestPrefix + "2/recycler" },
    { "-chunkFilePoolDir=./" + kTestPrefix + "2/chunkfilepool/" },
    { "-chunkFilePoolMetaPath=./" + kTestPrefix +
      "2/chunkfilepool.meta" },  // NOLINT
    std::string("-conf=") + kCSConfigPath,
    { "-raft_sync_segments=true" },
    std::string("--log_dir=") + kLogPath,
    { "--stderrthreshold=3" },
    { "-raftLogUri=curve://./" + kTestPrefix + "2/copysets" },
    { "-walFilePoolDir=./" + kTestPrefix + "2/walfilepool/" },
    { "-walFilePoolMetaPath=./" + kTestPrefix +
        "2/walfilepool.meta" },
};

const std::vector<std::string> chunkserverConf3{
    { "--graceful_quit_on_sigterm" },
    { "-chunkServerStoreUri=local://./" + kTestPrefix + "3/" },
    { "-chunkServerMetaUri=local://./" + kTestPrefix +
      "3/chunkserver.dat" },  // NOLINT
    { "-copySetUri=local://./" + kTestPrefix + "3/copysets" },
    { "-raftSnapshotUri=curve://./" + kTestPrefix + "3/copysets" },
    { "-recycleUri=local://./" + kTestPrefix + "3/recycler" },
    { "-chunkFilePoolDir=./" + kTestPrefix + "3/chunkfilepool/" },
    { "-chunkFilePoolMetaPath=./" + kTestPrefix +
      "3/chunkfilepool.meta" },  // NOLINT
    std::string("-conf=") + kCSConfigPath,
    { "-raft_sync_segments=true" },
    std::string("--log_dir=") + kLogPath,
    { "--stderrthreshold=3" },
    { "-raftLogUri=curve://./" + kTestPrefix + "3/copysets" },
    { "-walFilePoolDir=./" + kTestPrefix + "3/walfilepool/" },
    { "-walFilePoolMetaPath=./" + kTestPrefix +
        "3/walfilepool.meta" },
};

const std::vector<std::string> snapshotcloneserverConfigOptions{
    std::string("client.config_path=") + kSnapClientConfigPath,
    std::string("s3.config_path=") + kS3ConfigPath,
    std::string("metastore.db_name=") + kMdsDbName,
    std::string("server.snapshotPoolThreadNum=8"),
    std::string("server.snapshotCoreThreadNum=2"),
    std::string("server.clonePoolThreadNum=8"),
    std::string("server.createCloneChunkConcurrency=2"),
    std::string("server.recoverChunkConcurrency=2"),
    std::string("client.methodRetryTimeSec=1"),
    std::string("server.clientAsyncMethodRetryTimeSec=1"),
    std::string("etcd.endpoint=") + kEtcdClientIpPort,
    std::string("server.dummy.listen.port=") +
        kSnapshotCloneServerDummyServerPort,
    std::string("leader.campagin.prefix=") + kLeaderCampaginPrefix,
    std::string("server.backEndReferenceRecordScanIntervalMs=100"),
    std::string("server.backEndReferenceFuncScanIntervalMs=1000"),
};

const std::vector<std::string> snapshotcloneConf{
    std::string("--conf=") + kSCSConfigPath,
    std::string("--log_dir=") + kLogPath,
    { "--stderrthreshold=3" },
};

const std::vector<std::string> clientConfigOptions{
    std::string("mds.listen.addr=") + kMdsIpPort,
    std::string("global.logPath=") + kLogPath,
    std::string("mds.rpcTimeoutMS=4000"),
};

const char* testFile1_ = "/ItUser1/file1";
const char* testFile2_ = "/ItUser1/file2";
const char* testFile3_ = "/ItUser2/file3";
const char* testFile4_ = "/ItUser1/file3";
const char* testFile5_ = "/ItUser1/file4";
const char* testUser1_ = "ItUser1";
const char* testUser2_ = "ItUser2";

namespace curve {
namespace snapshotcloneserver {

class SnapshotCloneServerTest : public ::testing::Test {
 public:
    static void SetUpTestCase() {
        std::string mkLogDirCmd = std::string("mkdir -p ") + kLogPath;
        system(mkLogDirCmd.c_str());

        cluster_ = new CurveCluster();
        ASSERT_NE(nullptr, cluster_);

        //Initialize db
        system(std::string("rm -rf " + kTestPrefix + ".etcd").c_str());
        system(std::string("rm -rf " + kTestPrefix + "1").c_str());
        system(std::string("rm -rf " + kTestPrefix + "2").c_str());
        system(std::string("rm -rf " + kTestPrefix + "3").c_str());

        //Start etcd
        pid_t pid = cluster_->StartSingleEtcd(
            1, kEtcdClientIpPort, kEtcdPeerIpPort,
            std::vector<std::string>{ "--name=" + kTestPrefix });
        LOG(INFO) << "etcd 1 started on " << kEtcdPeerIpPort
                  << ", pid = " << pid;
        ASSERT_GT(pid, 0);

        cluster_->InitSnapshotCloneMetaStoreEtcd(kEtcdClientIpPort);

        cluster_->PrepareConfig<MDSConfigGenerator>(kMdsConfigPath,
                                                    mdsConfigOptions);

        //Start an mds
        pid = cluster_->StartSingleMDS(1, kMdsIpPort, kMdsDummyPort, mdsConf1,
                                       true);
        LOG(INFO) << "mds 1 started on " << kMdsIpPort << ", pid = " << pid;
        ASSERT_GT(pid, 0);

        //Creating a physical pool
        ASSERT_EQ(0, cluster_->PreparePhysicalPool(
                         1,
                         "./test/integration/snapshotcloneserver/"
                         "config/topo.json"));  // NOLINT

        // format chunkfilepool and walfilepool
        std::vector<std::thread> threadpool(3);

        threadpool[0] =
            std::thread(&CurveCluster::FormatFilePool, cluster_,
                        "./" + kTestPrefix + "1/chunkfilepool/",
                        "./" + kTestPrefix + "1/chunkfilepool.meta",
                        "./" + kTestPrefix + "1/chunkfilepool/", 1);
        threadpool[1] =
            std::thread(&CurveCluster::FormatFilePool, cluster_,
                        "./" + kTestPrefix + "2/chunkfilepool/",
                        "./" + kTestPrefix + "2/chunkfilepool.meta",
                        "./" + kTestPrefix + "2/chunkfilepool/", 1);
        threadpool[2] =
            std::thread(&CurveCluster::FormatFilePool, cluster_,
                        "./" + kTestPrefix + "3/chunkfilepool/",
                        "./" + kTestPrefix + "3/chunkfilepool.meta",
                        "./" + kTestPrefix + "3/chunkfilepool/", 1);
        for (int i = 0; i < 3; i++) {
            threadpool[i].join();
        }

        cluster_->PrepareConfig<CSClientConfigGenerator>(kCsClientConfigPath,
                                                         csClientConfigOptions);

        cluster_->PrepareConfig<S3ConfigGenerator>(kS3ConfigPath,
                                                   s3ConfigOptions);

        cluster_->PrepareConfig<CSConfigGenerator>(kCSConfigPath,
                                                   chunkserverConfigOptions);

        //Create chunkserver
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

        std::this_thread::sleep_for(std::chrono::seconds(5));

        //Create a logical pool and sleep for a period of time to let the underlying copyset select the primary first
        ASSERT_EQ(0, cluster_->PrepareLogicalPool(
                         1,
                         "./test/integration/snapshotcloneserver/config/"
                         "topo.json"));

        cluster_->PrepareConfig<SnapClientConfigGenerator>(
            kSnapClientConfigPath, snapClientConfigOptions);

        cluster_->PrepareConfig<SCSConfigGenerator>(
            kSCSConfigPath, snapshotcloneserverConfigOptions);

        pid = cluster_->StartSnapshotCloneServer(1, kSnapshotCloneServerIpPort,
                                                 snapshotcloneConf);
        LOG(INFO) << "SnapshotCloneServer 1 started on "
                  << kSnapshotCloneServerIpPort << ", pid = " << pid;
        ASSERT_GT(pid, 0);

        cluster_->PrepareConfig<ClientConfigGenerator>(kClientConfigPath,
                                                       clientConfigOptions);

        fileClient_ = new FileClient();
        fileClient_->Init(kClientConfigPath);

        UserInfo_t userinfo;
        userinfo.owner = "ItUser1";

        ASSERT_EQ(0, fileClient_->Mkdir("/ItUser1", userinfo));

        std::string fakeData(4096, 'x');
        ASSERT_TRUE(CreateAndWriteFile(testFile1_, testUser1_, fakeData));
        LOG(INFO) << "Write testFile1_ success.";

        ASSERT_TRUE(CreateAndWriteFile(testFile2_, testUser1_, fakeData));
        LOG(INFO) << "Write testFile2_ success.";

        UserInfo_t userinfo2;
        userinfo2.owner = "ItUser2";
        ASSERT_EQ(0, fileClient_->Mkdir("/ItUser2", userinfo2));

        ASSERT_TRUE(CreateAndWriteFile(testFile3_, testUser2_, fakeData));
        LOG(INFO) << "Write testFile3_ success.";

        ASSERT_EQ(0, fileClient_->Create(testFile4_, userinfo,
                                         10ULL * 1024 * 1024 * 1024));

        ASSERT_EQ(0, fileClient_->Create(testFile5_, userinfo,
                                         10ULL * 1024 * 1024 * 1024));
    }

    static bool CreateAndWriteFile(const std::string& fileName,
                                   const std::string& user,
                                   const std::string& dataSample) {
        UserInfo_t userinfo;
        userinfo.owner = user;
        int ret =
            fileClient_->Create(fileName, userinfo, 10ULL * 1024 * 1024 * 1024);
        if (ret < 0) {
            LOG(ERROR) << "Create fail, ret = " << ret;
            return false;
        }
        return WriteFile(fileName, user, dataSample);
    }

    static bool WriteFile(const std::string& fileName, const std::string& user,
                          const std::string& dataSample) {
        int ret = 0;
        UserInfo_t userinfo;
        userinfo.owner = user;
        int testfd1_ = fileClient_->Open(fileName, userinfo);
        if (testfd1_ < 0) {
            LOG(ERROR) << "Open fail, ret = " << testfd1_;
            return false;
        }
        //Write the first 4k data and two segments for each chunk
        uint64_t totalChunk = 2ULL * segmentSize / chunkSize;
        for (uint64_t i = 0; i < totalChunk / chunkGap; i++) {
            ret =
                fileClient_->Write(testfd1_, dataSample.c_str(),
                                   i * chunkSize * chunkGap, dataSample.size());
            if (ret < 0) {
                LOG(ERROR) << "Write Fail, ret = " << ret;
                return false;
            }
        }
        ret = fileClient_->Close(testfd1_);
        if (ret < 0) {
            LOG(ERROR) << "Close fail, ret = " << ret;
            return false;
        }
        return true;
    }

    static bool CheckFileData(const std::string& fileName,
                              const std::string& user,
                              const std::string& dataSample) {
        UserInfo_t userinfo;
        userinfo.owner = user;
        int dstFd = fileClient_->Open(fileName, userinfo);
        if (dstFd < 0) {
            LOG(ERROR) << "Open fail, ret = " << dstFd;
            return false;
        }

        int ret = 0;
        uint64_t totalChunk = 2ULL * segmentSize / chunkSize;
        for (uint64_t i = 0; i < totalChunk / chunkGap; i++) {
            char buf[4096];
            ret = fileClient_->Read(dstFd, buf, i * chunkSize * chunkGap, 4096);
            if (ret < 0) {
                LOG(ERROR) << "Read fail, ret = " << ret;
                return false;
            }
            std::string data(buf, 4096);
            if (data != dataSample) {
                LOG(ERROR) << "CheckFileData not Equal, data = [" << data
                           << "] , expect data = [" << dataSample << "].";
                return false;
            }
        }
        ret = fileClient_->Close(dstFd);
        if (ret < 0) {
            LOG(ERROR) << "Close fail, ret = " << ret;
            return false;
        }
        return true;
    }

    static void TearDownTestCase() {
        fileClient_->UnInit();
        delete fileClient_;
        fileClient_ = nullptr;
        ASSERT_EQ(0, cluster_->StopCluster());
        delete cluster_;
        cluster_ = nullptr;
        system(std::string("rm -rf " + kTestPrefix + ".etcd").c_str());
        system(std::string("rm -rf " + kTestPrefix + "1").c_str());
        system(std::string("rm -rf " + kTestPrefix + "2").c_str());
        system(std::string("rm -rf " + kTestPrefix + "3").c_str());
    }

    void SetUp() {}

    void TearDown() {}

    void PrepareSnapshotForTestFile1(std::string* uuid1) {
        if (!hasSnapshotForTestFile1_) {
            int ret = MakeSnapshot(testUser1_, testFile1_, "snap1", uuid1);
            ASSERT_EQ(0, ret);
            bool success1 =
                CheckSnapshotSuccess(testUser1_, testFile1_, *uuid1);
            ASSERT_TRUE(success1);
            hasSnapshotForTestFile1_ = true;
            snapIdForTestFile1_ = *uuid1;
        }
    }

    void WaitDeleteSnapshotForTestFile1() {
        if (hasSnapshotForTestFile1_) {
            ASSERT_EQ(0, DeleteAndCheckSnapshotSuccess(testUser1_, testFile1_,
                                                       snapIdForTestFile1_));
        }
    }

    static CurveCluster* cluster_;
    static FileClient* fileClient_;

    bool hasSnapshotForTestFile1_ = false;
    std::string snapIdForTestFile1_;
};

CurveCluster* SnapshotCloneServerTest::cluster_ = nullptr;
FileClient* SnapshotCloneServerTest::fileClient_ = nullptr;

//Regular test cases
//Scenario 1: Adding, deleting, and searching snapshots
TEST_F(SnapshotCloneServerTest, TestSnapshotAddDeleteGet) {
    std::string uuid1;
    int ret = 0;
    //Action 1: User testUser1_ Take a snapshot of non-existent files
    //Expected 1: Return file does not exist
    ret = MakeSnapshot(testUser1_, "/ItUser1/notExistFile", "snap1", &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    //Action 2: User testUser2_ For testFile1_ Take a snapshot
    //Expected 2: Failed to return user authentication
    ret = MakeSnapshot(testUser2_, testFile1_, "snap1", &uuid1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    //Action 3: User testUser1_ For testFile1_ Take a snapshot snap1.
    //Expected 3: Successful snapshot taking
    ret = MakeSnapshot(testUser1_, testFile1_, "snap1", &uuid1);
    ASSERT_EQ(0, ret);

    std::string fakeData(4096, 'y');
    ASSERT_TRUE(WriteFile(testFile1_, testUser1_, fakeData));
    ASSERT_TRUE(CheckFileData(testFile1_, testUser1_, fakeData));

    //Operation 4: Obtain snapshot information, user=testUser1, Filename=testFile1_
    //Expected 4: Return information for snapshot snap1
    bool success1 = CheckSnapshotSuccess(testUser1_, testFile1_, uuid1);
    ASSERT_TRUE(success1);

    //Operation 5: Obtain snapshot information, user=testUser2, Filename=testFile1_
    //Expected 5: User authentication failure returned
    FileSnapshotInfo info1;
    ret = GetSnapshotInfo(testUser2_, testFile1_, uuid1, &info1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    //Operation 6: Obtain snapshot information, user=testUser2, Filename=testFile2_
    //Expected 6: Return null
    std::vector<FileSnapshotInfo> infoVec;
    ret = ListFileSnapshotInfo(testUser2_, testFile2_, 10, 0, &infoVec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, infoVec.size());

    //Action 7: testUser2_ Delete snapshot snap1
    //Expected 7: User authentication failure returned
    ret = DeleteSnapshot(testUser2_, testFile1_, uuid1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    //Action 8: testUser1_ Delete testFile2_ Snapshot with ID snap1 for
    //Expected 8: Return file name mismatch
    ret = DeleteSnapshot(testUser1_, testFile2_, uuid1);
    ASSERT_EQ(kErrCodeFileNameNotMatch, ret);

    //Operation 9: testUser1_ Delete snapshot snap1
    //Expected 9: Successful deletion returned
    ret = DeleteAndCheckSnapshotSuccess(testUser1_, testFile1_, uuid1);
    ASSERT_EQ(0, ret);

    //Action 10: Obtain snapshot information, user=testUser1, Filename=testFile1_
    //Expected 10: Return empty
    ret = ListFileSnapshotInfo(testUser1_, testFile1_, 10, 0, &infoVec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, infoVec.size());

    //Operation 11: testUser1_ Delete snapshot snap1 (duplicate deletion)
    //Expected 11: Successful deletion returned
    ret = DeleteAndCheckSnapshotSuccess(testUser1_, testFile1_, uuid1);
    ASSERT_EQ(0, ret);

    //Restore testFile1_
    std::string fakeData2(4096, 'x');
    ASSERT_TRUE(WriteFile(testFile1_, testUser1_, fakeData2));
}

//Scenario 2: Cancel Snapshot
TEST_F(SnapshotCloneServerTest, TestCancelSnapshot) {
    std::string uuid1;
    int ret = MakeSnapshot(testUser1_, testFile1_, "snapToCancle", &uuid1);
    ASSERT_EQ(0, ret);

    bool success1 = false;
    bool isCancel = false;
    for (int i = 0; i < 600; i++) {
        FileSnapshotInfo info1;
        int retCode = GetSnapshotInfo(testUser1_, testFile1_, uuid1, &info1);
        if (retCode == 0) {
            if (info1.GetSnapshotInfo().GetStatus() == Status::pending ||
                info1.GetSnapshotInfo().GetStatus() == Status::canceling) {
                if (!isCancel) {
                    //Action 1: User testUser1_ For testFile1_ Take a snapshot snap1,
                    //TestUser2 before the snapshot is completed_ Cancel testFile1_ Snap1 of snapshot
                    //Expected 1: Failed to cancel user authentication
                    int retCode = CancelSnapshot(testUser2_, testFile1_, uuid1);
                    ASSERT_EQ(kErrCodeInvalidUser, retCode);

                    //Action 2: User testUser1_ For testFile1_ Take a snapshot snap1,
                    //TestUser1 before the snapshot is completed_ Cancel testFile1_
                    //A non-existent snapshot of
                    //Expected 2: Return kErrCodeCannotCancelFinished
                    retCode =
                        CancelSnapshot(testUser1_, testFile1_, "notExistUUId");
                    ASSERT_EQ(kErrCodeCannotCancelFinished, retCode);

                    //Action 3: User testUser1_ For testFile1_ Take a snapshot snap1,
                    //TestUser1 before the snapshot is completed_ Cancel testFile2_ Snap1 of snapshot
                    //Expected 3: Return file name mismatch
                    retCode = CancelSnapshot(testUser1_, testFile2_, uuid1);
                    ASSERT_EQ(kErrCodeFileNameNotMatch, retCode);

                    //Action 4: User testUser1_ For testFile1_ Take a snapshot,
                    //TestUser1 before the snapshot is completed_ Cancel snapshot snap1
                    //Expected 4: Successfully cancelled snapshot
                    retCode = CancelSnapshot(testUser1_, testFile1_, uuid1);
                    ASSERT_EQ(0, retCode);
                    isCancel = true;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(3000));
                continue;
            } else if (info1.GetSnapshotInfo().GetStatus() == Status::done) {
                success1 = false;
                break;
            } else {
                FAIL() << "Snapshot Fail On status = "
                       << static_cast<int>(info1.GetSnapshotInfo().GetStatus());
            }
        } else if (retCode == -8) {
            //Operation 5: Obtain snapshot information, user=testUser1, Filename=testFile1_
            //Expected 5: Return empty
            success1 = true;
            break;
        }
    }
    ASSERT_TRUE(success1);

    //Operation 6: After the snapshot is completed, testUser1_ Cancel testFile1_ Snap1 of snapshot
    //Expected 6: Returning a pending snapshot that does not exist or has been completed
    ret = CancelSnapshot(testUser1_, testFile1_, uuid1);
    ASSERT_EQ(kErrCodeCannotCancelFinished, ret);
}

//Scenario 3: Lazy snapshot clone scene
TEST_F(SnapshotCloneServerTest, TestSnapLazyClone) {
    std::string snapId;
    PrepareSnapshotForTestFile1(&snapId);

    //Operation 1: testUser1_ A snapshot with a clone that does not exist, fileName=SnapLazyClone1
    //Expected 1: Return snapshot does not exist
    std::string uuid1, uuid2, uuid3, uuid4, uuid5;
    int ret;
    ret = CloneOrRecover("Clone", testUser1_, "UnExistSnapId1",
                         "/ItUser1/SnapLazyClone1", true, &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    //Action 2: testUser2_ Clone snapshot snap1, fileName=SnapLazyClone1
    //Expected 2: User authentication failure returned
    ret = CloneOrRecover("Clone", testUser2_, snapId, "/ItUser2/SnapLazyClone1",
                         true, &uuid2);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    //Operation 3: testUser1_ Clone snapshot snap1, fileName=SnapLazyClone1
    //Expected 3 to return successful cloning
    std::string dstFile = "/ItUser1/SnapLazyClone1";
    ret = CloneOrRecover("Clone", testUser1_, snapId, dstFile, true, &uuid3);
    ASSERT_EQ(0, ret);

    //Operation 4: testUser1_ Clone block photo snap1, fileName=SnapLazyClone1 (duplicate clone)
    //Expected 4: Returns successful cloning (idempotent)
    ret = CloneOrRecover("Clone", testUser1_, snapId, "/ItUser1/SnapLazyClone1",
                         true, &uuid4);
    ASSERT_EQ(0, ret);

    // Flatten
    ret = Flatten(testUser1_, uuid3);
    ASSERT_EQ(0, ret);

    //Action 5: testUser1_ GetCloneTask
    //Expected 5: Return clone task for SnapLazyClone1
    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid3, true);
    ASSERT_TRUE(success1);

    //Operation 6: testUser2_ GetCloneTask
    //Expected 6: Return null
    std::vector<TaskCloneInfo> infoVec;
    ret = ListCloneTaskInfo(testUser2_, 10, 0, &infoVec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, infoVec.size());

    //Action 7: testUser2_ CleanCloneTask UUID is the UUID of SnapLazyClone1
    //Expected 7: User authentication failure returned
    ret = CleanCloneTask(testUser2_, uuid3);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    //Action 8: testUser1_ CleanCloneTask UUID is the UUID of SnapLazyClone1
    //Expected 8: Return execution successful
    ret = CleanCloneTask(testUser1_, uuid3);
    ASSERT_EQ(0, ret);

    //Waiting for cleaning to complete
    std::this_thread::sleep_for(std::chrono::seconds(3));

    //Operation 9: testUser1_ CleanCloneTask UUID is the UUID of SnapLazyClone1 (repeated execution)
    //Expected 9: Return execution successful
    ret = CleanCloneTask(testUser1_, uuid3);
    ASSERT_EQ(0, ret);

    //Action 10: testUser1_ GetCloneTask
    //Expected 10: Return empty
    TaskCloneInfo info;
    ret = GetCloneTaskInfo(testUser1_, uuid3, &info);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    //Verify data correctness
    std::string fakeData(4096, 'x');
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData));
}

//Scenario 4: Non lazy snapshot clone scenario
TEST_F(SnapshotCloneServerTest, TestSnapNotLazyClone) {
    std::string snapId;
    PrepareSnapshotForTestFile1(&snapId);

    //Operation 1: testUser1_ A snapshot with a clone that does not exist, fileName=SnapNotLazyClone1
    //Expected 1: Return snapshot does not exist
    std::string uuid1;
    int ret;
    ret = CloneOrRecover("Clone", testUser1_, "UnExistSnapId2",
                         "/ItUser1/SnapNotLazyClone1", false, &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    //Action 2: testUser2_ Clone snapshot snap1, fileName=SnapNotLazyClone1
    //Expected 2: User authentication failure returned
    ret = CloneOrRecover("Clone", testUser2_, snapId,
                         "/ItUser2/SnapNotLazyClone1", false, &uuid1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    //Operation 3: testUser1_ Clone snapshot snap1, fileName=SnapNotLazyClone1
    //Expected 3 to return successful cloning
    std::string dstFile = "/ItUser1/SnapNotLazyClone1";
    ret = CloneOrRecover("Clone", testUser1_, snapId, dstFile, false, &uuid1);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid1, true);
    ASSERT_TRUE(success1);

    //Operation 4: testUser1_ Clone block photo snap1,
    //FileName=SnapNotLazyClone1 (duplicate clone)
    //Expected 4: Returns successful cloning (idempotent)
    ret = CloneOrRecover("Clone", testUser1_, snapId,
                         "/ItUser1/SnapNotLazyClone1", false, &uuid1);
    ASSERT_EQ(0, ret);

    //Verify data correctness
    std::string fakeData(4096, 'x');
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData));
}

//Scenario 5: Lazy snapshot recovery scenario
TEST_F(SnapshotCloneServerTest, TestSnapLazyRecover) {
    std::string snapId;
    PrepareSnapshotForTestFile1(&snapId);

    //Operation 1: testUser1_ Recover snapshot that does not exist, fileName=testFile1_
    //Expected 1: Return snapshot does not exist
    std::string uuid1;
    int ret;
    ret = CloneOrRecover("Recover", testUser1_, "UnExistSnapId3", testFile1_,
                         true, &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    //Action 2: testUser2_ Recover snapshot snap1, fileName=testFile1_
    //Expected 2: User authentication failure returned
    ret =
        CloneOrRecover("Recover", testUser2_, snapId, testFile1_, true, &uuid1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    //Operation 3: testUser1_ Recover snapshot snap1, fileName=testFile1_
    //Expected 3 return recovery success
    ret =
        CloneOrRecover("Recover", testUser1_, snapId, testFile1_, true, &uuid1);
    ASSERT_EQ(0, ret);

    // Flatten
    ret = Flatten(testUser1_, uuid1);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid1, false);
    ASSERT_TRUE(success1);

    //Verify data correctness
    std::string fakeData(4096, 'x');
    ASSERT_TRUE(CheckFileData(testFile1_, testUser1_, fakeData));

    //Operation 4: testUser1_ Recover snapshot snap1, target file is a non-existent file
    //Expected 4: Return target file does not exist
    ret = CloneOrRecover("Recover", testUser1_, snapId, "/ItUser1/notExistFile",
                         true, &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);
}

//Scenario 6: Non lazy snapshot recovery scenario
TEST_F(SnapshotCloneServerTest, TestSnapNotLazyRecover) {
    std::string snapId;
    PrepareSnapshotForTestFile1(&snapId);

    //Operation 1: testUser1_ Recover snapshot that does not exist, fileName=testFile1_
    //Expected 1: Return snapshot does not exist
    std::string uuid1;
    int ret;
    ret = CloneOrRecover("Recover", testUser1_, "UnExistSnapId4", testFile1_,
                         false, &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    //Action 2: testUser2_ Recover snapshot snap1, fileName=testFile1_
    //Expected 2: User authentication failure returned
    ret = CloneOrRecover("Recover", testUser2_, snapId, testFile1_, false,
                         &uuid1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    //Operation 3: testUser1_ Recover snapshot snap1, fileName=testFile1_
    //Expected 3 return recovery success
    ret = CloneOrRecover("Recover", testUser1_, snapId, testFile1_, false,
                         &uuid1);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid1, false);
    ASSERT_TRUE(success1);

    //Verify data correctness
    std::string fakeData(4096, 'x');
    ASSERT_TRUE(CheckFileData(testFile1_, testUser1_, fakeData));

    //Operation 4: testUser1_ Recover snapshot snap1, target file is a non-existent file
    //Expected 4: Return target file does not exist
    ret = CloneOrRecover("Recover", testUser1_, snapId, "/ItUser1/notExistFile",
                         false, &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);
}

//Scenario 7: Lazy Mirror Clone Scene
TEST_F(SnapshotCloneServerTest, TestImageLazyClone) {
    //Operation 1: testUser1_ Clone does not exist in an image, fileName=ImageLazyClone1
    //Expected 1: Return file does not exist
    std::string uuid1, uuid2, uuid3, uuid4;
    int ret;
    ret = CloneOrRecover("Clone", testUser1_, "/UnExistFile",
                         "/ItUser1/ImageLazyClone1", true, &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    //Action 2: testUser1_ Clone image testFile1, FileName=ImageLazyClone1
    //Expected 2 to return successful cloning
    std::string dstFile = "/ItUser1/ImageLazyClone1";
    ret =
        CloneOrRecover("Clone", testUser1_, testFile1_, dstFile, true, &uuid2);
    ASSERT_EQ(0, ret);

    //Operation 3: testUser1_ Clone image testFile1,
    //FileName=ImageLazyClone1 (duplicate clone)
    //Expected 3: Returns successful cloning (idempotent)
    ret = CloneOrRecover("Clone", testUser1_, testFile1_,
                         "/ItUser1/ImageLazyClone1", true, &uuid3);
    ASSERT_EQ(0, ret);

    //Action 4: Take a snapshot snap1 of the file ImageLazyClone1 that has not completed the lazy clone
    //Expected 4: Abnormal file status returned
    ret = MakeSnapshot(testUser1_, testFile1_, "snap1", &uuid4);
    ASSERT_EQ(kErrCodeFileStatusInvalid, ret);
    FileSnapshotInfo info2;
    int retCode = GetSnapshotInfo(testUser1_, testFile1_, uuid4, &info2);
    ASSERT_EQ(kErrCodeFileNotExist, retCode);

    ASSERT_TRUE(WaitMetaInstalledSuccess(testUser1_, uuid2, true));

    //Verify data correctness before Flatten
    std::string fakeData1(4096, 'x');
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData1));

    // Flatten
    ret = Flatten(testUser1_, uuid2);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid2, true);
    ASSERT_TRUE(success1);

    //Verify data correctness after Flatten
    std::string fakeData2(4096, 'x');
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData2));
}

//Scenario 8: Non Lazy Mirror Clone Scene
TEST_F(SnapshotCloneServerTest, TestImageNotLazyClone) {
    //Operation 1: testUser1_ Clone does not exist in an image, fileName=ImageNotLazyClone1
    //Expected 1: Return snapshot does not exist
    std::string uuid1;
    int ret;
    ret = CloneOrRecover("Clone", testUser1_, "/UnExistFile",
                         "/ItUser1/ImageNotLazyClone1", false, &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    //Action 2: testUser1_ Clone image testFile1, FileName=ImageNotLazyClone1
    //Expected 2 to return successful cloning
    std::string dstFile = "/ItUser1/ImageNotLazyClone1";
    ret =
        CloneOrRecover("Clone", testUser1_, testFile1_, dstFile, false, &uuid1);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid1, true);
    ASSERT_TRUE(success1);

    //Operation 3: testUser1_ Clone image testFile1,
    //FileName=ImageNotLazyClone1 (duplicate clone)
    //Expected 3: Returns successful cloning (idempotent)
    ret = CloneOrRecover("Clone", testUser1_, testFile1_,
                         "/ItUser1/ImageNotLazyClone1", false, &uuid1);
    ASSERT_EQ(0, ret);

    //Verify data correctness
    std::string fakeData(4096, 'x');
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData));
}

//Scenario 9: The snapshot has a failure scenario
TEST_F(SnapshotCloneServerTest, TestSnapAndCloneWhenSnapHasError) {
    std::string snapId = "errorSnapUuid";
    SnapshotInfo snapInfo(snapId, testUser1_, testFile4_, "snapxxx", 0, 0, 0, 0,
                        0, 0, kDefaultPoolset, 0, Status::error);

    cluster_->metaStore_->AddSnapshot(snapInfo);

    pid_t pid = cluster_->RestartSnapshotCloneServer(1);
    LOG(INFO) << "SnapshotCloneServer 1 restarted, pid = " << pid;
    ASSERT_GT(pid, 0);
    std::string uuid1, uuid2;

    //Operation 1: lazy clone snapshot snap1
    //Expected 1: Exception in returning snapshot
    int ret = CloneOrRecover("Clone", testUser1_, snapId,
                             "/ItUser2/SnapLazyClone1", true, &uuid2);
    ASSERT_EQ(kErrCodeInvalidSnapshot, ret);

    //Action 2: Non lazy clone snapshot snap1
    //Expected 2: Exception in returning snapshot
    ret = CloneOrRecover("Clone", testUser1_, snapId,
                         "/ItUser2/SnapNotLazyClone1", false, &uuid2);
    ASSERT_EQ(kErrCodeInvalidSnapshot, ret);

    //Action 3: lazy snap1 recover from snapshot
    //Expected 3: Exception in returning snapshot
    ret =
        CloneOrRecover("Recover", testUser1_, snapId, testFile4_, true, &uuid2);
    ASSERT_EQ(kErrCodeInvalidSnapshot, ret);

    //Operation 4: Snap1 recover from snapshot without lazy
    //Expected 4: Exception in returning snapshot
    ret = CloneOrRecover("Recover", testUser1_, snapId, testFile4_, false,
                         &uuid2);
    ASSERT_EQ(kErrCodeInvalidSnapshot, ret);

    //Action 5: User testUser1_ For testFile4_ Take a snapshot snap1
    //Expectation 5: Clean failed snapshot and take snapshot successfully
    ret = MakeSnapshot(testUser1_, testFile4_, "snap1", &uuid1);
    ASSERT_EQ(0, ret);

    //Successfully verified snapshot
    bool success1 = CheckSnapshotSuccess(testUser1_, testFile4_, uuid1);
    ASSERT_TRUE(success1);

    //Verification cleaning failed, snapshot succeeded
    FileSnapshotInfo info1;
    int retCode = GetSnapshotInfo(testUser1_, testFile4_, snapId, &info1);
    ASSERT_EQ(kErrCodeFileNotExist, retCode);
}

//[Online issue repair] Clone failed, rollback delete clone volume, and create the same uuid volume again scenario
TEST_F(SnapshotCloneServerTest, TestCloneHasSameDest) {
    std::string uuid1, uuid2, uuid3, uuid4, uuid5, uuid6, uuid7;
    //Action 1: testUser1_ Clone image testFile1, FileName=CloneHasSameDestUUID
    //Expected 1 to return successful cloning
    std::string dstFile = "/ItUser1/CloneHasSameDest";
    int ret =
        CloneOrRecover("Clone", testUser1_, testFile1_, dstFile, true, &uuid1);
    ASSERT_EQ(0, ret);

    //Delete Clone Volume
    UserInfo_t userinfo;
    userinfo.owner = testUser1_;
    int ret2 = fileClient_->Unlink(dstFile, userinfo, false);
    ASSERT_EQ(0, ret2);


    //Action 2: testUser1_ Clone image testFile1_ again,
    // fileName=CloneHasSameDestUUID
    //Expected 2 to return successful cloning
    ret =
        CloneOrRecover("Clone", testUser1_, testFile1_, dstFile, true, &uuid2);
    ASSERT_EQ(0, ret);

    //Verify data correctness
    std::string fakeData(4096, 'x');
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData));

    //Operation 3: testUser1_ Clone image testFile1, FileName=CloneHasSameDest2
    //Expected 3 to return successful cloning
    dstFile = "/ItUser1/CloneHasSameDest2";
    ret =
        CloneOrRecover("Clone", testUser1_, testFile1_, dstFile, true, &uuid3);
    ASSERT_EQ(0, ret);

    //Delete Clone Volume
    UserInfo_t userinfo2;
    userinfo2.owner = testUser1_;
    ret2 = fileClient_->Unlink(dstFile, userinfo2, false);
    ASSERT_EQ(0, ret2);


    //Operation 4: testUser1_ Clone the image testFile2 again,
    // fileName=CloneHasSameDest2
    //Expected 4 to return successful cloning
    ret =
        CloneOrRecover("Clone", testUser1_, testFile2_, dstFile, true, &uuid4);
    ASSERT_EQ(0, ret);

    //Verify data correctness
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData));

    //Verify different situations when cloning lazyflag again
    //Action 5: testUser1_ Clone image testFile1, FileName=CloneHasSameDest3
    //Expected 5 to return successful cloning
    dstFile = "/ItUser1/CloneHasSameDest3";
    ret =
        CloneOrRecover("Clone", testUser1_, testFile1_, dstFile, true, &uuid5);
    ASSERT_EQ(0, ret);

    //Delete Clone Volume
    UserInfo_t userinfo3;
    userinfo2.owner = testUser1_;
    ret2 = fileClient_->Unlink(dstFile, userinfo2, false);
    ASSERT_EQ(0, ret2);

    //Operation 6: testUser1_ Non lazy clone image testFile2 again,
    // fileName=CloneHasSameDest3
    //Expected 6 to return successful cloning
    ret =
        CloneOrRecover("Clone", testUser1_, testFile2_, dstFile, false, &uuid6);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid6, true);
    ASSERT_TRUE(success1);

    //Verify data correctness
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData));

    //Delete Clone Volume
    UserInfo_t userinfo4;
    userinfo2.owner = testUser1_;
    ret2 = fileClient_->Unlink(dstFile, userinfo2, false);
    ASSERT_EQ(0, ret2);

    //Action 7: testUser1_ Non lazy clone image testFile2 again,
    // fileName=CloneHasSameDest3
    //Expected 7 to return successful cloning
    ret =
        CloneOrRecover("Clone", testUser1_, testFile2_, dstFile, true, &uuid7);
    ASSERT_EQ(0, ret);

    //Verify data correctness
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData));
}

//Lazy clone volume, delete clone volume, and then delete source volume. The source volume can be deleted if needed
TEST_F(SnapshotCloneServerTest, TestDeleteLazyCloneDestThenDeleteSrc) {
    //Action 1: testUser1_ Clone image testFile5_, Lazy clone two volumes dstFile1 and dstFile2
    //Expected 1 to return successful cloning
    std::string uuid1;
    std::string uuid2;
    std::string dstFile1 = "/dest1";
    std::string dstFile2 = "/dest2";
    UserInfo_t userinfo;
    userinfo.owner = testUser1_;
    int ret =
        CloneOrRecover("Clone", testUser1_, testFile5_, dstFile1, true, &uuid1);
    ASSERT_EQ(0, ret);

    ret =
        CloneOrRecover("Clone", testUser1_, testFile5_, dstFile2, true, &uuid2);
    ASSERT_EQ(0, ret);

    //Delete source volume, deletion failed, volume occupied

    ret = fileClient_->Unlink(testFile5_, userinfo, false);
    ASSERT_EQ(-27, ret);

    //Operation 2: Successfully delete the destination volume dstFile1, delete the source volume again
    //Expected 2 deletion failed, volume occupied
    ret = fileClient_->Unlink(dstFile1, userinfo, false);
    ASSERT_EQ(0, ret);

    ret = fileClient_->Unlink(testFile5_, userinfo, false);
    ASSERT_EQ(-27, ret);


    //Operation 3: Successfully delete the destination volume dstFile2, delete the source volume again
    //Expected 3 deletion successful
    ret = fileClient_->Unlink(dstFile2, userinfo, false);
    ASSERT_EQ(0, ret);

    ret = fileClient_->Unlink(testFile5_, userinfo, false);
    ASSERT_EQ(0, ret);

    //Action 4: Wait for a period of time to see if the garbage record can be deleted in the background
    bool noRecord = false;
    for (int i = 0; i < 100; i++) {
        TaskCloneInfo info;
        int ret1 = GetCloneTaskInfo(testUser1_, uuid1, &info);
        int ret2 = GetCloneTaskInfo(testUser1_, uuid2, &info);
        if (ret1 == kErrCodeFileNotExist && ret2 == kErrCodeFileNotExist) {
            noRecord = true;
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    }

    ASSERT_TRUE(noRecord);
}
}  // namespace snapshotcloneserver
}  // namespace curve
