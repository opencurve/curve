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
    { " --graceful_quit_on_sigterm" },
    std::string(" --confPath=") + kMdsConfigPath,
    std::string(" --log_dir=") + kLogPath,
    std::string(" --segmentSize=") + std::to_string(segmentSize),
    { " --stderrthreshold=3" },
};

const std::vector<std::string> chunkserverConfigOptions{
    std::string("mds.listen.addr=") + kMdsIpPort,
    std::string("curve.config_path=") + kCsClientConfigPath,
    std::string("s3.config_path=") + kS3ConfigPath,
    "walfilepool.use_chunk_file_pool=false",
    "walfilepool.enable_get_segment_from_pool=false"
};

const std::vector<std::string> csClientConfigOptions{
    std::string("mds.listen.addr=") + kMdsIpPort,
};

const std::vector<std::string> snapClientConfigOptions{
    std::string("mds.listen.addr=") + kMdsIpPort,
};

const std::vector<std::string> s3ConfigOptions{};

const std::vector<std::string> chunkserverConf1{
    { " --graceful_quit_on_sigterm" },
    { " -chunkServerStoreUri=local://./" + kTestPrefix + "1/" },
    { " -chunkServerMetaUri=local://./" + kTestPrefix +
      "1/chunkserver.dat" },  // NOLINT
    { " -copySetUri=local://./" + kTestPrefix + "1/copysets" },
    { " -raftSnapshotUri=curve://./" + kTestPrefix + "1/copysets" },
    { " -recycleUri=local://./" + kTestPrefix + "1/recycler" },
    { " -chunkFilePoolDir=./" + kTestPrefix + "1/chunkfilepool/" },
    { " -chunkFilePoolMetaPath=./" + kTestPrefix +
      "1/chunkfilepool.meta" },  // NOLINT
    std::string(" -conf=") + kCSConfigPath,
    { " -raft_sync_segments=true" },
    std::string(" --log_dir=") + kLogPath,
    { " --stderrthreshold=3" },
    { " -raftLogUri=curve://./" + kTestPrefix + "1/copysets" },
    { " -walFilePoolDir=./" + kTestPrefix + "1/walfilepool/" },
    { " -walFilePoolMetaPath=./" + kTestPrefix +
        "1/walfilepool.meta" },
};

const std::vector<std::string> chunkserverConf2{
    { " --graceful_quit_on_sigterm" },
    { " -chunkServerStoreUri=local://./" + kTestPrefix + "2/" },
    { " -chunkServerMetaUri=local://./" + kTestPrefix +
      "2/chunkserver.dat" },  // NOLINT
    { " -copySetUri=local://./" + kTestPrefix + "2/copysets" },
    { " -raftSnapshotUri=curve://./" + kTestPrefix + "2/copysets" },
    { " -recycleUri=local://./" + kTestPrefix + "2/recycler" },
    { " -chunkFilePoolDir=./" + kTestPrefix + "2/chunkfilepool/" },
    { " -chunkFilePoolMetaPath=./" + kTestPrefix +
      "2/chunkfilepool.meta" },  // NOLINT
    std::string(" -conf=") + kCSConfigPath,
    { " -raft_sync_segments=true" },
    std::string(" --log_dir=") + kLogPath,
    { " --stderrthreshold=3" },
    { " -raftLogUri=curve://./" + kTestPrefix + "2/copysets" },
    { " -walFilePoolDir=./" + kTestPrefix + "2/walfilepool/" },
    { " -walFilePoolMetaPath=./" + kTestPrefix +
        "2/walfilepool.meta" },
};

const std::vector<std::string> chunkserverConf3{
    { " --graceful_quit_on_sigterm" },
    { " -chunkServerStoreUri=local://./" + kTestPrefix + "3/" },
    { " -chunkServerMetaUri=local://./" + kTestPrefix +
      "3/chunkserver.dat" },  // NOLINT
    { " -copySetUri=local://./" + kTestPrefix + "3/copysets" },
    { " -raftSnapshotUri=curve://./" + kTestPrefix + "3/copysets" },
    { " -recycleUri=local://./" + kTestPrefix + "3/recycler" },
    { " -chunkFilePoolDir=./" + kTestPrefix + "3/chunkfilepool/" },
    { " -chunkFilePoolMetaPath=./" + kTestPrefix +
      "3/chunkfilepool.meta" },  // NOLINT
    std::string(" -conf=") + kCSConfigPath,
    { " -raft_sync_segments=true" },
    std::string(" --log_dir=") + kLogPath,
    { " --stderrthreshold=3" },
    { " -raftLogUri=curve://./" + kTestPrefix + "3/copysets" },
    { " -walFilePoolDir=./" + kTestPrefix + "3/walfilepool/" },
    { " -walFilePoolMetaPath=./" + kTestPrefix +
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
    std::string(" --conf=") + kSCSConfigPath,
    std::string(" --log_dir=") + kLogPath,
    { " --stderrthreshold=3" },
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

        // 初始化db
        system(std::string("rm -rf " + kTestPrefix + "t.etcd").c_str());
        system(std::string("rm -rf " + kTestPrefix + "1").c_str());
        system(std::string("rm -rf " + kTestPrefix + "2").c_str());
        system(std::string("rm -rf " + kTestPrefix + "3").c_str());

        // 启动etcd
        pid_t pid = cluster_->StartSingleEtcd(
            1, kEtcdClientIpPort, kEtcdPeerIpPort,
            std::vector<std::string>{ " --name " + kTestPrefix });
        LOG(INFO) << "etcd 1 started on " << kEtcdPeerIpPort
                  << ", pid = " << pid;
        ASSERT_GT(pid, 0);

        cluster_->InitSnapshotCloneMetaStoreEtcd(kEtcdClientIpPort);

        cluster_->PrepareConfig<MDSConfigGenerator>(kMdsConfigPath,
                                                    mdsConfigOptions);

        // 启动一个mds
        pid = cluster_->StartSingleMDS(1, kMdsIpPort, kMdsDummyPort, mdsConf1,
                                       true);
        LOG(INFO) << "mds 1 started on " << kMdsIpPort << ", pid = " << pid;
        ASSERT_GT(pid, 0);

        // 创建物理池
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

        // 创建chunkserver
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

        // 创建逻辑池, 并睡眠一段时间让底层copyset先选主
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
        // 每个chunk写前面4k数据, 写两个segment
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
        system(std::string("rm -rf " + kTestPrefix + "t.etcd").c_str());
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

// 常规测试用例
// 场景一：快照增加删除查找
TEST_F(SnapshotCloneServerTest, TestSnapshotAddDeleteGet) {
    std::string uuid1;
    int ret = 0;
    // 操作1：用户testUser1_对不存在的文件打快照
    // 预期1：返回文件不存在
    ret = MakeSnapshot(testUser1_, "/ItUser1/notExistFile", "snap1", &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    // 操作2：用户testUser2_对testFile1_打快照
    // 预期2：返回用户认证失败
    ret = MakeSnapshot(testUser2_, testFile1_, "snap1", &uuid1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    // 操作3：用户testUser1_对testFile1_打快照snap1。
    // 预期3：打快照成功
    ret = MakeSnapshot(testUser1_, testFile1_, "snap1", &uuid1);
    ASSERT_EQ(0, ret);

    std::string fakeData(4096, 'y');
    ASSERT_TRUE(WriteFile(testFile1_, testUser1_, fakeData));
    ASSERT_TRUE(CheckFileData(testFile1_, testUser1_, fakeData));

    // 操作4: 获取快照信息，user=testUser1_，filename=testFile1_
    // 预期4：返回快照snap1的信息
    bool success1 = CheckSnapshotSuccess(testUser1_, testFile1_, uuid1);
    ASSERT_TRUE(success1);

    // 操作5：获取快照信息，user=testUser2_，filename=testFile1_
    // 预期5：返回用户认证失败
    FileSnapshotInfo info1;
    ret = GetSnapshotInfo(testUser2_, testFile1_, uuid1, &info1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    // 操作6：获取快照信息，user=testUser2_，filename=testFile2_
    // 预期6：返回空
    std::vector<FileSnapshotInfo> infoVec;
    ret = ListFileSnapshotInfo(testUser2_, testFile2_, 10, 0, &infoVec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, infoVec.size());

    // 操作7：testUser2_删除快照snap1
    // 预期7：返回用户认证失败
    ret = DeleteSnapshot(testUser2_, testFile1_, uuid1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    // 操作8：testUser1_删除testFile2_的快照，ID为snap1
    // 预期8：返回文件名不匹配
    ret = DeleteSnapshot(testUser1_, testFile2_, uuid1);
    ASSERT_EQ(kErrCodeFileNameNotMatch, ret);

    // 操作9：testUser1_删除快照snap1
    // 预期9：返回删除成功
    ret = DeleteAndCheckSnapshotSuccess(testUser1_, testFile1_, uuid1);
    ASSERT_EQ(0, ret);

    //  操作10：获取快照信息，user=testUser1_，filename=testFile1_
    //  预期10：返回空
    ret = ListFileSnapshotInfo(testUser1_, testFile1_, 10, 0, &infoVec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, infoVec.size());

    // 操作11：testUser1_删除快照snap1（重复删除）
    // 预期11：返回删除成功
    ret = DeleteAndCheckSnapshotSuccess(testUser1_, testFile1_, uuid1);
    ASSERT_EQ(0, ret);

    // 复原testFile1_
    std::string fakeData2(4096, 'x');
    ASSERT_TRUE(WriteFile(testFile1_, testUser1_, fakeData2));
}

// 场景二：取消快照
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
                    // 操作1：用户testUser1_对testFile1_打快照snap1，
                    //        在快照未完成前testUser2_取消testFile1_的快照snap1
                    // 预期1：取消用户认证失败
                    int retCode = CancelSnapshot(testUser2_, testFile1_, uuid1);
                    ASSERT_EQ(kErrCodeInvalidUser, retCode);

                    // 操作2：用户testUser1_对testFile1_打快照snap1，
                    //        在快照未完成前testUser1_取消testFile1_
                    //        的不存在的快照
                    // 预期2：返回kErrCodeCannotCancelFinished
                    retCode =
                        CancelSnapshot(testUser1_, testFile1_, "notExistUUId");
                    ASSERT_EQ(kErrCodeCannotCancelFinished, retCode);

                    // 操作3：用户testUser1_对testFile1_打快照snap1，
                    //        在快照未完成前testUser1_取消testFile2_的快照snap1
                    // 预期3:  返回文件名不匹配
                    retCode = CancelSnapshot(testUser1_, testFile2_, uuid1);
                    ASSERT_EQ(kErrCodeFileNameNotMatch, retCode);

                    // 操作4：用户testUser1_对testFile1_打快照，
                    //        在快照未完成前testUser1_取消快照snap1
                    // 预期4：取消快照成功
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
            // 操作5：获取快照信息，user=testUser1_，filename=testFile1_
            // 预期5：返回空
            success1 = true;
            break;
        }
    }
    ASSERT_TRUE(success1);

    // 操作6:  在快照已完成后，testUser1_取消testFile1_的快照snap1
    // 预期6： 返回待取消的快照不存在或已完成
    ret = CancelSnapshot(testUser1_, testFile1_, uuid1);
    ASSERT_EQ(kErrCodeCannotCancelFinished, ret);
}

// 场景三：lazy快照克隆场景
TEST_F(SnapshotCloneServerTest, TestSnapLazyClone) {
    std::string snapId;
    PrepareSnapshotForTestFile1(&snapId);

    // 操作1: testUser1_ clone不存在的快照，fileName=SnapLazyClone1
    // 预期1：返回快照不存在
    std::string uuid1, uuid2, uuid3, uuid4, uuid5;
    int ret;
    ret = CloneOrRecover("Clone", testUser1_, "UnExistSnapId1",
                         "/ItUser1/SnapLazyClone1", true, &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    // 操作2：testUser2_ clone快照snap1，fileName=SnapLazyClone1
    // 预期2:  返回用户认证失败
    ret = CloneOrRecover("Clone", testUser2_, snapId, "/ItUser2/SnapLazyClone1",
                         true, &uuid2);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    // 操作3：testUser1_ clone 快照snap1，fileName=SnapLazyClone1
    // 预期3  返回克隆成功
    std::string dstFile = "/ItUser1/SnapLazyClone1";
    ret = CloneOrRecover("Clone", testUser1_, snapId, dstFile, true, &uuid3);
    ASSERT_EQ(0, ret);

    // 操作4： testUser1_ clone 块照snap1，fileName=SnapLazyClone1 （重复克隆）
    // 预期4：返回克隆成功（幂等）
    ret = CloneOrRecover("Clone", testUser1_, snapId, "/ItUser1/SnapLazyClone1",
                         true, &uuid4);
    ASSERT_EQ(0, ret);

    // Flatten
    ret = Flatten(testUser1_, uuid3);
    ASSERT_EQ(0, ret);

    // 操作5： testUser1_ GetCloneTask
    // 预期5：返回SnapLazyClone1的clone 任务
    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid3, true);
    ASSERT_TRUE(success1);

    // 操作6： testUser2_ GetCloneTask
    // 预期6： 返回空
    std::vector<TaskCloneInfo> infoVec;
    ret = ListCloneTaskInfo(testUser2_, 10, 0, &infoVec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, infoVec.size());

    // 操作7： testUser2_ CleanCloneTask UUID为SnapLazyClone1的UUID
    // 预期7：返回用户认证失败
    ret = CleanCloneTask(testUser2_, uuid3);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    // 操作8： testUser1_ CleanCloneTask UUID为SnapLazyClone1的UUID
    // 预期8：返回执行成功
    ret = CleanCloneTask(testUser1_, uuid3);
    ASSERT_EQ(0, ret);

    // 等待清理完成
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // 操作9： testUser1_ CleanCloneTask UUID为SnapLazyClone1的UUID（重复执行）
    // 预期9：返回执行成功
    ret = CleanCloneTask(testUser1_, uuid3);
    ASSERT_EQ(0, ret);

    // 操作10：testUser1_ GetCloneTask
    // 预期10：返回空
    TaskCloneInfo info;
    ret = GetCloneTaskInfo(testUser1_, uuid3, &info);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    // 验证数据正确性
    std::string fakeData(4096, 'x');
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData));
}

// 场景四：非lazy快照克隆场景
TEST_F(SnapshotCloneServerTest, TestSnapNotLazyClone) {
    std::string snapId;
    PrepareSnapshotForTestFile1(&snapId);

    // 操作1: testUser1_ clone不存在的快照，fileName=SnapNotLazyClone1
    // 预期1：返回快照不存在
    std::string uuid1;
    int ret;
    ret = CloneOrRecover("Clone", testUser1_, "UnExistSnapId2",
                         "/ItUser1/SnapNotLazyClone1", false, &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    // 操作2：testUser2_ clone快照snap1，fileName=SnapNotLazyClone1
    // 预期2:  返回用户认证失败
    ret = CloneOrRecover("Clone", testUser2_, snapId,
                         "/ItUser2/SnapNotLazyClone1", false, &uuid1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    // 操作3：testUser1_ clone 快照snap1，fileName=SnapNotLazyClone1
    // 预期3  返回克隆成功
    std::string dstFile = "/ItUser1/SnapNotLazyClone1";
    ret = CloneOrRecover("Clone", testUser1_, snapId, dstFile, false, &uuid1);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid1, true);
    ASSERT_TRUE(success1);

    // 操作4： testUser1_ clone 块照snap1，
    // fileName=SnapNotLazyClone1 （重复克隆）
    // 预期4：返回克隆成功（幂等）
    ret = CloneOrRecover("Clone", testUser1_, snapId,
                         "/ItUser1/SnapNotLazyClone1", false, &uuid1);
    ASSERT_EQ(0, ret);

    // 验证数据正确性
    std::string fakeData(4096, 'x');
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData));
}

// 场景五：lazy快照恢复场景
TEST_F(SnapshotCloneServerTest, TestSnapLazyRecover) {
    std::string snapId;
    PrepareSnapshotForTestFile1(&snapId);

    // 操作1: testUser1_ Recover不存在的快照，fileName=testFile1_
    // 预期1：返回快照不存在
    std::string uuid1;
    int ret;
    ret = CloneOrRecover("Recover", testUser1_, "UnExistSnapId3", testFile1_,
                         true, &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    // 操作2：testUser2_ Recover快照snap1，fileName=testFile1_
    // 预期2:  返回用户认证失败
    ret =
        CloneOrRecover("Recover", testUser2_, snapId, testFile1_, true, &uuid1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    // 操作3：testUser1_ Recover快照snap1，fileName=testFile1_
    // 预期3  返回恢复成功
    ret =
        CloneOrRecover("Recover", testUser1_, snapId, testFile1_, true, &uuid1);
    ASSERT_EQ(0, ret);

    // Flatten
    ret = Flatten(testUser1_, uuid1);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid1, false);
    ASSERT_TRUE(success1);

    // 验证数据正确性
    std::string fakeData(4096, 'x');
    ASSERT_TRUE(CheckFileData(testFile1_, testUser1_, fakeData));

    // 操作4：testUser1_ recover 快照snap1，目标文件为不存在的文件
    // 预期4:  返回目标文件不存在
    ret = CloneOrRecover("Recover", testUser1_, snapId, "/ItUser1/notExistFile",
                         true, &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);
}

// 场景六：非lazy快照恢复场景
TEST_F(SnapshotCloneServerTest, TestSnapNotLazyRecover) {
    std::string snapId;
    PrepareSnapshotForTestFile1(&snapId);

    // 操作1: testUser1_ Recover不存在的快照，fileName=testFile1_
    // 预期1：返回快照不存在
    std::string uuid1;
    int ret;
    ret = CloneOrRecover("Recover", testUser1_, "UnExistSnapId4", testFile1_,
                         false, &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    // 操作2：testUser2_ Recover快照snap1，fileName=testFile1_
    // 预期2:  返回用户认证失败
    ret = CloneOrRecover("Recover", testUser2_, snapId, testFile1_, false,
                         &uuid1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    // 操作3：testUser1_ Recover快照snap1，fileName=testFile1_
    // 预期3  返回恢复成功
    ret = CloneOrRecover("Recover", testUser1_, snapId, testFile1_, false,
                         &uuid1);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid1, false);
    ASSERT_TRUE(success1);

    // 验证数据正确性
    std::string fakeData(4096, 'x');
    ASSERT_TRUE(CheckFileData(testFile1_, testUser1_, fakeData));

    // 操作4：testUser1_ recover 快照snap1，目标文件为不存在的文件
    // 预期4:  返回目标文件不存在
    ret = CloneOrRecover("Recover", testUser1_, snapId, "/ItUser1/notExistFile",
                         false, &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);
}

// 场景七: lazy镜像克隆场景
TEST_F(SnapshotCloneServerTest, TestImageLazyClone) {
    // 操作1: testUser1_ clone不存在的镜像，fileName=ImageLazyClone1
    // 预期1：返回文件不存在
    std::string uuid1, uuid2, uuid3, uuid4;
    int ret;
    ret = CloneOrRecover("Clone", testUser1_, "/UnExistFile",
                         "/ItUser1/ImageLazyClone1", true, &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    // 操作2：testUser1_ clone 镜像testFile1_，fileName=ImageLazyClone1
    // 预期2  返回克隆成功
    std::string dstFile = "/ItUser1/ImageLazyClone1";
    ret =
        CloneOrRecover("Clone", testUser1_, testFile1_, dstFile, true, &uuid2);
    ASSERT_EQ(0, ret);

    // 操作3： testUser1_ clone 镜像testFile1_，
    // fileName=ImageLazyClone1 （重复克隆）
    // 预期3：返回克隆成功（幂等）
    ret = CloneOrRecover("Clone", testUser1_, testFile1_,
                         "/ItUser1/ImageLazyClone1", true, &uuid3);
    ASSERT_EQ(0, ret);

    // 操作4：对未完成lazy克隆的文件ImageLazyClone1打快照snap1
    // 预期4：返回文件状态异常
    ret = MakeSnapshot(testUser1_, testFile1_, "snap1", &uuid4);
    ASSERT_EQ(kErrCodeFileStatusInvalid, ret);
    FileSnapshotInfo info2;
    int retCode = GetSnapshotInfo(testUser1_, testFile1_, uuid4, &info2);
    ASSERT_EQ(kErrCodeFileNotExist, retCode);

    ASSERT_TRUE(WaitMetaInstalledSuccess(testUser1_, uuid2, true));

    // Flatten之前验证数据正确性
    std::string fakeData1(4096, 'x');
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData1));

    // Flatten
    ret = Flatten(testUser1_, uuid2);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid2, true);
    ASSERT_TRUE(success1);

    // Flatten之后验证数据正确性
    std::string fakeData2(4096, 'x');
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData2));
}

// 场景八：非lazy镜像克隆场景
TEST_F(SnapshotCloneServerTest, TestImageNotLazyClone) {
    // 操作1: testUser1_ clone不存在的镜像，fileName=ImageNotLazyClone1
    // 预期1：返回快照不存在
    std::string uuid1;
    int ret;
    ret = CloneOrRecover("Clone", testUser1_, "/UnExistFile",
                         "/ItUser1/ImageNotLazyClone1", false, &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    // 操作2：testUser1_ clone 镜像testFile1_，fileName=ImageNotLazyClone1
    // 预期2  返回克隆成功
    std::string dstFile = "/ItUser1/ImageNotLazyClone1";
    ret =
        CloneOrRecover("Clone", testUser1_, testFile1_, dstFile, false, &uuid1);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid1, true);
    ASSERT_TRUE(success1);

    // 操作3： testUser1_ clone 镜像testFile1_，
    // fileName=ImageNotLazyClone1 （重复克隆）
    // 预期3：返回克隆成功（幂等）
    ret = CloneOrRecover("Clone", testUser1_, testFile1_,
                         "/ItUser1/ImageNotLazyClone1", false, &uuid1);
    ASSERT_EQ(0, ret);

    // 验证数据正确性
    std::string fakeData(4096, 'x');
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData));
}

// 场景九：快照存在失败场景
TEST_F(SnapshotCloneServerTest, TestSnapAndCloneWhenSnapHasError) {
    std::string snapId = "errorSnapUuid";
    SnapshotInfo snapInfo(snapId, testUser1_, testFile4_, "snapxxx", 0, 0, 0, 0,
                        0, Status::error);

    cluster_->metaStore_->AddSnapshot(snapInfo);

    pid_t pid = cluster_->RestartSnapshotCloneServer(1);
    LOG(INFO) << "SnapshotCloneServer 1 restarted, pid = " << pid;
    ASSERT_GT(pid, 0);
    std::string uuid1, uuid2;

    // 操作1:  lazy clone 快照snap1
    // 预期1：返回快照存在异常
    int ret = CloneOrRecover("Clone", testUser1_, snapId,
                             "/ItUser2/SnapLazyClone1", true, &uuid2);
    ASSERT_EQ(kErrCodeInvalidSnapshot, ret);

    // 操作2：非lazy clone 快照snap1
    // 预期2：返回快照存在异常
    ret = CloneOrRecover("Clone", testUser1_, snapId,
                         "/ItUser2/SnapNotLazyClone1", false, &uuid2);
    ASSERT_EQ(kErrCodeInvalidSnapshot, ret);

    // 操作3：lazy 从 快照snap1 recover
    // 预期3：返回快照存在异常
    ret =
        CloneOrRecover("Recover", testUser1_, snapId, testFile4_, true, &uuid2);
    ASSERT_EQ(kErrCodeInvalidSnapshot, ret);

    // 操作4：非lazy 从 快照snap1 recover
    // 预期4：返回快照存在异常
    ret = CloneOrRecover("Recover", testUser1_, snapId, testFile4_, false,
                         &uuid2);
    ASSERT_EQ(kErrCodeInvalidSnapshot, ret);

    // 操作5：用户testUser1_对testFile4_打快照snap1
    // 预期5：清理失败快照，并打快照成功
    ret = MakeSnapshot(testUser1_, testFile4_, "snap1", &uuid1);
    ASSERT_EQ(0, ret);

    // 校验快照成功
    bool success1 = CheckSnapshotSuccess(testUser1_, testFile4_, uuid1);
    ASSERT_TRUE(success1);

    // 校验清理失败快照成功
    FileSnapshotInfo info1;
    int retCode = GetSnapshotInfo(testUser1_, testFile4_, snapId, &info1);
    ASSERT_EQ(kErrCodeFileNotExist, retCode);
}

// [线上问题修复]克隆失败，回滚删除克隆卷，再次创建同样的uuid的卷的场景
TEST_F(SnapshotCloneServerTest, TestCloneHasSameDest) {
    std::string uuid1, uuid2, uuid3, uuid4, uuid5, uuid6, uuid7;
    // 操作1：testUser1_ clone 镜像testFile1_，fileName=CloneHasSameDestUUID
    // 预期1  返回克隆成功
    std::string dstFile = "/ItUser1/CloneHasSameDest";
    int ret =
        CloneOrRecover("Clone", testUser1_, testFile1_, dstFile, true, &uuid1);
    ASSERT_EQ(0, ret);

    // 删除克隆卷
    UserInfo_t userinfo;
    userinfo.owner = testUser1_;
    int ret2 = fileClient_->Unlink(dstFile, userinfo, false);
    ASSERT_EQ(0, ret2);


    // 操作2：testUser1_ 再次clone 镜像testFile1_，
    // fileName=CloneHasSameDestUUID
    // 预期2  返回克隆成功
    ret =
        CloneOrRecover("Clone", testUser1_, testFile1_, dstFile, true, &uuid2);
    ASSERT_EQ(0, ret);

    // 验证数据正确性
    std::string fakeData(4096, 'x');
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData));

    // 操作3：testUser1_ clone 镜像testFile1_，fileName=CloneHasSameDest2
    // 预期3  返回克隆成功
    dstFile = "/ItUser1/CloneHasSameDest2";
    ret =
        CloneOrRecover("Clone", testUser1_, testFile1_, dstFile, true, &uuid3);
    ASSERT_EQ(0, ret);

    // 删除克隆卷
    UserInfo_t userinfo2;
    userinfo2.owner = testUser1_;
    ret2 = fileClient_->Unlink(dstFile, userinfo2, false);
    ASSERT_EQ(0, ret2);


    // 操作4：testUser1_ 再次clone 镜像testFile2_，
    // fileName=CloneHasSameDest2
    // 预期4  返回克隆成功
    ret =
        CloneOrRecover("Clone", testUser1_, testFile2_, dstFile, true, &uuid4);
    ASSERT_EQ(0, ret);

    // 验证数据正确性
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData));

    // 验证再次克隆lazyflag不同的情况
    // 操作5：testUser1_ clone 镜像testFile1_，fileName=CloneHasSameDest3
    // 预期5  返回克隆成功
    dstFile = "/ItUser1/CloneHasSameDest3";
    ret =
        CloneOrRecover("Clone", testUser1_, testFile1_, dstFile, true, &uuid5);
    ASSERT_EQ(0, ret);

    // 删除克隆卷
    UserInfo_t userinfo3;
    userinfo2.owner = testUser1_;
    ret2 = fileClient_->Unlink(dstFile, userinfo2, false);
    ASSERT_EQ(0, ret2);

    // 操作6：testUser1_ 再次非lazy clone 镜像testFile2_，
    // fileName=CloneHasSameDest3
    // 预期6  返回克隆成功
    ret =
        CloneOrRecover("Clone", testUser1_, testFile2_, dstFile, false, &uuid6);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid6, true);
    ASSERT_TRUE(success1);

    // 验证数据正确性
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData));

    // 删除克隆卷
    UserInfo_t userinfo4;
    userinfo2.owner = testUser1_;
    ret2 = fileClient_->Unlink(dstFile, userinfo2, false);
    ASSERT_EQ(0, ret2);

    // 操作7：testUser1_ 再次非lazy clone 镜像testFile2_，
    // fileName=CloneHasSameDest3
    // 预期7  返回克隆成功
    ret =
        CloneOrRecover("Clone", testUser1_, testFile2_, dstFile, true, &uuid7);
    ASSERT_EQ(0, ret);

    // 验证数据正确性
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fakeData));
}

// lazy克隆卷，删除克隆卷，再删除源卷，源卷需要可以删除
TEST_F(SnapshotCloneServerTest, TestDeleteLazyCloneDestThenDeleteSrc) {
    // 操作1：testUser1_ clone 镜像testFile5_，lazy克隆两个卷dstFile1，dstFile2
    // 预期1  返回克隆成功
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

    // 删除源卷，删除失败，卷被占用

    ret = fileClient_->Unlink(testFile5_, userinfo, false);
    ASSERT_EQ(-27, ret);

    // 操作2：删除目的卷dstFile1成功，再次删除源卷
    // 预期2 删除失败，卷被占用
    ret = fileClient_->Unlink(dstFile1, userinfo, false);
    ASSERT_EQ(0, ret);

    ret = fileClient_->Unlink(testFile5_, userinfo, false);
    ASSERT_EQ(-27, ret);


    // 操作3：删除目的卷dstFile2成功，再次删除源卷
    // 预期3 删除成功
    ret = fileClient_->Unlink(dstFile2, userinfo, false);
    ASSERT_EQ(0, ret);

    ret = fileClient_->Unlink(testFile5_, userinfo, false);
    ASSERT_EQ(0, ret);

    // 操作4： 等待一段时间，看垃圾记录后台能否删除
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
