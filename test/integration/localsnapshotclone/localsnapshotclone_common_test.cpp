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
 * Created Date: 2023-06-27
 * Author: xuchaojie
 */


#include <gtest/gtest.h>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <json/json.h>

#include "test/integration/cluster_common/cluster.h"
#include "src/client/libcurve_file.h"
#include "src/client/libcurve_snapshot.h"

using curve::CurveCluster;
using curve::client::FileClient;
using curve::client::SnapshotClient;
using curve::client::UserInfo_t;

const std::string kTestPrefix = "LSCTest";  // NOLINT
                                            //
const uint64_t chunkSize = 16ULL * 1024 * 1024;
const uint64_t segmentSize = 32ULL * 1024 * 1024;
const uint64_t chunkGap = 1;

const char* kEtcdClientIpPort = "127.0.0.1:11001";
const char* kEtcdPeerIpPort = "127.0.0.1:11002";
const char* kMdsIpPort = "127.0.0.1:11003";
const char* kChunkServerIpPort1 = "127.0.0.1:11004";
const char* kChunkServerIpPort2 = "127.0.0.1:11005";
const char* kChunkServerIpPort3 = "127.0.0.1:11006";

const int kMdsDummyPort = 11008;

const char* kLeaderCampaginPrefix = "localsnapshotcloneleaderlock0";

const std::string kLogPath = "./runlog/" + kTestPrefix + "Log";  // NOLINT
const std::string kMdsDbName = kTestPrefix + "DB";               // NOLINT
                                                                 //
const std::string kMdsConfigPath =                               // NOLINT
    "./test/integration/localsnapshotclone/config/" + kTestPrefix +
    "_mds.conf";

const std::string kCSConfigPath =                                // NOLINT
    "./test/integration/localsnapshotclone/config/" + kTestPrefix +
    "_chunkserver.conf";

const std::string kSnapClientConfigPath =                        // NOLINT
    "./test/integration/snapshotcloneserver/config/" + kTestPrefix +
    "_snap_client.conf";

const std::string kClientConfigPath =                            // NOLINT
    "./test/integration/localsnapshotclone/config/" + kTestPrefix +
    "_client.conf";

const std::vector<std::string> mdsConfigOptions{
    std::string("mds.listen.addr=") + kMdsIpPort,
    std::string("mds.etcd.endpoint=") + kEtcdClientIpPort,
    std::string("mds.DbName=") + kMdsDbName,
    std::string("mds.file.expiredTimeUs=50000"),
    std::string("mds.file.expiredTimeUs=10000"),
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
    "walfilepool.use_chunk_file_pool=false",
    "walfilepool.enable_get_segment_from_pool=false",
    "clone.disable_curve_client=true",
    "clone.disable_s3_adapter=true",
};

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
    { " -v 9" }
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
    { " -v 9" }
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
    { " -v 9" }
};

const std::vector<std::string> clientConfigOptions{
    std::string("mds.listen.addr=") + kMdsIpPort,
    std::string("global.logPath=") + kLogPath,
    std::string("mds.rpcTimeoutMS=4000"),
};

const std::vector<std::string> snapClientConfigOptions{
    std::string("mds.listen.addr=") + kMdsIpPort,
    std::string("global.logPath=") + kLogPath,
    std::string("mds.rpcTimeoutMS=4000"),
};


namespace curve {
namespace client {

class LocalSnapshotCloneTest: public ::testing::Test {
 public:
    static void SetUpTestCase() {
        std::string mkLogDirCmd = std::string("mkdir -p ") + kLogPath;
        system(mkLogDirCmd.c_str());

        cluster_ = new CurveCluster();
        ASSERT_NE(nullptr, cluster_);

        // 初始化db
        system(std::string("rm -rf " + kTestPrefix + ".etcd").c_str());
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
                         "./test/integration/localsnapshotclone/"
                         "config/topo.json"));  // NOLINT

        // format chunkfilepool and walfilepool
        std::vector<std::thread> threadpool(3);

        threadpool[0] =
            std::thread(&CurveCluster::FormatFilePool, cluster_,
                        "./" + kTestPrefix + "1/chunkfilepool/",
                        "./" + kTestPrefix + "1/chunkfilepool.meta",
                        "./" + kTestPrefix + "1/chunkfilepool/", 2);
        threadpool[1] =
            std::thread(&CurveCluster::FormatFilePool, cluster_,
                        "./" + kTestPrefix + "2/chunkfilepool/",
                        "./" + kTestPrefix + "2/chunkfilepool.meta",
                        "./" + kTestPrefix + "2/chunkfilepool/", 2);
        threadpool[2] =
            std::thread(&CurveCluster::FormatFilePool, cluster_,
                        "./" + kTestPrefix + "3/chunkfilepool/",
                        "./" + kTestPrefix + "3/chunkfilepool.meta",
                        "./" + kTestPrefix + "3/chunkfilepool/", 2);
        for (int i = 0; i < 3; i++) {
            threadpool[i].join();
        }

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
                         "./test/integration/localsnapshotclone/config/"
                         "topo.json"));

        cluster_->PrepareConfig<ClientConfigGenerator>(kClientConfigPath,
                                                       clientConfigOptions);

        fileClient_ = new FileClient();
        fileClient_->Init(kClientConfigPath);

        cluster_->PrepareConfig<SnapClientConfigGenerator>(
            kSnapClientConfigPath, snapClientConfigOptions);

        snapClient_ = new SnapshotClient();
        snapClient_->Init(kSnapClientConfigPath);

        UserInfo_t userinfo;
        userinfo.owner = "ItUser1";

        ASSERT_EQ(0, fileClient_->Mkdir("/ItUser1", userinfo));
    }

    static void TearDownTestCase() {
        fileClient_->UnInit();
        delete fileClient_;
        fileClient_ = nullptr;
        snapClient_->UnInit();
        delete snapClient_;
        snapClient_ = nullptr;
        ASSERT_EQ(0, cluster_->StopCluster());
        delete cluster_;
        cluster_ = nullptr;
        system(std::string("rm -rf " + kTestPrefix + ".etcd").c_str());
        system(std::string("rm -rf " + kTestPrefix + "1").c_str());
        system(std::string("rm -rf " + kTestPrefix + "2").c_str());
        system(std::string("rm -rf " + kTestPrefix + "3").c_str());
    }

    static bool CreateAndWriteFile(const std::string& fileName,
                                   const std::string& user,
                                   const std::string& dataSample) {
        if (CreateFile(fileName, user)) {
            return WriteFile(fileName, user, dataSample);
        } else {
            return false;
        }
    }

    static bool CreateFile(const std::string& fileName,
                           const std::string& user) {
        UserInfo_t userinfo;
        userinfo.owner = user;
        int ret =
            fileClient_->Create(fileName, userinfo, 10ULL * 1024 * 1024 * 1024);
        if (ret < 0) {
            LOG(ERROR) << "Create fail, ret = " << ret;
            return false;
        }
        return true;
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
            for (int i = 0; i < data.size(); i++) {
                if (data[i] != dataSample[i]) {
                    LOG(ERROR) << "CheckFileData not Equal, offset = "
                               << i * chunkSize * chunkGap
                               << ", diff i = " << i
                               << ", data = [" << data
                               << "] , expect data = ["
                               << dataSample << "].";
                    return false;
                }
            }
        }
        ret = fileClient_->Close(dstFd);
        if (ret < 0) {
            LOG(ERROR) << "Close fail, ret = " << ret;
            return false;
        }
        return true;
    }

    static bool WaitFlattenSuccess(const std::string& fileName,
                                   const UserInfo_t userinfo1) {
        FileStatus fileStatus = FileStatus::Flattening;
        uint32_t progress = 0;
        for (int i = 0; i < 30; i++) {
            if (0 != snapClient_->QueryFlattenStatus(fileName, userinfo1,
                    &fileStatus, &progress)) {
                LOG(ERROR) << "QueryFlattenStatus fail";
                return false;
            }
            if (fileStatus != FileStatus::Flattening) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        if (FileStatus::Created != fileStatus || progress != 100) {
            LOG(ERROR) << "WaitFlattenSuccess fail, fileStatus = "
                       << static_cast<int>(fileStatus)
                       << ", progress = " << progress;
            return false;
        }
        return true;
    }

    void SetUp() {}

    void TearDown() {}

    static CurveCluster* cluster_;
    static FileClient* fileClient_;
    static SnapshotClient *snapClient_;

    std::string testUser1_ = "ItUser1";
};

CurveCluster* LocalSnapshotCloneTest::cluster_ = nullptr;
FileClient* LocalSnapshotCloneTest::fileClient_ = nullptr;
SnapshotClient *LocalSnapshotCloneTest::snapClient_ = nullptr;


TEST_F(LocalSnapshotCloneTest, TestSnapshotOnceAndClone) {
    std::string testFile1_ = "/ItUser1/file1";
    std::string testFile1Clone1 = "/ItUser1/file1clone1";
    std::string testFile1Clone2 = "/ItUser1/file1clone2";

    ASSERT_TRUE(CreateFile(testFile1_, testUser1_));
    // 先对空卷打一次快照
    uint64_t seq0 = 0;
    std::string testFile1Snap0 = testFile1_ + "@snap0";
    UserInfo_t userinfo1;
    userinfo1.owner = testUser1_;
    ASSERT_EQ(0, snapClient_->CreateSnapShot(testFile1Snap0, userinfo1, &seq0));

    // testFile1_写x
    std::string fakeData(4096, 'x');
    ASSERT_TRUE(WriteFile(testFile1_, testUser1_, fakeData));
    LOG(INFO) << "Write testFile1_ success.";

    // testFile1_打快照
    std::string testFile1Snap1 = testFile1_ + "@snap1";
    uint64_t seq = 0;
    ASSERT_EQ(0, snapClient_->CreateSnapShot(testFile1Snap1, userinfo1, &seq));

    // testFile1_写y
    std::string fakeData2(4096, 'y');
    ASSERT_TRUE(WriteFile(testFile1_, testUser1_, fakeData2));

    ASSERT_EQ(0, snapClient_->ProtectSnapShot(testFile1Snap1, userinfo1));
    // 从testFile1_的快照克隆出testFile1Clone1, 并断言数据为x
    FInfo finfo;
    ASSERT_EQ(0, snapClient_->Clone(testFile1Snap1, testFile1Clone1, userinfo1,
            &finfo));

    ASSERT_TRUE(CheckFileData(testFile1Clone1, testUser1_, fakeData));

    // 从testFile1_的快照克隆出testFile1Clone2, 并断言数据为x
    FInfo finfo2;
    ASSERT_EQ(0, snapClient_->Clone(testFile1Snap1, testFile1Clone2, userinfo1,
            &finfo));
    ASSERT_TRUE(CheckFileData(testFile1Clone2, testUser1_, fakeData));

    // 写克隆卷testFile1Clone1，并断言
    std::string fakeData3(4096, 'a');
    ASSERT_TRUE(WriteFile(testFile1Clone1, testUser1_, fakeData3));
    ASSERT_TRUE(CheckFileData(testFile1Clone1, testUser1_, fakeData3));
    // 对克隆卷testFile1Clone1的写，不影响testFile1Clone2和testFile1
    ASSERT_TRUE(CheckFileData(testFile1Clone2, testUser1_, fakeData));
    ASSERT_TRUE(CheckFileData(testFile1_, testUser1_, fakeData2));

    // 写克隆卷testFile1Clone2，并断言
    std::string fakeData4(4096, 'b');
    ASSERT_TRUE(WriteFile(testFile1Clone2, testUser1_, fakeData4));
    ASSERT_TRUE(CheckFileData(testFile1Clone2, testUser1_, fakeData4));
    // 对克隆卷testFile1Clone2的写，不影响testFile1Clone1
    ASSERT_TRUE(CheckFileData(testFile1Clone1, testUser1_, fakeData3));
    ASSERT_TRUE(CheckFileData(testFile1_, testUser1_, fakeData2));

    // 写testFile1, 不影响testFileClone1和testFileClone2
    std::string fakeData5(4096, 'c');
    ASSERT_TRUE(WriteFile(testFile1_, testUser1_, fakeData5));
    ASSERT_TRUE(CheckFileData(testFile1_, testUser1_, fakeData5));
    ASSERT_TRUE(CheckFileData(testFile1Clone1, testUser1_, fakeData3));
    ASSERT_TRUE(CheckFileData(testFile1Clone2, testUser1_, fakeData4));
}


TEST_F(LocalSnapshotCloneTest, TestSnapshot3TimesAndClone) {
    std::string testFile2_ = "/ItUser1/file2";
    std::string testFile2Clone1 = "/ItUser1/file2clone1";
    std::string testFile2Clone2 = "/ItUser1/file2clone2";
    std::string testFile2Clone3 = "/ItUser1/file2clone3";
    // testFile2_写x
    std::string fakeData(4096, 'x');
    ASSERT_TRUE(CreateAndWriteFile(testFile2_, testUser1_, fakeData));
    LOG(INFO) << "Write testFile2_ success.";

    // testFile2_打快照1
    std::string testFile2Snap1 = testFile2_ + "@snap1";
    uint64_t seq = 0;
    UserInfo_t userinfo1;
    userinfo1.owner = testUser1_;
    ASSERT_EQ(0, snapClient_->CreateSnapShot(testFile2Snap1, userinfo1, &seq));

    // testFile2_写y
    std::string fakeData2(4096, 'y');
    ASSERT_TRUE(WriteFile(testFile2_, testUser1_, fakeData2));

    // testFile2_打快照2
    std::string testFile2Snap2 = testFile2_ + "@snap2";
    uint64_t seq2 = 0;
    ASSERT_EQ(0, snapClient_->CreateSnapShot(testFile2Snap2, userinfo1, &seq2));

    // testFile2_写z
    std::string fakeData3(4096, 'z');
    ASSERT_TRUE(WriteFile(testFile2_, testUser1_, fakeData3));

    // testFile2_打快照3
    std::string testFile2Snap3 = testFile2_ + "@snap3";
    uint64_t seq3 = 0;
    ASSERT_EQ(0, snapClient_->CreateSnapShot(testFile2Snap3, userinfo1, &seq3));

    // testFile2_写a
    std::string fakeData4(4096, 'a');
    ASSERT_TRUE(WriteFile(testFile2_, testUser1_, fakeData4));

    ASSERT_EQ(0, snapClient_->ProtectSnapShot(testFile2Snap1, userinfo1));
    // 从testFile2_的快照1克隆出testFile2Clone1, 并断言数据为x
    FInfo finfo;
    ASSERT_EQ(0, snapClient_->Clone(testFile2Snap1, testFile2Clone1, userinfo1,
            &finfo));

    ASSERT_TRUE(CheckFileData(testFile2Clone1, testUser1_, fakeData));

    ASSERT_EQ(0, snapClient_->ProtectSnapShot(testFile2Snap2, userinfo1));
    // 从testFile2_的快照2克隆出testFile2Clone2, 并断言数据为y
    FInfo finfo2;
    ASSERT_EQ(0, snapClient_->Clone(testFile2Snap2, testFile2Clone2, userinfo1,
            &finfo2));

    ASSERT_TRUE(CheckFileData(testFile2Clone2, testUser1_, fakeData2));

    ASSERT_EQ(0, snapClient_->ProtectSnapShot(testFile2Snap3, userinfo1));
    // 从testFile2_的快照3克隆出testFile2Clone3, 并断言数据为z
    FInfo finfo3;
    ASSERT_EQ(0, snapClient_->Clone(testFile2Snap3, testFile2Clone3, userinfo1,
            &finfo3));

    ASSERT_TRUE(CheckFileData(testFile2Clone3, testUser1_, fakeData3));

    // 写testFile2，不影响另外3个卷
    std::string fakeData5(4096, 'b');
    ASSERT_TRUE(WriteFile(testFile2_, testUser1_, fakeData5));
    ASSERT_TRUE(CheckFileData(testFile2_, testUser1_, fakeData5));
    ASSERT_TRUE(CheckFileData(testFile2Clone1, testUser1_, fakeData));
    ASSERT_TRUE(CheckFileData(testFile2Clone2, testUser1_, fakeData2));
    ASSERT_TRUE(CheckFileData(testFile2Clone3, testUser1_, fakeData3));

    // 写testFile2Clone1, 不影响另外3个卷
    std::string fakeData6(4096, 'c');
    ASSERT_TRUE(WriteFile(testFile2Clone1, testUser1_, fakeData6));
    ASSERT_TRUE(CheckFileData(testFile2Clone1, testUser1_, fakeData6));
    ASSERT_TRUE(CheckFileData(testFile2_, testUser1_, fakeData5));
    ASSERT_TRUE(CheckFileData(testFile2Clone2, testUser1_, fakeData2));
    ASSERT_TRUE(CheckFileData(testFile2Clone3, testUser1_, fakeData3));

    // 写testFile2Clone2, 不影响另外3个卷
    std::string fakeData7(4096, 'd');
    ASSERT_TRUE(WriteFile(testFile2Clone2, testUser1_, fakeData7));
    ASSERT_TRUE(CheckFileData(testFile2Clone2, testUser1_, fakeData7));
    ASSERT_TRUE(CheckFileData(testFile2Clone1, testUser1_, fakeData6));
    ASSERT_TRUE(CheckFileData(testFile2_, testUser1_, fakeData5));
    ASSERT_TRUE(CheckFileData(testFile2Clone3, testUser1_, fakeData3));

    // 写testFileClone3, 不影响另外3个卷
    std::string fakeData8(4096, 'e');
    ASSERT_TRUE(WriteFile(testFile2Clone3, testUser1_, fakeData8));
    ASSERT_TRUE(CheckFileData(testFile2Clone3, testUser1_, fakeData8));
    ASSERT_TRUE(CheckFileData(testFile2Clone2, testUser1_, fakeData7));
    ASSERT_TRUE(CheckFileData(testFile2Clone1, testUser1_, fakeData6));
    ASSERT_TRUE(CheckFileData(testFile2_, testUser1_, fakeData5));
}

TEST_F(LocalSnapshotCloneTest, TestSnapshotAndClone3Times) {
    std::string testFile3_ = "/ItUser1/file3";
    std::string testFile3Clone1 = "/ItUser1/file3clone1";
    std::string testFile3Clone11 = "/ItUser1/file3clone11";
    std::string testFile3Clone111 = "/ItUser1/file3clone111";

    // testFile3 写x
    std::string fakeData(4096, 'x');
    ASSERT_TRUE(CreateAndWriteFile(testFile3_, testUser1_, fakeData));
    LOG(INFO) << "Write testFile3_ success.";

    // 对testFile3 打快照
    std::string testFile3Snap1 = testFile3_ + "@snap1";
    uint64_t seq = 0;
    UserInfo_t userinfo1;
    userinfo1.owner = testUser1_;
    ASSERT_EQ(0, snapClient_->CreateSnapShot(testFile3Snap1, userinfo1, &seq));

    // 对testFile3 写y
    std::string fakeDatay(4096, 'y');
    ASSERT_TRUE(WriteFile(testFile3_, testUser1_, fakeDatay));

    ASSERT_EQ(0, snapClient_->ProtectSnapShot(testFile3Snap1, userinfo1));
    // 从testFile3 快照克隆出testFile3Clone1, 并断言数据为x
    FInfo finfo;
    ASSERT_EQ(0, snapClient_->Clone(testFile3Snap1, testFile3Clone1, userinfo1,
            &finfo));
    ASSERT_TRUE(CheckFileData(testFile3Clone1, testUser1_, fakeData));

    // flatten testFile3Clone1
    ASSERT_EQ(0, snapClient_->Flatten(testFile3Clone1, userinfo1));
    ASSERT_TRUE(WaitFlattenSuccess(testFile3Clone1, userinfo1));
    ASSERT_TRUE(CheckFileData(testFile3Clone1, testUser1_, fakeData));

    // 对testFile3Clone1 写z
    std::string fakeData3(4096, 'z');
    ASSERT_TRUE(WriteFile(testFile3Clone1, testUser1_, fakeData3));
    ASSERT_TRUE(CheckFileData(testFile3Clone1, testUser1_, fakeData3));

    // 对testFile3Clone1 打快照
    std::string testFile3Clone1Snap1 = testFile3Clone1 + "@snap1";
    uint64_t seq2 = 0;
    ASSERT_EQ(0, snapClient_->CreateSnapShot(
        testFile3Clone1Snap1, userinfo1, &seq2));

    // 对testFile3Clone1 写1
    std::string fakeData1(4096, '1');
    ASSERT_TRUE(WriteFile(testFile3Clone1, testUser1_, fakeData1));
    ASSERT_TRUE(CheckFileData(testFile3Clone1, testUser1_, fakeData1));

    ASSERT_EQ(0, snapClient_->ProtectSnapShot(
        testFile3Clone1Snap1, userinfo1));
    // 从testFile3Clone1的快照克隆出testFile3Clone11, 并断言数据为z
    FInfo finfo2;
    ASSERT_EQ(0, snapClient_->Clone(
        testFile3Clone1Snap1, testFile3Clone11, userinfo1, &finfo2));
    ASSERT_TRUE(CheckFileData(testFile3Clone11, testUser1_, fakeData3));

    // flatten testFile3Clone11
    ASSERT_EQ(0, snapClient_->Flatten(testFile3Clone11, userinfo1));
    ASSERT_TRUE(WaitFlattenSuccess(testFile3Clone11, userinfo1));
    ASSERT_TRUE(CheckFileData(testFile3Clone11, testUser1_, fakeData3));

    // 对testFile3Clone11 写2
    std::string fakeData2(4096, '2');
    ASSERT_TRUE(WriteFile(testFile3Clone11, testUser1_, fakeData2));
    ASSERT_TRUE(CheckFileData(testFile3Clone11, testUser1_, fakeData2));

    // 对testFile3Clone11 打快照
    std::string testFile3Clone11Snap1 = testFile3Clone11 + "@snap1";
    uint64_t seq3 = 0;
    ASSERT_EQ(0, snapClient_->CreateSnapShot(
        testFile3Clone11Snap1, userinfo1, &seq3));

    // testFile3Clone11写a
    std::string fakeData4(4096, 'a');
    ASSERT_TRUE(WriteFile(testFile3Clone11, testUser1_, fakeData4));
    ASSERT_TRUE(CheckFileData(testFile3Clone11, testUser1_, fakeData4));


    ASSERT_EQ(0, snapClient_->ProtectSnapShot(
        testFile3Clone11Snap1, userinfo1));
    // 从testFile3Clone11的快照克隆出testFile3Clone111, 并断言数据为z
    FInfo finfo3;
    ASSERT_EQ(0, snapClient_->Clone(
        testFile3Clone11Snap1, testFile3Clone111, userinfo1, &finfo3));
    ASSERT_TRUE(CheckFileData(testFile3Clone111, testUser1_, fakeData2));

    // flatten testFile3Clone111
    ASSERT_EQ(0, snapClient_->Flatten(testFile3Clone111, userinfo1));
    ASSERT_TRUE(WaitFlattenSuccess(testFile3Clone111, userinfo1));
    ASSERT_TRUE(CheckFileData(testFile3Clone111, testUser1_, fakeData2));

    // 写testFile3, 不影响另外3个卷
    std::string fakeData5(4096, 'b');
    ASSERT_TRUE(WriteFile(testFile3_, testUser1_, fakeData5));
    ASSERT_TRUE(CheckFileData(testFile3_, testUser1_, fakeData5));
    ASSERT_TRUE(CheckFileData(testFile3Clone1, testUser1_, fakeData1));
    ASSERT_TRUE(CheckFileData(testFile3Clone11, testUser1_, fakeData4));
    ASSERT_TRUE(CheckFileData(testFile3Clone111, testUser1_, fakeData2));

    // 写testFile3Clone1, 不影响另外3个卷
    std::string fakeData6(4096, 'c');
    ASSERT_TRUE(WriteFile(testFile3Clone1, testUser1_, fakeData6));
    ASSERT_TRUE(CheckFileData(testFile3Clone1, testUser1_, fakeData6));
    ASSERT_TRUE(CheckFileData(testFile3_, testUser1_, fakeData5));
    ASSERT_TRUE(CheckFileData(testFile3Clone11, testUser1_, fakeData4));
    ASSERT_TRUE(CheckFileData(testFile3Clone111, testUser1_, fakeData2));

    // 写testFile3Clone11, 不影响另外3个卷
    std::string fakeData7(4096, 'd');
    ASSERT_TRUE(WriteFile(testFile3Clone11, testUser1_, fakeData7));
    ASSERT_TRUE(CheckFileData(testFile3Clone11, testUser1_, fakeData7));
    ASSERT_TRUE(CheckFileData(testFile3Clone1, testUser1_, fakeData6));
    ASSERT_TRUE(CheckFileData(testFile3_, testUser1_, fakeData5));
    ASSERT_TRUE(CheckFileData(testFile3Clone111, testUser1_, fakeData2));

    // 写testFileClone111, 不影响另外3个卷
    std::string fakeData8(4096, 'e');
    ASSERT_TRUE(WriteFile(testFile3Clone111, testUser1_, fakeData8));
    ASSERT_TRUE(CheckFileData(testFile3Clone111, testUser1_, fakeData8));
    ASSERT_TRUE(CheckFileData(testFile3Clone11, testUser1_, fakeData7));
    ASSERT_TRUE(CheckFileData(testFile3Clone1, testUser1_, fakeData6));
    ASSERT_TRUE(CheckFileData(testFile3_, testUser1_, fakeData5));
}

// 测试克隆链中间的卷没有写过的场景
TEST_F(LocalSnapshotCloneTest, TestSnapshotAndCloneEmpty) {
    std::string testFile4_ = "/ItUser1/file4";
    std::string testFile4Clone1 = "/ItUser1/file4clone1";
    std::string testFile4Clone11 = "/ItUser1/file4clone11";

    // testFile4 写x
    std::string fakeData(4096, 'x');
    ASSERT_TRUE(CreateAndWriteFile(testFile4_, testUser1_, fakeData));
    LOG(INFO) << "Write testFile4_ success.";

    // 对testFile4 打快照
    std::string testFile4Snap1 = testFile4_ + "@snap1";
    uint64_t seq = 0;
    UserInfo_t userinfo1;
    userinfo1.owner = testUser1_;
    ASSERT_EQ(0, snapClient_->CreateSnapShot(testFile4Snap1, userinfo1, &seq));

    // 对testFile4 写y
    std::string fakeData2(4096, 'y');
    ASSERT_TRUE(WriteFile(testFile4_, testUser1_, fakeData2));
    ASSERT_TRUE(CheckFileData(testFile4_, testUser1_, fakeData2));

    ASSERT_EQ(0, snapClient_->ProtectSnapShot(testFile4Snap1, userinfo1));
    // 从testFile4 快照克隆出testFile4Clone1, 并断言数据为x
    FInfo finfo;
    ASSERT_EQ(0, snapClient_->Clone(testFile4Snap1, testFile4Clone1, userinfo1,
            &finfo));
    ASSERT_TRUE(CheckFileData(testFile4Clone1, testUser1_, fakeData));

    // 对testFile4Clone1 打快照
    std::string testFile4Clone1Snap1 = testFile4Clone1 + "@snap1";
    uint64_t seq2 = 0;
    ASSERT_EQ(0, snapClient_->CreateSnapShot(
        testFile4Clone1Snap1, userinfo1, &seq2));

    ASSERT_EQ(0, snapClient_->ProtectSnapShot(
        testFile4Clone1Snap1, userinfo1));
    // 从testFile4Clone1的快照克隆出testFile4Clone11, 并断言数据为x
    FInfo finfo2;
    ASSERT_EQ(0, snapClient_->Clone(
        testFile4Clone1Snap1, testFile4Clone11, userinfo1, &finfo2));
    ASSERT_TRUE(CheckFileData(testFile4Clone11, testUser1_, fakeData));

    // testFile4Clone11写a
    std::string fakeData4(4096, 'a');
    ASSERT_TRUE(WriteFile(testFile4Clone11, testUser1_, fakeData4));
    ASSERT_TRUE(CheckFileData(testFile4Clone11, testUser1_, fakeData4));
}

// 测试原始卷没写过的情况
TEST_F(LocalSnapshotCloneTest, TestSnapshotEmptyAndClone) {
    std::string testFile5_ = "/ItUser1/file5";
    std::string testFile5Clone1 = "/ItUser1/file5clone1";
    std::string testFile5Clone11 = "/ItUser1/file5clone11";

    // 创建testFile5
    std::string fakeDataNull(4096, '\0');
    ASSERT_TRUE(CreateFile(testFile5_, testUser1_));

    // 对testFile5 打快照
    std::string testFile5Snap1 = testFile5_ + "@snap1";
    uint64_t seq = 0;
    UserInfo_t userinfo1;
    userinfo1.owner = testUser1_;
    ASSERT_EQ(0, snapClient_->CreateSnapShot(testFile5Snap1, userinfo1, &seq));

    // 对testFile5 写x
    std::string fakeDatax(4096, 'x');
    ASSERT_TRUE(WriteFile(testFile5_, testUser1_, fakeDatax));
    ASSERT_TRUE(CheckFileData(testFile5_, testUser1_, fakeDatax));

    ASSERT_EQ(0, snapClient_->ProtectSnapShot(testFile5Snap1, userinfo1));
    // 从testFile5 快照克隆出testFile5Clone1, 并断言数据为null
    FInfo finfo;
    ASSERT_EQ(0, snapClient_->Clone(testFile5Snap1, testFile5Clone1, userinfo1,
            &finfo));
    ASSERT_TRUE(CheckFileData(testFile5Clone1, testUser1_, fakeDataNull));

    // 对testFile5Clone1 打快照
    std::string testFile5Clone1Snap1 = testFile5Clone1 + "@snap1";
    uint64_t seq2 = 0;
    ASSERT_EQ(0, snapClient_->CreateSnapShot(
        testFile5Clone1Snap1, userinfo1, &seq2));

    ASSERT_EQ(0, snapClient_->ProtectSnapShot(
        testFile5Clone1Snap1, userinfo1));

    // 从testFile5Clone1的快照克隆出testFile5Clone11, 并断言数据为null
    FInfo finfo2;
    ASSERT_EQ(0, snapClient_->Clone(
        testFile5Clone1Snap1, testFile5Clone11, userinfo1, &finfo2));
    ASSERT_TRUE(CheckFileData(testFile5Clone11, testUser1_, fakeDataNull));

    // testFile5Clone11写a
    std::string fakeDataa(4096, 'a');
    ASSERT_TRUE(WriteFile(testFile5Clone11, testUser1_, fakeDataa));
    ASSERT_TRUE(CheckFileData(testFile5Clone11, testUser1_, fakeDataa));
}


}  // namespace client
}  // namespace curve





