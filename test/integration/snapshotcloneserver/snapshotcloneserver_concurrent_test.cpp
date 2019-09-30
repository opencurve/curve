/*
 * Project: curve
 * Created Date: Sun Sep 29 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
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
#include "src/snapshotcloneserver/common/define.h"
#include "src/snapshotcloneserver/common/snapshotclone_meta_store.h"

using curve::CurveCluster;
using curve::client::FileClient;
using curve::client::UserInfo_t;


const std::vector<std::string> mdsConf1{
    {" --graceful_quit_on_sigterm"},
    {" --confPath=./test/integration/snapshotcloneserver/config/mds.conf.2"},
    {" --log_dir=./runlog/SCSConcurrentTestLog"},
};

const std::vector<std::string> chunkserverConf1{
    {" --graceful_quit_on_sigterm"},
    {" -chunkServerStoreUri=local://./ConSCSTest1/"},
    {" -chunkServerMetaUri=local://./ConSCSTest1/chunkserver.dat"},  // NOLINT
    {" -copySetUri=local://./ConSCSTest1/copysets"},
    {" -recycleUri=local://./ConSCSTest1/recycler"},
    {" -chunkFilePoolDir=./ConSCSTest1/chunkfilepool/"},
    {" -chunkFilePoolMetaPath=./ConSCSTest1/chunkfilepool.meta"},  // NOLINT
    {" -conf=./test/integration/snapshotcloneserver/config/chunkserver.conf.2"},
    {" -raft_sync_segments=true"},
    {" --log_dir=./runlog/SCSConcurrentTestLog"},
};

const std::vector<std::string> chunkserverConf2{
    {" --graceful_quit_on_sigterm"},
    {" -chunkServerStoreUri=local://./ConSCSTest2/"},
    {" -chunkServerMetaUri=local://./ConSCSTest2/chunkserver.dat"},  // NOLINT
    {" -copySetUri=local://./ConSCSTest2/copysets"},
    {" -recycleUri=local://./ConSCSTest2/recycler"},
    {" -chunkFilePoolDir=./ConSCSTest2/chunkfilepool/"},
    {" -chunkFilePoolMetaPath=./ConSCSTest2/chunkfilepool.meta"},  // NOLINT
    {" -conf=./test/integration/snapshotcloneserver/config/chunkserver.conf.2"},
    {" -raft_sync_segments=true"},
    {" --log_dir=./runlog/SCSConcurrentTestLog"},
};

const std::vector<std::string> chunkserverConf3{
    {" --graceful_quit_on_sigterm"},
    {" -chunkServerStoreUri=local://./ConSCSTest3/"},
    {" -chunkServerMetaUri=local://./ConSCSTest3/chunkserver.dat"},  // NOLINT
    {" -copySetUri=local://./ConSCSTest3/copysets"},
    {" -recycleUri=local://./ConSCSTest3/recycler"},
    {" -chunkFilePoolDir=./ConSCSTest3/chunkfilepool/"},
    {" -chunkFilePoolMetaPath=./ConSCSTest3/chunkfilepool.meta"},  // NOLINT
    {" -conf=./test/integration/snapshotcloneserver/config/chunkserver.conf.2"},
    {" -raft_sync_segments=true"},
    {" --log_dir=./runlog/SCSConcurrentTestLog"},
};
const std::vector<std::string> snapshotcloneConf{
    {" --conf=./test/integration/snapshotcloneserver/config/snapshot_clone_server.conf.2"},  //NOLINT
    {" --log_dir=./runlog/SCSConcurrentTestLog"},
};

const char* kEtcdClientIpPort = "127.0.0.1:10011";
const char* kEtcdPeerIpPort = "127.0.0.1:10012";
const char* kMdsIpPort = "127.0.0.1:10013";
const char* kChunkServerIpPort1 = "127.0.0.1:10014";
const char* kChunkServerIpPort2 = "127.0.0.1:10015";
const char* kChunkServerIpPort3 = "127.0.0.1:10016";
const char* kSnapshotCloneServerIpPort = "127.0.0.1:10017";

const char* testFile1_ = "/concurrentItUser1/file1";
const char* testFile2_ = "/concurrentItUser1/file2";  // 将在TestImage2Clone2Success中删除  //NOLINT
const char* testFile3_ = "/concurrentItUser2/file3";
const char* testFile4_ = "/concurrentItUser1/file3";
const char* testUser1_ = "concurrentItUser1";
const char* testUser2_ = "concurrentItUser2";

const uint64_t chunkSize = 16ULL * 1024 * 1024;
const uint64_t segmentSize = 1ULL * 1024 * 1024 * 1024;
const uint64_t chunkGap = 8;

namespace curve {
namespace snapshotcloneserver {

class SnapshotCloneServerTest : public ::testing::Test {
 public:
    static void SetUpTestCase() {
        system("mkdir -p ./runlog/SCSConcurrentTestLog");

        cluster_ = new CurveCluster();
        ASSERT_NE(nullptr, cluster_);

        // 初始化db
        cluster_->InitDB("SCSConcurrentTestDB");
        //在一开始清理数据库和文件
        cluster_->mdsRepo_->dropDataBase();
        system("rm -rf SCSConcurrentTest.etcd");
        system("rm -rf ConSCSTest1");
        system("rm -rf ConSCSTest2");
        system("rm -rf ConSCSTest3");


        // 启动etcd
        cluster_->StarSingleEtcd(1, kEtcdClientIpPort, kEtcdPeerIpPort,
        std::vector<std::string>{" --name SCSConcurrentTest"});

        // 启动一个mds
        cluster_->StartSingleMDS(1, kMdsIpPort, mdsConf1, true);

        // 创建物理池
        cluster_->PreparePhysicalPool(
        1, "./test/integration/snapshotcloneserver/config/topo2.txt"); // NOLINT

        // 格式化chunkfilepool
        std::vector<std::thread> threadpool(3);

        threadpool[0] = std::thread(&CurveCluster::FormatChunkFilePool,
            cluster_,
            "./ConSCSTest1/chunkfilepool/",
            "./ConSCSTest1/chunkfilepool.meta",
            "./ConSCSTest1/",
            30);
        threadpool[1] = std::thread(&CurveCluster::FormatChunkFilePool,
            cluster_,
            "./ConSCSTest2/chunkfilepool/",
            "./ConSCSTest2/chunkfilepool.meta",
            "./ConSCSTest2/",
            30);
        threadpool[2] = std::thread(&CurveCluster::FormatChunkFilePool,
            cluster_,
            "./ConSCSTest3/chunkfilepool/",
            "./ConSCSTest3/chunkfilepool.meta",
            "./ConSCSTest3/",
            30);

        for (int i = 0; i < 3; i++) {
            threadpool[i].join();
        }

        // 创建chunkserver
        cluster_->StartSingleChunkServer(
            1, kChunkServerIpPort1, chunkserverConf1);
        cluster_->StartSingleChunkServer(
            2, kChunkServerIpPort2, chunkserverConf2);
        cluster_->StartSingleChunkServer(
            3, kChunkServerIpPort3, chunkserverConf3);

        std::this_thread::sleep_for(std::chrono::seconds(5));

        // 创建逻辑池, 并睡眠一段时间让底层copyset先选主
        cluster_->PrepareLogicalPool(
        1,
        "./test/integration/snapshotcloneserver/config/topo2.txt",  // NOLINT
        100, "pool1");

        cluster_->StartSnapshotCloneServer(
            1, kSnapshotCloneServerIpPort, snapshotcloneConf);

        fileClient_ = new FileClient();
        fileClient_->Init("./test/integration/snapshotcloneserver/config/client2.conf");  // NOLINT

        UserInfo_t userinfo;
        userinfo.owner = "concurrentItUser1";

        ASSERT_EQ(0,
            fileClient_->Mkdir("/concurrentItUser1", userinfo));

        std::string fackData(4096, 'x');
        ASSERT_TRUE(CreateAndWriteFile(testFile1_, testUser1_, fackData));
        LOG(INFO) << "Write testFile1_ success.";

        ASSERT_TRUE(CreateAndWriteFile(testFile2_, testUser1_, fackData));
        LOG(INFO) << "Write testFile2_ success.";

        UserInfo_t userinfo2;
        userinfo2.owner = "concurrentItUser2";
        ASSERT_EQ(0,
            fileClient_->Mkdir("/concurrentItUser2", userinfo2));

        ASSERT_TRUE(CreateAndWriteFile(testFile3_, testUser2_, fackData));
        LOG(INFO) << "Write testFile3_ success.";

        ASSERT_EQ(0,
            fileClient_->Create(
                testFile4_, userinfo, 10ULL * 1024 * 1024 * 1024));
    }

    static bool CreateAndWriteFile(const std::string &fileName,
        const std::string &user,
        const std::string &dataSample) {
        UserInfo_t userinfo;
        userinfo.owner = user;
        int ret = fileClient_->Create(
                fileName, userinfo, 10ULL * 1024 * 1024 * 1024);
        if (ret < 0) {
            LOG(ERROR) << "Create fail, ret = " << ret;
            return false;
        }
        return WriteFile(fileName, user, dataSample);
    }

    static bool WriteFile(const std::string &fileName,
        const std::string &user,
        const std::string &dataSample) {
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
            ret = fileClient_->Write(
                testfd1_, dataSample.c_str(),
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

    static bool CheckFileData(const std::string &fileName,
        const std::string &user,
        const std::string &dataSample) {
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
            ret = fileClient_->Read(
            dstFd, buf, i * chunkSize * chunkGap, 4096);
            if (ret < 0) {
                LOG(ERROR) << "Read fail, ret = " << ret;
                return false;
            }
            std::string data(buf, 4096);
            if (data != dataSample) {
                LOG(ERROR) << "CheckFileData not Equal, data = ["
                            << data
                            << "] , expect data = ["
                            << dataSample
                            << "].";
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
        cluster_->StopCluster();
        cluster_->mdsRepo_->dropDataBase();
        delete cluster_;
        cluster_ = nullptr;
        system("rm -rf SCSConcurrentTest.etcd");
        system("rm -rf ConSCSTest1");
        system("rm -rf ConSCSTest2");
        system("rm -rf ConSCSTest3");
    }

    void SetUp() {
    }

    void TearDown() {
    }

    void PrepareSnapshotForTestFile1(std::string *uuid1) {
        if (!hasSnapshotForTestFile1_) {
            int ret = MakeSnapshot(testUser1_,
                testFile1_, "snap1", uuid1);
            ASSERT_EQ(0, ret);
            bool success1 = CheckSnapshotSuccess(testUser1_, testFile1_,
                *uuid1);
            ASSERT_TRUE(success1);
            hasSnapshotForTestFile1_ = true;
            snapIdForTestFile1_ = *uuid1;
        }
    }

    void WaitDeleteSnapshotForTestFile1() {
        if (hasSnapshotForTestFile1_) {
            ASSERT_EQ(0,
                DeleteAndCheckSnapshotSuccess(
                    testUser1_, testFile1_, snapIdForTestFile1_));
        }
    }

    static CurveCluster *cluster_;
    static FileClient *fileClient_;

    bool hasSnapshotForTestFile1_ = false;
    std::string snapIdForTestFile1_;
};

CurveCluster * SnapshotCloneServerTest::cluster_ = nullptr;
FileClient * SnapshotCloneServerTest::fileClient_ = nullptr;

// 并发测试用例

// 这个用例测试快照层数，放在最前面
TEST_F(SnapshotCloneServerTest, TestSameFile3Snapshot) {
    std::string uuid1, uuid2, uuid3;
    int ret = MakeSnapshot(testUser1_,
        testFile1_, "snap1", &uuid1);
    ASSERT_EQ(0, ret);
    ret = MakeSnapshot(testUser1_,
        testFile1_, "snap2", &uuid2);
    ASSERT_EQ(0, ret);
    ret = MakeSnapshot(testUser1_,
        testFile1_, "snap3", &uuid3);
    ASSERT_EQ(0, ret);
    bool success1 = CheckSnapshotSuccess(testUser1_,
        testFile1_, uuid1);
    ASSERT_TRUE(success1);

    bool success2 = CheckSnapshotSuccess(testUser1_,
        testFile1_, uuid2);
    ASSERT_TRUE(success2);

    bool success3 = CheckSnapshotSuccess(testUser1_,
        testFile1_, uuid3);
    ASSERT_TRUE(success3);


    // 快照层数设置为3，尝试再打一次快照，超过层数失败
    ret = MakeSnapshot(testUser1_,
        testFile1_, "snap3", &uuid3);
    ASSERT_EQ(kErrCodeSnapshotCountReachLimit, ret);

    ASSERT_EQ(0, DeleteAndCheckSnapshotSuccess(testUser1_, testFile1_, uuid1));
    ASSERT_EQ(0, DeleteAndCheckSnapshotSuccess(testUser1_, testFile1_, uuid2));
    ASSERT_EQ(0, DeleteAndCheckSnapshotSuccess(testUser1_, testFile1_, uuid3));
}

TEST_F(SnapshotCloneServerTest, TestCancelAndMakeSnaphotConcurrent) {
    std::string uuid1, uuid2;
    int ret = MakeSnapshot(testUser1_,
        testFile1_, "snapToCancle", &uuid1);
    ASSERT_EQ(0, ret);
    ret = MakeSnapshot(testUser1_,
        testFile1_, "snap2", &uuid2);
    ASSERT_EQ(0, ret);
    bool success1 = false;
    bool isCancel = false;
    for (int i = 0; i < 600; i++) {
        FileSnapshotInfo info1;
        int retCode = GetSnapshotInfo(
            testUser1_, testFile1_, uuid1, &info1);
        if (retCode == 0) {
            if (info1.GetSnapshotInfo().GetStatus() == Status::pending) {
                if (!isCancel) {
                    CancelSnapshot(testUser1_,
                        testFile1_, uuid1);
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
        } else {
            success1 = true;
        }
    }
    ASSERT_TRUE(success1);

    bool success2 = CheckSnapshotSuccess(testUser1_, testFile1_, uuid2);
    ASSERT_TRUE(success2);

    ASSERT_EQ(0, DeleteAndCheckSnapshotSuccess(testUser1_, testFile1_, uuid2));
}

TEST_F(SnapshotCloneServerTest, Test3File3Snapshot) {
    std::string uuid1, uuid2, uuid3;
    int ret = MakeSnapshot(testUser1_,
        testFile1_, "snap1", &uuid1);
    ASSERT_EQ(0, ret);
    ret = MakeSnapshot(testUser1_,
        testFile2_, "snap2", &uuid2);
    ASSERT_EQ(0, ret);
    ret = MakeSnapshot(testUser2_,
        testFile3_, "snap3", &uuid3);
    ASSERT_EQ(0, ret);
    bool success1 = CheckSnapshotSuccess(testUser1_, testFile1_, uuid1);
    ASSERT_TRUE(success1);

    bool success2 = CheckSnapshotSuccess(testUser1_, testFile2_, uuid2);
    ASSERT_TRUE(success2);

    bool success3 = CheckSnapshotSuccess(testUser2_, testFile3_, uuid3);
    ASSERT_TRUE(success3);
    ASSERT_EQ(0, DeleteAndCheckSnapshotSuccess(testUser1_, testFile1_, uuid1));
    ASSERT_EQ(0, DeleteAndCheckSnapshotSuccess(testUser1_, testFile2_, uuid2));
    ASSERT_EQ(0, DeleteAndCheckSnapshotSuccess(testUser2_, testFile3_, uuid3));
}

TEST_F(SnapshotCloneServerTest, TestSnapSameClone1Success) {
    std::string snapId;
    PrepareSnapshotForTestFile1(&snapId);

    std::string uuid1, uuid2;
    int ret1, ret2;
    ret1 = CloneOrRecover("Clone", testUser1_, snapId, "/clone1", true,
        &uuid1);
    ASSERT_EQ(0, ret1);

    ret2 = CloneOrRecover("Clone", testUser1_, snapId, "/clone1", true,
        &uuid2);
    ASSERT_EQ(kErrCodeFileExist, ret2);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid1, true);
    ASSERT_TRUE(success1);

    TaskCloneInfo info2;
    int retCode = GetCloneTaskInfo(
        testUser1_, uuid2, &info2);
    ASSERT_EQ(kErrCodeFileNotExist, retCode);
}

TEST_F(SnapshotCloneServerTest, TestSnap2Clone2Success) {
    std::string snapId;
    PrepareSnapshotForTestFile1(&snapId);

    std::string uuid1, uuid2;
    int ret1, ret2;
    ret1 = CloneOrRecover("Clone", testUser1_, snapId, "/clone1_1", true,
        &uuid1);
    ASSERT_EQ(0, ret1);

    ret2 = CloneOrRecover("Clone", testUser1_, snapId, "/clone1_2", true,
        &uuid2);
    ASSERT_EQ(0, ret2);

    bool success1 = false;
    bool firstDelete = false;
    for (int i = 0; i < 600; i++) {
        TaskCloneInfo info1;
        int retCode = GetCloneTaskInfo(
            testUser1_, uuid1, &info1);
        if (retCode != 0) {
            break;
        }
        if (info1.GetCloneInfo().GetStatus() == CloneStatus::cloning) {
            if (!firstDelete) {
                int retCode2 = DeleteSnapshot(
                    testUser1_, testFile1_, snapId);
                ASSERT_EQ(kErrCodeSnapshotCannotDeleteCloning, retCode2);
                firstDelete = true;
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(3000));
            }
            continue;
        } else if (info1.GetCloneInfo().GetStatus() == CloneStatus::done) {
            success1 = true;
            break;
        } else {
            FAIL() << "Clone Fail On status = "
                   << static_cast<int>(info1.GetCloneInfo().GetStatus());
        }
    }
    ASSERT_TRUE(firstDelete);
    ASSERT_TRUE(success1);

    bool success2 = CheckCloneOrRecoverSuccess(testUser1_, uuid2, true);
    ASSERT_TRUE(success2);

    int retCode3 = DeleteAndCheckSnapshotSuccess(
        testUser1_, testFile1_, snapId);
    ASSERT_EQ(0, retCode3);
}

TEST_F(SnapshotCloneServerTest, TestImage2Clone2Success) {
    std::string uuid1, uuid2;
    int ret1, ret2;
    ret1 = CloneOrRecover("Clone", testUser1_, testFile2_, "/clone2_1", true,
        &uuid1);
    ASSERT_EQ(0, ret1);

    ret2 = CloneOrRecover("Clone", testUser1_, testFile2_, "/clone2_2", true,
        &uuid2);
    ASSERT_EQ(0, ret2);

    UserInfo_t userinfo;
    userinfo.owner = testUser1_;

    bool success1 = false;
    bool firstDelete = false;
    for (int i = 0; i < 600; i++) {
        TaskCloneInfo info1;
        int retCode = GetCloneTaskInfo(
            testUser1_, uuid1, &info1);
        if (retCode != 0) {
            break;
        }
        if (info1.GetCloneInfo().GetStatus() == CloneStatus::cloning) {
            if (!firstDelete) {
                int retCode2 = fileClient_->Unlink(testFile2_, userinfo, false);
                ASSERT_EQ(-LIBCURVE_ERROR::DELETE_BEING_CLONED, retCode2);
                firstDelete = true;
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(3000));
            }
            continue;
        } else if (info1.GetCloneInfo().GetStatus() == CloneStatus::done) {
            success1 = true;
            break;
        } else {
            FAIL() << "Clone Fail On status = "
                   << static_cast<int>(info1.GetCloneInfo().GetStatus());
        }
    }
    ASSERT_TRUE(firstDelete);
    ASSERT_TRUE(success1);

    bool success2 = CheckCloneOrRecoverSuccess(testUser1_, uuid2, true);
    ASSERT_TRUE(success2);

    int retCode3 = fileClient_->Unlink(testFile2_, userinfo, false);
    ASSERT_EQ(0, retCode3);
}


TEST_F(SnapshotCloneServerTest, TestReadWriteWhenLazyCloneSnap) {
    std::string snapId;
    PrepareSnapshotForTestFile1(&snapId);

    std::string uuid1;
    std::string dstFile = "/concurrentItUser1/SnapLazyClone4Rw";
    int ret = CloneOrRecover("Clone", testUser1_,
        snapId, dstFile, true,
        &uuid1);
    ASSERT_EQ(0, ret);

    std::string fackData(4096, 'y');
    ASSERT_TRUE(WriteFile(dstFile, testUser1_, fackData));
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fackData));

    // 判断是否clone成功
    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid1, true);
    ASSERT_TRUE(success1);
}

TEST_F(SnapshotCloneServerTest, TestReadWriteWhenLazyCloneImage) {
    std::string uuid1;
    std::string dstFile = "/concurrentItUser1/ImageLazyClone4Rw";
    int ret = CloneOrRecover("Clone", testUser1_,
        testFile1_, dstFile, true,
        &uuid1);
    ASSERT_EQ(0, ret);

    std::string fackData(4096, 'y');
    ASSERT_TRUE(WriteFile(dstFile, testUser1_, fackData));
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fackData));

    // 判断是否clone成功
    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid1, true);
    ASSERT_TRUE(success1);
}

TEST_F(SnapshotCloneServerTest, TestReadWriteWhenLazyRecoverSnap) {
    std::string snapId;
    PrepareSnapshotForTestFile1(&snapId);

    std::string uuid1;
    std::string dstFile = testFile1_;
    int ret = CloneOrRecover("Recover", testUser1_,
        snapId, dstFile, true,
        &uuid1);
    ASSERT_EQ(0, ret);

    std::string fackData(4096, 'y');
    ASSERT_TRUE(WriteFile(dstFile, testUser1_, fackData));
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fackData));

    // 判断是否clone成功
    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid1, false);
    ASSERT_TRUE(success1);
}


}  // namespace snapshotcloneserver
}  // namespace curve






