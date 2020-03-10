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


const char* kLogPath = "./runlog/SCSTestLog";
const char* kMdsDbName = "SCSTestDB";
const char* kMdsConfigPath = "./test/integration/snapshotcloneserver/config/SCSTest_mds.conf";   // NOLINT

const char* kCSConfigPath = "./test/integration/snapshotcloneserver/config/SCSTest_chunkserver.conf";  // NOLINT

const char* kCsClientConfigPath = "./test/integration/snapshotcloneserver/config/SCSTest_cs_client.conf"; // NOLINT

const char* kSnapClientConfigPath = "./test/integration/snapshotcloneserver/config/SCSTest_snap_client.conf";  // NOLINT

const char* kS3ConfigPath = "./test/integration/snapshotcloneserver/config/SCSTest_s3.conf";  // NOLINT

const char* kSCSConfigPath = "./test/integration/snapshotcloneserver/config/SCSTest_scs.conf";  // NOLINT

const char* kClientConfigPath = "./test/integration/snapshotcloneserver/config/SCSTest_client.conf";  // NOLINT

const std::vector<std::string> mdsConfigOptions {
    std::string("mds.listen.addr=") + kMdsIpPort,
    std::string("mds.etcd.endpoint=") + kEtcdClientIpPort,
    std::string("mds.DbName=") + kMdsDbName
};

const std::vector<std::string> mdsConf1{
    {" --graceful_quit_on_sigterm"},
    std::string(" --confPath=") + kMdsConfigPath,
    std::string(" --log_dir=") + kLogPath,
    std::string(" --segmentSize=") + std::to_string(segmentSize),
    {" --stderrthreshold=3"},
};

const std::vector<std::string> chunkserverConfigOptions {
    std::string("mds.listen.addr=") + kMdsIpPort,
    std::string("curve.config_path=") + kCsClientConfigPath,
    std::string("s3.config_path=") + kS3ConfigPath,
};

const std::vector<std::string> csClientConfigOptions {
    std::string("mds.listen.addr=") + kMdsIpPort,
};

const std::vector<std::string> snapClientConfigOptions {
    std::string("mds.listen.addr=") + kMdsIpPort,
};

const std::vector<std::string> s3ConfigOptions {
};

const std::vector<std::string> chunkserverConf1{
    {" --graceful_quit_on_sigterm"},
    {" -chunkServerStoreUri=local://./SCSTest1/"},
    {" -chunkServerMetaUri=local://./SCSTest1/chunkserver.dat"},  // NOLINT
    {" -copySetUri=local://./SCSTest1/copysets"},
    {" -recycleUri=local://./SCSTest1/recycler"},
    {" -chunkFilePoolDir=./SCSTest1/chunkfilepool/"},
    {" -chunkFilePoolMetaPath=./SCSTest1/chunkfilepool.meta"},  // NOLINT
    std::string(" -conf=") + kCSConfigPath,
    {" -raft_sync_segments=true"},
    std::string(" --log_dir=") + kLogPath,
    {" --stderrthreshold=3"},
};

const std::vector<std::string> chunkserverConf2{
    {" --graceful_quit_on_sigterm"},
    {" -chunkServerStoreUri=local://./SCSTest2/"},
    {" -chunkServerMetaUri=local://./SCSTest2/chunkserver.dat"},  // NOLINT
    {" -copySetUri=local://./SCSTest2/copysets"},
    {" -recycleUri=local://./SCSTest2/recycler"},
    {" -chunkFilePoolDir=./SCSTest2/chunkfilepool/"},
    {" -chunkFilePoolMetaPath=./SCSTest2/chunkfilepool.meta"},  // NOLINT
    std::string(" -conf=") + kCSConfigPath,
    {" -raft_sync_segments=true"},
    std::string(" --log_dir=") + kLogPath,
    {" --stderrthreshold=3"},
};

const std::vector<std::string> chunkserverConf3{
    {" --graceful_quit_on_sigterm"},
    {" -chunkServerStoreUri=local://./SCSTest3/"},
    {" -chunkServerMetaUri=local://./SCSTest3/chunkserver.dat"},  // NOLINT
    {" -copySetUri=local://./SCSTest3/copysets"},
    {" -recycleUri=local://./SCSTest3/recycler"},
    {" -chunkFilePoolDir=./SCSTest3/chunkfilepool/"},
    {" -chunkFilePoolMetaPath=./SCSTest3/chunkfilepool.meta"},  // NOLINT
    std::string(" -conf=") + kCSConfigPath,
    {" -raft_sync_segments=true"},
    std::string(" --log_dir=") + kLogPath,
    {" --stderrthreshold=3"},
};

const std::vector<std::string> snapshotcloneserverConfigOptions {
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
    std::string("leader.campagin.prefix=") +
        kLeaderCampaginPrefix,
};

const std::vector<std::string> snapshotcloneConf{
    std::string(" --conf=") + kSCSConfigPath,
    std::string(" --log_dir=") + kLogPath,
    {" --stderrthreshold=3"},
};

const std::vector<std::string> clientConfigOptions {
    std::string("mds.listen.addr=") + kMdsIpPort,
};

const char* testFile1_ = "/ItUser1/file1";
const char* testFile2_ = "/ItUser1/file2";
const char* testFile3_ = "/ItUser2/file3";
const char* testFile4_ = "/ItUser1/file3";
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
        cluster_->InitDB(kMdsDbName);
        //在一开始清理数据库和文件
        cluster_->mdsRepo_->dropDataBase();
        system("rm -rf SCSTest.etcd");
        system("rm -rf SCSTest1");
        system("rm -rf SCSTest2");
        system("rm -rf SCSTest3");

        // 启动etcd
        cluster_->StartSingleEtcd(1, kEtcdClientIpPort, kEtcdPeerIpPort,
        std::vector<std::string>{" --name SCSTest"});

        cluster_->PrepareConfig<MDSConfigGenerator>(
            kMdsConfigPath,
            mdsConfigOptions);

        // 启动一个mds
        cluster_->StartSingleMDS(1, kMdsIpPort, kMdsDummyPort, mdsConf1, true);

        // 创建物理池
        cluster_->PreparePhysicalPool(
        1, "./test/integration/snapshotcloneserver/config/topo.txt"); // NOLINT


        // 格式化chunkfilepool
        std::vector<std::thread> threadpool(3);

        threadpool[0] = std::thread(&CurveCluster::FormatChunkFilePool,
            cluster_,
            "./SCSTest1/chunkfilepool/",
            "./SCSTest1/chunkfilepool.meta",
            "./SCSTest1/",
            1);
        threadpool[1] = std::thread(&CurveCluster::FormatChunkFilePool,
            cluster_,
            "./SCSTest2/chunkfilepool/",
            "./SCSTest2/chunkfilepool.meta",
            "./SCSTest2/",
            1);
        threadpool[2] = std::thread(&CurveCluster::FormatChunkFilePool,
            cluster_,
            "./SCSTest3/chunkfilepool/",
            "./SCSTest3/chunkfilepool.meta",
            "./SCSTest3/",
            1);

        for (int i = 0; i < 3; i++) {
            threadpool[i].join();
        }

        cluster_->PrepareConfig<CSClientConfigGenerator>(
            kCsClientConfigPath,
            csClientConfigOptions);

        cluster_->PrepareConfig<S3ConfigGenerator>(
            kS3ConfigPath,
            s3ConfigOptions);

        cluster_->PrepareConfig<CSConfigGenerator>(
            kCSConfigPath,
            chunkserverConfigOptions);

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
        "./test/integration/snapshotcloneserver/config/topo.txt",  // NOLINT
        100, "pool1");

        cluster_->PrepareConfig<SnapClientConfigGenerator>(
            kSnapClientConfigPath,
            snapClientConfigOptions);

        cluster_->PrepareConfig<SCSConfigGenerator>(
            kSCSConfigPath,
            snapshotcloneserverConfigOptions);

        cluster_->StartSnapshotCloneServer(
            1, kSnapshotCloneServerIpPort, snapshotcloneConf);

        cluster_->PrepareConfig<ClientConfigGenerator>(
            kClientConfigPath,
            clientConfigOptions);

        fileClient_ = new FileClient();
        fileClient_->Init(kClientConfigPath);

        UserInfo_t userinfo;
        userinfo.owner = "ItUser1";

        ASSERT_EQ(0,
            fileClient_->Mkdir("/ItUser1", userinfo));

        std::string fackData(4096, 'x');
        ASSERT_TRUE(CreateAndWriteFile(testFile1_, testUser1_, fackData));
        LOG(INFO) << "Write testFile1_ success.";

        ASSERT_TRUE(CreateAndWriteFile(testFile2_, testUser1_, fackData));
        LOG(INFO) << "Write testFile2_ success.";

        UserInfo_t userinfo2;
        userinfo2.owner = "ItUser2";
        ASSERT_EQ(0,
            fileClient_->Mkdir("/ItUser2", userinfo2));

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
        system("rm -rf SCSTest.etcd");
        system("rm -rf SCSTest1");
        system("rm -rf SCSTest2");
        system("rm -rf SCSTest3");
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

// 常规测试用例
// 场景一：快照增加删除查找
TEST_F(SnapshotCloneServerTest, TestSnapshotAddDeleteGet) {
    std::string uuid1;
    int ret = 0;
    // 操作1：用户ItUser1对file2打快照
    // 预期1：返回文件不存在
    ret = MakeSnapshot(testUser1_, "/ItUser1/notExistFile", "snap1", &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    // 操作2：用户ItUser2对file1打快照
    // 预期2：返回用户认证失败
    ret = MakeSnapshot(testUser2_, testFile1_, "snap1", &uuid1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    // 操作3：用户ItUser1对file1打快照snap1。
    // 预期3：打快照成功
    ret = MakeSnapshot(testUser1_, testFile1_, "snap1", &uuid1);
    ASSERT_EQ(0, ret);


    std::string fackData(4096, 'y');
    ASSERT_TRUE(WriteFile(testFile1_, testUser1_, fackData));
    ASSERT_TRUE(CheckFileData(testFile1_, testUser1_, fackData));

    // 操作4: 获取快照信息，user=ItUser1，filename=file1
    // 预期4：返回快照snap1的信息
    bool success1 = CheckSnapshotSuccess(testUser1_, testFile1_, uuid1);
    ASSERT_TRUE(success1);

    // 操作5：获取快照信息，user=ItUser2，filename=file1
    // 预期5：返回用户认证失败
    FileSnapshotInfo info1;
    ret = GetSnapshotInfo(testUser2_, testFile1_, uuid1, &info1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    // 操作6：获取快照信息，user=ItUser2，filename=file2
    // 预期6：返回空
    std::vector<FileSnapshotInfo> infoVec;
    ret = ListFileSnapshotInfo(testUser2_, testFile2_, 10, 0, &infoVec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, infoVec.size());

    // 操作7：ItUser2删除快照snap1
    // 预期7：返回用户认证失败
    ret = DeleteSnapshot(testUser2_, testFile1_, uuid1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    // 操作8：ItUser1删除file2的快照，ID为snap1
    // 预期8：返回文件名不匹配
    ret = DeleteSnapshot(testUser1_, testFile2_, uuid1);
    ASSERT_EQ(kErrCodeFileNameNotMatch, ret);

    // 操作9：ItUser1删除快照snap1
    // 预期9：返回删除成功
    ret = DeleteAndCheckSnapshotSuccess(testUser1_, testFile1_, uuid1);
    ASSERT_EQ(0, ret);

    //  操作10：获取快照信息，user=ItUser1，filename=file1
    //  预期10：返回空
    ret = ListFileSnapshotInfo(testUser1_, testFile1_, 10, 0, &infoVec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, infoVec.size());

    // 操作11：ItUser1删除快照snap1（重复删除）
    // 预期11：返回删除成功
    ret = DeleteAndCheckSnapshotSuccess(testUser1_, testFile1_, uuid1);
    ASSERT_EQ(0, ret);

    // 复原testFile1
    std::string fackData2(4096, 'x');
    ASSERT_TRUE(WriteFile(testFile1_, testUser1_, fackData2));
}

// 场景二：取消快照
TEST_F(SnapshotCloneServerTest, TestCancelSnapshot) {
    std::string uuid1;
    int ret = MakeSnapshot(testUser1_,
        testFile1_, "snapToCancle", &uuid1);
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
                    // 操作1：用户ItUser1对file1打快照snap1，在快照未完成前ItUser2取消file1的快照snap1  // NOLINT
                    // 预期1：取消用户认证失败
                    int retCode =
                        CancelSnapshot(testUser2_, testFile1_, uuid1);
                    ASSERT_EQ(kErrCodeInvalidUser, retCode);

                    // 操作2：用户ItUser1对file1打快照snap1，在快照未完成前ItUser1取消file1快照snap2  // NOLINT
                    // 预期2：返回kErrCodeCannotCancelFinished
                    retCode = CancelSnapshot(
                        testUser1_, testFile1_, "notExistUUId");
                    ASSERT_EQ(kErrCodeCannotCancelFinished, retCode);

                    // 操作3：用户ItUser1对file1打快照snap1，在快照未完成前ItUser1取消file2的快照snap1 // NOLINT
                    // 预期3:  返回文件名不匹配
                    retCode = CancelSnapshot(testUser1_, testFile2_, uuid1);
                    ASSERT_EQ(kErrCodeFileNameNotMatch, retCode);

                    // 操作4：用户ItUser1对file1打快照，在快照未完成前ItUser1取消快照snap1  // NOLINT
                    // 预期4：取消快照成功
                    retCode = CancelSnapshot(testUser1_,
                        testFile1_, uuid1);
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
            // 操作5：获取快照信息，user=ItUser1，filename=file1
            // 预期5：返回空
            success1 = true;
            break;
        }
    }
    ASSERT_TRUE(success1);

    // 操作6:  在快照已完成后，ItUser1取消file1的快照snap1
    // 预期6： 返回待取消的快照不存在或已完成
    ret = CancelSnapshot(
        testUser1_, testFile1_, uuid1);
    ASSERT_EQ(kErrCodeCannotCancelFinished, ret);
}

// 场景三：lazy快照克隆场景
TEST_F(SnapshotCloneServerTest, TestSnapLazyClone) {
    std::string snapId;
    PrepareSnapshotForTestFile1(&snapId);

    // 操作1: ItUser1 clone快照snap2，fileName=clone1
    // 预期1：返回快照不存在
    std::string uuid1, uuid2, uuid3, uuid4, uuid5;
    int ret;
    ret = CloneOrRecover(
        "Clone", testUser1_, "UnExistSnapId", "/ItUser1/SnapLazyClone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    // 操作2：ItUser2 clone快照snap1，fileName=clone1
    // 预期2:  返回用户认证失败
    ret = CloneOrRecover("Clone", testUser2_,
        snapId, "/ItUser2/SnapLazyClone1", true,
        &uuid2);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    // 操作3：ItUser1 clone 快照snap1，fileName=clone1
    // 预期3  返回克隆成功
    std::string dstFile = "/ItUser1/SnapLazyClone1";
    ret = CloneOrRecover("Clone", testUser1_,
        snapId, dstFile, true,
        &uuid3);
    ASSERT_EQ(0, ret);

    // 操作4： ItUser1 clone 块照snap1，fileName=clone1 （重复克隆）
    // 预期4：返回文件已存在
    ret = CloneOrRecover("Clone", testUser1_,
        snapId, "/ItUser1/SnapLazyClone1", true,
        &uuid4);
    ASSERT_EQ(kErrCodeFileExist, ret);

    // 操作5： ItUser1 GetCloneTask
    // 预期5：返回clone1的clone 任务
    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid3, true);
    ASSERT_TRUE(success1);

    // 操作6： ItUser2 GetCloneTask
    // 预期6： 返回空
    std::vector<TaskCloneInfo> infoVec;
    ret = ListCloneTaskInfo(
        testUser2_, 10, 0, &infoVec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, infoVec.size());

    // 操作7： ItUser2 CleanCloneTask UUID为clone1的UUID
    // 预期7：返回用户认证失败
    ret = CleanCloneTask(testUser2_, uuid3);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    // 操作8： ItUser1 CleanCloneTask UUID为clone1的UUID
    // 预期8：返回执行成功
    ret = CleanCloneTask(testUser1_, uuid3);
    ASSERT_EQ(0, ret);

    // 等待清理完成
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // 操作9： ItUser1 CleanCloneTask UUID为clone1的UUID（重复执行）
    // 预期9：返回执行成功
    ret = CleanCloneTask(testUser1_, uuid3);
    ASSERT_EQ(0, ret);

    // 操作10：ItUser1 GetCloneTask
    // 预期10：返回空
    TaskCloneInfo info;
    ret = GetCloneTaskInfo(testUser1_, uuid3, &info);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    // 验证数据正确性
    std::string fackData(4096, 'x');
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fackData));
}

// 场景四：非lazy快照克隆场景
TEST_F(SnapshotCloneServerTest, TestSnapNotLazyClone) {
    std::string snapId;
    PrepareSnapshotForTestFile1(&snapId);

    // 操作1: ItUser1 clone快照snap2，fileName=clone1
    // 预期1：返回快照不存在
    std::string uuid1;
    int ret;
    ret = CloneOrRecover(
        "Clone", testUser1_,
        "UnExistSnapId", "/ItUser1/SnapNotLazyClone1", false,
        &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    // 操作2：ItUser2 clone快照snap1，fileName=clone1
    // 预期2:  返回用户认证失败
    ret = CloneOrRecover("Clone", testUser2_,
        snapId, "/ItUser2/SnapNotLazyClone1", false,
        &uuid1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    // 操作3：ItUser1 clone 快照snap1，fileName=clone1
    // 预期3  返回克隆成功
    std::string dstFile = "/ItUser1/SnapNotLazyClone1";
    ret = CloneOrRecover("Clone", testUser1_,
        snapId, dstFile, false,
        &uuid1);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid1, true);
    ASSERT_TRUE(success1);

    // 操作4： ItUser1 clone 块照snap1，fileName=clone1 （重复克隆）
    // 预期4：返回文件已存在
    ret = CloneOrRecover("Clone", testUser1_,
        snapId, "/ItUser1/SnapNotLazyClone1", false,
        &uuid1);
    ASSERT_EQ(kErrCodeFileExist, ret);

    // 验证数据正确性
    std::string fackData(4096, 'x');
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fackData));
}

// 场景五：lazy快照恢复场景
TEST_F(SnapshotCloneServerTest, TestSnapLazyRecover) {
    std::string snapId;
    PrepareSnapshotForTestFile1(&snapId);

    // 操作1: ItUser1 Recover快照snap2，fileName=file1
    // 预期1：返回快照不存在
    std::string uuid1;
    int ret;
    ret = CloneOrRecover(
        "Recover", testUser1_, "UnExistSnapId", testFile1_, true,
        &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    // 操作2：ItUser2 Recover快照snap1，fileName=file1
    // 预期2:  返回用户认证失败
    ret = CloneOrRecover("Recover", testUser2_,
        snapId, testFile1_, true,
        &uuid1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    // 操作3：ItUser1 Recover快照snap1，fileName=file1
    // 预期3  返回恢复成功
    ret = CloneOrRecover("Recover", testUser1_,
        snapId, testFile1_, true,
        &uuid1);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid1, false);
    ASSERT_TRUE(success1);

    // 验证数据正确性
    std::string fackData(4096, 'x');
    ASSERT_TRUE(CheckFileData(testFile1_, testUser1_, fackData));

    // 操作4：ItUser1 recover 快照snap1，fileName=file2
    // 预期4:  返回目标文件不存在
    ret = CloneOrRecover("Recover", testUser1_,
        snapId, "/ItUser1/notExistFile", true,
        &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);
}

// 场景六：非lazy快照恢复场景
TEST_F(SnapshotCloneServerTest, TestSnapNotLazyRecover) {
    std::string snapId;
    PrepareSnapshotForTestFile1(&snapId);

    // 操作1: ItUser1 Recover快照snap2，fileName=clone1
    // 预期1：返回快照不存在
    std::string uuid1;
    int ret;
    ret = CloneOrRecover(
        "Recover", testUser1_, "UnExistSnapId", testFile1_, false,
        &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    // 操作2：ItUser2 Recover快照snap1，fileName=clone1
    // 预期2:  返回用户认证失败
    ret = CloneOrRecover("Recover", testUser2_,
        snapId, testFile1_, false,
        &uuid1);
    ASSERT_EQ(kErrCodeInvalidUser, ret);

    // 操作3：ItUser1 Recover快照snap1，fileName=clone1
    // 预期3  返回恢复成功
    ret = CloneOrRecover("Recover", testUser1_,
        snapId, testFile1_, false,
        &uuid1);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid1, false);
    ASSERT_TRUE(success1);

    // 验证数据正确性
    std::string fackData(4096, 'x');
    ASSERT_TRUE(CheckFileData(testFile1_, testUser1_, fackData));

    // 操作4：ItUser1 recover 快照snap1，fileName=file2
    // 预期4:  返回目标文件不存在
    ret = CloneOrRecover("Recover", testUser1_,
        snapId, "/ItUser1/notExistFile", false,
        &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);
}

// 场景七: lazy镜像克隆场景
TEST_F(SnapshotCloneServerTest, TestImageLazyClone) {
    // 操作1: ItUser1 clone不存在的镜像，fileName=clone1
    // 预期1：返回文件不存在
    std::string uuid1, uuid2, uuid3, uuid4;
    int ret;
    ret = CloneOrRecover(
        "Clone", testUser1_, "/UnExistFile", "/ItUser1/ImageLazyClone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    // 操作2：ItUser1 clone 镜像file1，fileName=clone1
    // 预期2  返回克隆成功
    std::string dstFile = "/ItUser1/ImageLazyClone1";
    ret = CloneOrRecover("Clone", testUser1_,
        testFile1_, dstFile, true,
        &uuid2);
    ASSERT_EQ(0, ret);

    // 操作3： ItUser1 clone 镜像file1，fileName=clone1 （重复克隆）
    // 预期3：返回文件已存在
    ret = CloneOrRecover("Clone", testUser1_,
        testFile1_, "/ItUser1/ImageLazyClone1", true,
        &uuid3);
    ASSERT_EQ(kErrCodeFileExist, ret);

    // 操作4：对未完成lazy克隆的文件clone1打快照snap1
    // 预期4：返回文件状态异常
    ret = MakeSnapshot(testUser1_, testFile1_, "snap1", &uuid4);
    ASSERT_EQ(kErrCodeFileStatusInvalid, ret);
    FileSnapshotInfo info2;
    int retCode = GetSnapshotInfo(
        testUser1_, testFile1_, uuid4, &info2);
    ASSERT_EQ(kErrCodeFileNotExist, retCode);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid2, true);
    ASSERT_TRUE(success1);

    // 验证数据正确性
    std::string fackData(4096, 'x');
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fackData));
}

// 场景八：非lazy镜像克隆场景
TEST_F(SnapshotCloneServerTest, TestImageNotLazyClone) {
    // 操作1: ItUser1 clone不存在的镜像，fileName=clone1
    // 预期1：返回快照不存在
    std::string uuid1;
    int ret;
    ret = CloneOrRecover(
        "Clone", testUser1_,
        "/UnExistFile", "/ItUser1/ImageNotLazyClone1", false,
        &uuid1);
    ASSERT_EQ(kErrCodeFileNotExist, ret);

    // 操作2：ItUser1 clone 镜像file1，fileName=clone1
    // 预期2  返回克隆成功
    std::string dstFile = "/ItUser1/ImageNotLazyClone1";
    ret = CloneOrRecover("Clone", testUser1_,
        testFile1_, dstFile, false,
        &uuid1);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1_, uuid1, true);
    ASSERT_TRUE(success1);

    // 操作3： ItUser1 clone 镜像file1，fileName=clone1 （重复克隆）
    // 预期3：返回文件已存在
    ret = CloneOrRecover("Clone", testUser1_,
        testFile1_, "/ItUser1/ImageNotLazyClone1", false,
        &uuid1);
    ASSERT_EQ(kErrCodeFileExist, ret);

    // 验证数据正确性
    std::string fackData(4096, 'x');
    ASSERT_TRUE(CheckFileData(dstFile, testUser1_, fackData));
}

// 场景九：快照存在失败场景
TEST_F(SnapshotCloneServerTest, TestSnapAndCloneWhenSnapHasError) {
    std::string snapId = "errorSnapUuid";
    SnapshotRepoItem sr(snapId,
            testUser1_,
            testFile4_,
            "snapxxx",
            0,
            0,
            0,
            0,
            0,
            static_cast<int>(Status::error));
    cluster_->snapshotcloneRepo_->InsertSnapshotRepoItem(sr);

    cluster_->RestartSnapshotCloneServer(1);

    std::string uuid1, uuid2;

    // 操作1:  lazy clone 快照snap1
    // 预期1：返回快照存在异常
    int ret = CloneOrRecover("Clone", testUser1_,
        snapId, "/ItUser2/SnapLazyClone1", true,
        &uuid2);
    ASSERT_EQ(kErrCodeInvalidSnapshot, ret);

    // 操作2：非lazy clone 快照snap1
    // 预期2：返回快照存在异常
    ret = CloneOrRecover("Clone", testUser1_,
        snapId, "/ItUser2/SnapNotLazyClone1", false,
        &uuid2);
    ASSERT_EQ(kErrCodeInvalidSnapshot, ret);

    // 操作3：lazy 从 快照snap1 recover
    // 预期3：返回快照存在异常
    ret = CloneOrRecover("Recover", testUser1_,
        snapId, testFile4_, true,
        &uuid2);
    ASSERT_EQ(kErrCodeInvalidSnapshot, ret);

    // 操作4：非lazy 从 快照snap1 recover
    // 预期4：返回快照存在异常
    ret = CloneOrRecover("Recover", testUser1_,
        snapId, testFile4_, false,
        &uuid2);
    ASSERT_EQ(kErrCodeInvalidSnapshot, ret);

    // 操作5：用户ItUser1对file1打快照snap2
    // 预期5：清理失败快照，并打快照成功
    ret = MakeSnapshot(testUser1_, testFile4_, "snap1", &uuid1);
    ASSERT_EQ(0, ret);

    // 校验快照成功
    bool success1 = CheckSnapshotSuccess(testUser1_, testFile4_, uuid1);
    ASSERT_TRUE(success1);

    // 校验清理失败快照成功
    FileSnapshotInfo info1;
    int retCode = GetSnapshotInfo(
        testUser1_, testFile4_, snapId, &info1);
    ASSERT_EQ(kErrCodeFileNotExist, retCode);
}

// 克隆恢复对存在失败情况下克隆不再做限制，因此此部分用例不再有效
// 场景十：克隆恢复存在失败场景
// TEST_F(SnapshotCloneServerTest, TestCloneAndRecoverWhenHasErrorTask) {
//    std::string snapId;
//    PrepareSnapshotForTestFile1(&snapId);
//    std::string errDstFile = "/ItUser1/errFile";
//    std::string taskId1 = "errTaskId1";
//    CloneRepoItem cr(taskId1,
//            testUser1_,
//            static_cast<uint8_t>(CloneTaskType::kClone),
//            snapId,
//            errDstFile,
//            0,
//            0,
//            0,
//            static_cast<uint8_t>(CloneFileType::kSnapshot),
//            true,
//            0,
//            static_cast<uint8_t>(CloneStatus::error));
//
//    cluster_->snapshotcloneRepo_->InsertCloneRepoItem(cr);
//
//    std::string taskId2 = "errTaskId2";
//    CloneRepoItem cr2(taskId2,
//            testUser1_,
//            static_cast<uint8_t>(CloneTaskType::kRecover),
//            snapId,
//            testFile4_,
//            0,
//            0,
//            0,
//            static_cast<uint8_t>(CloneFileType::kFile),
//            true,
//            0,
//            static_cast<uint8_t>(CloneStatus::error));
//
//    cluster_->snapshotcloneRepo_->InsertCloneRepoItem(cr2);
//
//    cluster_->RestartSnapshotCloneServer(1);
//
//    std::string uuid2;
//
//    // 操作1:  lazy clone 快照snap1为clone1
//    // 预期1：返回存在异常克隆/恢复任务
//    int ret = CloneOrRecover("Clone", testUser1_,
//        snapId, errDstFile, true,
//        &uuid2);
//    ASSERT_EQ(kErrCodeSnapshotCannotCreateWhenError, ret);
//
//    // 操作2：非lazy clone 快照snap1为clone1
//    // 预期2：返回存在异常克隆/恢复任务
//    ret = CloneOrRecover("Clone", testUser1_,
//        snapId, errDstFile, false,
//        &uuid2);
//    ASSERT_EQ(kErrCodeSnapshotCannotCreateWhenError, ret);
//
//    // 操作3:  lazy recover 快照snap1的file1
//    // 预期3：返回存在异常克隆/恢复任务
//    ret = CloneOrRecover("Recover", testUser1_,
//        snapId, testFile4_, true,
//        &uuid2);
//    ASSERT_EQ(kErrCodeSnapshotCannotCreateWhenError, ret);
//
//    // 操作4：非lazy recover 快照snap1为file1
//    // 预期4：返回存在异常克隆/恢复任务
//    ret = CloneOrRecover("Recover", testUser1_,
//        snapId, testFile4_, false,
//        &uuid2);
//    ASSERT_EQ(kErrCodeSnapshotCannotCreateWhenError, ret);
//
//    // 操作5:  从file1 lazy克隆为clone1
//    // 预期5：返回存在异常克隆/恢复任务
//    ret = CloneOrRecover("Clone", testUser1_,
//        testFile1_, errDstFile, true,
//        &uuid2);
//    ASSERT_EQ(kErrCodeSnapshotCannotCreateWhenError, ret);
//
//    // 操作6:  从file1 非lazy克隆为clone1
//    // 预期6：返回存在异常克隆/恢复任务
//    ret = CloneOrRecover("Clone", testUser1_,
//        testFile1_, errDstFile, false,
//        &uuid2);
//    ASSERT_EQ(kErrCodeSnapshotCannotCreateWhenError, ret);
//
//    // 清理数据
//    ret = CleanCloneTask(testUser1_, taskId1);
//    ASSERT_EQ(0, ret);
//    ret = CleanCloneTask(testUser1_, taskId2);
//    ASSERT_EQ(0, ret);
// }

}  // namespace snapshotcloneserver
}  // namespace curve






