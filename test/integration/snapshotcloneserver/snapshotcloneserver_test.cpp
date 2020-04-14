/*
 * Project: curve
 * Created Date: Saturday March 14th 2020
 * Author: hzsunjianliang
 * Copyright (c) 2020 netease
 */

#include <stdio.h>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <chrono>  // NOLINT
#include <thread>  // NOLINT

#include "test/integration/cluster_common/cluster.h"
#include "src/client/libcurve_file.h"
#include "src/snapshotcloneserver/snapshotclone_server.h"
#include "test/integration/snapshotcloneserver/test_snapshotcloneserver_helpler.h"
#include "test/util/config_generator.h"

const std::string kTestPrefix = "MainSCSTest";  // NOLINT

// 一些常数定义
const char* cloneTempDir_ = "/clone";
const char* mdsRootUser_ = "root";
const char* mdsRootPassword_ = "root_password";
const uint64_t segmentSize = 32ULL * 1024 * 1024;

const char* kEtcdClientIpPort = "127.0.0.1:10041";
const char* kEtcdPeerIpPort = "127.0.0.1:10042";
const char* kMdsIpPort = "127.0.0.1:10043";
const char* kSnapshotCloneServerIpPort = "127.0.0.1:10047";
const int kMdsDummyPort = 10048;

const char* kSnapshotCloneServerDummyServerPort = "12004";
const char* kLeaderCampaginPrefix = "snapshotcloneserverleaderlock4";

const std::string kLogPath = "./runlog/" + kTestPrefix + "Log";  // NOLINT
const std::string kMdsDbName = kTestPrefix + "DB";               // NOLINT
const std::string kEtcdName = kTestPrefix;                       // NOLINT
const std::string kMdsConfigPath =                               // NOLINT
    "./test/integration/snapshotcloneserver/config/" + kTestPrefix +
    "_mds.conf";
const std::string kSnapClientConfigPath =                        // NOLINT
    "./test/integration/snapshotcloneserver/config/" + kTestPrefix +
    "_snap_client.conf";
const std::string kS3ConfigPath =                                // NOLINT
    "./test/integration/snapshotcloneserver/config/" + kTestPrefix +
    "_s3.conf";
const std::string kSCSConfigPath =                               // NOLINT
    "./test/integration/snapshotcloneserver/config/" + kTestPrefix +
    "_scs.conf";

const std::vector<std::string> mdsConfigOptions{
    std::string("mds.listen.addr=") + kMdsIpPort,
    std::string("mds.etcd.endpoint=") + kEtcdClientIpPort,
    std::string("mds.DbName=") + kMdsDbName,
    std::string("mds.auth.rootPassword=") + mdsRootPassword_,
    "mds.enable.copyset.scheduler=false",
    "mds.enable.leader.scheduler=false",
    "mds.enable.recover.scheduler=false",
    "mds.enable.replica.scheduler=false",
    "mds.heartbeat.misstimeoutMs=10000",
    "mds.topology.TopologyUpdateToRepoSec=5",
};

const std::vector<std::string> mdsConf1{
    { " --graceful_quit_on_sigterm" },
    std::string(" --confPath=") + kMdsConfigPath,
    std::string(" --log_dir=") + kLogPath,
    std::string(" --segmentSize=") + std::to_string(segmentSize),
    { " --stderrthreshold=3" },
};

const std::vector<std::string> snapClientConfigOptions{
    std::string("mds.listen.addr=") + kMdsIpPort,
};

const std::vector<std::string> s3ConfigOptions{};

const std::vector<std::string> snapshotcloneserverConfigOptions{
    std::string("client.config_path=") + kSnapClientConfigPath,
    std::string("s3.config_path=") + kS3ConfigPath,
    std::string("metastore.db_name=") + kMdsDbName,
    std::string("server.snapshotPoolThreadNum=8"),
    std::string("server.snapshotCoreThreadNum=2"),
    std::string("server.clonePoolThreadNum=8"),
    std::string("server.createCloneChunkConcurrency=2"),
    std::string("server.recoverChunkConcurrency=2"),
    std::string("mds.rootUser=") + mdsRootUser_,
    std::string("mds.rootPassword=") + mdsRootPassword_,
    std::string("client.methodRetryTimeSec=1"),
    std::string("server.clientAsyncMethodRetryTimeSec=1"),
    std::string("etcd.endpoint=") + kEtcdClientIpPort,
    std::string("server.dummy.listen.port=") +
        kSnapshotCloneServerDummyServerPort,
    std::string("leader.campagin.prefix=") + kLeaderCampaginPrefix,
    std::string("server.address=") + kSnapshotCloneServerIpPort,
};

const std::vector<std::string> snapshotcloneConf{
    std::string(" --conf=") + kSCSConfigPath,
    std::string(" --log_dir=") + kLogPath,
    { " --stderrthreshold=3" },
};

namespace curve {
namespace snapshotcloneserver {

class SnapshotCloneServerMainTest : public ::testing::Test {
 public:
    void SetUp() {
        std::string mkLogDirCmd = std::string("mkdir -p ") + kLogPath;
        system(mkLogDirCmd.c_str());

        cluster_ = new CurveCluster();
        ASSERT_NE(nullptr, cluster_);

        // 初始化db
        ASSERT_EQ(0, cluster_->InitDB(kMdsDbName));
        // 在一开始清理数据库和文件
        cluster_->mdsRepo_->dropDataBase();
        cluster_->mdsRepo_->createDatabase();
        cluster_->mdsRepo_->useDataBase();
        cluster_->mdsRepo_->createAllTables();
        cluster_->snapshotcloneRepo_->useDataBase();
        cluster_->snapshotcloneRepo_->createAllTables();
        std::string rmcmd = "rm -rf " + std::string(kEtcdName) + ".etcd";
        system(rmcmd.c_str());

        // 启动etcd
        pid_t pid = cluster_->StartSingleEtcd(
            1, kEtcdClientIpPort, kEtcdPeerIpPort,
            std::vector<std::string>{ " --name " + std::string(kEtcdName) });
        LOG(INFO) << "etcd 1 started on " << kEtcdClientIpPort
                  << "::" << kEtcdPeerIpPort << ", pid = " << pid;
        ASSERT_GT(pid, 0);

        cluster_->PrepareConfig<MDSConfigGenerator>(kMdsConfigPath,
                                                    mdsConfigOptions);

        // 启动一个mds
        pid = cluster_->StartSingleMDS(1, kMdsIpPort, kMdsDummyPort, mdsConf1,
                                       true);
        LOG(INFO) << "mds 1 started on " << kMdsIpPort << ", pid = " << pid;
        ASSERT_GT(pid, 0);

        cluster_->PrepareConfig<S3ConfigGenerator>(kS3ConfigPath,
                                                   s3ConfigOptions);

        cluster_->PrepareConfig<SnapClientConfigGenerator>(
            kSnapClientConfigPath, snapClientConfigOptions);

        cluster_->PrepareConfig<SCSConfigGenerator>(
            kSCSConfigPath, snapshotcloneserverConfigOptions);
    }

    void TearDown() {
        ASSERT_EQ(0, cluster_->StopCluster());
        cluster_->mdsRepo_->dropDataBase();
        delete cluster_;
        cluster_ = nullptr;
    }

 public:
    CurveCluster* cluster_;
};

TEST_F(SnapshotCloneServerMainTest, testmain) {
    std::shared_ptr<Configuration> conf = std::make_shared<Configuration>();
    conf->SetConfigPath(kSCSConfigPath);

    ASSERT_TRUE(conf->LoadConfig());
    LOG(INFO) << kSCSConfigPath;
    conf->PrintConfig();

    SnapShotCloneServer* snapshotCloneServer = new SnapShotCloneServer(conf);

    snapshotCloneServer->InitAllSnapshotCloneOptions();

    snapshotCloneServer->StartDummy();

    snapshotCloneServer->StartCompaginLeader();

    ASSERT_TRUE(snapshotCloneServer->Init());

    ASSERT_TRUE(snapshotCloneServer->Start());

    std::this_thread::sleep_for(std::chrono::seconds(2));

    // 测试验证是否状态为active
    // "curl "127.0.0.1:port/vars/snapshotcloneserver_status"";
    std::string cmd =
        "curl \"127.0.0.1:" + std::string(kSnapshotCloneServerDummyServerPort) +
        "/vars/" + std::string(statusMetricName) + "\"";
    // snapshotcloneserver_status : "active\r\n"
    std::string expectResult = std::string(statusMetricName) + " : \"" +
                               std::string(ACTIVE) + "\"\r\n";

    FILE* fp = popen(cmd.c_str(), "r");
    ASSERT_TRUE(fp != nullptr);
    char buf[1024];
    fread(buf, sizeof(char), sizeof(buf), fp);
    pclose(fp);
    std::string result(buf);
    ASSERT_EQ(result, expectResult);

    snapshotCloneServer->Stop();
    LOG(INFO) << "snapshotCloneServer Stopped";
}
}  // namespace snapshotcloneserver
}  // namespace curve
