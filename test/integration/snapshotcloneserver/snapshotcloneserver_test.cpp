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
 * Created Date: Saturday March 14th 2020
 * Author: hzsunjianliang
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <stdio.h>

#include <chrono> // NOLINT
#include <thread> // NOLINT

#include "src/client/libcurve_file.h"
#include "src/snapshotcloneserver/snapshotclone_server.h"
#include "test/integration/cluster_common/cluster.h"
#include "test/integration/snapshotcloneserver/test_snapshotcloneserver_helpler.h"
#include "test/util/config_generator.h"

const std::string kTestPrefix = "MainSCSTest"; // NOLINT

// Some constant definitions
const char *cloneTempDir_ = "/clone";
const char *mdsRootUser_ = "root";
const char *mdsRootPassword_ = "root_password";
const uint64_t segmentSize = 32ULL * 1024 * 1024;

const char *kEtcdClientIpPort = "127.0.0.1:10041";
const char *kEtcdPeerIpPort = "127.0.0.1:10042";
const char *kMdsIpPort = "127.0.0.1:10043";
const char *kSnapshotCloneServerIpPort = "127.0.0.1:10047";
const int kMdsDummyPort = 10048;

const char *kSnapshotCloneServerDummyServerPort = "12004";
const char *kLeaderCampaginPrefix = "snapshotcloneserverleaderlock4";

const std::string kLogPath = "./runlog/" + kTestPrefix + "Log"; // NOLINT
const std::string kMdsDbName = kTestPrefix + "DB";              // NOLINT
const std::string kEtcdName = kTestPrefix;                      // NOLINT
const std::string kMdsConfigPath =                              // NOLINT
    "./test/integration/snapshotcloneserver/config/" + kTestPrefix +
    "_mds.conf";
const std::string kSnapClientConfigPath = // NOLINT
    "./test/integration/snapshotcloneserver/config/" + kTestPrefix +
    "_snap_client.conf";
const std::string kS3ConfigPath = // NOLINT
    "./test/integration/snapshotcloneserver/config/" + kTestPrefix + "_s3.conf";
const std::string kSCSConfigPath = // NOLINT
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
    std::string("mds.snapshotcloneclient.addr=") + kSnapshotCloneServerIpPort,
};

const std::vector<std::string> mdsConf1{
    {"--graceful_quit_on_sigterm"},
    std::string("--confPath=") + kMdsConfigPath,
    std::string("--log_dir=") + kLogPath,
    std::string("--segmentSize=") + std::to_string(segmentSize),
    {"--stderrthreshold=3"},
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
    std::string("server.backEndReferenceRecordScanIntervalMs=100"),
    std::string("server.backEndReferenceFuncScanIntervalMs=1000"),
};

const std::vector<std::string> snapshotcloneConf{
    std::string("--conf=") + kSCSConfigPath,
    std::string("--log_dir=") + kLogPath,
    {"--stderrthreshold=3"},
};

namespace curve
{
    namespace snapshotcloneserver
    {

        class SnapshotCloneServerMainTest : public ::testing::Test
        {
        public:
            void SetUp()
            {
                std::string mkLogDirCmd = std::string("mkdir -p ") + kLogPath;
                system(mkLogDirCmd.c_str());
                system("mkdir -p /data/log/curve ./fakes3");

                cluster_ = new CurveCluster();
                ASSERT_NE(nullptr, cluster_);

                // Initialize db
                std::string rmcmd = "rm -rf " + std::string(kEtcdName) + ".etcd";
                system(rmcmd.c_str());

                // Start etcd
                pid_t pid = cluster_->StartSingleEtcd(
                    1, kEtcdClientIpPort, kEtcdPeerIpPort,
                    std::vector<std::string>{"--name=" + std::string(kEtcdName)});
                LOG(INFO) << "etcd 1 started on " << kEtcdClientIpPort
                          << "::" << kEtcdPeerIpPort << ", pid = " << pid;
                ASSERT_GT(pid, 0);

                cluster_->PrepareConfig<MDSConfigGenerator>(kMdsConfigPath,
                                                            mdsConfigOptions);

                // Start an mds
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

            void TearDown()
            {
                ASSERT_EQ(0, cluster_->StopCluster());
                delete cluster_;
                cluster_ = nullptr;

                std::string rmcmd = "rm -rf " + std::string(kEtcdName) + ".etcd";
                system(rmcmd.c_str());
            }

        public:
            CurveCluster *cluster_;
        };

        TEST_F(SnapshotCloneServerMainTest, testmain)
        {
            std::shared_ptr<Configuration> conf = std::make_shared<Configuration>();
            conf->SetConfigPath(kSCSConfigPath);

            ASSERT_TRUE(conf->LoadConfig());
            LOG(INFO) << kSCSConfigPath;
            conf->PrintConfig();

            SnapShotCloneServer *snapshotCloneServer = new SnapShotCloneServer(conf);

            snapshotCloneServer->InitAllSnapshotCloneOptions();

            snapshotCloneServer->StartDummy();

            snapshotCloneServer->StartCompaginLeader();

            ASSERT_TRUE(snapshotCloneServer->Init());

            ASSERT_TRUE(snapshotCloneServer->Start());

            std::this_thread::sleep_for(std::chrono::seconds(2));

            // Test and verify if the status is active
            // "curl "127.0.0.1:port/vars/snapshotcloneserver_status"";
            std::string cmd =
                "curl \"127.0.0.1:" + std::string(kSnapshotCloneServerDummyServerPort) +
                "/vars/" + std::string(statusMetricName) + "\"";
            // snapshotcloneserver_status : "active\r\n"
            std::string expectResult = std::string(statusMetricName) + " : \"" +
                                       std::string(ACTIVE) + "\"\r\n";

            FILE *fp = popen(cmd.c_str(), "r");
            ASSERT_TRUE(fp != nullptr);
            char buf[1024];
            fread(buf, sizeof(char), sizeof(buf), fp);
            pclose(fp);
            std::string result(buf);
            ASSERT_EQ(result, expectResult);

            snapshotCloneServer->Stop();
            LOG(INFO) << "snapshotCloneServer Stopped";
        }
    } // namespace snapshotcloneserver
} // namespace curve
