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
 * Created Date: 19-09-02
 * Author: lixiaocui
 */

#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <thread>  //NOLINT
#include <chrono>  //NOLINT
#include "test/integration/cluster_common/cluster.h"

namespace curve {
const std::vector<std::string> mdsConf{
    { " --graceful_quit_on_sigterm" },
    { " --confPath=./conf/mds.conf" },
    { " --mdsDbName=cluster_common_curve_mds" },
    { " --sessionInterSec=30" },
};

const std::vector<std::string> chunkserverConf1{
    { " --graceful_quit_on_sigterm" },
    { " -chunkServerStoreUri=local://./basic1/" },
    { " -chunkServerMetaUri=local://./basic1/chunkserver.dat" },
    { " -copySetUri=local://./basic1/copysets" },
    { " -raftSnapshotUri=curve://./basic1/copysets" },
    { " -recycleUri=local://./basic1/recycler" },
    { " -chunkFilePoolDir=./basic1/chunkfilepool/" },
    { " -chunkFilePoolMetaPath=./basic1/chunkfilepool.meta" },
    { " -conf=./conf/chunkserver.conf.example" },
    { " -raft_sync_segments=true" },
    { " -enableChunkfilepool=false" }
};

const std::vector<std::string> chunkserverConf2{
    { " --graceful_quit_on_sigterm" },
    { " -chunkServerStoreUri=local://./basic2/" },
    { " -chunkServerMetaUri=local://./basic2/chunkserver.dat" },
    { " -copySetUri=local://./basic2/copysets" },
    { " -raftSnapshotUri=curve://./basic2/copysets" },
    { " -recycleUri=local://./basic2/recycler" },
    { " -chunkFilePoolDir=./basic2/chunkfilepool/" },
    { " -chunkFilePoolMetaPath=./basic2/chunkfilepool.meta" },
    { " -conf=./conf/chunkserver.conf.example" },
    { " -raft_sync_segments=true" },
    { " -enableChunkfilepool=false" }
};

const std::vector<std::string> chunkserverConf3{
    { " --graceful_quit_on_sigterm" },
    { " -chunkServerStoreUri=local://./basic3/" },
    { " -chunkServerMetaUri=local://./basic3/chunkserver.dat" },
    { " -copySetUri=local://./basic3/copysets" },
    { " -raftSnapshotUri=curve://./basic3/copysets" },
    { " -recycleUri=local://./basic3/recycler" },
    { " -chunkFilePoolDir=./basic3/chunkfilepool/" },
    { " -chunkFilePoolMetaPath=./basic3/chunkfilepool.meta" },
    { " -conf=./conf/chunkserver.conf.example" },
    { " -raft_sync_segments=true" },
    { " -enableChunkfilepool=false" }
};

class ClusterBasicTest : public ::testing::Test {
 protected:
    void SetUp() {
        curveCluster_ = std::make_shared<CurveCluster>();
        // TODO(lixiaocui): 需要用sudo去运行，后续打开
        // curveCluster_->BuildNetWork();
    }

    void TearDown() {
        ASSERT_EQ(0, curveCluster_->StopCluster());
    }

 protected:
    std::shared_ptr<CurveCluster> curveCluster_;
};

// TODO(lixiaocui): 需要sudo运行，ci变更后打开
TEST_F(ClusterBasicTest, DISABLED_test_start_stop_module1) {
    // 起etcd
    pid_t pid = curveCluster_->StartSingleEtcd(
        1, "127.0.0.1:2221", "127.0.0.1:2222",
        std::vector<std::string>{ " --name basic_test_start_stop_module1" });
    LOG(INFO) << "etcd 1 started on 127.0.0.1:2221:2222, pid = " << pid;
    ASSERT_GT(pid, 0);

    // 起mds
    pid = curveCluster_->StartSingleMDS(1, "192.168.200.1:3333", 3334, mdsConf,
                                        true);
    LOG(INFO) << "mds 1 started on 192.168.200.1:3333, pid = " << pid;
    ASSERT_GT(pid, 0);

    // 创建物理池
    ASSERT_EQ(
        0,
        curveCluster_->PreparePhysicalPool(
            1, "./test/integration/cluster_common/cluster_common_topo_1.json"));

    // 创建chunkserver
    pid =
        curveCluster_->StartSingleChunkServerInBackground(1, chunkserverConf1);
    LOG(INFO) << "chunkserver 1 started in background, pid = " << pid;
    ASSERT_GT(pid, 0);
    pid =
        curveCluster_->StartSingleChunkServerInBackground(2, chunkserverConf2);
    LOG(INFO) << "chunkserver 2 started in background, pid = " << pid;
    ASSERT_GT(pid, 0);
    pid =
        curveCluster_->StartSingleChunkServerInBackground(3, chunkserverConf3);
    LOG(INFO) << "chunkserver 3 started in background, pid = " << pid;
    ASSERT_GT(pid, 0);

    // 创建逻辑池和copyset
    ASSERT_EQ(0, curveCluster_->PrepareLogicalPool(
        1, "./test/integration/cluster_common/cluster_common_topo_1.json"));

    // 停掉chunkserver
    ASSERT_EQ(0, curveCluster_->StopChunkServer(1));
    ASSERT_EQ(0, curveCluster_->StopChunkServer(2));
    ASSERT_EQ(0, curveCluster_->StopChunkServer(3));
    // 停掉mds
    ASSERT_EQ(0, curveCluster_->StopMDS(1));
    // 停掉etcd
    ASSERT_EQ(0, curveCluster_->StopEtcd(1));

    system("rm -r test_start_stop_module1.etcd");
}

TEST_F(ClusterBasicTest, test_start_stop_module2) {
    std::string commonDir = "./runlog/ClusterBasicTest.test_start_stop_module2";
    ASSERT_EQ(0, system((std::string("rm -fr ") + commonDir).c_str()));
    ASSERT_EQ(0, system("rm -fr test_start_stop_module2.etcd"));
    ASSERT_EQ(0, system("rm -fr basic*"));
    ASSERT_EQ(0, system((std::string("mkdir ") + commonDir).c_str()));

    // 起etcd
    std::string etcdDir = commonDir + "/etcd.log";
    pid_t pid = curveCluster_->StartSingleEtcd(
        1, "127.0.0.1:2221", "127.0.0.1:2222",
        std::vector<std::string>{ " --name test_start_stop_module2" });
    LOG(INFO) << "etcd 1 started on 127.0.0.1:2221:2222, pid = " << pid;
    ASSERT_GT(pid, 0);
    ASSERT_TRUE(curveCluster_->WaitForEtcdClusterAvalible());

    // 起mds
    auto mdsConfbak = mdsConf;
    auto mdsDir = commonDir + "/mds";
    ASSERT_EQ(0, system((std::string("mkdir ") + mdsDir).c_str()));
    mdsConfbak.emplace_back(" -log_dir=" + mdsDir);
    mdsConfbak.emplace_back(" --etcdAddr=127.0.0.1:2221");
    pid = curveCluster_->StartSingleMDS(1, "127.0.0.1:3333", 3334, mdsConfbak,
                                        true);
    LOG(INFO) << "mds 1 started on 127.0.0.1:3333, pid = " << pid;
    ASSERT_GT(pid, 0);
    // 初始化mdsclient
    MetaServerOption_t op;
    op.mdsRPCTimeoutMs = 4000;
    op.metaaddrvec = std::vector<std::string>{ "127.0.0.1:3333" };
    ASSERT_EQ(0, curveCluster_->InitMdsClient(op));

    // 创建物理池
    ASSERT_EQ(
        0,
        curveCluster_->PreparePhysicalPool(
            1, "./test/integration/cluster_common/cluster_common_topo_2.json"));

    // 创建chunkserver
    auto copy1 = chunkserverConf1;
    std::string chunkserver1Dir = commonDir + "/chunkserver1";
    ASSERT_EQ(0, system((std::string("mkdir ") + chunkserver1Dir).c_str()));
    copy1.push_back(" -mdsListenAddr=127.0.0.1:3333");
    copy1.push_back(" -log_dir=" + chunkserver1Dir);
    pid = curveCluster_->StartSingleChunkServer(1, "127.0.0.1:2002", copy1);
    LOG(INFO) << "chunkserver 1 started on 127.0.0.1:2002, pid = " << pid;
    ASSERT_GT(pid, 0);

    auto copy2 = chunkserverConf2;
    std::string chunkserver2Dir = commonDir + "/chunkserver2";
    ASSERT_EQ(0, system((std::string("mkdir ") + chunkserver2Dir).c_str()));
    copy2.push_back(" -mdsListenAddr=127.0.0.1:3333");
    copy2.push_back(" -log_dir=" + chunkserver2Dir);
    pid = curveCluster_->StartSingleChunkServer(2, "127.0.0.1:2003", copy2);
    LOG(INFO) << "chunkserver 2 started on 127.0.0.1:2003, pid = " << pid;
    ASSERT_GT(pid, 0);

    auto copy3 = chunkserverConf3;
    std::string chunkserver3Dir = commonDir + "/chunkserver3";
    ASSERT_EQ(0, system((std::string("mkdir ") + chunkserver3Dir).c_str()));
    copy3.push_back(" -mdsListenAddr=127.0.0.1:3333");
    copy3.push_back(" -log_dir=" + chunkserver3Dir);
    pid = curveCluster_->StartSingleChunkServer(3, "127.0.0.1:2004", copy3);
    LOG(INFO) << "chunkserver 3 started on 127.0.0.1:2004, pid = " << pid;
    ASSERT_GT(pid, 0);

    // 创建逻辑池和copyset
    ASSERT_EQ(0, curveCluster_->PrepareLogicalPool(
        1, "./test/integration/cluster_common/cluster_common_topo_2.json"));

    // 创建文件
    ASSERT_EQ(0, curveCluster_->CreateFile("test", "test", "/basic_test",
                                           10 * 1024 * 1024 * 1024UL));

    // 获取当前正在服务的mds
    int curMds;
    ASSERT_TRUE(curveCluster_->CurrentServiceMDS(&curMds));
    ASSERT_EQ(1, curMds);

    // hang mds进程
    ASSERT_EQ(0, curveCluster_->HangMDS(1));
    // 创建文件失败
    ASSERT_NE(0, curveCluster_->CreateFile("test1", "test1", "/basic_test1",
                                           10 * 1024 * 1024 * 1024UL));
    // 恢复mds进程
    ASSERT_EQ(0, curveCluster_->RecoverHangMDS(1));
    // 创建文件成功
    ASSERT_EQ(0, curveCluster_->CreateFile("test2", "test2", "/basic_test2",
                                           10 * 1024 * 1024 * 1024UL));

    // 停掉chunkserver
    ASSERT_EQ(0, curveCluster_->StopChunkServer(1));
    ASSERT_EQ(0, curveCluster_->StopChunkServer(2));
    ASSERT_EQ(0, curveCluster_->StopChunkServer(3));
    // 停掉mds
    ASSERT_EQ(0, curveCluster_->StopMDS(1));
    // 停掉etcd
    ASSERT_EQ(0, curveCluster_->StopEtcd(1));
}

TEST_F(ClusterBasicTest, test_multi_mds_and_etcd) {
    std::string commonDir = "./runlog/ClusterBasicTest.test_multi_mds_and_etcd";
    ASSERT_EQ(0, system((std::string("rm -fr ") + commonDir).c_str()));
    ASSERT_EQ(0, system("rm -fr test_multi_etcd_node*.etcd"));
    ASSERT_EQ(0, system((std::string("mkdir ") + commonDir).c_str()));

    // 起三个etcd
    std::string etcdDir = commonDir + "/etcd";
    ASSERT_EQ(0, system((std::string("mkdir ") + etcdDir).c_str()));
    std::string etcdcluster =
        std::string(" --initial-cluster ") +
        std::string("'test_multi_etcd_node1=http://127.0.0.1:2302,") +
        std::string("test_multi_etcd_node2=http://127.0.0.1:2304,") +
        std::string("test_multi_etcd_node3=http://127.0.0.1:2306'");
    pid_t pid = curveCluster_->StartSingleEtcd(
        1, "127.0.0.1:2301", "127.0.0.1:2302",
        std::vector<std::string>{ " --name test_multi_etcd_node1",
                                  etcdcluster });
    LOG(INFO) << "etcd 1 started on 127.0.0.1:2301:2302, pid = " << pid;
    ASSERT_GT(pid, 0);
    ASSERT_FALSE(curveCluster_->WaitForEtcdClusterAvalible(3));

    pid = curveCluster_->StartSingleEtcd(
        2, "127.0.0.1:2303", "127.0.0.1:2304",
        std::vector<std::string>{ " --name test_multi_etcd_node2",
                                  etcdcluster });
    LOG(INFO) << "etcd 2 started on 127.0.0.1:2303:2304, pid = " << pid;
    ASSERT_GT(pid, 0);

    pid = curveCluster_->StartSingleEtcd(
        3, "127.0.0.1:2305", "127.0.0.1:2306",
        std::vector<std::string>{ " --name test_multi_etcd_node3",
                                  etcdcluster });
    LOG(INFO) << "etcd 3 started on 127.0.0.1:2305:2306, pid = " << pid;
    ASSERT_GT(pid, 0);
    ASSERT_TRUE(curveCluster_->WaitForEtcdClusterAvalible());

    // 起三mds
    std::string mds1Dir = commonDir + "/mds1";
    std::string mds2Dir = commonDir + "/mds2";
    std::string mds3Dir = commonDir + "/mds3";
    ASSERT_EQ(0, system((std::string("mkdir ") + mds1Dir).c_str()));
    ASSERT_EQ(0, system((std::string("mkdir ") + mds2Dir).c_str()));
    ASSERT_EQ(0, system((std::string("mkdir ") + mds3Dir).c_str()));
    std::string etcdClinetAddrs("127.0.0.1:2301,127.0.0.1:2303,127.0.0.1:2305");

    auto copy1 = mdsConf;
    copy1.emplace_back(" --etcdAddr=" + etcdClinetAddrs);
    copy1.emplace_back(" -log_dir=" + mds1Dir);
    pid = curveCluster_->StartSingleMDS(1, "127.0.0.1:2310", 2313, copy1, true);
    LOG(INFO) << "mds 1 started on 127.0.0.1:2310, pid = " << pid;
    ASSERT_GT(pid, 0);
    std::this_thread::sleep_for(std::chrono::seconds(1));

    auto copy2 = mdsConf;
    copy1.emplace_back(" --etcdAddr=" + etcdClinetAddrs);
    copy2.emplace_back(" -log_dir=" + mds2Dir);
    pid =
        curveCluster_->StartSingleMDS(2, "127.0.0.1:2311", 2314, copy2, false);
    LOG(INFO) << "mds 2 started on 127.0.0.1:2311, pid = " << pid;
    ASSERT_GT(pid, 0);

    auto copy3 = mdsConf;
    copy1.emplace_back(" --etcdAddr=" + etcdClinetAddrs);
    copy3.emplace_back(" -log_dir=" + mds3Dir);
    pid =
        curveCluster_->StartSingleMDS(3, "127.0.0.1:2312", 2315, copy3, false);
    LOG(INFO) << "mds 3 started on 127.0.0.1:2312, pid = " << pid;
    ASSERT_GT(pid, 0);

    // 获取当前正在服务的mds
    int curMds;
    ASSERT_TRUE(curveCluster_->CurrentServiceMDS(&curMds));
    ASSERT_EQ(1, curMds);

    // 停掉mds
    ASSERT_EQ(0, curveCluster_->StopMDS(1));
    ASSERT_EQ(0, curveCluster_->StopMDS(2));
    ASSERT_EQ(0, curveCluster_->StopMDS(3));
    // 停掉etcd
    ASSERT_EQ(0, curveCluster_->StopEtcd(1));
    ASSERT_EQ(0, curveCluster_->StopEtcd(2));
    ASSERT_EQ(0, curveCluster_->StopEtcd(3));
}
}  // namespace curve
