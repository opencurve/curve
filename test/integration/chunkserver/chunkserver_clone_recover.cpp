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
 * Created Date: 2020-03-03
 * Author: qinyi
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <json/json.h>
#include <stdio.h>

#include <fstream>
#include <sstream>

#include "include/client/libcurve.h"
#include "src/common/s3_adapter.h"
#include "src/common/timeutility.h"
#include "src/client/inflight_controller.h"
#include "src/chunkserver/cli2.h"
#include "src/common/concurrent/count_down_event.h"
#include "test/integration/common/chunkservice_op.h"
#include "test/integration/client/common/file_operation.h"
#include "test/integration/cluster_common/cluster.h"
#include "test/util/config_generator.h"

using curve::CurveCluster;
using curve::common::CountDownEvent;
using std::string;

#define PHYSICAL_POOL_NAME string("pool0")

#define ETCD_CLIENT_PORT string("9200")
#define ETCD_CLIENT_IP_PORT string("127.0.0.1:9200")
#define ETCD_PEER_PORT string("9201")
#define ETCD_PEER_IP_PORT string("127.0.0.1:9201")

#define MDS0_IP_PORT string("127.0.0.1:9202")
#define ALLMDS_IP_PORT string("127.0.0.1:9202")
#define MDS0_PORT_CONF string("9202")
#define MDS0_DUMMY_CONF string("9205")
#define MDS0_DUMMY_PORT 9205

#define CHUNK_SERVER0_PORT string("9208")
#define CHUNK_SERVER0_IP_PORT string("127.0.0.1:9208")
#define CHUNK_SERVER1_PORT string("9209")
#define CHUNK_SERVER1_IP_PORT string("127.0.0.1:9209")
#define CHUNK_SERVER2_PORT string("9210")
#define CHUNK_SERVER2_IP_PORT string("127.0.0.1:9210")

#define CLIENT_DUMMY_PORT string("9211")

#define CSCLONE_TEST_MDS_DBNAME string("CSClone_Test_DB")
#define ETCD_NAME string("CSClone_Test_ETCD")
#define kS3ConfigBucket string("CSClone_test_bucket")

#define MDS0_CONF_PATH string("./runlog/CSCloneTest/mds0.conf")
#define MDS1_CONF_PATH string("./runlog/CSCloneTest/mds1.conf")
#define MDS2_CONF_PATH string("./runlog/CSCloneTest/mds2.conf")

#define CSCLONE_BASE_DIR string("./runlog/CSCloneTest")
#define CHUNKSERVER_CONF_PATH string("./runlog/CSCloneTest/chunkserver.conf")
#define CHUNKSERVER0_BASE_DIR string("./runlog/CSCloneTest/cs0")
#define CHUNKSERVER1_BASE_DIR string("./runlog/CSCloneTest/cs1")
#define CHUNKSERVER2_BASE_DIR string("./runlog/CSCloneTest/cs2")

#define CURVEFS_FILENAME string("/test_clone_recover")
#define S3_BUCKET_NAME "cs_clone_recover_test"

#define KB 1024

CountDownEvent gCond(1);
int gIoRet;
const uint32_t kChunkSize = 16 * 1024 * 1024;
const uint32_t kChunkServerMaxIoSize = 64 * 1024;

const std::vector<string> mdsConf0{
    { " --confPath=" + MDS0_CONF_PATH },
    { " --log_dir=" + CSCLONE_BASE_DIR },
    { " --mdsDbName=" + CSCLONE_TEST_MDS_DBNAME },
    { " --sessionInterSec=20" },
    { " --etcdAddr=" + ETCD_CLIENT_IP_PORT },
};

const std::vector<string> mdsFileConf0{
    string("mds.listen.addr=") + MDS0_IP_PORT,
    string("mds.dummy.listen.port=") + MDS0_DUMMY_CONF,
    string("global.port=") + MDS0_PORT_CONF,
    string("mds.etcd.endpoint=") + ETCD_CLIENT_IP_PORT,
};

const std::vector<string> clientConf{
    string("mds.listen.addr=") + ALLMDS_IP_PORT,
    string("global.logPath=" + CSCLONE_BASE_DIR),
    string("chunkserver.rpcTimeoutMS=1000"),
    string("chunkserver.opMaxRetry=10"),
    string("global.metricDummyServerStartPort=" + CLIENT_DUMMY_PORT),
};

const string clientConfPath = CSCLONE_BASE_DIR + "/client.conf";  // NOLINT
const string kS3ConfigPath = CSCLONE_BASE_DIR + "/s3.conf";       // NOLINT

const std::vector<string> s3Conf{
    string("s3.snapshot_bucket_name=") + S3_BUCKET_NAME,
};

const std::vector<string> csCommonConf{
    string("mds.listen.addr=" + ALLMDS_IP_PORT),
    string("curve.config_path=" + clientConfPath),
    string("s3.config_path=" + kS3ConfigPath),
};

const std::vector<string> chunkserverConf1{
    { " -chunkServerStoreUri=local://" + CHUNKSERVER0_BASE_DIR },
    { " -chunkServerMetaUri=local://" + CHUNKSERVER0_BASE_DIR +
      "/chunkserver.dat" },
    { " -copySetUri=local://" + CHUNKSERVER0_BASE_DIR + "/copysets" },
    { " -raftSnapshotUri=curve://" + CHUNKSERVER0_BASE_DIR + "/copysets" },
    { " -raftLogUri=curve://" + CHUNKSERVER0_BASE_DIR + "/copysets" },
    { " -recycleUri=local://" + CHUNKSERVER0_BASE_DIR + "/recycler" },
    { " -chunkFilePoolDir=" + CHUNKSERVER0_BASE_DIR + "/chunkfilepool" },
    { " -chunkFilePoolMetaPath=" + CHUNKSERVER0_BASE_DIR +
      "/chunkfilepool.meta" },
    { " -conf=" + CHUNKSERVER_CONF_PATH },
    { " -raft_sync_segments=true" },
    { " --log_dir=" + CSCLONE_BASE_DIR },
    { " --graceful_quit_on_sigterm" },
    { " -chunkServerIp=127.0.0.1" },
    { " -chunkServerPort=" + CHUNK_SERVER0_PORT },
    { " -enableChunkfilepool=false" },
    { " -enableWalfilepool=false" },
    { " -walFilePoolDir=" + CHUNKSERVER0_BASE_DIR + "/walfilepool" },
    { " -walFilePoolMetaPath=" + CHUNKSERVER0_BASE_DIR + "/walfilepool.meta" }
};

const std::vector<string> chunkserverConf2{
    { " -chunkServerStoreUri=local://" + CHUNKSERVER1_BASE_DIR },
    { " -chunkServerMetaUri=local://" + CHUNKSERVER1_BASE_DIR +
      "/chunkserver.dat" },
    { " -copySetUri=local://" + CHUNKSERVER1_BASE_DIR + "/copysets" },
    { " -raftSnapshotUri=curve://" + CHUNKSERVER1_BASE_DIR + "/copysets" },
    { " -raftLogUri=curve://" + CHUNKSERVER1_BASE_DIR + "/copysets" },
    { " -recycleUri=local://" + CHUNKSERVER1_BASE_DIR + "/recycler" },
    { " -chunkFilePoolDir=" + CHUNKSERVER1_BASE_DIR + "/filepool" },
    { " -chunkFilePoolMetaPath=" + CHUNKSERVER1_BASE_DIR +
      "/chunkfilepool.meta" },
    { " -conf=" + CHUNKSERVER_CONF_PATH },
    { " -raft_sync_segments=true" },
    { " --log_dir=" + CSCLONE_BASE_DIR },
    { " --graceful_quit_on_sigterm" },
    { " -chunkServerIp=127.0.0.1" },
    { " -chunkServerPort=" + CHUNK_SERVER1_PORT },
    { " -enableChunkfilepool=false" },
    { " -enableWalfilepool=false" },
    { " -walFilePoolDir=" + CHUNKSERVER1_BASE_DIR + "/walfilepool" },
    { " -walFilePoolMetaPath=" + CHUNKSERVER1_BASE_DIR + "/walfilepool.meta" }
};

const std::vector<string> chunkserverConf3{
    { " -chunkServerStoreUri=local://" + CHUNKSERVER2_BASE_DIR },
    { " -chunkServerMetaUri=local://" + CHUNKSERVER2_BASE_DIR +
      "/chunkserver.dat" },
    { " -copySetUri=local://" + CHUNKSERVER2_BASE_DIR + "/copysets" },
    { " -raftSnapshotUri=curve://" + CHUNKSERVER2_BASE_DIR + "/copysets" },
    { " -raftLogUri=curve://" + CHUNKSERVER2_BASE_DIR + "/copysets" },
    { " -recycleUri=local://" + CHUNKSERVER2_BASE_DIR + "/recycler" },
    { " -chunkFilePoolDir=" + CHUNKSERVER2_BASE_DIR + "/filepool" },
    { " -chunkFilePoolMetaPath=" + CHUNKSERVER2_BASE_DIR +
      "/chunkfilepool.meta" },
    { " -conf=" + CHUNKSERVER_CONF_PATH },
    { " -raft_sync_segments=true" },
    { " --log_dir=" + CSCLONE_BASE_DIR },
    { " --graceful_quit_on_sigterm" },
    { " -chunkServerIp=127.0.0.1" },
    { " -chunkServerPort=" + CHUNK_SERVER2_PORT },
    { " -enableChunkfilepool=false" },
    { " -enableWalfilepool=false" },
    { " -walFilePoolDir=" + CHUNKSERVER2_BASE_DIR + "/walfilepool" },
    { " -walFilePoolMetaPath=" + CHUNKSERVER2_BASE_DIR + "/walfilepool.meta" }
};

namespace curve {
namespace chunkserver {

class CSCloneRecoverTest : public ::testing::Test {
 public:
    CSCloneRecoverTest()
        : logicPoolId_(1), copysetId_(1), chunkData1_(kChunkSize, 'X'),
          chunkData2_(kChunkSize, 'Y') {}

    void SetUp() {
        InitCloneRecoverTestDir();

        cluster_ = new CurveCluster();
        ASSERT_NE(nullptr, cluster_);
        cluster_->PrepareConfig<curve::ClientConfigGenerator>(clientConfPath,
                                                              clientConf);
        cluster_->PrepareConfig<curve::S3ConfigGenerator>(kS3ConfigPath,
                                                          s3Conf);
        cluster_->PrepareConfig<curve::MDSConfigGenerator>(MDS0_CONF_PATH,
                                                           mdsFileConf0);
        // 生成chunkserver配置文件
        cluster_->PrepareConfig<curve::CSConfigGenerator>(CHUNKSERVER_CONF_PATH,
                                                          csCommonConf);

        // 1. 启动etcd
        LOG(INFO) << "begin to start etcd";
        pid_t pid = cluster_->StartSingleEtcd(
            1, ETCD_CLIENT_IP_PORT, ETCD_PEER_IP_PORT,
            std::vector<string>{ " --name " + ETCD_NAME });
        LOG(INFO) << "etcd 1 started on " + ETCD_CLIENT_IP_PORT + ", pid = "
                  << pid;
        ASSERT_GT(pid, 0);
        ASSERT_TRUE(cluster_->WaitForEtcdClusterAvalible(5));

        // 2. 先启动一个mds，让其成为leader，然后再启动另外两个mds节点
        pid = cluster_->StartSingleMDS(0, MDS0_IP_PORT, MDS0_DUMMY_PORT,
                                       mdsConf0, true);
        LOG(INFO) << "mds 0 started on " + MDS0_IP_PORT + ", pid = " << pid;
        ASSERT_GT(pid, 0);
        std::this_thread::sleep_for(std::chrono::seconds(8));

        // 生成topo.json
        Json::Value topo;
        Json::Value servers;
        std::string chunkServerIpPort[] = {CHUNK_SERVER0_IP_PORT,
                                            CHUNK_SERVER1_IP_PORT,
                                            CHUNK_SERVER2_IP_PORT};
        for (int i = 0; i < 3; ++i) {
            Json::Value server;
            std::vector<std::string> ipPort;
            curve::common::SplitString(chunkServerIpPort[i], ":", &ipPort);
            std::string ip = ipPort[0];
            uint64_t port;
            ASSERT_TRUE(curve::common::StringToUll(ipPort[1], &port));
            server["externalip"] = ipPort[0];
            server["externalport"] = port;
            server["internalip"] = ipPort[0];
            server["internalport"] = port;
            server["name"] = std::string("server") + std::to_string(i);
            server["physicalpool"] = PHYSICAL_POOL_NAME;
            server["zone"] = std::string("zone") + std::to_string(i);
            server["poolset"] = std::string("default");
            servers.append(server);
        }
        topo["servers"] = servers;
        Json::Value logicalPools;
        Json::Value logicalPool;
        logicalPool["copysetnum"] = 1;
        logicalPool["name"] = "defaultLogicalPool";
        logicalPool["physicalpool"] = PHYSICAL_POOL_NAME;
        logicalPool["replicasnum"] = 3;
        logicalPool["scatterwidth"] = 0;
        logicalPool["type"] = 0;
        logicalPool["zonenum"] = 3;
        logicalPools.append(logicalPool);
        topo["logicalpools"] = logicalPools;

        std::ofstream topoConf(CSCLONE_BASE_DIR + "/topo.json");
        topoConf << topo.toStyledString();
        topoConf.close();

        // 3. 创建物理池
        string createPPCmd =
            string("./bazel-bin/tools/curvefsTool") +
            string(" -cluster_map=" + CSCLONE_BASE_DIR + "/topo.json") +
            string(" -mds_addr=" + ALLMDS_IP_PORT) +
            string(" -op=create_physicalpool") + string(" -stderrthreshold=0") +
            string(" -rpcTimeOutMs=10000") + string(" -minloglevel=0");

        int ret = 0;
        int retry = 0;
        while (retry < 5) {
            LOG(INFO) << "exec createPPCmd: " << createPPCmd;
            ret = system(createPPCmd.c_str());
            if (ret == 0)
                break;
            retry++;
        }
        ASSERT_EQ(ret, 0);

        // 4. 创建chunkserver
        pid = cluster_->StartSingleChunkServer(1, CHUNK_SERVER0_IP_PORT,
                                               chunkserverConf1);
        LOG(INFO) << "chunkserver 1 started on " + CHUNK_SERVER0_IP_PORT +
                         ", pid = "
                  << pid;
        ASSERT_GT(pid, 0);
        pid = cluster_->StartSingleChunkServer(2, CHUNK_SERVER1_IP_PORT,
                                               chunkserverConf2);
        LOG(INFO) << "chunkserver 2 started on " + CHUNK_SERVER1_IP_PORT +
                         ", pid = "
                  << pid;
        ASSERT_GT(pid, 0);
        pid = cluster_->StartSingleChunkServer(3, CHUNK_SERVER2_IP_PORT,
                                               chunkserverConf3);
        LOG(INFO) << "chunkserver 3 started on " + CHUNK_SERVER2_IP_PORT +
                         ", pid = "
                  << pid;
        ASSERT_GT(pid, 0);

        std::this_thread::sleep_for(std::chrono::seconds(5));

        // 5. 创建逻辑池, 并睡眠一段时间让底层copyset先选主
        string createLPCmd =
            string("./bazel-bin/tools/curvefsTool") +
            string(" -cluster_map=" + CSCLONE_BASE_DIR + "/topo.json") +
            string(" -mds_addr=" + ALLMDS_IP_PORT) +
            string(" -op=create_logicalpool") +
            string(" -stderrthreshold=0 -minloglevel=0");
        ret = 0;
        retry = 0;
        while (retry < 5) {
            LOG(INFO) << "exec createLPCmd: " << createLPCmd;
            ret = system(createLPCmd.c_str());
            if (ret == 0)
                break;
            retry++;
        }
        ASSERT_EQ(ret, 0);
        std::this_thread::sleep_for(std::chrono::seconds(5));

        // 获取chunkserver主节点
        logicPoolId_ = 1;
        copysetId_ = 1;
        ASSERT_EQ(0, chunkSeverGetLeader());
        struct ChunkServiceOpConf conf0 = { &leaderPeer_, logicPoolId_,
                                            copysetId_, 5000 };
        opConf_ = conf0;

        // 6. 初始化client配置
        LOG(INFO) << "init globalclient";
        ret = Init(clientConfPath.c_str());
        ASSERT_EQ(ret, 0);

        // 7. 先睡眠5s，让chunkserver选出leader
        std::this_thread::sleep_for(std::chrono::seconds(5));

        s3Adapter_.Init(kS3ConfigPath);
        s3Adapter_.SetBucketName(S3_BUCKET_NAME);
        if (!s3Adapter_.BucketExist())
            ASSERT_EQ(0, s3Adapter_.CreateBucket());
        s3ObjExisted_ = false;
    }

    void TearDown() {
        UnInit();

        ASSERT_EQ(0, cluster_->StopCluster());
        delete cluster_;

        if (s3ObjExisted_) {
            s3Adapter_.DeleteObject("test1");
            s3Adapter_.DeleteObject("test2");
        }
        s3Adapter_.Deinit();

        system(("rm -rf " + ETCD_NAME + ".etcd").c_str());
        system(("rm -rf " + CHUNKSERVER0_BASE_DIR).c_str());
        system(("rm -rf " + CHUNKSERVER1_BASE_DIR).c_str());
        system(("rm -rf " + CHUNKSERVER2_BASE_DIR).c_str());
    }

    void InitCloneRecoverTestDir() {
        ASSERT_EQ(0, system(("rm -rf " + ETCD_NAME + ".etcd").c_str()));
        ASSERT_EQ(0, system(("rm -rf " + CHUNKSERVER0_BASE_DIR).c_str()));
        ASSERT_EQ(0, system(("rm -rf " + CHUNKSERVER1_BASE_DIR).c_str()));
        ASSERT_EQ(0, system(("rm -rf " + CHUNKSERVER2_BASE_DIR).c_str()));
        ASSERT_EQ(0, system(("mkdir -p " + CHUNKSERVER0_BASE_DIR).c_str()));
        ASSERT_EQ(0, system(("mkdir -p " + CHUNKSERVER1_BASE_DIR).c_str()));
        ASSERT_EQ(0, system(("mkdir -p " + CHUNKSERVER2_BASE_DIR).c_str()));
        ASSERT_EQ(
            0,
            system(("mkdir " + CHUNKSERVER0_BASE_DIR + "/copysets").c_str()));
        ASSERT_EQ(
            0,
            system(("mkdir " + CHUNKSERVER1_BASE_DIR + "/copysets").c_str()));
        ASSERT_EQ(
            0,
            system(("mkdir " + CHUNKSERVER2_BASE_DIR + "/copysets").c_str()));
        ASSERT_EQ(
            0,
            system(("mkdir " + CHUNKSERVER0_BASE_DIR + "/recycler").c_str()));
        ASSERT_EQ(
            0,
            system(("mkdir " + CHUNKSERVER1_BASE_DIR + "/recycler").c_str()));
        ASSERT_EQ(
            0,
            system(("mkdir " + CHUNKSERVER2_BASE_DIR + "/recycler").c_str()));
        ASSERT_EQ(
            0,
            system(("mkdir " + CHUNKSERVER0_BASE_DIR + "/filepool").c_str()));
        ASSERT_EQ(
            0,
            system(("mkdir " + CHUNKSERVER1_BASE_DIR + "/filepool").c_str()));
        ASSERT_EQ(
            0,
            system(("mkdir " + CHUNKSERVER2_BASE_DIR + "/filepool").c_str()));
    }

    /**下发一个写请求并等待完成
     * @param:  offset是当前需要下发IO的偏移
     * @param:  size是下发IO的大小
     * @return: IO是否成功完成
     */
    bool HandleAioWriteRequest(uint64_t offset, uint64_t size,
                               const char* data) {
        gCond.Reset(1);
        gIoRet = 0;

        auto writeCallBack = [](CurveAioContext* context) {
            gIoRet = context->ret;
            char* buffer = reinterpret_cast<char*>(context->buf);
            delete[] buffer;
            delete context;
            // 无论IO是否成功，只要返回，就触发cond
            gCond.Signal();
        };

        char* buffer = new char[size];
        memcpy(buffer, data, size);
        CurveAioContext* context = new CurveAioContext();
        context->op = LIBCURVE_OP::LIBCURVE_OP_WRITE;
        context->offset = offset;
        context->length = size;
        context->buf = buffer;
        context->cb = writeCallBack;

        int ret;
        if (ret = AioWrite(fd_, context)) {
            LOG(ERROR) << "failed to send aio write request, err="
                       << ret;
            return false;
        }

        gCond.Wait();
        if (gIoRet < size) {
            LOG(ERROR) << "write failed, ret=" << gIoRet;
            return false;
        }
        return true;
    }

    /**下发一个读请求并等待完成
     * @param:  offset是当前需要下发IO的偏移
     * @param:  size是下发IO的大小
     * @data:   读出的数据
     * @return: IO是否成功完成
     */
    bool HandleAioReadRequest(uint64_t offset, uint64_t size, char* data) {
        gCond.Reset(1);
        gIoRet = 0;

        auto readCallBack = [](CurveAioContext* context) {
            gIoRet = context->ret;
            delete context;
            // 无论IO是否成功，只要返回，就触发cond
            gCond.Signal();
        };

        CurveAioContext* context = new CurveAioContext();
        context->op = LIBCURVE_OP::LIBCURVE_OP_READ;
        context->offset = offset;
        context->length = size;
        context->buf = data;
        context->cb = readCallBack;
        int ret;
        if (ret = AioRead(fd_, context)) {
            LOG(ERROR) << "failed to send aio read request, err="
                       << ret;
            return false;
        }

        gCond.Wait();
        if (gIoRet < size) {
            LOG(ERROR) << "read failed, ret=" << gIoRet;
            return false;
        }
        return true;
    }

    int chunkSeverGetLeader() {
        Peer peer1, peer2, peer3;
        peer1.set_address(CHUNK_SERVER0_IP_PORT + ":0");
        peer2.set_address(CHUNK_SERVER1_IP_PORT + ":0");
        peer3.set_address(CHUNK_SERVER2_IP_PORT + ":0");
        PeerId peerId1(peer1.address());
        PeerId peerId2(peer2.address());
        PeerId peerId3(peer3.address());
        Configuration csConf;
        csConf.add_peer(peerId1);
        csConf.add_peer(peerId2);
        csConf.add_peer(peerId3);
        butil::Status status = curve::chunkserver::GetLeader(
            logicPoolId_, copysetId_, csConf, &leaderPeer_);
        if (status.ok())
            return 0;
        else
            return -1;
    }

    int TransferLeaderToFollower() {
        Peer peer1, peer2, peer3, follower;
        peer1.set_address(CHUNK_SERVER0_IP_PORT + ":0");
        peer2.set_address(CHUNK_SERVER1_IP_PORT + ":0");
        peer3.set_address(CHUNK_SERVER2_IP_PORT + ":0");
        PeerId peerId1(peer1.address());
        PeerId peerId2(peer2.address());
        PeerId peerId3(peer3.address());
        Configuration csConf;
        csConf.add_peer(peerId1);
        csConf.add_peer(peerId2);
        csConf.add_peer(peerId3);
        if (peer1.address() == leaderPeer_.address()) {
            follower = peer2;
        } else {
            follower = peer1;
        }
        LOG(INFO) << "transfer leader from " << leaderPeer_.address() << " to "
                  << follower.address();
        braft::cli::CliOptions options;
        options.max_retry = 3;
        options.timeout_ms = 5000;
        butil::Status status = curve::chunkserver::TransferLeader(
            logicPoolId_, copysetId_, csConf, follower, options);
        if (!status.ok()) {
            LOG(ERROR) << "transfer leader failed.";
            return -1;
        }

        // 先睡眠5s，让chunkserver选出leader
        std::this_thread::sleep_for(std::chrono::seconds(5));
        status = curve::chunkserver::GetLeader(logicPoolId_, copysetId_, csConf,
                                               &leaderPeer_);
        LOG(INFO) << "Current leader is " << leaderPeer_.address();
        if (status.ok())
            return 0;
        else
            return -1;
    }

    void prepareSourceDataInCurve() {
        // 创建一个curveFS文件
        LOG(INFO) << "create source curveFS file: " << CURVEFS_FILENAME;
        fd_ = curve::test::FileCommonOperation::Open(CURVEFS_FILENAME, "curve");
        ASSERT_NE(fd_, -1);

        // 写数据到curveFS的第1个chunk
        LOG(INFO) << "Write first 16MB of source curveFS file";
        ASSERT_TRUE(HandleAioWriteRequest(0, kChunkSize, chunkData1_.c_str()));

        // 读出数据进行验证
        std::unique_ptr<char[]> temp(new char[kChunkSize]);
        ASSERT_TRUE(HandleAioReadRequest(0, kChunkSize, temp.get()));
        ASSERT_EQ(0, strncmp(chunkData1_.c_str(), temp.get(), kChunkSize));

        // 写数据到curveFS的第2个chunk
        LOG(INFO) << "Write second 16MB of source curveFS file";
        ASSERT_TRUE(
            HandleAioWriteRequest(kChunkSize, kChunkSize, chunkData2_.c_str()));

        // 读出数据进行验证
        ASSERT_TRUE(HandleAioReadRequest(kChunkSize, kChunkSize, temp.get()));
        ASSERT_EQ(0, strncmp(chunkData2_.c_str(), temp.get(), kChunkSize));

        LOG(INFO) << "Prepare curveFS file done";
        curve::test::FileCommonOperation::Close(fd_);
    }

    void prepareSourceDataInS3() {
        ASSERT_EQ(0, s3Adapter_.PutObject("test1", chunkData1_));
        string temp(kChunkSize, '0');
        ASSERT_EQ(0, s3Adapter_.GetObject("test1", &temp));
        ASSERT_EQ(chunkData1_, temp);
        ASSERT_EQ(0, s3Adapter_.PutObject("test2", chunkData2_));
        ASSERT_EQ(0, s3Adapter_.GetObject("test2", &temp));
        ASSERT_EQ(chunkData2_, temp);
        s3ObjExisted_ = true;
    }

    int fd_;
    CurveCluster* cluster_;
    std::map<int, string> ipMap_;
    std::map<int, std::vector<string>> configMap_;

    LogicPoolID logicPoolId_;
    CopysetID copysetId_;
    Peer leaderPeer_;
    struct ChunkServiceOpConf opConf_;

    const string chunkData1_;
    const string chunkData2_;
    curve::common::S3Adapter s3Adapter_;
    bool s3ObjExisted_;
};

// 场景一：通过ReadChunk从curve恢复克隆文件
TEST_F(CSCloneRecoverTest, CloneFromCurveByReadChunk) {
    LOG(INFO) << "current case: CloneFromCurveByReadChunk";

    // 0. 在curve中写入源数据
    prepareSourceDataInCurve();

    // 1. 创建克隆文件
    ChunkServiceVerify verify(&opConf_);
    ChunkID cloneChunk1 = 331;
    ChunkID cloneChunk2 = 332;
    SequenceNum sn0 = 0;
    SequenceNum sn1 = 1;
    SequenceNum sn2 = 2;
    string location(CURVEFS_FILENAME + ":0@cs");
    LOG(INFO) << "clone chunk1 from " << location;
    ASSERT_EQ(0, verify.VerifyCreateCloneChunk(cloneChunk1, location, sn0, sn1,
                                               kChunkSize));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));
    // 重复克隆
    ASSERT_EQ(0, verify.VerifyCreateCloneChunk(cloneChunk1, location, sn0, sn1,
                                               kChunkSize));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));

    location.assign(CURVEFS_FILENAME + ":" + std::to_string(kChunkSize) +
                    "@cs");
    LOG(INFO) << "clone chunk2 from " << location;
    ASSERT_EQ(0, verify.VerifyCreateCloneChunk(cloneChunk2, location, sn0, sn1,
                                               kChunkSize));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk2, sn1, NULL_SN, string("")));

    // 2. 通过readchunk恢复克隆文件
    std::shared_ptr<string> cloneData1(new string(chunkData1_));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, 0, 8 * KB,
                                        cloneData1.get()));

    string temp(8 * KB, 'a');
    ASSERT_EQ(0, verify.VerifyWriteChunk(cloneChunk1, sn1, 0, 8 * KB,
                                         temp.c_str(), cloneData1.get()));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, 0, 8 * KB,
                                        cloneData1.get()));
    temp.assign(8 * KB, 'b');
    ASSERT_EQ(0, verify.VerifyWriteChunk(cloneChunk1, sn1, 4 * KB, 8 * KB,
                                         temp.c_str(), cloneData1.get()));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, 0, 12 * KB,
                                        cloneData1.get()));

    // 通过ReadChunk读遍clone chunk1的所有pages
    for (int offset = 0; offset < kChunkSize; offset += kChunkServerMaxIoSize) {
        ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, offset,
                                            kChunkServerMaxIoSize,
                                            cloneData1.get()));
    }

    /**
     * clone文件遍读后不会转换为普通chunk1文件
     * 通过增大版本进行写入，
     * 如果是clone chunk，写会失败; 如果是普通chunk，则会产生快照文件。
     */
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));
    ASSERT_EQ(CHUNK_OP_STATUS_FAILURE_UNKNOWN,
              verify.VerifyWriteChunk(cloneChunk1, sn2, 0, 8 * KB, temp.c_str(),
                                      nullptr));

    // 删除文件
    ASSERT_EQ(0, verify.VerifyDeleteChunk(cloneChunk1, sn1));
    ASSERT_EQ(0, verify.VerifyDeleteChunk(cloneChunk2, sn1));
}

// 场景二：通过RecoverChunk从curve恢复克隆文件
TEST_F(CSCloneRecoverTest, CloneFromCurveByRecoverChunk) {
    LOG(INFO) << "current case: CloneFromCurveByRecoverChunk";

    // 0. 在curve中写入源数据
    prepareSourceDataInCurve();

    // 1. 创建克隆文件
    ChunkServiceVerify verify(&opConf_);
    ChunkID cloneChunk1 = 333;
    ChunkID cloneChunk2 = 334;
    SequenceNum sn0 = 0;
    SequenceNum sn1 = 1;
    SequenceNum sn2 = 2;
    string location(CURVEFS_FILENAME + ":0@cs");
    LOG(INFO) << "clone chunk1 from " << location;
    ASSERT_EQ(0, verify.VerifyCreateCloneChunk(cloneChunk1, location, sn0, sn1,
                                               kChunkSize));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));
    // 重复克隆
    ASSERT_EQ(0, verify.VerifyCreateCloneChunk(cloneChunk1, location, sn0, sn1,
                                               kChunkSize));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));

    location.assign(CURVEFS_FILENAME + ":" + std::to_string(kChunkSize) +
                    "@cs");
    LOG(INFO) << "clone chunk2 from " << location;
    ASSERT_EQ(0, verify.VerifyCreateCloneChunk(cloneChunk2, location, sn0, sn1,
                                               kChunkSize));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk2, sn1, NULL_SN, string("")));

    // 2. 通过RecoverChunk恢复克隆文件
    std::shared_ptr<string> cloneData1(new string(chunkData1_));
    ASSERT_EQ(0, verify.VerifyRecoverChunk(cloneChunk1, 0, 8 * KB));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, 0, 8 * KB,
                                        cloneData1.get()));

    string temp(8 * KB, 'c');
    ASSERT_EQ(0, verify.VerifyWriteChunk(cloneChunk1, sn1, 0, 8 * KB,
                                         temp.c_str(), cloneData1.get()));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));

    ASSERT_EQ(0, verify.VerifyRecoverChunk(cloneChunk1, 0, 8 * KB));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, 0, 8 * KB,
                                        cloneData1.get()));

    temp.assign(8 * KB, 'd');
    ASSERT_EQ(0, verify.VerifyWriteChunk(cloneChunk1, sn1, 4 * KB, 8 * KB,
                                         temp.c_str(), cloneData1.get()));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));

    ASSERT_EQ(0, verify.VerifyRecoverChunk(cloneChunk1, 0, 12 * KB));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, 0, 12 * KB,
                                        cloneData1.get()));

    // 通过RecoverChunk恢复clone chunk1的所有pages
    for (int offset = 0; offset < kChunkSize; offset += kChunkServerMaxIoSize) {
        ASSERT_EQ(0, verify.VerifyRecoverChunk(cloneChunk1, offset,
                                               kChunkServerMaxIoSize));
    }

    /**
     * 预期clone文件会转换为普通chunk1文件
     * 通过增大版本进行写入，
     * 如果是clone chunk，写会失败; 如果是普通chunk，则会产生快照文件，写成功。
     */
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));
    ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
              verify.VerifyWriteChunk(cloneChunk1, sn2, 0, 8 * KB, temp.c_str(),
                                      nullptr));

    // 删除文件
    ASSERT_EQ(0, verify.VerifyDeleteChunk(cloneChunk1, sn2));
    ASSERT_EQ(0, verify.VerifyDeleteChunk(cloneChunk2, sn1));
}

// 场景三：lazy allocate场景下读克隆文件
TEST_F(CSCloneRecoverTest, CloneFromCurveByReadChunkWhenLazyAlloc) {
    LOG(INFO) << "current case: CloneFromCurveByReadChunkWhenLazyAlloc";

    // 0. 在curve中写入源数据
    prepareSourceDataInCurve();

    // 1. chunk文件不存在
    ChunkServiceVerify verify(&opConf_);
    ChunkID cloneChunk1 = 331;
    SequenceNum sn0 = 0;
    SequenceNum sn1 = 1;
    SequenceNum sn2 = 2;
    string sourceFile = CURVEFS_FILENAME;
    LOG(INFO) << "clone chunk1 from " << sourceFile;
    std::shared_ptr<string> cloneData1(new string(chunkData1_));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, 0, 8 * KB,
                                        cloneData1.get(), CURVEFS_FILENAME, 0));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));

    string temp(8 * KB, 'a');
    ASSERT_EQ(0,
              verify.VerifyWriteChunk(cloneChunk1, sn1, 0, 8 * KB, temp.c_str(),
                                      cloneData1.get(), CURVEFS_FILENAME, 0));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));
    ASSERT_EQ(CHUNK_OP_STATUS_FAILURE_UNKNOWN,
              verify.VerifyWriteChunk(cloneChunk1, sn2, 0, 8 * KB, temp.c_str(),
                                      nullptr));

    // 将leader切换到follower
    ASSERT_EQ(0, TransferLeaderToFollower());
    // 2. 通过readchunk恢复克隆文件
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, 0, 12 * KB,
                                        cloneData1.get(), CURVEFS_FILENAME, 0));

    temp.assign(8 * KB, 'b');
    ASSERT_EQ(0, verify.VerifyWriteChunk(cloneChunk1, sn1, 4 * KB, 8 * KB,
                                         temp.c_str(), cloneData1.get(),
                                         CURVEFS_FILENAME, 0));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, 0, 12 * KB,
                                        cloneData1.get(), CURVEFS_FILENAME, 0));

    // 通过ReadChunk读遍clone chunk1的所有pages
    string ioBuf(kChunkServerMaxIoSize, 'c');
    for (int offset = 0; offset < kChunkSize; offset += kChunkServerMaxIoSize) {
        ASSERT_EQ(0, verify.VerifyWriteChunk(
                         cloneChunk1, sn1, offset, kChunkServerMaxIoSize,
                         ioBuf.c_str(), cloneData1.get(), CURVEFS_FILENAME, 0));
    }
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, 0, 12 * KB,
                                        cloneData1.get(), CURVEFS_FILENAME, 0));

    /**
     * clone文件遍写后会转换为普通chunk1文件
     * 通过增大版本进行写入，
     * 如果是clone chunk，写会失败; 如果是普通chunk，则会产生快照文件。
     */
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));
    ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
              verify.VerifyWriteChunk(cloneChunk1, sn2, 0, 8 * KB, temp.c_str(),
                                      nullptr));

    // 删除文件
    ASSERT_EQ(0, verify.VerifyDeleteChunk(cloneChunk1, sn2));
}

// 场景四：通过ReadChunk从S3恢复克隆文件
TEST_F(CSCloneRecoverTest, CloneFromS3ByReadChunk) {
    LOG(INFO) << "current case: CloneFromS3ByReadChunk";

    // 0. 在S3中写入源数据
    prepareSourceDataInS3();

    // 1. 创建克隆文件
    ChunkServiceVerify verify(&opConf_);
    ChunkID cloneChunk1 = 335;
    ChunkID cloneChunk2 = 336;
    SequenceNum sn0 = 0;
    SequenceNum sn1 = 1;
    SequenceNum sn2 = 2;
    string location("test1@s3");
    LOG(INFO) << "clone chunk1 from " << location;
    ASSERT_EQ(0, verify.VerifyCreateCloneChunk(cloneChunk1, location, sn0, sn1,
                                               kChunkSize));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));
    // 重复克隆
    ASSERT_EQ(0, verify.VerifyCreateCloneChunk(cloneChunk1, location, sn0, sn1,
                                               kChunkSize));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));

    location.assign("test2@s3");
    LOG(INFO) << "clone chunk2 from " << location;
    ASSERT_EQ(0, verify.VerifyCreateCloneChunk(cloneChunk2, location, sn0, sn1,
                                               kChunkSize));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk2, sn1, NULL_SN, string("")));

    // 2. 通过readchunk恢复克隆文件
    std::shared_ptr<string> cloneData1(new string(chunkData1_));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, 0, 8 * KB,
                                        cloneData1.get()));

    string temp(8 * KB, 'a');
    ASSERT_EQ(0, verify.VerifyWriteChunk(cloneChunk1, sn1, 0, 8 * KB,
                                         temp.c_str(), cloneData1.get()));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, 0, 8 * KB,
                                        cloneData1.get()));
    temp.assign(8 * KB, 'b');
    ASSERT_EQ(0, verify.VerifyWriteChunk(cloneChunk1, sn1, 4 * KB, 8 * KB,
                                         temp.c_str(), cloneData1.get()));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, 0, 12 * KB,
                                        cloneData1.get()));

    // 通过ReadChunk读遍clone chunk1的所有pages
    for (int offset = 0; offset < kChunkSize; offset += kChunkServerMaxIoSize) {
        ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, offset,
                                            kChunkServerMaxIoSize,
                                            cloneData1.get()));
    }

    /**
     * 预期clone文件遍读后不会转换为普通chunk1文件
     * 通过增大版本进行写入，
     * 如果是clone chunk，写会失败; 如果是普通chunk，则会产生快照文件。
     */
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));
    ASSERT_EQ(CHUNK_OP_STATUS_FAILURE_UNKNOWN,
              verify.VerifyWriteChunk(cloneChunk1, sn2, 0, 8 * KB, temp.c_str(),
                                      nullptr));

    // 删除文件
    ASSERT_EQ(0, verify.VerifyDeleteChunk(cloneChunk1, sn1));
    ASSERT_EQ(0, verify.VerifyDeleteChunk(cloneChunk2, sn1));
}

// 场景五：通过RecoverChunk从S3恢复克隆文件
TEST_F(CSCloneRecoverTest, CloneFromS3ByRecoverChunk) {
    LOG(INFO) << "current case: CloneFromS3ByRecoverChunk";

    // 0. 在S3中写入源数据
    prepareSourceDataInS3();

    // 1. 创建克隆文件
    ChunkServiceVerify verify(&opConf_);
    ChunkID cloneChunk1 = 337;
    ChunkID cloneChunk2 = 338;
    SequenceNum sn0 = 0;
    SequenceNum sn1 = 1;
    SequenceNum sn2 = 2;
    string location("test1@s3");
    LOG(INFO) << "clone chunk1 from " << location;
    ASSERT_EQ(0, verify.VerifyCreateCloneChunk(cloneChunk1, location, sn0, sn1,
                                               kChunkSize));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));
    // 重复克隆
    ASSERT_EQ(0, verify.VerifyCreateCloneChunk(cloneChunk1, location, sn0, sn1,
                                               kChunkSize));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));

    location.assign("test2@s3");
    LOG(INFO) << "clone chunk2 from " << location;
    ASSERT_EQ(0, verify.VerifyCreateCloneChunk(cloneChunk2, location, sn0, sn1,
                                               kChunkSize));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk2, sn1, NULL_SN, string("")));

    // 2. 通过RecoverChunk恢复克隆文件
    std::shared_ptr<string> cloneData1(new string(chunkData1_));
    ASSERT_EQ(0, verify.VerifyRecoverChunk(cloneChunk1, 0, 8 * KB));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, 0, 8 * KB,
                                        cloneData1.get()));

    string temp(8 * KB, 'c');
    ASSERT_EQ(0, verify.VerifyWriteChunk(cloneChunk1, sn1, 0, 8 * KB,
                                         temp.c_str(), cloneData1.get()));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));

    ASSERT_EQ(0, verify.VerifyRecoverChunk(cloneChunk1, 0, 8 * KB));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, 0, 8 * KB,
                                        cloneData1.get()));

    temp.assign(8 * KB, 'd');
    ASSERT_EQ(0, verify.VerifyWriteChunk(cloneChunk1, sn1, 4 * KB, 8 * KB,
                                         temp.c_str(), cloneData1.get()));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));

    ASSERT_EQ(0, verify.VerifyRecoverChunk(cloneChunk1, 0, 12 * KB));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn1, 0, 12 * KB,
                                        cloneData1.get()));

    // 通过RecoverChunk恢复clone chunk1的所有pages
    for (int offset = 0; offset < kChunkSize; offset += kChunkServerMaxIoSize) {
        ASSERT_EQ(0, verify.VerifyRecoverChunk(cloneChunk1, offset,
                                               kChunkServerMaxIoSize));
    }

    /**
     * 预期clone文件会转换为普通chunk1文件
     * 通过增大版本进行写入，
     * 如果是clone chunk，写会失败; 如果是普通chunk，则会产生快照文件。
     */
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn1, NULL_SN, string("")));
    ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
              verify.VerifyWriteChunk(cloneChunk1, sn2, 0, 8 * KB, temp.c_str(),
                                      nullptr));

    // 删除文件
    ASSERT_EQ(0, verify.VerifyDeleteChunk(cloneChunk1, sn2));
    ASSERT_EQ(0, verify.VerifyDeleteChunk(cloneChunk2, sn1));
}

// 场景六：通过ReadChunk从S3恢复
TEST_F(CSCloneRecoverTest, RecoverFromS3ByReadChunk) {
    LOG(INFO) << "current case: RecoverFromS3ByReadChunk";

    // 0. 构造数据上传到S3，模拟转储
    prepareSourceDataInS3();

    // 1. 创建克隆文件
    ChunkServiceVerify verify(&opConf_);
    ChunkID cloneChunk1 = 339;
    ChunkID cloneChunk2 = 340;
    SequenceNum sn2 = 2;
    SequenceNum sn3 = 3;
    SequenceNum sn4 = 4;
    string location("test1@s3");
    LOG(INFO) << "clone chunk1 from " << location;
    ASSERT_EQ(0, verify.VerifyCreateCloneChunk(cloneChunk1, location, sn3, sn2,
                                               kChunkSize));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn2, NULL_SN, string("")));
    // 重复克隆
    ASSERT_EQ(0, verify.VerifyCreateCloneChunk(cloneChunk1, location, sn3, sn2,
                                               kChunkSize));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn2, NULL_SN, string("")));

    // 2. 通过readchunk恢复克隆文件
    std::shared_ptr<string> cloneData1(new string(chunkData1_));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn3, 0, 8 * KB,
                                        cloneData1.get()));

    string temp(8 * KB, 'a');
    ASSERT_EQ(0, verify.VerifyWriteChunk(cloneChunk1, sn3, 0, 8 * KB,
                                         temp.c_str(), cloneData1.get()));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn3, NULL_SN, string("")));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn3, 0, 8 * KB,
                                        cloneData1.get()));
    temp.assign(8 * KB, 'b');
    ASSERT_EQ(0, verify.VerifyWriteChunk(cloneChunk1, sn3, 4 * KB, 8 * KB,
                                         temp.c_str(), cloneData1.get()));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn3, NULL_SN, string("")));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn3, 0, 12 * KB,
                                        cloneData1.get()));

    // 通过ReadChunk读遍clone chunk1的所有pages
    for (int offset = 0; offset < kChunkSize; offset += kChunkServerMaxIoSize) {
        ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn3, offset,
                                            kChunkServerMaxIoSize,
                                            cloneData1.get()));
    }

    /**
     * 预期clone文件不会转换为普通chunk1文件
     * 通过增大版本进行写入，
     * 如果是clone chunk，写会失败; 如果是普通chunk，则会产生快照文件。
     */
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn3, NULL_SN, string("")));
    ASSERT_EQ(CHUNK_OP_STATUS_FAILURE_UNKNOWN,
              verify.VerifyWriteChunk(cloneChunk1, sn4, 0, 8 * KB, temp.c_str(),
                                      nullptr));

    // 删除文件
    ASSERT_EQ(0, verify.VerifyDeleteChunk(cloneChunk1, sn3));
}

// 场景七：通过RecoverChunk从S3恢复
TEST_F(CSCloneRecoverTest, RecoverFromS3ByRecoverChunk) {
    LOG(INFO) << "current case: RecoverFromS3ByRecoverChunk";

    // 0. 在S3中写入源数据
    prepareSourceDataInS3();

    // 1. 创建克隆文件
    ChunkServiceVerify verify(&opConf_);
    ChunkID cloneChunk1 = 341;
    ChunkID cloneChunk2 = 342;
    SequenceNum sn2 = 2;
    SequenceNum sn3 = 3;
    SequenceNum sn4 = 4;
    string location("test1@s3");
    LOG(INFO) << "clone chunk1 from " << location;
    ASSERT_EQ(0, verify.VerifyCreateCloneChunk(cloneChunk1, location, sn3, sn2,
                                               kChunkSize));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn2, NULL_SN, string("")));
    // 重复克隆
    ASSERT_EQ(0, verify.VerifyCreateCloneChunk(cloneChunk1, location, sn3, sn2,
                                               kChunkSize));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn2, NULL_SN, string("")));

    // 2. 通过RecoverChunk恢复克隆文件
    std::shared_ptr<string> cloneData1(new string(chunkData1_));
    ASSERT_EQ(0, verify.VerifyRecoverChunk(cloneChunk1, 0, 8 * KB));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn3, 0, 8 * KB,
                                        cloneData1.get()));

    string temp(8 * KB, 'c');
    ASSERT_EQ(0, verify.VerifyWriteChunk(cloneChunk1, sn3, 0, 8 * KB,
                                         temp.c_str(), cloneData1.get()));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn3, NULL_SN, string("")));

    ASSERT_EQ(0, verify.VerifyRecoverChunk(cloneChunk1, 0, 8 * KB));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn3, 0, 8 * KB,
                                        cloneData1.get()));

    temp.assign(8 * KB, 'd');
    ASSERT_EQ(0, verify.VerifyWriteChunk(cloneChunk1, sn3, 4 * KB, 8 * KB,
                                         temp.c_str(), cloneData1.get()));
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn3, NULL_SN, string("")));

    ASSERT_EQ(0, verify.VerifyRecoverChunk(cloneChunk1, 0, 12 * KB));
    ASSERT_EQ(0, verify.VerifyReadChunk(cloneChunk1, sn3, 0, 12 * KB,
                                        cloneData1.get()));

    // 通过RecoverChunk恢复clone chunk1的所有pages
    for (int offset = 0; offset < kChunkSize; offset += kChunkServerMaxIoSize) {
        ASSERT_EQ(0, verify.VerifyRecoverChunk(cloneChunk1, offset,
                                               kChunkServerMaxIoSize));
    }

    /**
     * 预期clone文件会转换为普通chunk1文件
     * 通过增大版本进行写入，
     * 如果是clone chunk，写会失败; 如果是普通chunk，则会产生快照文件。
     */
    ASSERT_EQ(0,
              verify.VerifyGetChunkInfo(cloneChunk1, sn3, NULL_SN, string("")));
    ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
              verify.VerifyWriteChunk(cloneChunk1, sn4, 0, 8 * KB, temp.c_str(),
                                      nullptr));

    // 删除文件
    ASSERT_EQ(0, verify.VerifyDeleteChunk(cloneChunk1, sn4));
}

}  // namespace chunkserver
}  // namespace curve
