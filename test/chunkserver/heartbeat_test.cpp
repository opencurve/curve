/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/12/23  Wenyu Zhou   Initial version
 */

#include "test/chunkserver/heartbeat_test.h"

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <string>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/configuration.h"
#include "src/chunkserver/heartbeat.h"
#include "src/chunkserver/cli.h"

#include "src/chunkserver/chunkserverStorage/chunkserver_adaptor_util.h"
#include "test/client/fake/fakeMDS.h"

uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 16 * 1024 * 1024;   // NOLINT
std::string metaserver_addr = "127.0.0.1:9300";   // NOLINT

namespace curve {
namespace chunkserver {

const LogicPoolID   poolId = 666;
const CopysetID     copysetId = 888;

// For easy debuggin usage
// #define DVLOG(level) LOG(INFO)

static char* confPath[3] = {
    "test/chunkserver/chunkserver.conf.0",
    "test/chunkserver/chunkserver.conf.1",
    "test/chunkserver/chunkserver.conf.2",
};

int RmDirData(std::string uri) {
    char cmd[1024] = "";
    std::string dir = FsAdaptorUtil::GetPathFromUri(uri);
    CHECK(dir != "") << "rmdir got empty dir string, halting immediately.";
    snprintf(cmd, sizeof(cmd) - 1, "rm -rf %s/*", dir.c_str());

    DVLOG(6) << "executing command: " << cmd;

    return system(cmd);
}

int RmFile(std::string uri) {
    char cmd[1024] = "";
    std::string path = FsAdaptorUtil::GetPathFromUri(uri);

    snprintf(cmd, sizeof(cmd) - 1, "rm -f %s", path.c_str());

    DVLOG(6) << "executing command: " << cmd;

    return system(cmd);
}

int RemovePeersData(bool rmChunkServerMeta) {
    common::Configuration conf;
    for (int i = 0; i < 3; i ++) {
        conf.SetConfigPath(confPath[i]);
        CHECK(conf.LoadConfig()) << "load conf err";

        std::string res;
        LOG_IF(FATAL, !conf.GetStringValue("copyset.chunk_data_uri", &res));
        if (RmDirData(res)) {
            LOG(ERROR) << "Failed to remove node " << i
                       << " data dir: " << strerror(errno);
            return -1;
        }

        LOG_IF(FATAL, !conf.GetStringValue("copyset.raft_log_uri", &res));
        if (RmDirData(res)) {
            LOG(ERROR) << "Failed to remove node " << i
                       << " log dir: " << strerror(errno);
            return -1;
        }

        LOG_IF(FATAL, !conf.GetStringValue("copyset.raft_log_uri", &res));
        if (RmDirData(res)) {
            LOG(ERROR) << "Failed to remove node " << i
                       << " raft meta dir: " << strerror(errno);
            return -1;
        }

        LOG_IF(FATAL, !conf.GetStringValue("copyset.raft_snapshot_uri", &res));
        if (RmDirData(res)) {
            LOG(ERROR) << "Failed to remove node " << i
                       << " raft snapshot dir: " << strerror(errno);
            return -1;
        }

        LOG_IF(FATAL, !conf.GetStringValue("copyset.recycler_uri", &res));
        if (RmDirData(res)) {
            LOG(ERROR) << "Failed to remove node " << i
                       << " raft recycler dir: " << strerror(errno);
            return -1;
        }

        LOG_IF(FATAL, !conf.GetStringValue("chunkserver.meta_uri", &res));
        if (rmChunkServerMeta) {
            if (RmFile(res)) {
                LOG(ERROR) << "Failed to remove node " << i
                           << " chunkserver meta file: " << strerror(errno);
                return -1;
            }
        }
    }

    return 0;
}

class HeartbeatTest : public ::testing::Test {
 public:
    HeartbeatTest()  {
        HeartbeatTest::activeTest_ = this;
        handlerReady_.store(false, std::memory_order_release);
    }

    std::atomic<bool>& GetReady() {
        return handlerReady_;
    }

    std::mutex& GetMutex() {
        return hbMtx_;
    }

    std::condition_variable& GetCV() {
        return hbCV_;
    }

    void ReleaseHeartbeat();

    void CleanPeer(const std::string& peer);
    void CreateCopysetPeers(const std::string& conf);
    void WaitCopysetReady(const std::string& conf);
    void TransferLeaderSync(const std::string& conf,
                            const std::string& newLeader);

    void SetHeartbeatInfo(::google::protobuf::RpcController* cntl,
                          const HeartbeatRequest* request,
                          HeartbeatResponse* response,
                          ::google::protobuf::Closure* done);

    void GetHeartbeat(::google::protobuf::RpcController** cntl,
                      const HeartbeatRequest** request,
                      HeartbeatResponse** response,
                      ::google::protobuf::Closure** done);

    void PutHeartbeat();

    static void HeartbeatCallback(::google::protobuf::RpcController* controller,
                                  const HeartbeatRequest* request,
                                  HeartbeatResponse* response,
                                  ::google::protobuf::Closure* done);

    void SetUp() {
        RemovePeersData();
        size_t      filesize =  10uL * 1024 * 1024 * 1024;
        std::string filename = "test.img";
        std::string confPath = "conf/client.conf";

        mds_ = new FakeMDS(filename);

        mds_->SetChunkServerHeartbeatCallback(HeartbeatTest::HeartbeatCallback);
        LOG(INFO) << "Hooked up Heartbeat callback.";

        mds_->Initialize();
        mds_->StartService();
    }

    void TearDown() {
        ReleaseHeartbeat();
        mds_->UnInitialize();
        delete mds_;
    }

 private:
    FakeMDS*                    mds_;

    mutable std::mutex          hbMtx_;
    std::condition_variable     hbCV_;
    std::atomic<bool>           handlerReady_;

    ::google::protobuf::RpcController*  cntl_;
    const HeartbeatRequest*             req_;
    HeartbeatResponse*                  resp_;
    ::google::protobuf::Closure*        done_;

    static HeartbeatTest*               activeTest_;
};

HeartbeatTest* HeartbeatTest::activeTest_ = nullptr;

void HeartbeatTest::CleanPeer(const std::string& peer) {
    /*
     * Fetch heartbeat request and answer it with cleaning peer response
     */
    ::google::protobuf::RpcController*  cntl;
    ::google::protobuf::Closure*        done;
    const HeartbeatRequest*             req;
    HeartbeatResponse*                  resp;

    LOG(INFO) << "Cleaning peer " << peer;

    int64_t t0 = butil::monotonic_time_ms();
    int64_t t1 = butil::monotonic_time_ms();
    while (t1 - t0 < 30 * 1000) {
        GetHeartbeat(&cntl, &req, &resp, &done);
        brpc::ClosureGuard done_guard(done);

        DVLOG(6) << "Chunkserver ID: " << req->chunkserverid()
                 << ", IP: " << req->ip() << ", port: " << req->port()
                 << ", copyset count: " << req->copysetcount()
                 << ", leader count: " << req->leadercount();

        std::string sender = req->ip() + ":" + std::to_string(req->port())
                             + ":0";
        if (sender != peer) {
            continue;
        }
        if (req->copysetinfos_size() >= 1) {
            int i = 0;
            for (; i < req->copysetinfos_size(); i ++) {
                if ( req->copysetinfos(i).logicalpoolid() == poolId &&
                     req->copysetinfos(i).copysetid() == copysetId ) {
                    break;
                }
            }
            if (i >= req->copysetinfos_size()) {
                break;
            }

            const curve::mds::heartbeat::CopySetInfo& info =
                req->copysetinfos(i);

            std::string peersStr = info.peers(0).address();

            DVLOG(6) << "Copyset " << i << " <" << info.logicalpoolid()
                     << ", " << info.copysetid()
                     << ">, epoch: " << info.epoch()
                     << ", leader: " << info.leaderpeer().address()
                     << ", peers: " << peersStr;

            if (info.has_configchangeinfo()) {
                const ConfigChangeInfo& cxInfo = info.configchangeinfo();
                DVLOG(6) << "Config change info: peer: "
                         << cxInfo.peer().address()
                         << ", finished: " << cxInfo.finished()
                         << ", errno: " << cxInfo.err().errtype()
                         << ", errmsg: " << cxInfo.err().errmsg();
            }

            {
                // answer with cleaning peer response
                CopySetConf* conf = resp->add_needupdatecopysets();

                conf->set_logicalpoolid(poolId);
                conf->set_copysetid(copysetId);
                conf->set_epoch(info.epoch());

                LOG(INFO) << "Answer peer " << sender
                          << " with cleaning peer response";
            }
        } else {
            break;
        }
        t1 = butil::monotonic_time_ms();
    }

    ASSERT_LE(t1 - t0, 30 * 1000);
    LOG(INFO) << "Cleaning peer " << peer << " finished successfully";
}

void HeartbeatTest::CreateCopysetPeers(const std::string& confStr) {
    braft::Configuration    conf;
    ASSERT_EQ(0, conf.parse_from(confStr));

    std::vector<braft::PeerId> peers;
    conf.list_peers(&peers);
    std::vector<braft::PeerId>::iterator it;
    for (it = peers.begin(); it != peers.end(); it++) {
        int retry = 30;
        while (retry > 0) {
            brpc::Channel channel;
            ASSERT_EQ(0, channel.Init(it->addr, NULL));

            brpc::Controller cntl;
            CopysetRequest request;
            CopysetResponse response;

            cntl.set_timeout_ms(3000);
            request.set_logicpoolid(poolId);
            request.set_copysetid(copysetId);
            std::vector<braft::PeerId>::iterator peer;
            for (peer = peers.begin(); peer != peers.end(); peer++) {
                request.add_peerid(peer->to_string());
            }

            curve::chunkserver::CopysetService_Stub copyset_stub(&channel);
            LOG(INFO) << "Creating copyset <" << poolId << ", " << copysetId
                      << ">: " << conf
                      << " on peer: " << it->addr;
            copyset_stub.CreateCopysetNode(&cntl,
                                           &request,
                                           &response,
                                           nullptr);

            if (cntl.Failed()) {
                LOG(ERROR) << "Creating copyset failed: "
                           << cntl.ErrorCode() << " " << cntl.ErrorText();
            } else if (COPYSET_OP_STATUS_EXIST == response.status()) {
                LOG(INFO) << "Skipped creating existed copyset <"
                          << poolId << ", " << copysetId << ">: " << conf
                          << " on peer: " << it->addr;
                break;
            } else if (COPYSET_OP_STATUS_SUCCESS == response.status()) {
                break;
            }

            LOG(INFO) << "Create copyset failed: " << response.status()
                      << ", retrying again.";
            --retry;
            sleep(1);
        }
    }
}

void HeartbeatTest::WaitCopysetReady(const std::string& confStr) {
    braft::PeerId peerId;
    butil::Status status;
    Configuration conf;

    ASSERT_EQ(0, conf.parse_from(confStr));

    int64_t t0 = butil::monotonic_time_ms();
    while (true) {
        status = GetLeader(poolId, copysetId, conf, &peerId);
        if (status.ok()) {
            break;
        }
        int64_t t1 = butil::monotonic_time_ms();
        ASSERT_LT(t1 - t0, 15 * 1000);
    }
}

void HeartbeatTest::TransferLeaderSync(const std::string& confStr,
                                       const std::string& newLeader) {
    braft::PeerId peerId;
    butil::Status status;
    Configuration conf;

    braft::cli::CliOptions opt;
    opt.timeout_ms = 3000;
    opt.max_retry = 3;

    ASSERT_EQ(0, conf.parse_from(confStr));
    ASSERT_EQ(0, peerId.parse(newLeader));

    int64_t t0 = butil::monotonic_time_ms();
    while (true) {
        status = TransferLeader(poolId, copysetId, conf, peerId, opt);
        if (status.ok()) {
            break;
        }
        int64_t t1 = butil::monotonic_time_ms();
        ASSERT_LT(t1 - t0, 10 * 1000);
    }
}

void HeartbeatTest::ReleaseHeartbeat() {
    mds_->SetChunkServerHeartbeatCallback(nullptr);
    LOG(INFO) << "Release Heartbeat callback.";
    PutHeartbeat();
}

void HeartbeatTest::SetHeartbeatInfo(::google::protobuf::RpcController* cntl,
                                     const HeartbeatRequest* request,
                                     HeartbeatResponse* response,
                                     ::google::protobuf::Closure* done) {
    cntl_ = cntl;
    req_ = request;
    resp_ = response;
    done_ = done;
}

void HeartbeatTest::GetHeartbeat(::google::protobuf::RpcController** cntl,
                                 const HeartbeatRequest** request,
                                 HeartbeatResponse** response,
                                 ::google::protobuf::Closure** done) {
    std::unique_lock<std::mutex> lock(GetMutex());

    handlerReady_.store(true, std::memory_order_release);
    GetCV().wait(lock);

    *cntl = cntl_;
    *request = req_;
    *response = resp_;
    *done = done_;

    cntl_ = nullptr;
    req_ = nullptr;
    resp_ = nullptr;
    done_ = nullptr;
}

void HeartbeatTest::PutHeartbeat() {
    std::unique_lock<std::mutex> lock(GetMutex());

    if (done_) {
        brpc::ClosureGuard done_guard(done_);

        ASSERT_EQ(false, handlerReady_.load(std::memory_order_acquire));
        return;
    }
}

void HeartbeatTest::HeartbeatCallback(::google::protobuf::RpcController* cntl,
                                      const HeartbeatRequest* request,
                                      HeartbeatResponse* response,
                                      ::google::protobuf::Closure* done) {
    {
        std::unique_lock<std::mutex> lock(activeTest_->GetMutex());
        if (!activeTest_->GetReady().load(std::memory_order_acquire)) {
            brpc::ClosureGuard done_guard(done);
            return;
        }
    }
    {
        std::unique_lock<std::mutex> lock(activeTest_->GetMutex());
        activeTest_->GetReady().store(false, std::memory_order_release);
        activeTest_->SetHeartbeatInfo(cntl, request, response, done);
        activeTest_->GetCV().notify_all();
    }
}

TEST_F(HeartbeatTest, TransferLeader) {
    /*
     * Create test copyset
     */
    std::string peers = "127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0";
    std::string destPeers[3] = {
        "127.0.0.1:8200:0",
        "127.0.0.1:8201:0",
        "127.0.0.1:8202:0",
    };

    CreateCopysetPeers(peers);
    WaitCopysetReady(peers);

    /*
     * Fetch heartbeat request and answer it with transfer leader response
     */
    ::google::protobuf::RpcController*  cntl;
    ::google::protobuf::Closure*        done;
    const HeartbeatRequest*             req;
    HeartbeatResponse*                  resp;

    int dest = 0;
    int64_t t0 = butil::monotonic_time_ms();
    int64_t t1 = butil::monotonic_time_ms();
    while (t1 - t0 < 60 * 1000 && dest < 3) {
        GetHeartbeat(&cntl, &req, &resp, &done);
        brpc::ClosureGuard done_guard(done);

        DVLOG(6) << "Chunkserver ID: " << req->chunkserverid()
                 << ", IP: " << req->ip() << ", port: " << req->port()
                 << ", copyset count: " << req->copysetcount()
                 << ", leader count: " << req->leadercount();
        if (req->copysetinfos_size() >= 1) {
            int i = 0;

            for (; i < req->copysetinfos_size(); i ++) {
                if ( req->copysetinfos(i).logicalpoolid() == poolId &&
                     req->copysetinfos(i).copysetid() == copysetId ) {
                    break;
                }
            }
            ASSERT_LT(i, req->copysetinfos_size());

            const curve::mds::heartbeat::CopySetInfo& info =
                req->copysetinfos(i);

            std::string peersStr = "";
            for (int j = 0; j < info.peers_size(); j ++) {
                peersStr += info.peers(j).address() + ",";
            }

            DVLOG(6) << "Copyset " << i << " <" << info.logicalpoolid()
                     << ", " << info.copysetid()
                     << ">, epoch: " << info.epoch()
                     << ", leader: " << info.leaderpeer().address()
                     << ", peers: " << peersStr;

            if (info.has_configchangeinfo()) {
                const ConfigChangeInfo& cxInfo = info.configchangeinfo();
                DVLOG(6) << "Config change info: peer: "
                         << cxInfo.peer().address()
                         << ", finished: " << cxInfo.finished()
                         << ", errno: " << cxInfo.err().errtype()
                         << ", errmsg: " << cxInfo.err().errmsg();
            }

            if (info.leaderpeer().address() == destPeers[dest]) {
                ++dest;
                if (dest >= 3) {
                    break;
                } else {
                    continue;
                }
            }

            std::string sender = req->ip() + ":" + std::to_string(req->port())
                                 + ":0";

            if (info.leaderpeer().address() == sender) {
                // answer with adding peer response
                CopySetConf* conf = resp->add_needupdatecopysets();

                conf->set_logicalpoolid(poolId);
                conf->set_copysetid(copysetId);
                for (int j = 0; j < info.peers_size(); j ++) {
                    auto replica = conf->add_peers();
                    replica->set_address(info.peers(j).address());
                }

                auto replica = new ::curve::common::Peer();
                replica->set_address(destPeers[dest]);
                conf->set_epoch(info.epoch());
                conf->set_type(curve::mds::heartbeat::TRANSFER_LEADER);
                conf->set_allocated_configchangeitem(replica);

                LOG(INFO) << "Answer peer " << sender
                          << " with transfering leader to " << destPeers[dest];
            }
        }
        t1 = butil::monotonic_time_ms();
    }
    ASSERT_LE(t1 - t0, 60 * 1000);

    LOG(INFO) << "Transfering leader finished successfully";
    CleanPeer("127.0.0.1:8200:0");
    CleanPeer("127.0.0.1:8201:0");
    CleanPeer("127.0.0.1:8202:0");
}

TEST_F(HeartbeatTest, RemovePeer) {
    /*
     * Create test copyset
     */
    std::string oldPeers = "127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0";
    std::string leaderPeer = "127.0.0.1:8200:0";
    std::string destPeer = "127.0.0.1:8202:0";

    CreateCopysetPeers(oldPeers);
    WaitCopysetReady(oldPeers);
    TransferLeaderSync(oldPeers, leaderPeer);

    LOG(INFO) << "Created test copyset for removing peer";

    /*
     * Fetch heartbeat request and answer it with removing peer response
     */
    ::google::protobuf::RpcController*  cntl;
    ::google::protobuf::Closure*        done;
    const HeartbeatRequest*             req;
    HeartbeatResponse*                  resp;

    int64_t t0 = butil::monotonic_time_ms();
    int64_t t1 = butil::monotonic_time_ms();
    while (t1 - t0 < 30 * 1000) {
        GetHeartbeat(&cntl, &req, &resp, &done);
        brpc::ClosureGuard done_guard(done);

        DVLOG(6) << "Chunkserver ID: " << req->chunkserverid()
                 << ", IP: " << req->ip() << ", port: " << req->port()
                 << ", copyset count: " << req->copysetcount()
                 << ", leader count: " << req->leadercount();
        if (req->copysetinfos_size() >= 1) {
            int i = 0;

            for (; i < req->copysetinfos_size(); i ++) {
                if ( req->copysetinfos(i).logicalpoolid() == poolId &&
                     req->copysetinfos(i).copysetid() == copysetId ) {
                    break;
                }
            }
            ASSERT_LT(i, req->copysetinfos_size());

            const curve::mds::heartbeat::CopySetInfo& info =
                req->copysetinfos(i);

            std::string peersStr = "";
            for (int j = 0; j < info.peers_size(); j ++) {
                peersStr += info.peers(j).address() + ",";
            }

            DVLOG(6) << "Copyset " << i << " <" << info.logicalpoolid()
                     << ", " << info.copysetid()
                     << ">, epoch: " << info.epoch()
                     << ", leader: " << info.leaderpeer().address()
                     << ", peers: " << peersStr;

            if (info.has_configchangeinfo()) {
                const ConfigChangeInfo& cxInfo = info.configchangeinfo();
                DVLOG(6) << "Config change info: peer: "
                         << cxInfo.peer().address()
                         << ", finished: " << cxInfo.finished()
                         << ", errno: " << cxInfo.err().errtype()
                         << ", errmsg: " << cxInfo.err().errmsg();
            }

            std::string sender = req->ip() + ":" + std::to_string(req->port())
                                 + ":0";
            if (sender == destPeer) {
                continue;
            }

            bool taskFinished = true;
            for (int j = 0; j < info.peers_size(); j ++) {
                if (info.peers(j).address() == destPeer) {
                    taskFinished = false;
                }
            }
            if (taskFinished) {
                break;
            }

            if (info.leaderpeer().address() == sender) {
                // answer with removing peer response
                CopySetConf* conf = resp->add_needupdatecopysets();

                conf->set_logicalpoolid(poolId);
                conf->set_copysetid(copysetId);
                for (int j = 0; j < info.peers_size(); j ++) {
                    auto replica = conf->add_peers();
                    replica->set_address(info.peers(j).address());
                }
                auto replica = new ::curve::common::Peer();
                replica->set_address(destPeer);
                conf->set_epoch(info.epoch());
                conf->set_type(curve::mds::heartbeat::REMOVE_PEER);
                conf->set_allocated_configchangeitem(replica);

                LOG(INFO) << "Answer peer " << sender
                          << " with removing peer response";
            }
        }
        t1 = butil::monotonic_time_ms();
    }
    ASSERT_LE(t1 - t0, 30 * 1000);

    LOG(INFO) << "Removing peer finished successfully";
    CleanPeer("127.0.0.1:8200:0");
    CleanPeer("127.0.0.1:8201:0");
    CleanPeer("127.0.0.1:8202:0");
}

TEST_F(HeartbeatTest, CleanPeer_after_Configchange) {
    /*
     * Create test copyset
     */
    std::string peer = "127.0.0.1:8202:0";

    CreateCopysetPeers(peer);
    WaitCopysetReady(peer);

    LOG(INFO) << "Created test copyset for cleaning peer";

    /*
     * Fetch heartbeat request and answer it with cleaning peer response
     */
    ::google::protobuf::RpcController*  cntl;
    ::google::protobuf::Closure*        done;
    const HeartbeatRequest*             req;
    HeartbeatResponse*                  resp;

    LOG(INFO) << "Cleaning peer " << peer;

    bool taskSent = false;
    int64_t t0 = butil::monotonic_time_ms();
    int64_t t1 = butil::monotonic_time_ms();
    while (t1 - t0 < 30 * 1000) {
        GetHeartbeat(&cntl, &req, &resp, &done);
        brpc::ClosureGuard done_guard(done);

        LOG(INFO) << "Chunkserver ID: " << req->chunkserverid()
                 << ", IP: " << req->ip() << ", port: " << req->port()
                 << ", copyset count: " << req->copysetcount()
                 << ", leader count: " << req->leadercount();

        std::string sender = req->ip() + ":" + std::to_string(req->port())
                             + ":0";
        if (sender != peer) {
            continue;
        }
        if (req->copysetinfos_size() >= 1) {
            int i = 0;
            for (; i < req->copysetinfos_size(); i ++) {
                if ( req->copysetinfos(i).logicalpoolid() == poolId &&
                     req->copysetinfos(i).copysetid() == copysetId ) {
                    break;
                }
            }
            if (i >= req->copysetinfos_size()) {
                ASSERT_EQ(true, taskSent);
                break;
            }

            const curve::mds::heartbeat::CopySetInfo& info =
                req->copysetinfos(i);

            std::string peersStr = info.peers(0).address();

            LOG(INFO) << "Copyset " << i << " <" << info.logicalpoolid()
                     << ", " << info.copysetid()
                     << ">, epoch: " << info.epoch()
                     << ", leader: " << info.leaderpeer().address()
                     << ", peers: " << peersStr;

            if (info.has_configchangeinfo()) {
                const ConfigChangeInfo& cxInfo = info.configchangeinfo();
                LOG(INFO) << "Config change info: peer: "
                         << cxInfo.peer().address()
                         << ", finished: " << cxInfo.finished()
                         << ", errno: " << cxInfo.err().errtype()
                         << ", errmsg: " << cxInfo.err().errmsg();
            }

            {
                // answer with cleaning peer response
                CopySetConf* conf = resp->add_needupdatecopysets();

                conf->set_logicalpoolid(poolId);
                conf->set_copysetid(copysetId);
                conf->set_epoch(info.epoch());

                taskSent = true;
                LOG(INFO) << "Answer peer " << sender
                          << " with cleaning peer response";
            }
        } else {
            ASSERT_EQ(true, taskSent);
            break;
        }
        t1 = butil::monotonic_time_ms();
    }

    ASSERT_LE(t1 - t0, 30 * 1000);
    LOG(INFO) << "Cleaning peer " << peer << " finished successfully";
    CleanPeer("127.0.0.1:8200:0");
    CleanPeer("127.0.0.1:8201:0");
    CleanPeer("127.0.0.1:8202:0");
}

TEST_F(HeartbeatTest, CleanPeer_not_exist_in_MDS) {
    // 在chunkserver2上创建一个copyset
    std::string peer = "127.0.0.1:8202:0";

    CreateCopysetPeers(peer);
    WaitCopysetReady(peer);

    LOG(INFO) << "Created test copyset for cleaning peer";

    ::google::protobuf::RpcController*  cntl;
    ::google::protobuf::Closure*        done;
    const HeartbeatRequest*             req;
    HeartbeatResponse*                  resp;

    LOG(INFO) << "Cleaning peer " << peer;

    bool taskSent = false;
    int64_t t0 = butil::monotonic_time_ms();
    int64_t t1 = butil::monotonic_time_ms();
    while (t1 - t0 < 30 * 1000) {
        GetHeartbeat(&cntl, &req, &resp, &done);
        brpc::ClosureGuard done_guard(done);

        LOG(INFO) << "Chunkserver ID: " << req->chunkserverid()
                 << ", IP: " << req->ip() << ", port: " << req->port()
                 << ", copyset count: " << req->copysetcount()
                 << ", leader count: " << req->leadercount();

        std::string sender = req->ip() + ":" + std::to_string(req->port())
                             + ":0";
        if (sender != peer) {
            continue;
        }
        if (req->copysetinfos_size() >= 1) {
            // 捕获心跳中的request, 判断心跳中是否还上报了该copyset
            int i = 0;
            for (; i < req->copysetinfos_size(); i ++) {
                if ( req->copysetinfos(i).logicalpoolid() == poolId &&
                     req->copysetinfos(i).copysetid() == copysetId ) {
                    break;
                }
            }
            // 如果i>request种copyset数量
            // 说明该copyset已经不存在了，说明copyset已经删除
            // 标记taskSent为true, 删除成功
            if (i >= req->copysetinfos_size()) {
                ASSERT_EQ(true, taskSent);
                break;
            }

            const curve::mds::heartbeat::CopySetInfo& info =
                req->copysetinfos(i);

            std::string peersStr = info.peers(0).address();

            LOG(INFO) << "Copyset " << i << " <" << info.logicalpoolid()
                     << ", " << info.copysetid()
                     << ">, epoch: " << info.epoch()
                     << ", leader: " << info.leaderpeer().address()
                     << ", peers: " << peersStr;

            if (info.has_configchangeinfo()) {
                const ConfigChangeInfo& cxInfo = info.configchangeinfo();
                LOG(INFO) << "Config change info: peer: "
                         << cxInfo.peer().address()
                         << ", finished: " << cxInfo.finished()
                         << ", errno: " << cxInfo.err().errtype()
                         << ", errmsg: " << cxInfo.err().errmsg();
            }

            {
                // 设置response为空配置
                CopySetConf* conf = resp->add_needupdatecopysets();

                conf->set_logicalpoolid(poolId);
                conf->set_copysetid(copysetId);
                conf->set_epoch(0);

                taskSent = true;
                LOG(INFO) << "Answer peer " << sender
                          << " with cleaning peer response";
            }
        } else {
            // request中没有心跳应该删除成功
            ASSERT_EQ(true, taskSent);
            break;
        }
        t1 = butil::monotonic_time_ms();
    }

    // 心跳交互的到删除的时间应该在设置时间之内
    ASSERT_LE(t1 - t0, 30 * 1000);
    LOG(INFO) << "Cleaning peer " << peer << " finished successfully";
    CleanPeer("127.0.0.1:8200:0");
    CleanPeer("127.0.0.1:8201:0");
    CleanPeer("127.0.0.1:8202:0");
}

TEST_F(HeartbeatTest, AddPeer) {
    /*
     * Create test copyset
     */
    std::string oldPeers = "127.0.0.1:8200:0,127.0.0.1:8201:0";
    std::string newPeer = "127.0.0.1:8202:0";

    CreateCopysetPeers(oldPeers);
    CreateCopysetPeers(newPeer);

    WaitCopysetReady(oldPeers);
    WaitCopysetReady(newPeer);

    LOG(INFO) << "Created test copyset for adding peer";

    /*
     * Fetch heartbeat request and answer it with adding peer response
     */
    ::google::protobuf::RpcController*  cntl;
    ::google::protobuf::Closure*        done;
    const HeartbeatRequest*             req;
    HeartbeatResponse*                  resp;

    int64_t t0 = butil::monotonic_time_ms();
    int64_t t1 = butil::monotonic_time_ms();
    while (t1 - t0 < 30 * 1000) {
        GetHeartbeat(&cntl, &req, &resp, &done);
        brpc::ClosureGuard done_guard(done);

        DVLOG(6) << "Chunkserver ID: " << req->chunkserverid()
                 << ", IP: " << req->ip() << ", port: " << req->port()
                 << ", copyset count: " << req->copysetcount()
                 << ", leader count: " << req->leadercount();
        if (req->copysetinfos_size() >= 1) {
            int i = 0;

            for (; i < req->copysetinfos_size(); i ++) {
                if ( req->copysetinfos(i).logicalpoolid() == poolId &&
                     req->copysetinfos(i).copysetid() == copysetId ) {
                    break;
                }
            }
            ASSERT_LT(i, req->copysetinfos_size());

            const curve::mds::heartbeat::CopySetInfo& info =
                req->copysetinfos(i);

            std::string peersStr = "";
            for (int j = 0; j < info.peers_size(); j ++) {
                peersStr += info.peers(j).address() + ",";
            }

            DVLOG(6) << "Copyset " << i << " <" << info.logicalpoolid()
                     << ", " << info.copysetid()
                     << ">, epoch: " << info.epoch()
                     << ", leader: " << info.leaderpeer().address()
                     << ", peers: " << peersStr;

            if (info.has_configchangeinfo()) {
                const ConfigChangeInfo& cxInfo = info.configchangeinfo();
                DVLOG(6) << "Config change info: peer: "
                         << cxInfo.peer().address()
                         << ", finished: " << cxInfo.finished()
                         << ", errno: " << cxInfo.err().errtype()
                         << ", errmsg: " << cxInfo.err().errmsg();
            }

            std::string sender = req->ip() + ":" + std::to_string(req->port())
                                 + ":0";
            if (sender == newPeer) {
                continue;
            }

            bool taskFinished = false;
            for (int j = 0; j < info.peers_size(); j ++) {
                if (info.peers(j).address() == newPeer) {
                    taskFinished = true;
                }
            }
            if (taskFinished) {
                break;
            }

            if (info.leaderpeer().address() == sender) {
                // answer with adding peer response
                CopySetConf* conf = resp->add_needupdatecopysets();
                conf->set_logicalpoolid(poolId);
                conf->set_copysetid(copysetId);
                for (int j = 0; j < info.peers_size(); j ++) {
                    auto replica = conf->add_peers();
                    replica->set_address(info.peers(j).address());
                }
                auto replica = new ::curve::common::Peer();
                replica->set_address(newPeer);
                conf->set_epoch(info.epoch());
                conf->set_type(curve::mds::heartbeat::ADD_PEER);
                conf->set_allocated_configchangeitem(replica);

                LOG(INFO) << "Answer peer " << sender
                          << " with adding peer response";
            }
        }
        t1 = butil::monotonic_time_ms();
    }
    ASSERT_LE(t1 - t0, 30 * 1000);

    LOG(INFO) << "Adding peer finished successfully";
}

}  // namespace chunkserver
}  // namespace curve
