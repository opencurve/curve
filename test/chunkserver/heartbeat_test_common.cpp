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
 * Project: Curve
 *
 * History:
 *          2018/12/23  Wenyu Zhou   Initial version
 */

#include "test/chunkserver/heartbeat_test_common.h"

uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 16 * 1024 * 1024;   // NOLINT

static const char* confPath[3] = {
    "./8200/chunkserver.conf",
    "./8201/chunkserver.conf",
    "./8202/chunkserver.conf",
};

namespace curve {
namespace chunkserver {

HeartbeatTestCommon* HeartbeatTestCommon::hbtestCommon_ = nullptr;

void HeartbeatTestCommon::CleanPeer(
    LogicPoolID poolId, CopysetID copysetId, const std::string& peer) {
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

            {
                // answer with cleaning peer response
                CopySetConf* conf = resp->add_needupdatecopysets();

                conf->set_logicalpoolid(poolId);
                conf->set_copysetid(copysetId);
                conf->set_epoch(info.epoch());
            }
        } else {
            break;
        }
        t1 = butil::monotonic_time_ms();
    }

    ASSERT_LE(t1 - t0, 30 * 1000);
    LOG(INFO) << "Cleaning peer " << peer << " finished successfully";
}

void HeartbeatTestCommon::CreateCopysetPeers(
    LogicPoolID poolId, CopysetID copysetId,
    const std::vector<std::string> &cslist, const std::string& confStr) {
    braft::Configuration conf;
    ASSERT_EQ(0, conf.parse_from(confStr));
    std::vector<braft::PeerId> confPeers;
    conf.list_peers(&confPeers);

    for (auto it = cslist.begin(); it != cslist.end(); it++) {
        int retry = 30;
        while (retry > 0) {
            brpc::Channel channel;
            ASSERT_EQ(0, channel.Init((*it).c_str(), NULL));

            brpc::Controller cntl;
            CopysetRequest request;
            CopysetResponse response;

            cntl.set_timeout_ms(3000);
            request.set_logicpoolid(poolId);
            request.set_copysetid(copysetId);
            for (auto peer = confPeers.begin();
                peer != confPeers.end(); peer++) {
                request.add_peerid(peer->to_string());
            }

            curve::chunkserver::CopysetService_Stub copyset_stub(&channel);
            copyset_stub.CreateCopysetNode(&cntl, &request, &response, nullptr);

            if (cntl.Failed()) {
                LOG(ERROR) << "Creating copyset failed: "
                           << cntl.ErrorCode() << " " << cntl.ErrorText();
            } else if (COPYSET_OP_STATUS_EXIST == response.status()) {
                LOG(INFO) << "Skipped creating existed copyset <"
                          << poolId << ", " << copysetId << ">: " << conf
                          << " on peer: " << *it;
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

void HeartbeatTestCommon::WaitCopysetReady(
    LogicPoolID poolId, CopysetID copysetId, const std::string& confStr) {
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

void HeartbeatTestCommon::TransferLeaderSync(
    LogicPoolID poolId, CopysetID copysetId,
    const std::string& confStr, const std::string& newLeader) {
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

void HeartbeatTestCommon::ReleaseHeartbeat() {
    mds_->SetChunkServerHeartbeatCallback(nullptr);
    LOG(INFO) << "Release Heartbeat callback.";
    std::unique_lock<std::mutex> lock(hbtestCommon_->GetMutex());
    if (done_) {
        brpc::ClosureGuard done_guard(done_);

        ASSERT_EQ(false, handlerReady_.load(std::memory_order_acquire));
        return;
    }
}

void HeartbeatTestCommon::SetHeartbeatInfo(
    ::google::protobuf::RpcController* cntl,
    const HeartbeatRequest* request,
    HeartbeatResponse* response,
    ::google::protobuf::Closure* done) {
    cntl_ = cntl;
    req_ = request;
    resp_ = response;
    done_ = done;
}

void HeartbeatTestCommon::GetHeartbeat(
    ::google::protobuf::RpcController** cntl,
    const HeartbeatRequest** request,
    HeartbeatResponse** response,
    ::google::protobuf::Closure** done) {
    std::unique_lock<std::mutex> lock(hbtestCommon_->GetMutex());

    handlerReady_.store(true, std::memory_order_release);
    hbtestCommon_->GetCV().wait(lock);

    *cntl = cntl_;
    *request = req_;
    *response = resp_;
    *done = done_;

    cntl_ = nullptr;
    req_ = nullptr;
    resp_ = nullptr;
    done_ = nullptr;
}

void HeartbeatTestCommon::HeartbeatCallback(
    ::google::protobuf::RpcController* cntl,
    const HeartbeatRequest* request,
    HeartbeatResponse* response,
    ::google::protobuf::Closure* done) {
    {
        std::unique_lock<std::mutex> lock(hbtestCommon_->GetMutex());
        if (!hbtestCommon_->GetReady().load(std::memory_order_acquire)) {
            brpc::ClosureGuard done_guard(done);
            return;
        }
    }
    {
        std::unique_lock<std::mutex> lock(hbtestCommon_->GetMutex());
        hbtestCommon_->GetReady().store(false, std::memory_order_release);
        hbtestCommon_->SetHeartbeatInfo(cntl, request, response, done);
        hbtestCommon_->GetCV().notify_all();
    }
}

bool HeartbeatTestCommon::SameCopySetInfo(
        const ::curve::mds::heartbeat::CopySetInfo &orig,
        const ::curve::mds::heartbeat::CopySetInfo &expect) {
    if (!expect.IsInitialized()) {
        if (!orig.IsInitialized()) {
            return true;
        } else {
            return false;
        }
    }

    if (orig.logicalpoolid() != expect.logicalpoolid()) {
        return false;
    }

    if (orig.copysetid() != expect.copysetid()) {
        return false;
    }

    if (orig.peers_size() != expect.peers_size()) {
        return false;
    }

    if (orig.leaderpeer().address() != expect.leaderpeer().address()) {
        return false;
    }

    for (int i = 0; i < orig.peers_size(); i++) {
        if (orig.peers(i).address() != expect.peers(i).address()) {
            return false;
        }
    }
    if (expect.has_configchangeinfo()) {
        if (!orig.has_configchangeinfo()) {
            return false;
        }
        if (orig.configchangeinfo().type() !=
            expect.configchangeinfo().type()) {
            return false;
        }
        if (orig.configchangeinfo().peer().address() !=
            expect.configchangeinfo().peer().address()) {
            return false;
        }
    } else if (orig.has_configchangeinfo()) {
        return false;
    }

    return true;
}

bool HeartbeatTestCommon::WailForConfigChangeOk(
    const ::curve::mds::heartbeat::CopySetConf &conf,
    ::curve::mds::heartbeat::CopySetInfo expectedInfo,
    int timeLimit) {
    ::google::protobuf::RpcController*  cntl;
    ::google::protobuf::Closure*        done;
    const HeartbeatRequest*             req;
    HeartbeatResponse*                  resp;

    int64_t startTime = butil::monotonic_time_ms();
    bool leaderPeerSet = expectedInfo.has_leaderpeer();
    std::string leader;
    while (butil::monotonic_time_ms() - startTime < timeLimit) {
        GetHeartbeat(&cntl, &req, &resp, &done);
        brpc::ClosureGuard done_guard(done);

        // Get the leader of the current copyset
         std::string sender =
            req->ip() + ":" + std::to_string(req->port()) + ":0";
        if (1 == req->copysetinfos_size()) {
            leader = req->copysetinfos(0).leaderpeer().address();
            if (leader.find("0.0.0.0") != std::string::npos) {
                continue;
            }
        } else if (0 == req->copysetinfos_size()) {
            if (!expectedInfo.has_logicalpoolid() && sender == leader &&
                leader.find("0.0.0.0") == std::string::npos) {
                return true;
            } else {
                continue;
            }
        }

        // If the current req is sent by the leader, determine whether the content in the req meets the requirements
        // If it meets the requirements, return true; If it does not meet the requirements, set the content in resp
        if (leader == sender) {
            if (!leaderPeerSet) {
                auto peer = new ::curve::common::Peer();
                peer->set_address(leader);
                expectedInfo.set_allocated_leaderpeer(peer);
            }

            // Determine whether the req meets the requirements, and return true if it meets the requirements
            if (req->copysetinfos_size() == 1) {
                if (SameCopySetInfo(req->copysetinfos(0), expectedInfo)) {
                    return true;
                }
                LOG(INFO) << "req->copysetinfos:"
                    << req->copysetinfos(0).DebugString()
                    << ", expectedInfo: " << expectedInfo.DebugString();
            } else if (req->copysetinfos_size() == 0) {
                if (SameCopySetInfo(
                    ::curve::mds::heartbeat::CopySetInfo{}, expectedInfo)) {
                    return true;
                }
            }

            // Not meeting the requirements to set resp
            if (req->copysetinfos_size() == 1) {
                auto build = resp->add_needupdatecopysets();
                if (!build->has_epoch()) {
                    *build = conf;
                }
                build->set_epoch(req->copysetinfos(0).epoch());
            }
        }
    }
    return false;
}

int RmFile(std::string uri) {
    char cmd[1024] = "";
    std::string path = UriParser::GetPathFromUri(uri);
    snprintf(cmd, sizeof(cmd) - 1, "rm -f %s", path.c_str());
    return system(cmd);
}

int RmDirData(std::string uri) {
    char cmd[1024] = "";
    std::string dir = UriParser::GetPathFromUri(uri);
    CHECK(dir != "") << "rmdir got empty dir string, halting immediately.";
    snprintf(cmd, sizeof(cmd) - 1, "rm -rf %s/*", dir.c_str());

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

}  // namespace chunkserver
}  // namespace curve
