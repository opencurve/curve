/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: Fri Aug  6 17:10:54 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/copyset_node.h"

#include <braft/protobuf_file.h>
#include <braft/util.h>
#include <brpc/channel.h>
#include <glog/logging.h>

#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/memory/memory.h"
#include "curvefs/src/metaserver/copyset/meta_operator_closure.h"
#include "curvefs/src/metaserver/copyset/metric.h"
#include "curvefs/src/metaserver/copyset/raft_log_codec.h"
#include "curvefs/src/metaserver/copyset/snapshot_closure.h"
#include "curvefs/src/metaserver/copyset/utils.h"
#include "src/common/timeutility.h"
#include "src/common/uri_parser.h"

static bvar::LatencyRecorder g_oprequest_propose_latency("oprequest_propose");
static bvar::LatencyRecorder g_concurrent_apply_wait_latency(
    "concurrent_apply_wait");

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::curve::common::TimeUtility;
using ::curve::common::UriParser;

const char* kConfEpochFilename = "conf.epoch";
const char* kMetaDataFilename = "metadata";

CopysetNode::CopysetNode(PoolId poolId, CopysetId copysetId,
                         const braft::Configuration& conf)
    : poolId_(poolId),
      copysetId_(copysetId),
      groupId_(ToGroupId(poolId_, copysetId_)),
      name_(ToGroupIdString(poolId_, copysetId_)),
      conf_(conf),
      epoch_(0),
      options_(),
      leaderTerm_(-1),
      peerId_(),
      raftNode_(),
      copysetDataPath_(),
      metaStore_(),
      appliedIndex_(0),
      epochFile_(),
      applyQueue_(nullptr),
      lastSnapshotIndex_(0),
      metric_(absl::make_unique<OperatorApplyMetric>(poolId_, copysetId_)) {}

CopysetNode::~CopysetNode() { Stop(); }

bool CopysetNode::Init(const CopysetNodeOptions& options) {
    options_ = options;

    std::string protocol =
        UriParser::ParseUri(options.dataUri, &copysetDataPath_);

    if (protocol.empty()) {
        LOG(ERROR) << "Not support protocol, error data path: "
                   << options.dataUri << ", copyset: " << name_;
        return false;
    }

    copysetDataPath_ = copysetDataPath_ + "/" + groupId_;

    epochFile_ = absl::make_unique<ConfEpochFile>(options_.localFileSystem);

    // init apply queue
    applyQueue_ = absl::make_unique<ApplyQueue>();
    if (!applyQueue_->Start(options_.applyQueueOption)) {
        LOG(ERROR) << "Start apply queue failed";
        return false;
    }

    // create metastore
    metaStore_ = absl::make_unique<MetaStoreImpl>();

    InitRaftNodeOptions();

    butil::ip_t ip;
    butil::str2ip(options_.ip.c_str(), &ip);
    butil::EndPoint addr(ip, options_.port);

    peerId_ = braft::PeerId(addr, 0);
    raftNode_ = absl::make_unique<RaftNode>(groupId_, peerId_);

    return true;
}

bool CopysetNode::Start() {
    if (!raftNode_) {
        LOG(ERROR) << "RaftNode didn't created, copyset: " << name_;
        return false;
    }

    if (0 != raftNode_->init(options_.raftNodeOptions)) {
        LOG(ERROR) << "Fail to init raft node, copyset: " << name_;
        return false;
    }

    LOG(INFO) << "Run copyset success, copyset: " << name_;
    return true;
}

void CopysetNode::Stop() {
    if (raftNode_) {
        raftNode_->shutdown(nullptr);
        raftNode_->join();
        raftNode_.reset();
    }

    if (applyQueue_) {
        applyQueue_->Flush();
        applyQueue_->Stop();
        applyQueue_.reset();
    }
}

int CopysetNode::LoadConfEpoch(const std::string& file) {
    PoolId loadPoolId = 0;
    CopysetId loadCopysetId = 0;
    uint64_t loadEpoch = 0;

    int ret = epochFile_->Load(file, &loadPoolId, &loadCopysetId, &loadEpoch);
    if (ret == 0) {
        if (poolId_ != loadPoolId || copysetId_ != loadCopysetId) {
            LOG(ERROR) << "LoadConfEpoch failed, poolid or copysetid is not "
                          "identical, copyset: "
                       << name_ << ", current poolid: " << poolId_
                       << ", current copysetid: " << copysetId_
                       << ", conf poolid: " << loadPoolId
                       << ", conf copysetid: " << loadCopysetId;
            return -1;
        } else {
            epoch_.store(loadEpoch, std::memory_order_relaxed);
        }
    }

    return ret;
}

int CopysetNode::SaveConfEpoch(const std::string& file) {
    return epochFile_->Save(file, poolId_, copysetId_,
                            epoch_.load(std::memory_order_acquire));
}

void CopysetNode::UpdateAppliedIndex(uint64_t index) {
    uint64_t curIndex = appliedIndex_.load(std::memory_order_relaxed);

    if (index <= curIndex) {
        return;
    }

    while (!appliedIndex_.compare_exchange_strong(curIndex, index,
                                                  std::memory_order_relaxed)) {
        if (index <= curIndex) {
            return;
        }
    }
}

bool CopysetNode::GetLeaderStatus(braft::NodeStatus* leaderStatus) {
    braft::NodeStatus status;
    raftNode_->get_status(&status);

    if (status.leader_id.is_empty()) {
        return false;
    }

    // current node is leader
    if (status.leader_id == status.peer_id) {
        *leaderStatus = std::move(status);
        return true;
    }

    return FetchLeaderStatus(status.leader_id, leaderStatus);
}

void CopysetNode::on_apply(braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        braft::AsyncClosureGuard doneGuard(iter.done());

        if (iter.done()) {
            MetaOperatorClosure* metaClosure =
                dynamic_cast<MetaOperatorClosure*>(iter.done());
            CHECK(metaClosure != nullptr) << "dynamic cast failed";
            metaClosure->GetOperator()->timerPropose.stop();
            g_oprequest_propose_latency
                << metaClosure->GetOperator()->timerPropose.u_elapsed();
            butil::Timer timer;
            timer.start();
            auto task =
                std::bind(&MetaOperator::OnApply, metaClosure->GetOperator(),
                          iter.index(), doneGuard.release(),
                          TimeUtility::GetTimeofDayUs());
            applyQueue_->Push(metaClosure->GetOperator()->HashCode(),
                              std::move(task));
            timer.stop();
            g_concurrent_apply_wait_latency << timer.u_elapsed();
        } else {
            // parse request from raft-log
            auto metaOperator = RaftLogCodec::Decode(this, iter.data());
            CHECK(metaOperator != nullptr) << "Decode raft log failed";
            butil::Timer timer;
            timer.start();
            auto hashcode = metaOperator->HashCode();
            auto task =
                std::bind(&MetaOperator::OnApplyFromLog, metaOperator.release(),
                          TimeUtility::GetTimeofDayUs());
            applyQueue_->Push(hashcode, std::move(task));
            timer.stop();
            g_concurrent_apply_wait_latency << timer.u_elapsed();
        }
    }
}

void CopysetNode::on_shutdown() {
    LOG(INFO) << "Copyset: " << name_ << " is shutdown";
}

class OnSnapshotSaveDoneClosureImpl : public OnSnapshotSaveDoneClosure {
 public:
    OnSnapshotSaveDoneClosureImpl(CopysetNode* node,
                                  braft::SnapshotWriter* writer,
                                  braft::Closure* snapDone,
                                  RaftSnapshotMetric::MetricContext* ctx)
        : node_(node), writer_(writer), snapDone_(snapDone), ctx_(ctx) {}

    void Run() override {
        std::unique_ptr<OnSnapshotSaveDoneClosureImpl> selfGuard(this);
        brpc::ClosureGuard doneGuard(snapDone_);

        if (ctx_->success) {
            writer_->add_file(kMetaDataFilename);
        }

        RaftSnapshotMetric::GetInstance().OnSnapshotSaveDone(ctx_);
    }

    void SetSuccess() override {
        ctx_->success = true;
        LOG(INFO) << "Copyset " << node_->Name() << " save snapshot success";
    }

    void SetError(MetaStatusCode code) override {
        ctx_->success = false;
        snapDone_->status().set_error(code, "Save metadata failed, err: %s",
                                      MetaStatusCode_Name(code).c_str());
        LOG(ERROR) << "Copyset " << node_->Name()
                   << " save snapshot failed, err: "
                   << MetaStatusCode_Name(code);
    }

 private:
    CopysetNode* node_;
    braft::SnapshotWriter* writer_;
    braft::Closure* snapDone_;
    RaftSnapshotMetric::MetricContext* ctx_;
};

void CopysetNode::on_snapshot_save(braft::SnapshotWriter* writer,
                                   braft::Closure* done) {
    LOG(INFO) << "Copyset " << name_ << " saving snapshot to '"
              << writer->get_path() << "'";

    brpc::ClosureGuard doneGuard(done);

    auto metricCtx = RaftSnapshotMetric::GetInstance().OnSnapshotSaveStart();
    auto cleanMetricIfFailed = absl::MakeCleanup([metricCtx]() {
        metricCtx->success = false;
        RaftSnapshotMetric::GetInstance().OnSnapshotSaveDone(metricCtx);
    });

    // flush all flying operators
    applyQueue_->Flush();

    // save conf
    std::string confFile = writer->get_path() + "/" + kConfEpochFilename;
    if (0 != SaveConfEpoch(confFile)) {
        LOG(ERROR) << "Copyset " << name_
                   << " save snapshot failed, save epoch file failed";
        done->status().set_error(errno, "Save conf file failed, error = %s",
                                 strerror(errno));
        return;
    }

    writer->add_file(kConfEpochFilename);

    std::string metaDataFile = writer->get_path() + "/" + kMetaDataFilename;

    // TODO(wuhanqing): MetaStore::Save will start a thread and do task
    // asynchronously, after task completed it will call
    // OnSnapshotSaveDoneImpl::Run
    // BUT, this manner is not so clear, maybe it better to make thing
    // asynchronous directly in here
    metaStore_->Save(metaDataFile, new OnSnapshotSaveDoneClosureImpl(
                                       this, writer, done, metricCtx));
    doneGuard.release();

    // `Cancel` only available for rvalue
    std::move(cleanMetricIfFailed).Cancel();
}

int CopysetNode::on_snapshot_load(braft::SnapshotReader* reader) {
    LOG(INFO) << "Copyset " << name_ << " begin to load snapshot from '"
              << reader->get_path() << "'";

    // load conf
    std::string confFile = reader->get_path() + "/" + kConfEpochFilename;
    if (options_.localFileSystem->FileExists(confFile)) {
        if (0 != LoadConfEpoch(confFile)) {
            LOG(ERROR) << "Copyset " << name_
                       << " load conf file failed, conf file: '" << confFile
                       << "'";
            return -1;
        }
    }

    // load metadata
    metaStore_->Clear();
    std::string metadataFile = reader->get_path() + "/" + kMetaDataFilename;
    if (options_.localFileSystem->FileExists(metadataFile)) {
        if (!metaStore_->Load(metadataFile)) {
            LOG(ERROR) << "Copyset " << name_
                       << " load snapshot failed, metastore load return "
                          "failed, metadata file: '"
                       << metadataFile << "'";
            return -1;
        }
    } else {
        LOG(INFO) << "Copyset " << name_
                  << "load snapshot not found metadata, metadata file: '"
                  << metadataFile << "'";
    }

    braft::SnapshotMeta meta;
    reader->load_meta(&meta);
    if (0 != meta.old_peers_size()) {
        conf_.reset();
        for (int i = 0; i < meta.peers_size(); ++i) {
            conf_.add_peer(meta.peers(i));
        }
    }

    auto prevLastSnapshotIndex = lastSnapshotIndex_;
    lastSnapshotIndex_ = meta.last_included_index();

    LOG(INFO) << "Copyset " << name_ << " load snapshot from '"
              << reader->get_path()
              << "' success, update last snapshot index from "
              << prevLastSnapshotIndex << " to " << lastSnapshotIndex_;

    return 0;
}

void CopysetNode::on_leader_start(int64_t term) {
    leaderTerm_.store(term, std::memory_order_release);

    LOG(INFO) << "Copyset: " << name_ << ", peer id: " << peerId_.to_string()
              << " become leader, term is " << term;
}

void CopysetNode::on_leader_stop(const butil::Status& status) {
    int64_t prevTerm = leaderTerm_.exchange(-1, std::memory_order_release);

    LOG(INFO) << "Copyset: " << name_ << ", peer id: " << peerId_.to_string()
              << " stepped down, previous term is " << prevTerm
              << ", reason: " << status.error_str();
}

void CopysetNode::on_error(const braft::Error& e) {
    LOG(FATAL) << "Copyset: " << name_ << ", peer id: " << peerId_.to_string()
               << " meet error";
}

void CopysetNode::on_configuration_committed(const braft::Configuration& conf,
                                             int64_t index) {
    // TODO(wuhanqing): implement with heartbeat
}

void CopysetNode::on_stop_following(const braft::LeaderChangeContext& ctx) {
    LOG(INFO) << "Copyset: " << name_ << ", peer id: " << peerId_.to_string()
              << ", stops following " << ctx;
}

void CopysetNode::on_start_following(const braft::LeaderChangeContext& ctx) {
    LOG(INFO) << "Copyset: " << name_ << ", peer id: " << peerId_.to_string()
              << ", starts following " << ctx;
}

void CopysetNode::InitRaftNodeOptions() {
    options_.raftNodeOptions.initial_conf = conf_;
    options_.raftNodeOptions.fsm = this;
    options_.raftNodeOptions.node_owns_fsm = false;

    options_.raftNodeOptions.log_uri = options_.raftNodeOptions.log_uri + "/" +
                                       groupId_ + "/" +
                                       std::string(RAFT_LOG_DIR);
    options_.raftNodeOptions.raft_meta_uri =
        options_.raftNodeOptions.raft_meta_uri + "/" + groupId_ + "/" +
        std::string(RAFT_META_DIR);
    options_.raftNodeOptions.snapshot_uri =
        options_.raftNodeOptions.snapshot_uri + "/" + groupId_ + "/" +
        std::string(RAFT_SNAP_DIR);
}

bool CopysetNode::FetchLeaderStatus(const braft::PeerId& leaderId,
                                    braft::NodeStatus* leaderStatus) {
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel channel;

    if (channel.Init(leaderId.addr, nullptr) != 0) {
        LOG(WARNING) << "Init channel to leader failed, leader address: "
                     << leaderId.addr << ", copyset: " << name_;
        return false;
    }

    CopysetStatusRequest request;
    CopysetStatusResponse response;

    request.set_poolid(poolId_);
    request.set_copysetid(copysetId_);
    request.mutable_peer()->set_address(leaderId.to_string());

    CopysetService_Stub stub(&channel);
    stub.GetCopysetStatus(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(WARNING) << "Get leader status failed, leader address: "
                     << leaderId.addr << ", error: " << cntl.ErrorText()
                     << ", copyset: " << name_;
        return false;
    }

    if (response.status() != COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS) {
        LOG(WARNING) << "Get leader status failed, leader address: "
                     << leaderId.addr << ", response status: "
                     << COPYSET_OP_STATUS_Name(response.status())
                     << ", copyset: " << name_;
        return false;
    }

    if (!response.has_copysetstatus()) {
        LOG(WARNING) << "Get leader status failed, leader address: "
                     << leaderId.addr
                     << ", response is success, but doesn't have copyset "
                        "status, response: "
                     << response.ShortDebugString() << ", copyset: " << name_;
        return false;
    }

    const auto& copysetStatus = response.copysetstatus();
    leaderStatus->state = static_cast<braft::State>(copysetStatus.state());
    leaderStatus->peer_id.parse(copysetStatus.peer().address());
    leaderStatus->leader_id.parse(copysetStatus.leader().address());
    leaderStatus->readonly = copysetStatus.readonly();
    leaderStatus->term = copysetStatus.term();
    leaderStatus->committed_index = copysetStatus.committedindex();
    leaderStatus->known_applied_index = copysetStatus.knownappliedindex();
    leaderStatus->pending_index = copysetStatus.pendingindex();
    leaderStatus->pending_queue_size = copysetStatus.pendingqueuesize();
    leaderStatus->applying_index = copysetStatus.applyingindex();
    leaderStatus->first_index = copysetStatus.firstindex();
    leaderStatus->last_index = copysetStatus.lastindex();
    leaderStatus->disk_index = copysetStatus.diskindex();

    return true;
}

void CopysetNode::ListPeers(std::vector<Peer>* peers) const {
    std::vector<braft::PeerId> tmpPeers;

    {
        std::lock_guard<Mutex> lock(confMtx_);
        conf_.list_peers(&tmpPeers);
    }

    for (auto& p : tmpPeers) {
        Peer peer;
        peer.set_address(p.to_string());
        peers->emplace_back(std::move(peer));
    }
}

std::list<PartitionInfo> CopysetNode::GetPartitionInfoList() {
    return metaStore_->GetPartitionInfoList();
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
