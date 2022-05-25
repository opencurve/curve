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
#include "absl/utility/utility.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/copyset/copyset_node_manager.h"
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
static bvar::LatencyRecorder g_concurrent_apply_from_log_wait_latency(
    "concurrent_apply_from_log_wait");

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::curve::common::TimeUtility;
using ::curve::common::UriParser;

namespace {
const char* const kConfEpochFilename = "conf.epoch";
const char* const kStorageDataPath = "storage_data";
}  // namespace

CopysetNode::CopysetNode(PoolId poolId, CopysetId copysetId,
                         const braft::Configuration& conf,
                         CopysetNodeManager* nodeManager)
    : poolId_(poolId),
      copysetId_(copysetId),
      groupId_(ToGroupId(poolId_, copysetId_)),
      name_(ToGroupIdString(poolId_, copysetId_)),
      conf_(conf),
      nodeManager_(nodeManager),
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
      latestLoadSnapshotIndex_(0),
      confChangeMtx_(),
      ongoingConfChange_(),
      metric_(absl::make_unique<OperatorMetric>(poolId_, copysetId_)),
      isLoading_(false) {}

CopysetNode::~CopysetNode() {
    Stop();
    raftNode_.reset();
    applyQueue_.reset();
    metaStore_.reset();
}

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

    int ret = options_.localFileSystem->Mkdir(copysetDataPath_);
    if (ret != 0) {
        LOG(ERROR) << "Failed to create copyset dir `" << copysetDataPath_
                   << "`, error: " << berror();
        return false;
    }

    epochFile_ = absl::make_unique<ConfEpochFile>(options_.localFileSystem);

    // init apply queue
    applyQueue_ = absl::make_unique<ApplyQueue>();
    if (!applyQueue_->Start(options_.applyQueueOption)) {
        LOG(ERROR) << "Start apply queue failed";
        return false;
    }

    options_.storageOptions.dataDir = copysetDataPath_ + "/" + kStorageDataPath;

    // create metastore
    options_.storageOptions.localFileSystem = options_.localFileSystem;
    metaStore_ = MetaStoreImpl::Create(this, options_.storageOptions);
    if (metaStore_ == nullptr) {
        LOG(ERROR) << "Failed to create meta store";
        return false;
    }

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
    }

    if (applyQueue_) {
        applyQueue_->Flush();
        applyQueue_->Stop();
    }

    if (metaStore_) {
        LOG_IF(ERROR, metaStore_->Clear() != true)
            << "Failed to clear metastore, copyset: " << name_;
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
            g_concurrent_apply_from_log_wait_latency << timer.u_elapsed();
        }
    }
}

void CopysetNode::on_shutdown() {
    LOG(INFO) << "Copyset: " << name_ << " is shutdown";
}

namespace {

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

    braft::SnapshotWriter* GetSnapshotWriter() const override {
        return writer_;
    }

 private:
    CopysetNode* node_;
    braft::SnapshotWriter* writer_;
    braft::Closure* snapDone_;
    RaftSnapshotMetric::MetricContext* ctx_;
};

}  // namespace

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

    // TODO(wuhanqing): MetaStore::Save will start a thread and do task
    // asynchronously, after task completed it will call
    // OnSnapshotSaveDoneImpl::Run
    // BUT, this manner is not so clear, maybe it better to make thing
    // asynchronous directly in here
    metaStore_->Save(writer->get_path(), new OnSnapshotSaveDoneClosureImpl(
                                             this, writer, done, metricCtx));
    doneGuard.release();

    // `Cancel` only available for rvalue
    std::move(cleanMetricIfFailed).Cancel();
}

namespace {

class CopysetLoadingGuard {
 public:
    explicit CopysetLoadingGuard(std::atomic_bool& flag) : flag_(flag) {
        flag_.store(true, std::memory_order_release);
    }

    ~CopysetLoadingGuard() {
        flag_.store(false, std::memory_order_release);
    }

 private:
    std::atomic_bool& flag_;
};

}  // namespace

int CopysetNode::on_snapshot_load(braft::SnapshotReader* reader) {
    LOG(INFO) << "Copyset " << name_ << " begin to load snapshot from '"
              << reader->get_path() << "'";

    CopysetLoadingGuard guard(isLoading_);
    // load conf
    const std::string confFile = reader->get_path() + "/" + kConfEpochFilename;

    // TODO(wuhanqing): remove the file exists test, it shouldn't happens
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
    bool succ = metaStore_->Load(reader->get_path());
    if (!succ) {
        LOG(ERROR) << "Copyset " << name_
                   << " load snapshot failed, metastore load return "
                      "failed";
        return -1;
    }

    braft::SnapshotMeta meta;
    reader->load_meta(&meta);
    auto prevIndex =
        absl::exchange(latestLoadSnapshotIndex_, meta.last_included_index());
    LOG(INFO) << "Copyset " << name_ << " load snapshot from '"
              << reader->get_path()
              << "' success, update load snapshot index from " << prevIndex
              << " to " << latestLoadSnapshotIndex_;

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
               << " meet error: " << e;
}

void CopysetNode::on_configuration_committed(const braft::Configuration& conf,
                                             int64_t index) {
    braft::Configuration oldconf;

    {
        std::lock_guard<Mutex> lk(confMtx_);
        oldconf = absl::exchange(conf_, conf);

        // load snapshot also call this function, but it shouldn't increase
        // epoch
        if (index != latestLoadSnapshotIndex_) {
            epoch_.fetch_add(1, std::memory_order_acq_rel);
        }
    }

    LOG(INFO) << "Copyset: " << name_
              << " committed new configuration, peer id: "
              << peerId_.to_string() << ", new conf: " << conf
              << ", old conf: " << oldconf << ", index: " << index
              << ", epoch: " << epoch_.load(std::memory_order_relaxed);
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

    int rc = channel.Init(leaderId.addr, nullptr);
    if (rc != 0) {
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

// if copyset is loading, return false;
// if copyset is not loading, and metastore returns fales, retry;
// if copyset is not loading, and metastore returns true, return true and get
// partition info list success.
bool CopysetNode::GetPartitionInfoList(
                    std::list<PartitionInfo> *partitionInfoList) {
    uint32_t retryCount = 0;
    while (true) {
        if (IsLoading()) {
            LOG(INFO) << "Copyset is loading, return empty partition list";
            return false;
        }
        bool ret = metaStore_->GetPartitionInfoList(partitionInfoList);
        if (ret) {
            return true;
        }
        LOG(WARNING) << "Copyset is not loading, but GetPartitionInfoList fail,"
                     << " retryCount = " << retryCount++;
    }
}

void CopysetNode::OnConfChangeComplete() {
    std::lock_guard<Mutex> lk(confChangeMtx_);
    ongoingConfChange_.Reset();
}

butil::Status CopysetNode::TransferLeader(const Peer& peer) {
    if (!nodeManager_->IsLoadFinished()) {
        auto st = butil::Status(EBUSY,
                                "Reject transfer leader because all copysets "
                                "are still loading, copyset: %s",
                                name_.c_str());
        LOG(WARNING) << st.error_str();
        return st;
    }

    braft::PeerId target;
    int rc = target.parse(peer.address());
    if (rc != 0) {
        auto st = butil::Status(EINVAL, "Peer %s is not valid, copyset: %s",
                                peer.address().c_str(), name_.c_str());
        LOG(WARNING) << st.error_str();
        return st;
    }

    if (raftNode_->leader_id() == target) {
        LOG(WARNING) << "Skipped transferring leader to itself, copyset: "
                     << name_ << ", target: " << target;
        return butil::Status::OK();
    }

    std::lock_guard<Mutex> lk(confChangeMtx_);
    auto status = ReadyDoConfChange();
    if (!status.ok()) {
        LOG(WARNING) << status.error_str();
        return status;
    }

    ongoingConfChange_ =
        OngoingConfChange(ConfigChangeType::TRANSFER_LEADER, peer);

    LOG(INFO) << "Transferring leader to peer: " << target
              << ", copyset: " << name_;

    rc = raftNode_->transfer_leadership_to(target);
    if (rc != 0) {
        auto status = butil::Status(
            rc, "Falied to transfer leader of copyset %s to peer %s, error: %s",
            name_.c_str(), peerId_.to_string().c_str(), berror(rc));
        LOG(ERROR) << status.error_str();
        return status;
    }

    return butil::Status::OK();
}

void CopysetNode::DoAddOrRemovePeer(ConfigChangeType type, const Peer& peer,
                                    braft::Closure* done) {
    CHECK(type == ConfigChangeType::ADD_PEER ||
          type == ConfigChangeType::REMOVE_PEER);

    butil::Status status;
    auto doneGuard = absl::MakeCleanup([&status, &done]() {
        if (done) {
            done->status() = status;
            done->Run();
        }
    });

    braft::PeerId target;
    int rc = target.parse(peer.address());
    if (rc != 0) {
        status = butil::Status(EINVAL, "Peer %s is not valid, copyset: %s",
                               peer.address().c_str(), name_.c_str());
        LOG(WARNING) << status.error_str();
        return;
    }

    std::vector<braft::PeerId> currentPeers;
    {
        std::lock_guard<Mutex> lk(confMtx_);
        conf_.list_peers(&currentPeers);
    }

    bool hasTargetPeer = false;
    for (auto& peer : currentPeers) {
        if (peer == target) {
            hasTargetPeer = true;
            break;
        }
    }

    if (type == ConfigChangeType::ADD_PEER && hasTargetPeer) {
        status.set_error(EEXIST,
                         "Peer %s is already a member of current copyset: %s",
                         target.to_string().c_str(), name_.c_str());
        LOG(WARNING) << status.error_str();
        return;
    } else if (type == ConfigChangeType::REMOVE_PEER && !hasTargetPeer) {
        status.set_error(ENOENT,
                         "Peer %s is not a member of current copyset: %s",
                         target.to_string().c_str(), name_.c_str());
        LOG(WARNING) << status.error_str();
        return;
    }

    confChangeMtx_.lock();
    status = ReadyDoConfChange();
    if (!status.ok()) {
        confChangeMtx_.unlock();
        LOG(WARNING) << status.error_str();
        return;
    }

    std::move(doneGuard).Cancel();
    ongoingConfChange_ = OngoingConfChange(type, peer);
    OnConfChangeDone* confChangeDone =
        new OnConfChangeDone(this, done, ongoingConfChange_);
    confChangeMtx_.unlock();

    return type == ConfigChangeType::ADD_PEER
               ? raftNode_->add_peer(target, confChangeDone)
               : raftNode_->remove_peer(target, confChangeDone);
}

void CopysetNode::AddPeer(const Peer& peer, braft::Closure* done) {
    return DoAddOrRemovePeer(ConfigChangeType::ADD_PEER, peer, done);
}

void CopysetNode::RemovePeer(const Peer& peer, braft::Closure* done) {
    return DoAddOrRemovePeer(ConfigChangeType::REMOVE_PEER, peer, done);
}

void CopysetNode::ChangePeers(const std::vector<Peer>& newPeers,
                              braft::Closure* done) {
    butil::Status status;
    auto doneGuard = absl::MakeCleanup([&status, &done]() {
        if (done) {
            done->status() = status;
            done->Run();
        }
    });

    std::vector<braft::PeerId> newPeerIds;
    for (auto& peer : newPeers) {
        newPeerIds.emplace_back(peer.address());
    }

    Configuration newConf(newPeerIds);
    Configuration adding;
    Configuration removing;

    {
        std::lock_guard<Mutex> lk(confMtx_);
        newConf.diffs(conf_, &adding, &removing);
    }

    if (adding.size() != 1 || removing.size() != 1) {
        status = butil::Status(EPERM,
                               "Only support change one peer at a "
                               "time is supported, copyset: %s",
                               name_.c_str());
        LOG(WARNING) << "Only change one peer at a time is supported, adding: "
                     << adding << ", removing: " << removing;
        return;
    }

    confChangeMtx_.lock();
    status = ReadyDoConfChange();
    if (!status.ok()) {
        confChangeMtx_.unlock();
        LOG(WARNING) << status.error_str();
        return;
    }

    std::move(doneGuard).Cancel();
    Peer alterPeer;
    alterPeer.set_address(adding.begin()->to_string());
    ongoingConfChange_ =
        OngoingConfChange(ConfigChangeType::CHANGE_PEER, std::move(alterPeer));
    OnConfChangeDone* confChangeDone =
        new OnConfChangeDone(this, done, ongoingConfChange_);
    confChangeMtx_.unlock();

    return raftNode_->change_peers(newConf, confChangeDone);
}

void CopysetNode::GetConfChange(ConfigChangeType* type, Peer* alterPeer) {
    std::lock_guard<Mutex> lk(confChangeMtx_);

    if (ongoingConfChange_.type == ConfigChangeType::TRANSFER_LEADER) {
        // raft transfer leader doesn't have a callback, so get the latest
        // transferring status from raft node
        braft::NodeStatus status;
        raftNode_->get_status(&status);
        if (status.state == braft::State::STATE_TRANSFERRING) {
            *type = ConfigChangeType::TRANSFER_LEADER;
            *alterPeer = ongoingConfChange_.alterPeer;
        } else {
            *type = ConfigChangeType::NONE;
            ongoingConfChange_.Reset();
        }
        return;
    } else if (ongoingConfChange_.type == ConfigChangeType::NONE ||
               !IsLeaderTerm()) {
        *type = ConfigChangeType::NONE;
        return;
    }

    *type = ongoingConfChange_.type;
    *alterPeer = ongoingConfChange_.alterPeer;
    return;
}

bool CopysetNode::HasOngoingConfChange() {
    switch (ongoingConfChange_.type) {
        case ConfigChangeType::NONE:
            return false;
        case ConfigChangeType::TRANSFER_LEADER:
            break;
        case ConfigChangeType::ADD_PEER:
        case ConfigChangeType::REMOVE_PEER:
        case ConfigChangeType::CHANGE_PEER:
        default:
            return true;
    }

    // raft transfer leader doesn't have a callback, so get the latest
    // transferring status from raft node
    braft::NodeStatus status;
    raftNode_->get_status(&status);
    if (status.state == braft::State::STATE_TRANSFERRING) {
        return true;
    } else {
        ongoingConfChange_.Reset();
        return false;
    }
}

butil::Status CopysetNode::ReadyDoConfChange() {
    if (!IsLeaderTerm()) {
        return butil::Status(EPERM, "Not a leader, copyset: %s", name_.c_str());
    } else if (HasOngoingConfChange()) {
        return butil::Status(EBUSY,
                             "Doing another configurations change, copyset: %s",
                             name_.c_str());
    }

    return butil::Status::OK();
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
