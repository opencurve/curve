/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/12/20  Wenyu Zhou   Initial version
 */

#include <sys/statvfs.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <braft/closure_helper.h>

#include <vector>

#include "src/fs/fs_common.h"
#include "src/chunkserver/heartbeat.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_adaptor_util.h"

namespace curve {
namespace chunkserver {

using curve::fs::FileSystemInfo;

CopysetNodeManager* CopysetInfo::copysetNodeManager_ = nullptr;

void CopysetInfo::SetCopysetNodeManager(CopysetNodeManager* man) {
    copysetNodeManager_ = man;
}

CopysetInfo::CopysetInfo(LogicPoolID poolId, CopysetID copysetId) :
        poolId_(poolId),
        copysetId_(copysetId),
        term_(0) {
    taskOngoing_.store(false, std::memory_order_release);
    newTask_.store(false, std::memory_order_release);
    copyset_ = CopysetNodePtr(nullptr);
    task_ = HeartbeatTaskPtr(new HeartbeatTask());
}

CopysetInfo::~CopysetInfo() {
}

CopysetID CopysetInfo::GetCopysetId() {
    return copysetId_;
}

LogicPoolID CopysetInfo::GetLogicPoolId() {
    return poolId_;
}

GroupNid CopysetInfo::GetGroupId() {
    return ToGroupNid(poolId_, copysetId_);
}

uint64_t CopysetInfo::GetEpoch() {
    return copyset_->GetConfEpoch();
}

PeerId& CopysetInfo::GetPeerInTask() {
    return task_->GetPeerId();
}

CopysetNodePtr CopysetInfo::GetCopysetNode() {
    return copyset_;
}

void CopysetInfo::SetCopysetNode(CopysetNodePtr copyset) {
    poolId_ = copyset->GetLogicPoolId();
    copysetId_ = copyset->GetCopysetId();
    copyset_ = copyset;
}

bool CopysetInfo::IsTaskOngoing() {
    return taskOngoing_.load(std::memory_order_acquire);
}

bool CopysetInfo::HasTask() {
    return newTask_.load(std::memory_order_acquire);
}

void CopysetInfo::ReleaseTask() {
    newTask_.store(false, std::memory_order_release);
}

uint64_t CopysetInfo::GetTerm() {
    return term_;
}

void CopysetInfo::UpdateTerm(uint64_t term) {
    term_ = term;
}

int CopysetInfo::GetNode(scoped_refptr<braft::NodeImpl>* node) {
    braft::NodeManager* const nm = braft::NodeManager::GetInstance();
    std::vector<scoped_refptr<braft::NodeImpl>> nodes;
    std::string groupId = std::to_string(ToGroupNid(poolId_, copysetId_));

    nm->get_nodes_by_group_id(groupId, &nodes);
    if (nodes.empty()) {
        LOG(ERROR) << "Failed to get copyset node "
                   << ToGroupIdStr(poolId_, copysetId_);
        return ENOENT;
    } else if (nodes.size() > 1) {
        LOG(ERROR) << "Multiple copyset nodes exist in current chunkserver "
                   << ToGroupIdStr(poolId_, copysetId_);
        return EINVAL;
    }
    *node = nodes.front();

    return 0;
}

bool CopysetInfo::IsLeader() {
    scoped_refptr<braft::NodeImpl> node;
    if (GetNode(&node)) {
        LOG(ERROR) << "Failed to get node impl of copyset "
                   << ToGroupIdStr(poolId_, copysetId_);
        return false;
    }

    return node->is_leader();
}

int CopysetInfo::GetLeader(PeerId* leader) {
    scoped_refptr<braft::NodeImpl> node;
    if (GetNode(&node)) {
        LOG(ERROR) << "Failed to get node impl of copyset "
                   << ToGroupIdStr(poolId_, copysetId_);
        return -1;
    }

    *leader = node->leader_id();

    return 0;
}

int CopysetInfo::ListPeers(std::vector<PeerId>* peers) {
    copyset_->ListPeers(peers);

    return 0;
}

void CopysetInfo::NewTask(TASK_TYPE type, const PeerId& peerId) {
    TaskStatus status = TaskStatus::OK();

    taskOngoing_.store(true, std::memory_order_release);
    newTask_.store(true, std::memory_order_release);
    task_->NewTask(type, peerId);

    switch (type) {
    case TASK_TYPE_TRANSFER_LEADER:
        status = TransferLeader(peerId);
        break;
    case TASK_TYPE_ADD_PEER:
        status = AddPeer(peerId);
        break;
    case TASK_TYPE_REMOVE_PEER:
        status = RemovePeer(peerId);
        break;
    case TASK_TYPE_CLEAN_PEER:
        status = CleanPeer();
        break;
    case TASK_TYPE_NONE:
        // Ignore for invalid task cases
        LOG(INFO) << "Ignore invalid TASK_TYPE_NONE task";
        break;
    default:
        LOG(FATAL) << "Impossible operatioon type" << type;
        break;
    }
    if (!status.ok()) {
        FinishTask(status);
    }
}

static void AsyncFinishTask(CopysetInfoPtr info, const TaskStatus& status) {
    info->FinishTask(status);
}

void CopysetInfo::FinishTask(const TaskStatus& status) {
    task_->SetStatus(status);
    taskOngoing_.store(false, std::memory_order_release);
}

TaskStatus CopysetInfo::TransferLeader(const PeerId& peerId) {
    TaskStatus status;
    scoped_refptr<braft::NodeImpl> node;
    if (GetNode(&node) != 0) {
        status = TaskStatus(EINVAL, "Failed to get node of copyset <%u, %u>",
                            poolId_, copysetId_);
        LOG(ERROR) << status.error_str();

        return status;
    }

    if (node->leader_id() == peerId) {
        TaskStatus status = TaskStatus::OK();
        DVLOG(6) << "Skipped transferring leader to leader itself: " << peerId;

        FinishTask(status);
        return status;
    }

    int rc = node->transfer_leadership_to(peerId);
    if (rc != 0) {
        status = TaskStatus(rc, "Failed to transfer leader of copyset <%u, %u>"
                                " to peer %s, error: %s",
                            poolId_, copysetId_, peerId.to_string().c_str(),
                            berror(rc));
        LOG(ERROR) << status.error_str();

        return status;
    }

    status = TaskStatus::OK();
    FinishTask(status);

    LOG(INFO) << "Transferred leader of copyset "
              << ToGroupIdStr(poolId_, copysetId_) << " to peer " <<  peerId;

    return status;
}

TaskStatus CopysetInfo::AddPeer(const PeerId& peerId) {
    scoped_refptr<braft::NodeImpl> node;
    if (GetNode(&node) != 0) {
        TaskStatus status(EINVAL, "Failed to get node impl of copyset <%u, %u>",
                          poolId_, copysetId_);
        LOG(ERROR) << status.error_str();

        return status;
    }

    std::vector<PeerId> peers;
    if (ListPeers(&peers) != 0) {
        TaskStatus status(-1, "Failed to get peer list of copyset <%u, %u>",
                          poolId_, copysetId_);
        LOG(ERROR) << status.error_str();

        return status;
    }

    for (auto peer : peers) {
        if (peer == peerId) {
            TaskStatus status = TaskStatus::OK();
            DVLOG(6) << peerId << " is already a member of copyset "
                     << ToGroupIdStr(poolId_, copysetId_)
                     << ", skip adding peer";

            FinishTask(status);
            return status;
        }
    }

    braft::Closure* addPeerDone = braft::NewCallback(AsyncFinishTask,
            CopysetInfoPtr(this->shared_from_this()));
    node->add_peer(peerId, addPeerDone);

    return TaskStatus::OK();
}

TaskStatus CopysetInfo::RemovePeer(const PeerId& peerId) {
    scoped_refptr<braft::NodeImpl> node;
    if (GetNode(&node) != 0) {
        TaskStatus status(EINVAL, "Failed to get node impl of copyset <%u, %u>",
                          poolId_, copysetId_);
        LOG(ERROR) << status.error_str();

        return status;
    }

    std::vector<PeerId> peers;
    if (ListPeers(&peers) != 0) {
        TaskStatus status(-1, "Failed to get peer list of copyset <%u, %u>",
                          poolId_, copysetId_);
        LOG(ERROR) << status.error_str();

        return status;
    }

    bool peerValid = false;
    for (auto peer : peers) {
        if (peer == peerId) {
            peerValid = true;
            break;
        }
    }

    if (!peerValid) {
        TaskStatus status = TaskStatus::OK();
        DVLOG(6) << peerId << " is not a member of copyset "
                 << ToGroupIdStr(poolId_, copysetId_) << ", skip removing";

        FinishTask(status);
        return status;
    }

    braft::Closure* removePeerDone = braft::NewCallback(AsyncFinishTask,
            CopysetInfoPtr(this->shared_from_this()));
    node->remove_peer(peerId, removePeerDone);

    return TaskStatus::OK();
}

TaskStatus CopysetInfo::CleanPeer() {
    CopysetNodeManager* copysetMan = CopysetInfo::copysetNodeManager_;
    if (!copysetMan->PurgeCopysetNodeData(poolId_, copysetId_)) {
        LOG(ERROR) << "Failed to clean copyset "
                   << ToGroupIdStr(poolId_, copysetId_) << " and its data.";

        return TaskStatus(-1, "Failed to clean copyset");
    }

    LOG(INFO) << "Successfully cleaned copyset "
              << ToGroupIdStr(poolId_, copysetId_) << " and its data.";

    return TaskStatus::OK();
}

TaskStatus& CopysetInfo::GetStatus() {
    return task_->GetStatus();
}

void HeartbeatTask::SetStatus(const TaskStatus& status) {
    status_ = status;
}

TaskStatus& HeartbeatTask::GetStatus() {
    return status_;
}

PeerId& HeartbeatTask::GetPeerId() {
    return peerId_;
}

TASK_TYPE HeartbeatTask::GetType() {
    return type_;
}

void HeartbeatTask::NewTask(TASK_TYPE type, const PeerId& peerId) {
    type_ = type;
    peerId_ = peerId;
}

int Heartbeat::Init(const HeartbeatOptions &options) {
    toStop_.store(false, std::memory_order_release);
    options_ = options;
    term_ = 0;

    dataDirPath_ = FsAdaptorUtil::GetPathFromUri(options_.dataUri);

    butil::ip_t mdsIp;
    butil::ip_t csIp;
    if (butil::str2ip(options_.mdsIp.c_str(), &mdsIp) < 0) {
        LOG(ERROR) << "Invalid MDS IP provided: " << options_.mdsIp;
        return -1;
    }
    if (butil::str2ip(options_.ip.c_str(), &csIp) < 0) {
        LOG(ERROR) << "Invalid Chunkserver IP provided: " << options_.ip;
        return -1;
    }
    if (options_.mdsPort <= 0 || options_.mdsPort >= 65535) {
        LOG(ERROR) << "Invalid MDS port provided: " << options_.mdsPort;
        return -1;
    }
    if (options_.port <= 0 || options_.port >= 65535) {
        LOG(ERROR) << "Invalid Chunkserver port provided: " << options_.port;
        return -1;
    }
    mdsEp_ = butil::EndPoint(mdsIp, options_.mdsPort);
    csEp_ = butil::EndPoint(csIp, options_.port);
    LOG(INFO) << "MDS address: " << options_.mdsIp << ":" << options_.mdsPort;
    LOG(INFO) << "Chunkserver address: " << options_.ip << ":" << options_.port;

    CopysetInfo::SetCopysetNodeManager(options_.copysetNodeManager);

    return 0;
}

int Heartbeat::Run() {
    hbThread_ = std::unique_ptr<std::thread>(
            new std::thread(std::bind(Heartbeat::HeartbeatWorker, this)));

    return 0;
}

int Heartbeat::Stop() {
    LOG(INFO) << "Stopping Heartbeat manager.";
    toStop_.store(true, std::memory_order_release);

    hbThread_->join();
    LOG(INFO) << "Stopped Heartbeat manager.";

    return 0;
}

int Heartbeat::Fini() {
    Stop();

    LOG(INFO) << "Heartbeat manager cleaned up.";
    return 0;
}

int Heartbeat::GetFileSystemSpaces(size_t* capacity, size_t* avail) {
    int ret;
    struct FileSystemInfo info;

    ret = options_.fs->Statfs(dataDirPath_, &info);
    if (ret != 0) {
        LOG(ERROR) << "Failed to get file system space information, "
                   << " error message: " << strerror(errno);
        return -1;
    }

    *capacity = info.total;
    *avail = info.available;

    return 0;
}

int Heartbeat::BuildCopysetInfo(curve::mds::heartbeat::CopysetInfo* info,
                                CopysetNodePtr copyset) {
    int ret;
    LogicPoolID poolId = copyset->GetLogicPoolId();
    CopysetID copysetId = copyset->GetCopysetId();
    CopysetInfoPtr copysetInfo = GetCopysetInfo(copyset);

    info->set_logicalpoolid(poolId);
    info->set_copysetid(copysetId);
    info->set_epoch(copyset->GetConfEpoch());

    std::vector<PeerId> peers;

    ret = copysetInfo->ListPeers(&peers);
    if (ret != 0) {
        LOG(ERROR) << "Failed to get peer list of copyset "
                   << ToGroupIdStr(poolId, copysetId);
        return -1;
    }
    for (PeerId peer : peers) {
        info->add_peers(peer.to_string().c_str());
    }

    PeerId leader;
    ret = copysetInfo->GetLeader(&leader);
    if (ret != 0) {
        LOG(ERROR) << "Failed to get leader of copyset "
                   << ToGroupIdStr(poolId, copysetId);
        return -1;
    }
    info->set_leaderpeer(leader.to_string());

    /*
     * TODO(wenyu): copyset stats field will not be valid until performance
     * monitoring feature is ready
     */

    if (!copysetInfo->HasTask()) {
        return 0;
    }

    ConfigChangeInfo* confChxInfo = new ConfigChangeInfo();
    confChxInfo->set_peer(copysetInfo->GetPeerInTask().to_string());
    confChxInfo->set_finished(!copysetInfo->IsTaskOngoing());
    if (!copysetInfo->IsTaskOngoing()) {
        CandidateError* err = new CandidateError();
        err->set_errtype(copysetInfo->GetStatus().error_code());
        err->set_errmsg(copysetInfo->GetStatus().error_str());

        confChxInfo->set_allocated_err(err);
        copysetInfo->ReleaseTask();
    }

    info->set_allocated_configchangeinfo(confChxInfo);

    return 0;
}

int Heartbeat::UpdateCopysetInfo(const std::vector<CopysetNodePtr>& copysets) {
    /*
     * 此处term为心跳周期号，每个心跳一个term号，跟raft的term没有关系，用来区别
     * 活跃与非活跃Copyset跟踪项
     */
    ++term_;

    // Update term of available copysets
    for (CopysetNodePtr copyset : copysets) {
        LogicPoolID poolId = copyset->GetLogicPoolId();
        CopysetID copysetId = copyset->GetCopysetId();
        GroupNid groupId = ToGroupNid(poolId, copysetId);

        if (copysets_.count(groupId) == 0) {
            copysets_[groupId] = CopysetInfoPtr(new CopysetInfo(poolId,
                                                                copysetId));
        }
        CopysetInfoPtr copysetInfo = copysets_[groupId];
        if (copysetInfo->GetCopysetNode() != copyset) {
            copysetInfo->SetCopysetNode(copyset);
        }
        copysetInfo->UpdateTerm(term_);
    }

    return 0;
}

void Heartbeat::CleanAgingCopysetInfo() {
    for (auto copyset : copysets_) {
        CopysetInfoPtr info = copyset.second;
        if (info->GetTerm() < term_ && (!info->HasTask())) {
            LOG(INFO) << "Removing copyset info "
                      << ToGroupIdStr(info->GetLogicPoolId(),
                                    info->GetCopysetId());
            copysets_.erase(info->GetGroupId());
        }
    }
}

CopysetInfoPtr Heartbeat::GetCopysetInfo(LogicPoolID poolId,
                                         CopysetID copysetId) {
    GroupNid groupId = ToGroupNid(poolId, copysetId);
    if (copysets_.count(groupId) == 0) {
        copysets_[groupId] = CopysetInfoPtr(new CopysetInfo(poolId, copysetId));
    }
    return copysets_[groupId];
}

CopysetInfoPtr Heartbeat::GetCopysetInfo(CopysetNodePtr copyset) {
    return GetCopysetInfo(copyset->GetLogicPoolId(), copyset->GetCopysetId());
}

int Heartbeat::BuildRequest(HeartbeatRequest* req) {
    int ret;

    req->set_chunkserverid(options_.chunkserverId);
    req->set_token(options_.chunkserverToken);
    req->set_ip(options_.ip);
    req->set_port(options_.port);

    /*
     * TODO(wenyu): DiskState field is not valid yet until disk health feature
     * is ready
     */
    curve::mds::heartbeat::DiskState* diskState =
                    new curve::mds::heartbeat::DiskState();
    diskState->set_errtype(0);
    diskState->set_errmsg("");
    req->set_allocated_diskstate(diskState);
    /*
     * TODO(wenyu): stats field will not be valid until performance monitoring
     * is ready
     */
    curve::mds::heartbeat::ChunkServerStatisticInfo* stats =
                    new curve::mds::heartbeat::ChunkServerStatisticInfo();
    stats->set_readrate(0);
    stats->set_writerate(0);
    stats->set_readiops(0);
    stats->set_writeiops(0);
    req->set_allocated_stats(stats);

    size_t cap, avail;
    ret = GetFileSystemSpaces(&cap, &avail);
    if (ret != 0) {
        LOG(ERROR) << "Failed to get file system space information for path "
                   << dataDirPath_;
        return -1;
    }
    req->set_diskcapacity(cap);
    req->set_diskused(cap - avail);

    std::vector<CopysetNodePtr> copysets;
    CopysetNodeManager* copysetMan = options_.copysetNodeManager;
    copysetMan->GetAllCopysetNodes(&copysets);

    UpdateCopysetInfo(copysets);

    req->set_copysetcount(copysets.size());
    int leaders = 0;
    // Inactive copysets are skipped and will be purged later
    for (CopysetNodePtr copyset : copysets) {
        curve::mds::heartbeat::CopysetInfo* info = req->add_copysetinfos();

        ret = BuildCopysetInfo(info, copyset);
        if (ret != 0) {
            LOG(ERROR) << "Failed to build heartbeat information of copyset "
                       << ToGroupIdStr(copyset->GetLogicPoolId(),
                                     copyset->GetCopysetId());
            return -1;
        }
        if (GetCopysetInfo(copyset)->IsLeader()) {
            ++leaders;
        }
    }
    req->set_leadercount(leaders);

    return 0;
}

void Heartbeat::DumpHeartbeatRequest(const HeartbeatRequest& request) {
    DVLOG(6) << "Heartbeat request: Chunkserver ID: "
             << request.chunkserverid()
             << ", IP: " << request.ip() << ", port: " << request.port()
             << ", copyset count: " << request.copysetcount()
             << ", leader count: " << request.leadercount();
    for (int i = 0; i < request.copysetinfos_size(); i ++) {
        const curve::mds::heartbeat::CopysetInfo& info =
            request.copysetinfos(i);

        std::string peersStr = "";
        for (int j = 0; j < info.peers_size(); j ++) {
            peersStr += info.peers(j) + ",";
        }

        DVLOG(6) << "Copyset " << i << " "
                 << ToGroupIdStr(info.logicalpoolid(), info.copysetid())
                 << ", epoch: " << info.epoch()
                 << ", leader: " << info.leaderpeer()
                 << ", peers: " << peersStr;

        if (info.has_configchangeinfo()) {
            const ConfigChangeInfo& cxInfo = info.configchangeinfo();
            DVLOG(6) << "Config change info: peer: " << cxInfo.peer()
                     << ", finished: " << cxInfo.finished()
                     << ", errno: " << cxInfo.err().errtype()
                     << ", errmsg: " << cxInfo.err().errmsg();
        }
    }
}

void Heartbeat::DumpHeartbeatResponse(const HeartbeatResponse& response) {
    int count = response.needupdatecopysets_size();
    if (count > 0) {
        DVLOG(6) << "Received " << count << " config change commands:";
        for (int i = 0; i < count; i ++) {
            CopysetConf conf = response.needupdatecopysets(i);

            int type = (conf.has_type()) ? conf.type() : 0;
            std::string item = (conf.has_configchangeitem()) ?
                conf.configchangeitem() : "";

            std::string peersStr = "";
            for (int j = 0; j < conf.peers_size(); j ++) {
                peersStr += conf.peers(j);
            }

            DVLOG(6) << "Config change " << i << ": "
                     << "Copyset < " << conf.logicalpoolid()
                     << ", " << conf.copysetid() << ">, epoch: "
                     << conf.epoch() << ", Peers: " << peersStr
                     << ", type: " << type << ", item: " << item;
        }
    } else {
        DVLOG(6) << "Received no config change command.";
    }
}

int Heartbeat::SendHeartbeat(const HeartbeatRequest& request,
                             HeartbeatResponse* response) {
    brpc::Channel channel;
    if (channel.Init(mdsEp_, NULL) != 0) {
        LOG(ERROR) << "Fail to init channel to MDS " << mdsEp_;
    }

    curve::mds::heartbeat::HeartbeatService_Stub stub(&channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(options_.timeout);

    DVLOG(6) << "Sending heartbeat to MDS " << mdsEp_;
    DumpHeartbeatRequest(request);

    stub.ChunkServerHeartbeat(&cntl, &request, response, nullptr);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to send heartbeat to MDS " << mdsEp_ << ","
                   << " cntl error: " << cntl.ErrorText();
        return -1;
    } else {
        DumpHeartbeatResponse(*response);
    }

    return 0;
}

int Heartbeat::ExecTask(const HeartbeatResponse& response) {
    int count = response.needupdatecopysets_size();
    for (int i = 0; i < count; i ++) {
        CopysetConf conf = response.needupdatecopysets(i);
        GroupNid groupId = ToGroupNid(conf.logicalpoolid(), conf.copysetid());

        // std::vector<PeerId> peers;
        uint64_t epoch = conf.epoch();

        // Perform validations
        if (copysets_.count(groupId) == 0) {
            TaskStatus status(ENOENT, "Failed to find copyset <%u, %u>",
                              conf.logicalpoolid(), conf.copysetid());
            LOG(ERROR) << status.error_str();

            copysets_[groupId] = CopysetInfoPtr(
                    new CopysetInfo(conf.logicalpoolid(), conf.copysetid()));
            CopysetInfoPtr copysetInfo = copysets_[groupId];
            copysetInfo->NewTask(TASK_TYPE_NONE, PeerId());
            copysetInfo->FinishTask(status);
            continue;
        }
        CopysetInfoPtr copysetInfo = copysets_[groupId];
        if (epoch < copysetInfo->GetEpoch()) {
            TaskStatus status(EINVAL, "Invalid epoch aginast copyset <%u, %u>"
                                      " expected epoch: %lu, recevied: %lu",
                              conf.logicalpoolid(),
                              conf.copysetid(),
                              copysetInfo->GetEpoch(),
                              epoch);
            LOG(ERROR) << status.error_str();

            copysetInfo->NewTask(TASK_TYPE_NONE, PeerId());
            copysetInfo->FinishTask(status);
            continue;
        }

        // Determine if to clean peer or not
        bool cleanPeer = true;
        for (int j = 0; j < conf.peers_size(); j ++) {
            std::string epStr = std::string(butil::endpoint2str(csEp_).c_str());
            if (conf.peers(j).find(epStr) != std::string::npos) {
                cleanPeer = false;
                break;
            }
        }
        if (cleanPeer) {
            LOG(INFO) << "Clean peer " << csEp_ << " of copyset "
                      << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
            copysetInfo->NewTask(TASK_TYPE_CLEAN_PEER, PeerId());
            continue;
        }

        bool confChx = conf.has_type();
        if (!confChx) {
            TaskStatus status(EINVAL, "Failed to parse task against copyset"
                                      "<%u, %u>",
                              conf.logicalpoolid(), conf.copysetid());
            LOG(ERROR) << status.error_str();

            copysetInfo->NewTask(TASK_TYPE_NONE, PeerId());
            copysetInfo->FinishTask(status);
            continue;
        }
        PeerId peerId;
        std::string peerStr = conf.configchangeitem();
        if (peerId.parse(peerStr) != 0) {
            TaskStatus status(EINVAL, "Failed to parse peerId against copyset"
                                      "<%u, %u>, received peerId %s",
                              conf.logicalpoolid(), conf.copysetid(),
                              peerStr.c_str());
            LOG(ERROR) << status.error_str();

            copysetInfo->NewTask(TASK_TYPE_NONE, PeerId());
            copysetInfo->FinishTask(status);
            continue;
        }
        switch (conf.type()) {
        case curve::mds::heartbeat::TRANSFER_LEADER:
            LOG(INFO) << "Transfer leader to " << peerId << " on copyset "
                      << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
            copysetInfo->NewTask(TASK_TYPE_TRANSFER_LEADER, peerId);
            break;
        case curve::mds::heartbeat::ADD_PEER:
            LOG(INFO) << "Adding peer " << peerId << " to copyset "
                      << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
            copysetInfo->NewTask(TASK_TYPE_ADD_PEER, peerId);
            break;
        case curve::mds::heartbeat::REMOVE_PEER:
            LOG(INFO) << "Removing peer " << peerId << " from copyset "
                      << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
            copysetInfo->NewTask(TASK_TYPE_REMOVE_PEER, peerId);
            break;
        default:
            TaskStatus status(EINVAL, "Invalid operation %d against copyset"
                                      "<%u, %u>",
                              conf.type(),
                              conf.logicalpoolid(), conf.copysetid());
            LOG(ERROR) << status.error_str();

            copysetInfo->NewTask(TASK_TYPE_NONE, PeerId());
            copysetInfo->FinishTask(status);
            break;
        }
    }

    return 0;
}

void Heartbeat::WaitForNextHeartbeat() {
    // TODO(wenyu): triggering with more accurate timer
    static int64_t t0 = butil::monotonic_time_ms();
    int64_t t1 = butil::monotonic_time_ms();

    int rest = options_.interval - (t1 - t0) / 1000;
    rest = (rest < 0) ? 0 : rest;

    t0 = t1;
    DVLOG(6) << "Heartbeat waiting interval";

    sleep(rest);
    DVLOG(6) << "Heartbeat finished waiting";
}

void Heartbeat::HeartbeatWorker(Heartbeat *heartbeat) {
    int ret;

    LOG(INFO) << "Starting Heartbeat worker thread.";

    while (!heartbeat->toStop_.load(std::memory_order_acquire)) {
        HeartbeatRequest req;
        HeartbeatResponse resp;

        DVLOG(1) << "building heartbeat info";
        ret = heartbeat->BuildRequest(&req);
        if (ret != 0) {
            LOG(ERROR) << "Failed to build heartbeat request";
            sleep(1);
            continue;
        }

        DVLOG(1) << "sending heartbeat info";
        ret = heartbeat->SendHeartbeat(req, &resp);
        if (ret != 0) {
            LOG(ERROR) << "Failed to send heartbeat to MDS";
            sleep(1);
            continue;
        }

        heartbeat->CleanAgingCopysetInfo();

        DVLOG(1) << "executing heartbeat info";
        ret = heartbeat->ExecTask(resp);
        if (ret != 0) {
            LOG(ERROR) << "Failed to execute heartbeat tasks";
            sleep(1);
            continue;
        }

        heartbeat->WaitForNextHeartbeat();
    }

    LOG(INFO) << "Heartbeat worker thread stopped.";
}

}  // namespace chunkserver
}  // namespace curve
