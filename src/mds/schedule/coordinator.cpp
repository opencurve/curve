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
 * Created Date: Mon Nov 17 2018
 * Author: lixiaocui
 */

#include <glog/logging.h>
#include <string>
#include <memory>
#include <thread>  //NOLINT
#include "src/mds/schedule/coordinator.h"
#include "src/mds/topology/topology_item.h"

namespace curve {
namespace mds {
namespace schedule {
/**
 * use curl -L mdsIp:port/flags/enableCopySetScheduler?setvalue=true
 * for dynamic parameter configuration
 */
static bool pass_bool(const char*, bool) { return true; }
DEFINE_bool(enableCopySetScheduler, true, "switch of copyset scheduler");
DEFINE_validator(enableCopySetScheduler, &pass_bool);
DEFINE_bool(enableLeaderScheduler, true, "switch of leader scheduler");
DEFINE_validator(enableLeaderScheduler, &pass_bool);
DEFINE_bool(enableReplicaScheduler, true, "switch of replica scheduler");
DEFINE_validator(enableReplicaScheduler, &pass_bool);
DEFINE_bool(enableRecoverScheduler, true, "switch of recover scheduler");
DEFINE_validator(enableRecoverScheduler, &pass_bool);

Coordinator::Coordinator(const std::shared_ptr<TopoAdapter> &topo) {
    this->topo_ = topo;
}

Coordinator::~Coordinator() {
    Stop();
}

void Coordinator::InitScheduler(
    const ScheduleOption &conf, std::shared_ptr<ScheduleMetrics> metrics) {
    conf_ = conf;

    opController_ =
        std::make_shared<OperatorController>(conf.operatorConcurrent, metrics);

    if (conf.enableLeaderScheduler) {
        schedulerController_[SchedulerType::LeaderSchedulerType] =
            std::make_shared<LeaderScheduler>(conf, topo_, opController_);
        LOG(INFO) << "init leader scheduler ok!";
    }

    if (conf.enableCopysetScheduler) {
        schedulerController_[SchedulerType::CopySetSchedulerType] =
            std::make_shared<CopySetScheduler>(conf, topo_, opController_);
        LOG(INFO) << "init copySet scheduler ok!";
    }

    if (conf.enableRecoverScheduler) {
        schedulerController_[SchedulerType::RecoverSchedulerType] =
            std::make_shared<RecoverScheduler>(conf, topo_, opController_);
        LOG(INFO) << "init recover scheduler ok!";
    }

    if (conf.enableReplicaScheduler) {
        schedulerController_[SchedulerType::ReplicaSchedulerType] =
            std::make_shared<ReplicaScheduler>(conf, topo_, opController_);
        LOG(INFO) << "init replica scheduler ok!";
    }
}

void Coordinator::Run() {
    for (auto &v : schedulerController_) {
        runSchedulerThreads_[v.first] = common::Thread(
            &Coordinator::RunScheduler, this, v.second, v.first);
    }
}

void Coordinator::Stop() {
    sleeper_.interrupt();
    for (auto &v : schedulerController_) {
        if (runSchedulerThreads_.find(v.first) == runSchedulerThreads_.end()) {
            continue;
        }
        runSchedulerThreads_[v.first].join();
        runSchedulerThreads_.erase(v.first);
    }
}

ChunkServerIdType Coordinator::CopySetHeartbeat(
    const ::curve::mds::topology::CopySetInfo &originInfo,
    const ::curve::mds::heartbeat::ConfigChangeInfo &configChInfo,
    ::curve::mds::heartbeat::CopySetConf *out) {
    // transfer copyset info format from topology to scheduler
    CopySetInfo info;
    if (!topo_->CopySetFromTopoToSchedule(originInfo, &info)) {
        LOG(ERROR) << "coordinator cannot convert copyset("
                   << originInfo.GetLogicalPoolId() << ","
                   << originInfo.GetId()
                   << ") from heartbeat topo form to schedule form error";
        return ::curve::mds::topology::UNINTIALIZE_ID;
    }
    info.configChangeInfo = configChInfo;

    // check if there's any operator on specified copyset
    Operator op;
    if (!opController_->GetOperatorById(info.id, &op)) {
        return ::curve::mds::topology::UNINTIALIZE_ID;
    }
    LOG(INFO) << "find operator on" << info.CopySetInfoStr() << "), operator: "
              << op.OpToString();

    // Update the status of the operator according to the copyset information
    // reported by the leader, return true if there's any new configuration
    CopySetConf res;
    bool hasOrder = opController_->ApplyOperator(info, &res);
    if (hasOrder) {
        LOG(INFO) << "going to order operator " << op.OpToString();
        // determine whether the epoch and startEpoch are the same,
        // if not, the operator will not be dispatched
        // scenario: The MDS has already dispatch the operator, and the
        //           copyset has finished but not yet report. At this time
        //           the MDS restart and generate new operator on this copyset.
        //           this operator should not be dispatched and should be
        //           removed
        if (info.epoch != op.startEpoch) {
            LOG(WARNING) << "Operator " << op.OpToString()
                         << "on " << info.CopySetInfoStr()
                         << " is stale, remove operator";
            opController_->RemoveOperator(info.id);
            return ::curve::mds::topology::UNINTIALIZE_ID;
        }

        // the operator should not be dispacthed if the candidate
        // of addPeer or transferLeader or changePeer is offline
        ChunkServerInfo chunkServer;
        if (!topo_->GetChunkServerInfo(res.configChangeItem, &chunkServer)) {
            LOG(ERROR) << "coordinator can not get chunkServer "
                    << res.configChangeItem << " from topology";
            opController_->RemoveOperator(info.id);
            return ::curve::mds::topology::UNINTIALIZE_ID;
        }
        bool needCheckType = (res.type == ConfigChangeType::ADD_PEER ||
            res.type == ConfigChangeType::TRANSFER_LEADER ||
            res.type == ConfigChangeType::CHANGE_PEER);
        if (needCheckType && chunkServer.IsOffline()) {
            LOG(WARNING) << "candidate chunkserver " << chunkServer.info.id
                       << " is offline, abort config change";
            opController_->RemoveOperator(info.id);
            return ::curve::mds::topology::UNINTIALIZE_ID;
        }

        // build the copysetConf need to be returned in heartbeat
        // if build failed, remove the operator
        if (!BuildCopySetConf(res, out)) {
            LOG(ERROR) << "build copyset conf for " << info.CopySetInfoStr()
                       << ") fail, remove operator";
            opController_->RemoveOperator(info.id);
            return ::curve::mds::topology::UNINTIALIZE_ID;
        }

        LOG(INFO) << "order operator " << op.OpToString() << " on "
                  << info.CopySetInfoStr() << " success";
        return res.configChangeItem;
    }

    return ::curve::mds::topology::UNINTIALIZE_ID;
}

int Coordinator::RapidLeaderSchedule(PoolIdType lpid) {
    auto rapidLeaderScheduler = std::make_shared<RapidLeaderScheduler>(
                                    conf_, topo_, opController_, lpid);
    return rapidLeaderScheduler->Schedule();
}

int Coordinator::QueryChunkServerRecoverStatus(
    const std::vector<ChunkServerIdType> &idList,
    std::map<ChunkServerIdType, bool> *statusMap) {
    std::vector<ChunkServerInfo> infos;

    // to get the chunkserver infos if the input is a empty vector
    if (idList.empty()) {
        infos = topo_->GetChunkServerInfos();
    }
    for (auto id : idList) {
        ChunkServerInfo info;
        bool getOk = topo_->GetChunkServerInfo(id, &info);
        if (!getOk) {
            LOG(ERROR) << "invalid chunkserver id: " << id;
            return kScheduleErrInvalidQueryChunkserverID;
        }
        infos.emplace_back(info);
    }

    // Iterate whether each chunkserver is recovering
    // recovering: chunkserver offline but has recover task on it //NOLINT
    for (const ChunkServerInfo &info : infos) {
        bool recover = IsChunkServerRecover(info);
        if (recover) {
            (*statusMap)[info.info.id] = true;
        } else {
            (*statusMap)[info.info.id] = false;
        }
    }

    return kScheduleErrCodeSuccess;
}

void Coordinator::RunScheduler(
    const std::shared_ptr<Scheduler> &s, SchedulerType type) {
    while (sleeper_.wait_for(std::chrono::seconds(s->GetRunningInterval()))) {
        if (ScheduleNeedRun(type)) {
            s->Schedule();
        }
    }
    LOG(INFO) << ScheduleName(type) << " exit.";
}

bool Coordinator::BuildCopySetConf(
    const CopySetConf &res, ::curve::mds::heartbeat::CopySetConf *out) {
    // build the copysetConf need to be returned in heartbeat
    out->set_logicalpoolid(res.id.first);
    out->set_copysetid(res.id.second);
    out->set_epoch(res.epoch);
    out->set_type(res.type);

    // set candidate
    ChunkServerInfo chunkServer;
    if (!topo_->GetChunkServerInfo(res.configChangeItem, &chunkServer)) {
        LOG(ERROR) << "coordinator can not get chunkServer "
                << res.configChangeItem << " from topology";
        return false;
    }

    auto replica = new ::curve::common::Peer();
    replica->set_id(res.configChangeItem);
    replica->set_address(::curve::mds::topology::BuildPeerId(
        chunkServer.info.ip, chunkServer.info.port, 0));
    out->set_allocated_configchangeitem(replica);

    // set old
    if (res.oldOne != ::curve::mds::topology::UNINTIALIZE_ID) {
        if (!topo_->GetChunkServerInfo(res.oldOne, &chunkServer)) {
            LOG(ERROR) << "coordinator can not get chunkServer "
                    << res.oldOne << " from topology";
            return false;
        }

        auto replica = new ::curve::common::Peer();
        replica->set_id(res.oldOne);
        replica->set_address(::curve::mds::topology::BuildPeerId(
            chunkServer.info.ip, chunkServer.info.port, 0));
        out->set_allocated_oldpeer(replica);
    }

    // set peers
    for (auto peer : res.peers) {
        auto replica = out->add_peers();
        replica->set_id(peer.id);
        replica->set_address(::curve::mds::topology::BuildPeerId(
            peer.ip, peer.port, 0));
    }

    return true;
}

bool Coordinator::ChunkserverGoingToAdd(
    ChunkServerIdType csId, CopySetKey key) {
    Operator op;
    // no operator on copyset
    if (!opController_->GetOperatorById(key, &op)) {
        return false;
    }

    // the operator type is 'add' and new chunkserve = csId
    AddPeer *res = dynamic_cast<AddPeer *>(op.step.get());
    LOG(INFO) << "find operator " << op.OpToString();
    if (res != nullptr && csId == res->GetTargetPeer()) {
        LOG(INFO) << "chunkserver " << csId
                  << " is target of pending operator " << op.OpToString();
        return true;
    }

    // the operator type is 'change' and target = csId
    ChangePeer *cres = dynamic_cast<ChangePeer *>(op.step.get());
    LOG(INFO) << "find operator " << op.OpToString();
    if (cres != nullptr && csId == cres->GetTargetPeer()) {
        LOG(INFO) << "chunkserver " << csId
                  << " is target of pending operator " << op.OpToString();
        return true;
    }

    return false;
}

bool Coordinator::ScheduleNeedRun(SchedulerType type) {
    switch (type) {
        case SchedulerType::CopySetSchedulerType:
            return FLAGS_enableCopySetScheduler;

        case SchedulerType::LeaderSchedulerType:
            return FLAGS_enableLeaderScheduler;

        case SchedulerType::RecoverSchedulerType:
            return FLAGS_enableRecoverScheduler;

        case SchedulerType::ReplicaSchedulerType:
            return FLAGS_enableReplicaScheduler;
    }
}

std::string Coordinator::ScheduleName(SchedulerType type) {
    switch (type) {
        case SchedulerType::CopySetSchedulerType:
            return "CopySetScheduler";

        case SchedulerType::LeaderSchedulerType:
            return "LeaderScheduler";

        case SchedulerType::RecoverSchedulerType:
            return "RecoverScheduler";

        case SchedulerType::ReplicaSchedulerType:
            return "ReplicaScheduler";
    }
}

std::shared_ptr<OperatorController> Coordinator::GetOpController() {
    return opController_;
}

bool Coordinator::IsChunkServerRecover(const ChunkServerInfo &info) {
    // Non-offline state, it will not be recovered
    if (!info.IsOffline()) {
        return false;
    }

    // if the chunkserver is offline, check if there's any corresponding high
    // priority changePeer task
    std::vector<Operator> opList = opController_->GetOperators();
    for (Operator &op : opList) {
        if (op.priority != OperatorPriority::HighPriority) {
            continue;
        }

        auto instance = dynamic_cast<ChangePeer *>(op.step.get());
        if (instance == nullptr) {
            continue;
        }

        if (instance->GetOldPeer() == info.info.id) {
            return true;
        }
    }

    // check if there's any migrating copyset on the chunkserver
    std::vector<CopySetInfo> copysetInfos =
        topo_->GetCopySetInfosInChunkServer(info.info.id);
    for (CopySetInfo &csInfo : copysetInfos) {
        if (csInfo.configChangeInfo.type() == ConfigChangeType::CHANGE_PEER) {
            return true;
        }
    }

    return false;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
