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
 * @Project: curve
 * @Date: 2021-11-8 11:01:48
 * @Author: chenwei
 */

#include "curvefs/src/mds/schedule/coordinator.h"
#include <glog/logging.h>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <thread>  //NOLINT
#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/src/mds/schedule/operatorFactory.h"
#include "curvefs/src/mds/topology/topology_item.h"

namespace curvefs {
namespace mds {
namespace schedule {
/**
 * use curl -L mdsIp:port/flags/enableRecoverScheduler?setvalue=true
 * for dynamic parameter configuration
 */
static bool pass_bool(const char *, bool) { return true; }
DEFINE_bool(enableRecoverScheduler, true, "switch of recover scheduler");
DEFINE_validator(enableRecoverScheduler, &pass_bool);
DEFINE_bool(enableCopySetScheduler, true, "switch of copyset scheduler");
DEFINE_validator(enableCopySetScheduler, &pass_bool);


Coordinator::Coordinator(const std::shared_ptr<TopoAdapter> &topo) {
    this->topo_ = topo;
}

Coordinator::~Coordinator() { Stop(); }

void Coordinator::InitScheduler(const ScheduleOption &conf,
                                std::shared_ptr<ScheduleMetrics> metrics) {
    conf_ = conf;

    opController_ =
        std::make_shared<OperatorController>(conf.operatorConcurrent, metrics);

    if (conf.enableRecoverScheduler) {
        schedulerController_[SchedulerType::RecoverSchedulerType] =
            std::make_shared<RecoverScheduler>(conf, topo_, opController_);
        LOG(INFO) << "init recover scheduler ok!";
    }

    if (conf.enableCopysetScheduler) {
        schedulerController_[SchedulerType::CopysetSchedulerType] =
            std::make_shared<CopySetScheduler>(conf, topo_, opController_);
        LOG(INFO) << "init copyset scheduler ok!";
    }
}

void Coordinator::Run() {
    for (auto &v : schedulerController_) {
        runSchedulerThreads_[v.first] = curve::common::Thread(
            &Coordinator::RunScheduler, this, std::cref(v.second), v.first);
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

MetaServerIdType Coordinator::CopySetHeartbeat(
    const ::curvefs::mds::topology::CopySetInfo &originInfo,
    const ::curvefs::mds::heartbeat::ConfigChangeInfo &configChInfo,
    ::curvefs::mds::heartbeat::CopySetConf *out) {
    // transfer copyset info format from topology to scheduler
    CopySetInfo info;
    if (!topo_->CopySetFromTopoToSchedule(originInfo, &info)) {
        LOG(ERROR) << "coordinator cannot convert copyset("
                   << originInfo.GetPoolId() << "," << originInfo.GetId()
                   << ") from heartbeat topo form to schedule form error";
        return ::curvefs::mds::topology::UNINITIALIZE_ID;
    }
    info.configChangeInfo = configChInfo;

    // check if there's any operator on specified copyset
    Operator op;
    if (!opController_->GetOperatorById(info.id, &op)) {
        return ::curvefs::mds::topology::UNINITIALIZE_ID;
    }
    LOG(INFO) << "find operator on" << info.CopySetInfoStr()
              << "), operator: " << op.OpToString();

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
            LOG(WARNING) << "Operator " << op.OpToString() << "on "
                         << info.CopySetInfoStr()
                         << " is stale, remove operator";
            opController_->RemoveOperator(info.id);
            return ::curvefs::mds::topology::UNINITIALIZE_ID;
        }

        // the operator should not be dispacthed if the candidate
        // of addPeer or transferLeader or changePeer is offline
        MetaServerInfo metaServer;
        if (!topo_->GetMetaServerInfo(res.configChangeItem, &metaServer)) {
            LOG(ERROR) << "coordinator can not get metaServer "
                       << res.configChangeItem << " from topology";
            opController_->RemoveOperator(info.id);
            return ::curvefs::mds::topology::UNINITIALIZE_ID;
        }
        bool needCheckType = (res.type == ConfigChangeType::ADD_PEER ||
                              res.type == ConfigChangeType::TRANSFER_LEADER ||
                              res.type == ConfigChangeType::CHANGE_PEER);
        if (needCheckType && metaServer.IsOffline()) {
            LOG(WARNING) << "candidate metaserver " << metaServer.info.id
                         << " is offline, abort config change";
            opController_->RemoveOperator(info.id);
            return ::curvefs::mds::topology::UNINITIALIZE_ID;
        }

        // build the copysetConf need to be returned in heartbeat
        // if build failed, remove the operator
        if (!BuildCopySetConf(res, out)) {
            LOG(ERROR) << "build copyset conf for " << info.CopySetInfoStr()
                       << ") fail, remove operator";
            opController_->RemoveOperator(info.id);
            return ::curvefs::mds::topology::UNINITIALIZE_ID;
        }

        LOG(INFO) << "order operator " << op.OpToString() << " on "
                  << info.CopySetInfoStr() << " success";
        return res.configChangeItem;
    }

    return ::curvefs::mds::topology::UNINITIALIZE_ID;
}

ScheduleStatusCode Coordinator::QueryMetaServerRecoverStatus(
    const std::vector<MetaServerIdType> &idList,
    std::map<MetaServerIdType, bool> *statusMap) {
    std::vector<MetaServerInfo> infos;

    // if idList is empty, get all metaserver info
    if (idList.empty()) {
        infos = topo_->GetMetaServerInfos();
    } else {
        for (auto id : idList) {
            MetaServerInfo info;
            bool getOk = topo_->GetMetaServerInfo(id, &info);
            if (!getOk) {
                LOG(ERROR) << "invalid metaserver id: " << id;
                return ScheduleStatusCode::InvalidQueryMetaserverID;
            }
            infos.emplace_back(std::move(info));
        }
    }

    // Iterate to check whether each metaserver is recovering
    // recovering: metaserver offline but has recover task on it
    for (const MetaServerInfo &info : infos) {
        (*statusMap)[info.info.id] = IsMetaServerRecover(info);
    }

    return ScheduleStatusCode::Success;
}

void Coordinator::RunScheduler(const std::shared_ptr<Scheduler> &s,
                               SchedulerType type) {
    while (sleeper_.wait_for(std::chrono::seconds(s->GetRunningInterval()))) {
        if (ScheduleNeedRun(type)) {
            s->Schedule();
        }
    }
    LOG(INFO) << ScheduleName(type) << " exit.";
}

bool Coordinator::BuildCopySetConf(
    const CopySetConf &res, ::curvefs::mds::heartbeat::CopySetConf *out) {
    // build the copysetConf need to be returned in heartbeat
    out->set_poolid(res.id.first);
    out->set_copysetid(res.id.second);
    out->set_epoch(res.epoch);
    out->set_type(res.type);

    // set candidate
    MetaServerInfo metaServer;
    if (!topo_->GetMetaServerInfo(res.configChangeItem, &metaServer)) {
        LOG(ERROR) << "coordinator can not get metaServer "
                   << res.configChangeItem << " from topology";
        return false;
    }

    auto replica = new curvefs::common::Peer();
    replica->set_id(res.configChangeItem);
    replica->set_address(::curvefs::mds::topology::BuildPeerIdWithIpPort(
        metaServer.info.ip, metaServer.info.port, 0));
    out->set_allocated_configchangeitem(replica);

    // set old
    if (res.oldOne != ::curvefs::mds::topology::UNINITIALIZE_ID) {
        if (!topo_->GetMetaServerInfo(res.oldOne, &metaServer)) {
            LOG(ERROR) << "coordinator can not get metaServer " << res.oldOne
                       << " from topology";
            return false;
        }

        auto replica = new curvefs::common::Peer();
        replica->set_id(res.oldOne);
        replica->set_address(::curvefs::mds::topology::BuildPeerIdWithIpPort(
            metaServer.info.ip, metaServer.info.port, 0));
        out->set_allocated_oldpeer(replica);
    }

    // set peers
    for (auto &peer : res.peers) {
        auto replica = out->add_peers();
        replica->set_id(peer.id);
        replica->set_address(::curvefs::mds::topology::BuildPeerIdWithIpPort(
            peer.ip, peer.port, 0));
    }

    return true;
}

bool Coordinator::MetaserverGoingToAdd(MetaServerIdType msId, CopySetKey key) {
    Operator op;
    // no operator on copyset
    if (!opController_->GetOperatorById(key, &op)) {
        return false;
    }

    // the operator type is 'add' and new metaserver = msId
    AddPeer *res = dynamic_cast<AddPeer *>(op.step.get());
    LOG(INFO) << "find operator " << op.OpToString();
    if (res != nullptr && msId == res->GetTargetPeer()) {
        LOG(INFO) << "metaserver " << msId << " is target of pending operator "
                  << op.OpToString();
        return true;
    }

    // the operator type is 'change' and target = msId
    ChangePeer *cres = dynamic_cast<ChangePeer *>(op.step.get());
    LOG(INFO) << "find operator " << op.OpToString();
    if (cres != nullptr && msId == cres->GetTargetPeer()) {
        LOG(INFO) << "metaserver " << msId << " is target of pending operator "
                  << op.OpToString();
        return true;
    }

    return false;
}

bool Coordinator::ScheduleNeedRun(SchedulerType type) {
    switch (type) {
        case SchedulerType::RecoverSchedulerType:
            return FLAGS_enableRecoverScheduler;
        case SchedulerType::CopysetSchedulerType:
            return FLAGS_enableCopySetScheduler;
        default:
            return false;
    }
}

std::string Coordinator::ScheduleName(SchedulerType type) {
    switch (type) {
        case SchedulerType::RecoverSchedulerType:
            return "RecoverScheduler";
        case SchedulerType::CopysetSchedulerType:
            return "CopySetScheduler";
        default:
            return "Unknown";
    }
}

std::shared_ptr<OperatorController> Coordinator::GetOpController() {
    return opController_;
}

bool Coordinator::IsMetaServerRecover(const MetaServerInfo &info) {
    // Non-offline state, it will not be recovered
    if (!info.IsOffline()) {
        return false;
    }

    // if the metaserver is offline, check if there's any corresponding high
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

    // check if there's any migrating copyset on the metaserver
    std::vector<CopySetInfo> copysetInfos =
        topo_->GetCopySetInfosInMetaServer(info.info.id);
    for (CopySetInfo &csInfo : copysetInfos) {
        if (csInfo.configChangeInfo.type() == ConfigChangeType::CHANGE_PEER) {
            return true;
        }
    }

    return false;
}

}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
