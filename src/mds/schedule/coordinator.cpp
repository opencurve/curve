/*
 * Project: curve
 * Created Date: Mon Nov 17 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
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
 * 使用curl -L mdsIp:port/flags/enableCopySetScheduler?setvalue=true
 * 可以实现动态修改参数
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
    schedulerRunning_ = false;
}

Coordinator::~Coordinator() {
    Stop();
}

void Coordinator::InitScheduler(
    const ScheduleOption &conf, std::shared_ptr<ScheduleMetrics> metrics) {
    opController_ =
        std::make_shared<OperatorController>(conf.operatorConcurrent, metrics);

    if (conf.enableLeaderScheduler) {
        schedulerController_[SchedulerType::LeaderSchedulerType] =
            std::make_shared<LeaderScheduler>(opController_,
                                              conf.leaderSchedulerIntervalSec,
                                              conf.chunkserverCoolingTimeSec,
                                              conf.transferLeaderTimeLimitSec,
                                              conf.removePeerTimeLimitSec,
                                              conf.addPeerTimeLimitSec,
                                              conf.scatterWithRangePerent,
                                              topo_);
        LOG(INFO) << "init leader scheduler ok!";
    }

    if (conf.enableCopysetScheduler) {
        schedulerController_[SchedulerType::CopySetSchedulerType] =
            std::make_shared<CopySetScheduler>(opController_,
                                               conf.copysetSchedulerIntervalSec,
                                               conf.transferLeaderTimeLimitSec,
                                               conf.removePeerTimeLimitSec,
                                               conf.addPeerTimeLimitSec,
                                               conf.copysetNumRangePercent,
                                               conf.scatterWithRangePerent,
                                               topo_);
        LOG(INFO) << "init copySet scheduler ok!";
    }

    if (conf.enableRecoverScheduler) {
        schedulerController_[SchedulerType::RecoverSchedulerType] =
            std::make_shared<RecoverScheduler>(opController_,
                                               conf.recoverSchedulerIntervalSec,
                                               conf.transferLeaderTimeLimitSec,
                                               conf.removePeerTimeLimitSec,
                                               conf.addPeerTimeLimitSec,
                                               conf.scatterWithRangePerent,
                                               conf.chunkserverFailureTolerance,
                                               topo_);
        LOG(INFO) << "init recover scheduler ok!";
    }

    if (conf.enableReplicaScheduler) {
        schedulerController_[SchedulerType::ReplicaSchedulerType] =
            std::make_shared<ReplicaScheduler>(opController_,
                                               conf.replicaSchedulerIntervalSec,
                                               conf.transferLeaderTimeLimitSec,
                                               conf.removePeerTimeLimitSec,
                                               conf.addPeerTimeLimitSec,
                                               conf.scatterWithRangePerent,
                                               topo_);
        LOG(INFO) << "init replica scheduler ok!";
    }
}

void Coordinator::Run() {
    SetSchedulerRunning(true);
    // run different scheduler at interval in different threads
    for (auto &v : schedulerController_) {
        runSchedulerThreads_[v.first] = common::Thread(
            &Coordinator::RunScheduler, this, v.second, v.first);
    }
}

void Coordinator::Stop() {
    if (schedulerRunning_) {
        SetSchedulerRunning(false);
        for (auto &v : schedulerController_) {
            runSchedulerThreads_[v.first].join();
        }
    }
}

ChunkServerIdType Coordinator::CopySetHeartbeat(
    const ::curve::mds::topology::CopySetInfo &originInfo,
    const ::curve::mds::heartbeat::ConfigChangeInfo &configChInfo,
    ::curve::mds::heartbeat::CopySetConf *out) {
    // 将toplogy中copyset转换成schedule中copyset的形式
    CopySetInfo info;
    if (!topo_->CopySetFromTopoToSchedule(originInfo, &info)) {
        LOG(ERROR) << "coordinator cannot convert copySet(logicalPoolId:"
                   << originInfo.GetLogicalPoolId() << ", copySetId:"
                   << originInfo.GetId()
                   << ") from heartbeat topo form to schedule form error";
        return ::curve::mds::topology::UNINTIALIZE_ID;
    }
    info.configChangeInfo = configChInfo;

    // 查看指定copyset上是否有operator
    Operator op;
    if (!opController_->GetOperatorById(info.id, &op)) {
        return ::curve::mds::topology::UNINTIALIZE_ID;
    }
    LOG(INFO) << "find operator on" << info.CopySetInfoStr() << "), operator: "
              << op.OpToString();

    // 根据leader上报的copyset信息更新operator的状态
    // 如果有新的配置要下发，返回为true
    CopySetConf res;
    bool hasOrder = opController_->ApplyOperator(info, &res);
    if (hasOrder) {
        LOG(INFO) << "going to order operator " << op.OpToString();
        // 判断epoch和startEpoch是否一致，如果不一致，operator不下发
        // 场景： 如果mds已经下发了operator, 并且copyset完成，但心跳还未上报；
        // 此时mds重启，在该copyset生成新的operator，不下发并移除该operator
        if (info.epoch != op.startEpoch) {
            LOG(WARNING) << "Operator " << op.OpToString()
                         << "on " << info.CopySetInfoStr()
                         << " is stale, remove operator";
            opController_->RemoveOperator(info.id);
            return ::curve::mds::topology::UNINTIALIZE_ID;
        }

        // 如果addOperator或者transferLeader的candidate是offline状态，operator不应该下发 //NOLINT
        ChunkServerInfo chunkServer;
        if (!topo_->GetChunkServerInfo(res.configChangeItem, &chunkServer)) {
            LOG(ERROR) << "coordinator can not get chunkServer "
                    << res.configChangeItem << " from topology";
            opController_->RemoveOperator(info.id);
            return ::curve::mds::topology::UNINTIALIZE_ID;
        }
        bool needCheckType = (res.type == ConfigChangeType::ADD_PEER ||
            res.type == ConfigChangeType::TRANSFER_LEADER);
        if (needCheckType && chunkServer.IsOffline()) {
            LOG(WARNING) << "candidate chunkserver " << chunkServer.info.id
                       << " is offline, abort config change";
            opController_->RemoveOperator(info.id);
            return ::curve::mds::topology::UNINTIALIZE_ID;
        }

        // build心跳中需要返回的copysetConf, 如果build失败, 移除operator
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

void Coordinator::RunScheduler(
    const std::shared_ptr<Scheduler> &s, SchedulerType type) {
    while (schedulerRunning_) {
        std::this_thread::
        sleep_for(std::chrono::seconds(s->GetRunningInterval()));

        if (ScheduleNeedRun(type)) {
            s->Schedule();
        }
    }
    LOG(INFO) << "scheduler exit.";
}

bool Coordinator::BuildCopySetConf(
    const CopySetConf &res, ::curve::mds::heartbeat::CopySetConf *out) {
    // build心跳中需要返回的copysetConf
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

    // set 副本
    for (auto peer : res.peers) {
        auto replica = out->add_peers();
        replica->set_id(peer.id);
        replica->set_address(::curve::mds::topology::BuildPeerId(
            peer.ip, peer.port, 0));
    }

    return true;
}

bool Coordinator::ChunkserverGoingToAdd(
    ChunkServerIDType csId, CopySetKey key) {
    Operator op;
    // copyset上没有需要变更的operator,
    if (!opController_->GetOperatorById(key, &op)) {
        return false;
    }

    // 该operator是add类型, 并且add=csId
    AddPeer *res = dynamic_cast<AddPeer *>(op.step.get());
    LOG(INFO) << "find operator " << op.OpToString();
    if (res != nullptr && csId == res->GetTargetPeer()) {
        LOG(INFO) << "chunkserver " << csId
                  << " is target of pending operator " << op.OpToString();
        return true;
    }

    return false;
}


void Coordinator::SetSchedulerRunning(bool flag) {
    std::lock_guard<std::mutex> guard(mutex_);
    schedulerRunning_ = flag;
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

std::shared_ptr<OperatorController> Coordinator::GetOpController() {
    return opController_;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
