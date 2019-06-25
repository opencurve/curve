/*
 * Project: curve
 * Created Date: Mon Nov 19 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include "src/mds/common/mds_define.h"
#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/operatorFactory.h"

using ::curve::mds::topology::UNINTIALIZE_ID;

namespace curve {
namespace mds {
namespace schedule {
int RecoverScheduler::Schedule() {
    LOG(INFO) << "recoverScheduler begin.";
    int oneRoundGenOp = 0;

    // 如果一个server上超过一定数量的chunkserver挂掉，将这些chunkserver统计
    // 到excludes中
    std::set<ChunkServerIdType> excludes;
    CalculateExcludesChunkServer(&excludes);

    for (auto copysetInfo : topo_->GetCopySetInfos()) {
        // 跳过正在做配置变更的copyset
        Operator op;
        if (opController_->GetOperatorById(copysetInfo.id, &op)) {
            continue;
        }

        if (copysetInfo.HasCandidate()) {
            LOG(WARNING) << "copySet(logicalPoolId:" << copysetInfo.id.first
                         << ", copySetId:" << copysetInfo.id.second
                         << ") already has candidate: "
                         << copysetInfo.candidatePeerInfo.id;
            continue;
        }

        std::set<ChunkServerIdType> offlinelists;
        // 检查offline的副本
        for (auto peer : copysetInfo.peers) {
            ChunkServerInfo csInfo;
            if (!topo_->GetChunkServerInfo(peer.id, &csInfo)) {
                LOG(WARNING) << "recover scheduler: can not get " << peer.id
                             << " from topology" << std::endl;
                continue;
            }

            if (!csInfo.IsOffline()) {
                continue;
            } else {
                offlinelists.emplace(peer.id);
                LOG(ERROR) << "recoverSchdeuler find chunkServer "
                           << peer.id << " offline, please check";
            }
        }

        // 如果所有的copyset副本都是online状态，不做处理
        if (offlinelists.size() == 0) {
            continue;
        }

        // 如果超过半数的副本挂掉，报警
        int deadBound =
            copysetInfo.peers.size() - (copysetInfo.peers.size()/2 + 1);
        if (offlinelists.size() > deadBound) {
            LOG(ERROR) << "recoverSchdeuler find copyset(logicalPoolId:"
                       << copysetInfo.id.first
                       << ", copySetId:" << copysetInfo.id.second
                       << ") has " << offlinelists.size()
                       << " replica offline, cannot repair, please check";
            continue;
        }

        // excludes中的offline副本不做恢复
        for (auto offline : offlinelists) {
            if (excludes.count(offline) > 0) {
                LOG(ERROR) << "can not recover offline chunkserver " << offline
                          << ", because it's server has more than "
                          << chunkserverFailureTolerance_
                          << " offline chunkservers";
                offlinelists.erase(offline);
            }
        }

        if (offlinelists.size() <= 0) {
            continue;
        }

        // 修复其中一个挂掉的副本
        Operator fixRes;
        ChunkServerIDType target;
        // 修复副本失败
        if (!FixOfflinePeer(
                copysetInfo, *offlinelists.begin(), &fixRes, &target)) {
            LOG(ERROR) << "recoverScheduler can not find a healthy"
                          " chunkServer to fix offline one "
                       << *offlinelists.begin() << " in copyset("
                       << copysetInfo.id.first << "," << copysetInfo.id.second
                       << ")";
            continue;
        // 修复副本成功，但加入到controller失败
        } else if (!opController_->AddOperator(fixRes)) {
            LOG(ERROR) << "recover scheduler add operator "
                       << fixRes.OpToString() << " fail";
            continue;
        // 修复副本成功，加入controller成功
        } else {
            LOG(INFO) << "recoverScheduler generate operator:"
                        << fixRes.OpToString() << "for copySet("
                        << copysetInfo.id.first << ", copySetId:"
                        << copysetInfo.id.second << ")";
            // target为初始值，说明直接移除了offline的副本
            if (target == UNINTIALIZE_ID) {
                oneRoundGenOp++;
                continue;
            }

            // target不为初始值
            // 添加operator成功之后，应该在target上创建copyset,
            // 如果创建失败，删除该operator
            if (!topo_->CreateCopySetAtChunkServer(copysetInfo.id, target)) {
                LOG(ERROR) << "coordinator create copySet(logicalPoolId: "
                            << copysetInfo.id.first << ",copySetId: "
                            << copysetInfo.id.second << ") on chunkServer: "
                            << target << " error, delete operator"
                            << fixRes.OpToString();
                opController_->RemoveOperator(copysetInfo.id);
                continue;
            }
            oneRoundGenOp++;
        }
    }
    LOG(INFO) << "recoverScheduler generate " << oneRoundGenOp
              << " operators at this round";
    return oneRoundGenOp;
}

int64_t RecoverScheduler::GetRunningInterval() {
    return runInterval_;
}

bool RecoverScheduler::FixOfflinePeer(
    const CopySetInfo &info, ChunkServerIdType peerId,
    Operator *op, ChunkServerIDType *target) {
    assert(op != nullptr);
    // check the number of replicas first
    auto standardReplicaNum =
        topo_->GetStandardReplicaNumInLogicalPool(info.id.first);
    if (standardReplicaNum <= 0) {
        LOG(WARNING) << "RecoverScheduler find logical pool "
                     << info.id.first << " standard num "
                     << standardReplicaNum << " invalid";
        return false;
    }

    if (info.peers.size() > standardReplicaNum) {
        // remove the dead one
        *op = operatorFactory.CreateRemovePeerOperator(
            info, peerId, OperatorPriority::HighPriority);
        op->timeLimit = std::chrono::seconds(removeTimeSec_);
        *target = UNINTIALIZE_ID;
        return true;
    }

    // select peers to add
    auto csId = SelectBestPlacementChunkServer(info, peerId);
    if (csId == UNINTIALIZE_ID) {
        LOG(ERROR) << "recoverScheduler can not select chunkServer to "
                      "repair copySet(logicalPoolId: "
                   << info.id.first << ",copySetId: " << info.id.second
                   << "), witch replica: " << peerId << " is offline";
        return false;
    } else {
        *op = operatorFactory.CreateAddPeerOperator(
            info, csId, OperatorPriority::HighPriority);
        op->timeLimit = std::chrono::seconds(addTimeSec_);
        *target = csId;
        return true;
    }
}

void RecoverScheduler::CalculateExcludesChunkServer(
    std::set<ChunkServerIdType> *excludes) {
    // 统计每个server上offline chunkserver list
    std::map<ServerIdType, std::vector<ChunkServerIdType>> offlineCS;
    for (auto cs : topo_->GetChunkServerInfos()) {
        if (!cs.IsOffline()) {
            continue;
        }

        if (offlineCS.count(cs.info.serverId) <= 0) {
            offlineCS[cs.info.serverId] =
                std::vector<ChunkServerIdType>{cs.info.id};
        } else {
            offlineCS[cs.info.serverId].emplace_back(cs.info.id);
        }
    }

    for (auto item : offlineCS) {
        if (item.second.size() >= chunkserverFailureTolerance_) {
            LOG(ERROR) << "server " << item.first << " has "
                       << item.second.size() << " offline chunkservers";
            for (auto cs : item.second) {
                excludes->emplace(cs);
            }
        }
    }
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve

