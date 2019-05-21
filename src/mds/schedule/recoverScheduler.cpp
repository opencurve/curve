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
int RecoverScheduler::Schedule(const std::shared_ptr<TopoAdapter> &topo) {
    LOG(INFO) << "recoverScheduler begin.";
    int oneRoundGenOp = 0;
    for (auto copysetInfo : topo->GetCopySetInfos()) {
        // 跳过正在做配置变更的copyset
        Operator op;
        if (opController_->GetOperatorById(copysetInfo.id, &op)) {
            continue;
        }

        if (copysetInfo.configChangeInfo.IsInitialized()) {
            LOG(WARNING) << "copySet(logicalPoolId:" << copysetInfo.id.first
                         << ", copySetId:" << copysetInfo.id.second
                         << ") configchangeInfo has been initialized";
            continue;
        }

        std::vector<ChunkServerIdType> offlinelists;
        // 检查offline的副本
        for (auto peer : copysetInfo.peers) {
            ChunkServerInfo csInfo;
            if (!topo->GetChunkServerInfo(peer.id, &csInfo)) {
                LOG(WARNING) << "recover scheduler: can not get " << peer.id
                             << " from topology" << std::endl;
                continue;
            }

            if (!csInfo.IsOffline()) {
                continue;
            } else {
                offlinelists.emplace_back(peer.id);
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

        // 修复其中一个挂掉的副本
        Operator fixRes;
        if (!FixOfflinePeer(topo, copysetInfo, offlinelists[0], &fixRes)) {
            LOG(ERROR) << "recoverScheduler can not find a healthy"
                          " chunkServer to fix offline one "
                       << offlinelists[0];
        } else if (opController_->AddOperator(fixRes)) {
            LOG(INFO) << "recoverScheduler generate operator:"
                        << fixRes.OpToString() << "for copySet("
                        << copysetInfo.id.first << ", copySetId:"
                        << copysetInfo.id.second << ")";
            oneRoundGenOp++;
        } else {
            LOG(ERROR) << "recover scheduler add operator "
                       << fixRes.OpToString() << " fail";
        }
    }
    LOG(INFO) << "recoverScheduler generate " << oneRoundGenOp
              << " operators at this round";
    return oneRoundGenOp;
}

int64_t RecoverScheduler::GetRunningInterval() {
    return runInterval_;
}

bool RecoverScheduler::FixOfflinePeer(const std::shared_ptr<TopoAdapter> &topo,
                                      const CopySetInfo &info,
                                      ChunkServerIdType peerId,
                                      Operator *op) {
    assert(op != nullptr);
    // check the number of replicas first
    auto standardReplicaNum =
        topo->GetStandardReplicaNumInLogicalPool(info.id.first);
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
        op->timeLimit = std::chrono::seconds(
            GetRemovePeerTimeLimitSec());
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
    } else if (!topo->CreateCopySetAtChunkServer(info.id, csId)) {
        LOG(ERROR) << "recoverScheduler create copySet(logicalPoolId: "
                   << info.id.first << ",copySetId: " << info.id.second
                   << ") on chunkServer: " << peerId << " error";
        return false;
    } else {
        *op = operatorFactory.CreateAddPeerOperator(
            info, csId, OperatorPriority::HighPriority);
        op->timeLimit =
            std::chrono::seconds(GetAddPeerTimeLimitSec());
        DVLOG(6) << "recoverScheduler create operator: " << op->OpToString()
                 << " success";
        return true;
    }
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve

