/*
 * Project: curve
 * Created Date: Fri Dec 21 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <stdlib.h>
#include <time.h>
#include <glog/logging.h>
#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/operatorFactory.h"

namespace curve {
namespace mds {
namespace schedule {
int LeaderScheduler::Schedule(const std::shared_ptr<TopoAdapter> &topo) {
    LOG(INFO) << "leaderScheduler begin.";
    int oneRoundGenOp = 0;

    // 找出leader数目最多和最少的chunkServer
    int maxLeaderCount = -1;
    int maxId = -1;
    int minLeaderCount = -1;
    int minId = -1;
    for (auto csInfo : topo->GetChunkServerInfos()) {
        if (csInfo.IsOffline()) {
            LOG(ERROR) << "leaderScheduler find chunkServer:" << csInfo.info.id
                       << " is offline, please check!";
            continue;
        }

        if (maxLeaderCount == -1 || csInfo.leaderCount > maxLeaderCount) {
            maxId = csInfo.info.id;
            maxLeaderCount = csInfo.leaderCount;
        }

        if (minLeaderCount == -1 || csInfo.leaderCount < minLeaderCount) {
            minId = csInfo.info.id;
            minLeaderCount = csInfo.leaderCount;
        }
    }

    LOG(INFO) << "leaderScheduler select two chunkserver, (id:" << maxId
              << ", leaderCount:" << maxLeaderCount << "), (id:" << minId
              << ", leaderCount:" << minLeaderCount << ")";

    // leader (最大值-最小值 <= 1) 的时候无需进行transfer
    if (maxLeaderCount >= 0 &&
        minLeaderCount >= 0 &&
        maxLeaderCount - minLeaderCount <= 1) {
        return oneRoundGenOp;
    }

    // leader数目较多的chunkserver, 随机一个选择leader copyset
    // 将它的leader transfer到其他副本上
    if (maxId > 0) {
        Operator transferLeaderOutOp;
        if (transferLeaderOut(maxId, topo, &transferLeaderOutOp) >= 0) {
            if (opController_->AddOperator(transferLeaderOutOp)) {
                oneRoundGenOp += 1;
                return oneRoundGenOp;
            }
        }
    }

    // leader数目较少的chunkserver，选择follower copyset
    // 将它的leader tansfer到该chunkserver上
    if (minId > 0) {
        Operator transferLeaderInOp;
        if (transferLeaderIn(minId, topo, &transferLeaderInOp) >= 0) {
            if (opController_->AddOperator(transferLeaderInOp)) {
                oneRoundGenOp += 1;
                return oneRoundGenOp;
            }
        }
    }

    return oneRoundGenOp;
}

int LeaderScheduler::transferLeaderOut(
    ChunkServerIdType source,
    const std::shared_ptr<TopoAdapter> &topo,
    Operator *op) {
    // 找出该chunkserver上所有的leaderCopyset作为备选
    std::vector<CopySetInfo> candidateInfos;
    for (auto &cInfo : topo->GetCopySetInfos()) {
        // 跳过follower copyset
        if (cInfo.leader != source) {
           continue;
        }

        // 跳过正在变更的copyset
        if (cInfo.configChangeInfo.IsInitialized()) {
            LOG(WARNING) << "copySet(" << cInfo.id.first
                        << "," << cInfo.id.second
                        << ") configchangeInfo is on config change";
            continue;
        }

        candidateInfos.emplace_back(cInfo);
    }

    if (candidateInfos.size() <= 0) {
        return -1;
    }

    int retryTimes = 1;
    while (retryTimes < maxRetryTransferLeader) {
        // 任意选择一个copyset
        srand((unsigned)time(NULL));
        auto selectedCopySet = candidateInfos[rand()%candidateInfos.size()];

        // 从follower中选择leader数目最少
        int targetId = -1;
        int targetLeaderCount = -1;
        for (auto peerInfo : selectedCopySet.peers) {
            ChunkServerInfo csInfo;
            if (!topo->GetChunkServerInfo(peerInfo.id, &csInfo)) {
                LOG(ERROR) << "leaderScheduler cannot get info of chukServer:"
                        << peerInfo.id;
                return -1;
            }

            // 跳过有副本不在线的copyset
            if (csInfo.IsOffline()) {
                break;
            }

            if (source == peerInfo.id) {
                continue;
            }

            if (targetId <= 0 || csInfo.leaderCount < targetLeaderCount) {
                targetId = csInfo.info.id;
                targetLeaderCount = csInfo.leaderCount;
            }
        }

        if (targetId <= 0) {
            retryTimes++;
            continue;
        } else {
            *op = operatorFactory.CreateTransferLeaderOperator(
            selectedCopySet, targetId, OperatorPriority::NormalPriority);
            op->timeLimit =
                std::chrono::seconds(GetTransferLeaderTimeLimitSec());
            return 0;
        }
    }

    return -1;
}

int LeaderScheduler::transferLeaderIn(
    ChunkServerIdType target,
    const std::shared_ptr<TopoAdapter> &topo,
    Operator *op) {
    // 从target中选择一个follower copyset, 把它的leader迁移到target上
    std::vector<CopySetInfo> candidateInfos;
    for (auto &cInfo : topo->GetCopySetInfos()) {
        // 跳过leader copyset
        if (cInfo.leader == target || !cInfo.ContainPeer(target)) {
            continue;
        }

        // 跳过正在配置变更的copyset
        if (cInfo.configChangeInfo.IsInitialized()) {
            LOG(WARNING) << "copySet(" << cInfo.id.first
                        << "," << cInfo.id.second
                        << ") is on config change";
            continue;
        }

        // 有副本不在线也要跳过
        if (copySetHealthy(cInfo, topo)) {
            candidateInfos.emplace_back(cInfo);
        }
    }

    if (candidateInfos.size() <= 0) {
        return -1;
    }

    srand((unsigned)time(NULL));
    auto selectedCopySet = candidateInfos[rand()%candidateInfos.size()];

    // 把leader tansfer到target上
    *op = operatorFactory.CreateTransferLeaderOperator(
        selectedCopySet, target, OperatorPriority::NormalPriority);
    op->timeLimit = std::chrono::seconds(GetTransferLeaderTimeLimitSec());
    return 0;
}

bool LeaderScheduler::copySetHealthy(
      const CopySetInfo &cInfo, const std::shared_ptr<TopoAdapter> &topo) {
    bool healthy = true;
    for (auto peer : cInfo.peers) {
        ChunkServerInfo csInfo;
        if (!topo->GetChunkServerInfo(peer.id, &csInfo)) {
            LOG(ERROR) << "leaderScheduler cannot get info of chukServer:"
                        << peer.id;
            healthy = false;
            break;
        }

        if (csInfo.IsOffline()) {
            LOG(ERROR) << "leaderScheduler find chunkServer:"
                        << csInfo.info.id
                        << " is offline, please check!";
            healthy = false;
            break;
        }
    }
    return healthy;
}

int64_t LeaderScheduler::GetRunningInterval() {
    return runInterval_;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
