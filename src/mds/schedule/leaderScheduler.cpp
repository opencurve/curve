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
 * Created Date: Fri Dec 21 2018
 * Author: lixiaocui
 */

#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <glog/logging.h>
#include <algorithm>
#include <random>
#include <limits>
#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/operatorFactory.h"

namespace curve {
namespace mds {
namespace schedule {
int LeaderScheduler::Schedule() {
    LOG(INFO) << "leaderScheduler begin.";

    for (auto lid : topo_->GetLogicalpools()) {
        DoLeaderSchedule(lid);
    }
    return 1;
}

int LeaderScheduler::DoLeaderSchedule(PoolIdType lid) {
    int oneRoundGenOp = 0;

    // 找出leader数目最多和最少的chunkServer
    int maxLeaderCount = -1;
    int maxId = -1;
    int minLeaderCount = -1;
    int minId = -1;
    std::vector<ChunkServerInfo> csInfos
        = topo_->GetChunkServersInLogicalPool(lid);
    static std::random_device rd;
    static std::mt19937 g(rd());
    std::shuffle(csInfos.begin(), csInfos.end(), g);

    for (auto csInfo : csInfos) {
        if (csInfo.IsOffline()) {
            continue;
        }

        if (maxLeaderCount == -1 || csInfo.leaderCount > maxLeaderCount) {
            maxId = csInfo.info.id;
            maxLeaderCount = csInfo.leaderCount;
        }

        if (minLeaderCount == -1 || csInfo.leaderCount < minLeaderCount) {
            // 因为只有minLeaderCount的才会作为目标节点，这里只需要判断目标节点是否刚启动
            if (!coolingTimeExpired(csInfo.startUpTime)) {
                continue;
            }
            minId = csInfo.info.id;
            minLeaderCount = csInfo.leaderCount;
        }
    }

    LOG(INFO) << "leaderScheduler select two chunkserver, (id:" << maxId
              << ", maxLeaderCount:" << maxLeaderCount << "), (id:" << minId
              << ", minleaderCount:" << minLeaderCount << ")";

    // leader (最大值-最小值 <= 1) 的时候无需进行transfer
    if (maxLeaderCount >= 0 &&
        minLeaderCount >= 0 &&
        maxLeaderCount - minLeaderCount <= 1) {
        LOG(INFO) << "leaderScheduler no need to generate transferLeader op";
        return oneRoundGenOp;
    }

    // leader数目较多的chunkserver, 随机一个选择leader copyset
    // 将它的leader transfer到其他副本上
    if (maxId > 0) {
        Operator transferLeaderOutOp;
        CopySetInfo selectedCopySet;
        if (transferLeaderOut(maxId, maxLeaderCount, lid, &transferLeaderOutOp,
            &selectedCopySet)) {
            if (opController_->AddOperator(transferLeaderOutOp)) {
                oneRoundGenOp += 1;
                LOG(INFO) << "leaderScheduler generatre operator "
                          << transferLeaderOutOp.OpToString()
                          << " for " << selectedCopySet.CopySetInfoStr()
                          << " from transfer leader out";
                return oneRoundGenOp;
            }
        }
    }

    // leader数目较少的chunkserver，选择follower copyset
    // 将它的leader tansfer到该chunkserver上
    if (minId > 0) {
        Operator transferLeaderInOp;
        CopySetInfo selectedCopySet;
        if (transferLeaderIn(minId, minLeaderCount, lid, &transferLeaderInOp,
            &selectedCopySet)) {
            if (opController_->AddOperator(transferLeaderInOp)) {
                oneRoundGenOp += 1;
                LOG(INFO) << "leaderScheduler generatre operator "
                          << transferLeaderInOp.OpToString()
                          << " for " << selectedCopySet.CopySetInfoStr()
                          << " from transfer leader in";
                return oneRoundGenOp;
            }
        }
    }

    return oneRoundGenOp;
}

bool LeaderScheduler::transferLeaderOut(ChunkServerIdType source, int count,
    PoolIdType lid, Operator *op, CopySetInfo *selectedCopySet) {
    // 找出该chunkserver上所有的leaderCopyset作为备选
    std::vector<CopySetInfo> candidateInfos;
    for (auto &cInfo : topo_->GetCopySetInfosInLogicalPool(lid)) {
        // 跳过follower copyset
        if (cInfo.leader != source) {
           continue;
        }

        // 跳过正在变更的copyset
        if (cInfo.HasCandidate()) {
            LOG(INFO) << cInfo.CopySetInfoStr() << " is on config change";
            continue;
        }

        candidateInfos.emplace_back(cInfo);
    }

    if (candidateInfos.size() <= 0) {
        return false;
    }

    int retryTimes = 1;
    while (retryTimes < maxRetryTransferLeader) {
        // 任意选择一个copyset
        srand((unsigned)time(NULL));
        *selectedCopySet = candidateInfos[rand()%candidateInfos.size()];

        // 从follower中选择leader数目最少
        ChunkServerIdType targetId = UNINTIALIZE_ID;
        uint32_t targetLeaderCount = std::numeric_limits<uint32_t>::max();
        uint64_t targetStartUpTime = 0;
        for (auto peerInfo : selectedCopySet->peers) {
            ChunkServerInfo csInfo;
            if (!topo_->GetChunkServerInfo(peerInfo.id, &csInfo)) {
                LOG(ERROR) << "leaderScheduler cannot get info of chunkServer: "
                           << peerInfo.id;
                return false;
            }

            // 跳过有副本不在线的copyset
            if (csInfo.IsOffline()) {
                break;
            }

            if (source == peerInfo.id) {
                continue;
            }

            if (csInfo.leaderCount < targetLeaderCount) {
                targetId = csInfo.info.id;
                targetLeaderCount = csInfo.leaderCount;
                targetStartUpTime = csInfo.startUpTime;
            }
        }

        if (targetId == UNINTIALIZE_ID ||
            count - 1 < targetLeaderCount + 1 ||
            !coolingTimeExpired(targetStartUpTime)) {
            retryTimes++;
            continue;
        } else {
            *op = operatorFactory.CreateTransferLeaderOperator(
            *selectedCopySet, targetId, OperatorPriority::NormalPriority);
            op->timeLimit =
                std::chrono::seconds(transTimeSec_);
            return true;
        }
    }

    return false;
}

bool LeaderScheduler::transferLeaderIn(ChunkServerIdType target, int count,
    PoolIdType lid, Operator *op, CopySetInfo *selectedCopySet) {
    // 从target中选择follower copyset, 把它的leader迁移到target上
    std::vector<CopySetInfo> candidateInfos;
    for (auto &cInfo : topo_->GetCopySetInfosInLogicalPool(lid)) {
        // 跳过leader copyset
        if (cInfo.leader == target || !cInfo.ContainPeer(target)) {
            continue;
        }

        // 跳过正在配置变更的copyset
        if (cInfo.HasCandidate()) {
            LOG(INFO) << cInfo.CopySetInfoStr() << " is on config change";
            continue;
        }

        // 有副本不在线也要跳过
        if (copySetHealthy(cInfo)) {
            candidateInfos.emplace_back(cInfo);
        }
    }

    if (candidateInfos.size() <= 0) {
        return false;
    }

    srand((unsigned)time(NULL));
    int retryTimes = 1;
    while (retryTimes < maxRetryTransferLeader) {
        // 从candidate中随机选取一个copyset
        *selectedCopySet = candidateInfos[rand()%candidateInfos.size()];

        // 获取selectedCopySet的leader上的leaderCount 和 target上的leaderCount
        ChunkServerInfo sourceInfo;
        if (!topo_->GetChunkServerInfo(selectedCopySet->leader, &sourceInfo)) {
            LOG(ERROR) << "leaderScheduler cannot get info of chukServer:"
                       << selectedCopySet->leader;
            retryTimes++;
            continue;
        }

        if (sourceInfo.leaderCount - 1 < count + 1) {
            retryTimes++;
            continue;
        }

        // 把leader tansfer到target上
        *op = operatorFactory.CreateTransferLeaderOperator(
            *selectedCopySet, target, OperatorPriority::NormalPriority);
        op->timeLimit = std::chrono::seconds(transTimeSec_);
        return true;
    }

    return false;
}

bool LeaderScheduler::copySetHealthy(const CopySetInfo &cInfo) {
    bool healthy = true;
    for (auto peer : cInfo.peers) {
        ChunkServerInfo csInfo;
        if (!topo_->GetChunkServerInfo(peer.id, &csInfo)) {
            LOG(ERROR) << "leaderScheduler cannot get info of chukServer:"
                        << peer.id;
            healthy = false;
            break;
        }

        if (csInfo.IsOffline()) {
            healthy = false;
            break;
        }
    }
    return healthy;
}

bool LeaderScheduler::coolingTimeExpired(uint64_t startUpTime) {
    if (startUpTime == 0) {
        return false;
    }

    struct timeval tm;
    gettimeofday(&tm, NULL);
    return tm.tv_sec - startUpTime > chunkserverCoolingTimeSec_;
}

int64_t LeaderScheduler::GetRunningInterval() {
    return runInterval_;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
