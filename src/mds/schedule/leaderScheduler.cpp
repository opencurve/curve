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
    LOG(INFO) << "schedule: leaderScheduler begin.";
    int oneRoundGenOp = 0;
    for (auto lid : topo_->GetLogicalpools()) {
        oneRoundGenOp += DoLeaderSchedule(lid);
    }

    LOG(INFO) << "schedule: leaderScheduler end, generate operator num "
              << oneRoundGenOp;
    return oneRoundGenOp;
}

int LeaderScheduler::DoLeaderSchedule(PoolIdType lid) {
    int oneRoundGenOp = 0;

    // find chunkserver with maximum and minimum number of leaders
    int maxLeaderCount = -1;
    int maxId = -1;
    int minLeaderCount = -1;
    int minId = -1;
    std::vector<ChunkServerInfo> csInfos =
        topo_->GetChunkServersInLogicalPool(lid);
    static std::random_device rd;
    static std::mt19937 g(rd());
    std::shuffle(csInfos.begin(), csInfos.end(), g);

    for (auto csInfo : csInfos) {
        // skip offline chunkserver or pendding chunkserver
        if (csInfo.IsOffline() || csInfo.IsPendding()) {
            continue;
        }

        if (maxLeaderCount == -1 ||
            static_cast<int>(csInfo.leaderCount) > maxLeaderCount) {
            maxId = csInfo.info.id;
            maxLeaderCount = csInfo.leaderCount;
        }

        if (minLeaderCount == -1 ||
            static_cast<int>(csInfo.leaderCount) < minLeaderCount) {
            // the chunkserver with minLeaderCount and not in coolingTime
            // can be the transfer target
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

    // leader scheduling is not required when
    // (maxLeaderCount-minLeaderCount <= 1)
    if (maxLeaderCount >= 0 && minLeaderCount >= 0 &&
        maxLeaderCount - minLeaderCount <= 1) {
        LOG(INFO) << "leaderScheduler no need to generate transferLeader op";
        return oneRoundGenOp;
    }

    // for chunkserver that has the most leaders, pick a leader copyset (the
    // copyset with the chunkserver as its leader) randomly and transfer the
    // leader replica (the leader role) to the replica (in the same copyset)
    // that has the least leader number
    if (maxId > 0) {
        Operator transferLeaderOutOp;
        CopySetInfo selectedCopySet;
        if (transferLeaderOut(maxId, maxLeaderCount, lid, &transferLeaderOutOp,
                              &selectedCopySet)) {
            if (opController_->AddOperator(transferLeaderOutOp)) {
                oneRoundGenOp += 1;
                LOG(INFO) << "leaderScheduler generatre operator "
                          << transferLeaderOutOp.OpToString() << " for "
                          << selectedCopySet.CopySetInfoStr()
                          << " from transfer leader out";
                return oneRoundGenOp;
            }
        }
    }

    // for the chunkserver that has the least leaders, choose a follower copyset
    // (the copyset that has a follower replica on the chunkserver) randomly and
    // transfer the leader (role) to this chunkserver
    if (minId > 0) {
        Operator transferLeaderInOp;
        CopySetInfo selectedCopySet;
        if (transferLeaderIn(minId, minLeaderCount, lid, &transferLeaderInOp,
                             &selectedCopySet)) {
            if (opController_->AddOperator(transferLeaderInOp)) {
                oneRoundGenOp += 1;
                LOG(INFO) << "leaderScheduler generatre operator "
                          << transferLeaderInOp.OpToString() << " for "
                          << selectedCopySet.CopySetInfoStr()
                          << " from transfer leader in";
                return oneRoundGenOp;
            }
        }
    }

    return oneRoundGenOp;
}

bool LeaderScheduler::transferLeaderOut(ChunkServerIdType source, int count,
                                        PoolIdType lid, Operator *op,
                                        CopySetInfo *selectedCopySet) {
    // find all copyset with source chunkserver as its leader as the candidate
    std::vector<CopySetInfo> candidateInfos;
    for (auto &cInfo : topo_->GetCopySetInfosInLogicalPool(lid)) {
        // skip those copysets that the source is the follower in it
        if (cInfo.leader != source) {
            continue;
        }

        // skip the copyset under configuration changing
        if (cInfo.HasCandidate()) {
            LOG(INFO) << cInfo.CopySetInfoStr() << " is on config change";
            continue;
        }

        candidateInfos.emplace_back(cInfo);
    }

    if (candidateInfos.size() == 0) {
        return false;
    }

    int retryTimes = 1;
    while (retryTimes < maxRetryTransferLeader) {
        // select a copyset from candidates randomly
        srand((unsigned)time(NULL));
        *selectedCopySet = candidateInfos[rand() % candidateInfos.size()];
        // choose the chunkserver with least leaders from follower
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

            // if any of the chunkserver is offline in the selected copyset,
            // stop this round and retry until reach the retry time.
            if (csInfo.IsOffline()) {
                break;
            }

            // can not transfer to pendding chunkserver
            if (csInfo.IsPendding()) {
                continue;
            }

            // can not transfer to myself
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
            count - 1 < static_cast<int>(targetLeaderCount + 1) ||
            !coolingTimeExpired(targetStartUpTime)) {
            retryTimes++;
            continue;
        } else {
            *op = operatorFactory.CreateTransferLeaderOperator(
                *selectedCopySet, targetId, OperatorPriority::NormalPriority);
            op->timeLimit = std::chrono::seconds(transTimeSec_);
            return true;
        }
    }

    return false;
}

bool LeaderScheduler::transferLeaderIn(ChunkServerIdType target, int count,
                                       PoolIdType lid, Operator *op,
                                       CopySetInfo *selectedCopySet) {
    // find the copyset on follower and transfer leader to the target
    std::vector<CopySetInfo> candidateInfos;
    for (auto &cInfo : topo_->GetCopySetInfosInLogicalPool(lid)) {
        // skip those copyset with the target chunkserver as its leader and
        // and without the copyset
        if (cInfo.leader == target || !cInfo.ContainPeer(target)) {
            continue;
        }

        // skip copyset with configuration changing undergoing
        if (cInfo.HasCandidate()) {
            LOG(INFO) << cInfo.CopySetInfoStr() << " is on config change";
            continue;
        }

        // skip copyset with any offline chunkserver
        if (copySetHealthy(cInfo)) {
            candidateInfos.emplace_back(cInfo);
        }
    }

    if (candidateInfos.size() == 0) {
        return false;
    }

    srand((unsigned)time(NULL));
    int retryTimes = 1;
    while (retryTimes < maxRetryTransferLeader) {
        // select a copyset randomly from candidates
        *selectedCopySet = candidateInfos[rand() % candidateInfos.size()];

        // fetch the leader number of the leader of the selected copyset and
        // the target
        ChunkServerInfo sourceInfo;
        if (!topo_->GetChunkServerInfo(selectedCopySet->leader, &sourceInfo)) {
            LOG(ERROR) << "leaderScheduler cannot get info of chukServer:"
                       << selectedCopySet->leader;
            retryTimes++;
            continue;
        }

        if (static_cast<int>(sourceInfo.leaderCount - 1) < count + 1) {
            retryTimes++;
            continue;
        }

        // transfer leader to the target
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

int64_t LeaderScheduler::GetRunningInterval() { return runInterval_; }
}  // namespace schedule
}  // namespace mds
}  // namespace curve
