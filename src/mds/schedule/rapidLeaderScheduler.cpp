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

#include <glog/logging.h>
#include <algorithm>
#include "src/mds/common/mds_define.h"
#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/operatorFactory.h"
#include "src/mds/schedule/scheduler_helper.h"

namespace curve {
namespace mds {
namespace schedule {

int RapidLeaderScheduler::Schedule() {
    std::vector<PoolIdType> existLpoolsVec = topo_->GetLogicalpools();

    // schedule for every logical pool ID when lpid is uninitialized (0)
    if (lpoolId_ == UNINTIALIZE_ID) {
        for (PoolIdType lid : existLpoolsVec) {
            DoRapidLeaderSchedule(lid);
        }

        return kScheduleErrCodeSuccess;
    }

    // for specified logical pool when logical pool ID is larger than 0
    bool exist = (std::find(existLpoolsVec.begin(),
        existLpoolsVec.end(), lpoolId_) != existLpoolsVec.end());
    if (!exist) {
        LOG(WARNING) << "RapidLeaderSchedule find logicalpool "
            << lpoolId_ << " invalid";
        return kScheduleErrCodeInvalidLogicalPool;
    }

    DoRapidLeaderSchedule(lpoolId_);
    return kScheduleErrCodeSuccess;
}

void RapidLeaderScheduler::DoRapidLeaderSchedule(LogicalPoolIdType lid) {
    // calculate the leader distribution of current logical pool
    LeaderStatInLogicalPool stat;
    stat.lid = lid;
    if (!LeaderStatInSpecifiedLogicalPool(&stat)) {
        return;
    }

    int genNum = 0;
    for (auto copysetInChunkserver : stat.distribute) {
        ChunkServerIdType curChunkServer = copysetInChunkserver.first;
        std::vector<CopySetInfo> copysetInfosInCS = copysetInChunkserver.second;

        for (auto copysetInfoItem : copysetInfosInCS) {
            // operator exist for this copyset
            Operator op;
            if (opController_->GetOperatorById(copysetInfoItem.id, &op)) {
                continue;
            }

            ChunkServerIdType target = SelectTargetPeer(
                curChunkServer, copysetInfoItem, stat);
            if (target == UNINTIALIZE_ID) {
                continue;
            }

            bool success = GenerateLeaderChangeOperatorForCopySet(
                copysetInfoItem, target);

            // update leader number on chunkserver
            if (success) {
                stat.leaderNumInChunkServer[target] += 1;
                stat.leaderNumInChunkServer[curChunkServer] -= 1;
                genNum++;
            }
        }
    }

    LOG(INFO) << "RapidLeaderScheduler generate " << genNum
        << " operators in logical pool " << lid;
    return;
}

bool RapidLeaderScheduler::LeaderStatInSpecifiedLogicalPool(
    LeaderStatInLogicalPool *stat) {
    auto chunkserverVec =
        topo_->GetChunkServersInLogicalPool(stat->lid);
    auto copysetVec =
        topo_->GetCopySetInfosInLogicalPool(stat->lid);
    if (chunkserverVec.size() == 0 || copysetVec.size() == 0) {
        LOG(INFO) << "RapidLeaderScheduler find chunkserverSize="
            << chunkserverVec.size() << ", copysetSize="
            << copysetVec.size() << " in logicalPool=" << stat->lid;
        return false;
    }

    // get leader number on every chunkserver
    for (const auto &info : chunkserverVec) {
        stat->leaderNumInChunkServer[info.info.id] = info.leaderCount;
    }

    // get copyset info list for every chunkserver
    SchedulerHelper::CopySetDistributionInOnlineChunkServer(
        copysetVec, chunkserverVec, &stat->distribute);

    // calculate average leader number for every chunkserver
    stat->avgLeaderNum = copysetVec.size() / chunkserverVec.size();

    return true;
}

ChunkServerIdType RapidLeaderScheduler::SelectTargetPeer(
    ChunkServerIdType curChunkServerId, const CopySetInfo &info,
    const LeaderStatInLogicalPool &stat) {
    ChunkServerIdType selected = UNINTIALIZE_ID;

    // return uninitialize ID if current chunkserver is not the leader replica
    bool curChunkServerIsLeader = (info.leader == curChunkServerId);
    if (!curChunkServerIsLeader) {
        return selected;
    }

    // also return uninitialize ID if peers number is no more than 1
    bool copysetPeerNumMoreThanOne = (info.peers.size() > 1);
    if (!copysetPeerNumMoreThanOne) {
        return selected;
    }

    int possibleSelected = MinLeaderNumInCopySetPeers(info, stat);
    if (possibleSelected == curChunkServerId) {
        return selected;
    }

    // determine whether the replica with least leader number is a possible target //NOLINT
    if (!PossibleTargetPeerConfirm(curChunkServerId, possibleSelected, stat)) {
        return selected;
    }

    selected = possibleSelected;
    return selected;
}

ChunkServerIdType RapidLeaderScheduler::MinLeaderNumInCopySetPeers(
    const CopySetInfo &info, const LeaderStatInLogicalPool &stat) {
    int minLeaderCount = stat.leaderNumInChunkServer.at(info.peers[0].id);
    ChunkServerIdType target = info.peers[0].id;
    for (auto peer : info.peers) {
        if (stat.leaderNumInChunkServer.at(peer.id) < minLeaderCount) {
            minLeaderCount = stat.leaderNumInChunkServer.at(peer.id);
            target = peer.id;
        }
    }

    return target;
}

bool RapidLeaderScheduler::PossibleTargetPeerConfirm(
    ChunkServerIdType origLeader, ChunkServerIdType targetLeader,
    const LeaderStatInLogicalPool &stat) {
    // the target chunkserver should satisfy the requrement below:
    // 1. the difference of leader number between source and target is greater than 1. //NOLINT
    // 2. current number of leader on a source node should be greater than the average value //NOLINT
    int leaderNumInOriginCS = stat.leaderNumInChunkServer.at(origLeader);
    int leaderNumInTargetCS = stat.leaderNumInChunkServer.at(targetLeader);

    return  leaderNumInOriginCS - leaderNumInTargetCS > 1 &&
        leaderNumInOriginCS > stat.avgLeaderNum;
}

bool RapidLeaderScheduler::GenerateLeaderChangeOperatorForCopySet(
    const CopySetInfo &info, ChunkServerIdType targetLeader) {

    auto op = operatorFactory.CreateTransferLeaderOperator(
        info, targetLeader, OperatorPriority::NormalPriority);
    op.timeLimit = std::chrono::seconds(transTimeSec_);

    if (!opController_->AddOperator(op)) {
        LOG(WARNING) << "leaderScheduler generatre operator "
                    << op.OpToString()
                    << " for " << info.CopySetInfoStr()
                    << " fail, add to operator controller fail";
        return false;
    } else {
        LOG(INFO) << "leaderScheduler generatre operator "
                    << op.OpToString()
                    << " for " << info.CopySetInfoStr() << " success";
    }

    return true;
}

}  // namespace schedule
}  // namespace mds
}  // namespace curve
