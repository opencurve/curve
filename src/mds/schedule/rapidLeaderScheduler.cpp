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

    // lpid_为0, 在所有logicalpool做快速leader均衡
    if (lpoolId_ == UNINTIALIZE_ID) {
        for (PoolIdType lid : existLpoolsVec) {
            DoRapidLeaderSchedule(lid);
        }

        return kScheduleErrCodeSuccess;
    }


    // lpid_大于0, 在指定logicalpool做快速leader均衡
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

void RapidLeaderScheduler::DoRapidLeaderSchedule(LogicalPoolIDType lid) {
    // 统计当前逻辑池中leader的分布情况
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
            // 该copyset有operator
            Operator op;
            if (opController_->GetOperatorById(copysetInfoItem.id, &op)) {
                continue;
            }

            // 选择目的迁移节点
            ChunkServerIdType target = SelectTargetPeer(
                curChunkServer, copysetInfoItem, stat);
            if (target == UNINTIALIZE_ID) {
                continue;
            }

            // 生成operator
            bool success = GenerateLeaderChangeOperatorForCopySet(
                copysetInfoItem, target);

            // 更新chunkserver上leader的数目
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
    // 获取chunkserverInfo list 和 copyset list
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

    // 获取每个chunkserver上面leader的数量
    for (const auto &info : chunkserverVec) {
        stat->leaderNumInChunkServer[info.info.id] = info.leaderCount;
    }

    // 获取每个chunkserver上面的copysetInfo list
    SchedulerHelper::CopySetDistributionInOnlineChunkServer(
        copysetVec, chunkserverVec, &stat->distribute);

    // 计算每个chunkserver上leader均值
    stat->avgLeaderNum = copysetVec.size() / chunkserverVec.size();

    return true;
}

ChunkServerIdType RapidLeaderScheduler::SelectTargetPeer(
    ChunkServerIdType curChunkServerId, const CopySetInfo &info,
    const LeaderStatInLogicalPool &stat) {
    ChunkServerIdType selected = UNINTIALIZE_ID;

    // curChunkServerId 上不是leader副本
    bool curChunkServerIsLeader = (info.leader == curChunkServerId);
    if (!curChunkServerIsLeader) {
        return selected;
    }

    // copyset的副本数目小于等于1个
    bool copysetPeerNumMoreThanOne = (info.peers.size() > 1);
    if (!copysetPeerNumMoreThanOne) {
        return selected;
    }

    // copyset所有副本中leader数目最小的副本
    int possibleSelected = MinLeaderNumInCopySetPeers(info, stat);
    if (possibleSelected == curChunkServerId) {
        return selected;
    }

    // 判断是否可以迁移到leader数目最小的副本上
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
    // 目的节点需要满足以下条件：
    // 1. 源节点和目的节点上leader的数量差大于1
    // 2. 源节点上当前leader的数量大于均值
    int leaderNumInOriginCS = stat.leaderNumInChunkServer.at(origLeader);
    int leaderNumInTargetCS = stat.leaderNumInChunkServer.at(targetLeader);

    return  leaderNumInOriginCS - leaderNumInTargetCS > 1 &&
        leaderNumInOriginCS > stat.avgLeaderNum;
}

bool RapidLeaderScheduler::GenerateLeaderChangeOperatorForCopySet(
    const CopySetInfo &info, ChunkServerIdType targetLeader) {
    // 生成operator
    auto op = operatorFactory.CreateTransferLeaderOperator(
        info, targetLeader, OperatorPriority::NormalPriority);
    op.timeLimit = std::chrono::seconds(transTimeSec_);

    // 添加到controller
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
