/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * @Date: 2022-04-07 14:15:49
 * @Author: chenwei
 */

#include "curvefs/src/mds/schedule/operatorFactory.h"
#include "curvefs/src/mds/schedule/scheduler.h"

namespace curvefs {
namespace mds {
namespace schedule {
/**
 * use curl -L mdsIp:port/flags/enableRapidLeaderScheduler?setvalue=true
 * for dynamic parameter configuration
 *
 * If this value is set true, ignore metaserverCoolingTimeSec_
 * when choose metaserver as a leader when leader schedule.
 *
 * if this value is set true, when no leader schedule operation at this round,
 * set the value to false automaticlly.
 */
static bool pass_bool(const char *, bool) { return true; }
DEFINE_bool(enableRapidLeaderScheduler, false, "rapid leader scheduler once");
DEFINE_validator(enableRapidLeaderScheduler, &pass_bool);

int LeaderScheduler::Schedule() {
    LOG(INFO) << "LeaderScheduler begin";

    int oneRoundGenOp = 0;
    for (auto poolId : topo_->Getpools()) {
        oneRoundGenOp += LeaderSchedulerForPool(poolId);
    }

    LOG(INFO) << "LeaderScheduler generate " << oneRoundGenOp
              << " operators at this round";

    if (FLAGS_enableRapidLeaderScheduler && oneRoundGenOp == 0) {
        LOG(INFO) << "LeaderScheduler enable rapid leader scheduler and "
                  << " generate no operation in this round, "
                  << "set enableRapidLeaderScheduler to false";
        FLAGS_enableRapidLeaderScheduler = false;
    }
    return oneRoundGenOp;
}

int64_t LeaderScheduler::GetRunningInterval() const { return runInterval_; }

int LeaderScheduler::LeaderSchedulerForPool(PoolIdType poolId) {
    int oneRoundGenOp = 0;
    uint16_t replicaNum = topo_->GetStandardReplicaNumInPool(poolId);
    if (replicaNum == 0) {
        LOG(ERROR) << "leader scheduler for pool = " << poolId
                   << " fail, replicaNum is 0";
        return 0;
    }

    // find metaserver with maximum and minimum number of leaders
    double maxMoreNum = -1;
    int maxId = -1;
    double minLessNum = 0;
    int minId = -1;
    MetaServerInfo moreLeaderMetaserver;
    MetaServerInfo lessLeaderMetaserver;
    std::vector<MetaServerInfo> msInfos = topo_->GetMetaServersInPool(poolId);
    thread_local static std::random_device rd;
    thread_local static std::mt19937 randomGenerator(rd());
    std::shuffle(msInfos.begin(), msInfos.end(), randomGenerator);

    for (const auto& msInfo : msInfos) {
        // skip unhealthy metaserver
        if (!msInfo.IsHealthy()) {
            continue;
        }

        double standardLeaderNum = msInfo.copysetNum * 1.0 / replicaNum;
        // if different > 0, it means the leader num on metaserver is more than
        // stardard leader num, vice versa
        double different = msInfo.leaderNum - standardLeaderNum;

        // if leader num is appropriate, skip this metaserver.
        if (different > -1 && different < 1) {
            continue;
        }

        // leader too less
        if (different < -1) {
            if (minId == -1 || different < minLessNum) {
                // the metaserver with minLeaderCount and not in coolingTime
                // can be the transfer target
                if (!FLAGS_enableRapidLeaderScheduler &&
                    !CoolingTimeExpired(msInfo.startUpTime)) {
                    continue;
                }
                minId = msInfo.info.id;
                minLessNum = different;
                lessLeaderMetaserver = msInfo;
            }
            continue;
        }

        // leader too many
        if (different > 1) {
            if (maxId == -1 || different > maxMoreNum) {
                maxId = msInfo.info.id;
                maxMoreNum = different;
                moreLeaderMetaserver = msInfo;
            }
        }
    }

    if (maxId == -1 && minId == -1) {
        LOG(INFO) << "leaderScheduler find no metaserver need transfer leader"
                  << " in pool, poolId = " << poolId;
        return 0;
    }

    // for metaserver that has the most leaders, pick a leader copyset (the
    // copyset with the metaserver as its leader) randomly and transfer the
    // leader replica (the leader role) to the replica (in the same copyset)
    // that has the least leader number
    if (maxId != -1) {
        LOG(INFO) << "leaderScheduler select one metaserver on pool " << poolId
                  << ", which has more leader, metaserverId = " << maxId
                  << ", leaderNum = " << moreLeaderMetaserver.leaderNum
                  << ", copysetNum = " << moreLeaderMetaserver.copysetNum;
        Operator transferLeaderOutOp;
        CopySetInfo selectedCopySet;
        if (TransferLeaderOut(maxId, replicaNum, poolId, &transferLeaderOutOp,
                              &selectedCopySet)) {
            if (opController_->AddOperator(transferLeaderOutOp)) {
                oneRoundGenOp += 1;
                LOG(INFO) << "leaderScheduler generate operator "
                          << transferLeaderOutOp.OpToString() << " for "
                          << selectedCopySet.CopySetInfoStr()
                          << " from transfer leader out";
                return oneRoundGenOp;
            } else {
                LOG(WARNING) << "leaderScheduler generate operator "
                          << transferLeaderOutOp.OpToString() << " for "
                          << selectedCopySet.CopySetInfoStr()
                          << " from transfer leader out, but add operator fail";
            }
        }
    }

    // for the metaserver that has the least leaders, choose a follower copyset
    // (the copyset that has a follower replica on the metaserver) randomly and
    // transfer the leader (role) to this metaserver
    if (minId != -1) {
        LOG(INFO) << "leaderScheduler select one metaserver on pool " << poolId
                  << ", which has less leader, metaserverId = " << minId
                  << ", leaderNum = " << lessLeaderMetaserver.leaderNum
                  << ", copysetNum = " << lessLeaderMetaserver.copysetNum;
        Operator transferLeaderInOp;
        CopySetInfo selectedCopySet;
        if (TransferLeaderIn(minId, replicaNum, poolId, &transferLeaderInOp,
                             &selectedCopySet)) {
            if (opController_->AddOperator(transferLeaderInOp)) {
                oneRoundGenOp += 1;
                LOG(INFO) << "leaderScheduler generate operator "
                          << transferLeaderInOp.OpToString() << " for "
                          << selectedCopySet.CopySetInfoStr()
                          << " from transfer leader in";
                return oneRoundGenOp;
            } else {
                LOG(WARNING) << "leaderScheduler generate operator "
                          << transferLeaderInOp.OpToString() << " for "
                          << selectedCopySet.CopySetInfoStr()
                          << " from transfer leader in, but add operator fail";
            }
        }
    }

    return oneRoundGenOp;
}

// 1. select copyset in the target same pool, skip the copyset which leader not
//    the target, has candidata, not healthy.
// 2. shuffle the copyset
// 3. for each copyset, select the largest leader num less than the standard num
//    metaserver, transfer the leader from source to the selected metaserver
// 4. create the operator
bool LeaderScheduler::TransferLeaderOut(MetaServerIdType source,
                                        uint16_t replicaNum, PoolIdType poolId,
                                        Operator *op,
                                        CopySetInfo *selectedCopySet) {
    // find all copyset with source metaserver as its leader as the candidate
    std::vector<CopySetInfo> candidateInfos;
    for (auto &cInfo : topo_->GetCopySetInfosInMetaServer(source)) {
        // skip those copysets that the source is the follower in it
        if (cInfo.leader != source) {
            continue;
        }

        // skip the copyset under configuration changing
        if (cInfo.HasCandidate()) {
            LOG(INFO) << cInfo.CopySetInfoStr() << " is on config change";
            continue;
        }

        // skip copyset with any offline metaserver
        if (CopySetHealthy(cInfo)) {
            candidateInfos.emplace_back(std::move(cInfo));
        }
    }

    if (candidateInfos.empty()) {
        LOG(INFO) << "can not find enough candidate copyset for metaserver "
                  << source << " to transfer leader out";
        return false;
    }

    thread_local static std::random_device rd;
    thread_local static std::mt19937 randomGenerator(rd());
    std::shuffle(candidateInfos.begin(), candidateInfos.end(), randomGenerator);

    for (const auto &copysetInfo : candidateInfos) {
        MetaServerIdType targetId = UNINITIALIZE_ID;
        double minDifferent = 1;
        for (auto peerInfo : copysetInfo.peers) {
            // can not transfer to myself
            if (source == peerInfo.id) {
                continue;
            }

            MetaServerInfo msInfo;
            if (!topo_->GetMetaServerInfo(peerInfo.id, &msInfo)) {
                LOG(WARNING) << "leaderScheduler cannot get info of"
                             << " metaServer: " << peerInfo.id;
                break;
            }

            // if metaserver is unhealthy in the selected copyset,
            // skip to next copyset
            if (!msInfo.IsHealthy()) {
                break;
            }

            if (!FLAGS_enableRapidLeaderScheduler &&
                !CoolingTimeExpired(msInfo.startUpTime)) {
                continue;
            }

            // select the largest leader num less than the standard num
            double standardLeaderNum = msInfo.copysetNum * 1.0 / replicaNum;
            double different = (msInfo.leaderNum + 1.0) - standardLeaderNum;
            if (different < 1 && different < minDifferent) {
                targetId = msInfo.info.id;
                minDifferent = different;
            }
        }

        if (targetId != UNINITIALIZE_ID) {
            *selectedCopySet = copysetInfo;
            *op = operatorFactory.CreateTransferLeaderOperator(
                copysetInfo, targetId, OperatorPriority::NormalPriority);
            op->timeLimit = std::chrono::seconds(transTimeSec_);
            return true;
        }
    }

    LOG(INFO) << "can not select target metaserver for metaserver " << source
              << " to transfer leader out";
    return false;
}

// 1. select copyset in the target same pool, skip the copyset which leader is
//    target, not contain the target, has candidata, not healthy.
// 2. shuffle the copyset
// 3. for each copyset, determine whether the copyset can migrate
//    its leader to the target
// 4. create the operator
bool LeaderScheduler::TransferLeaderIn(MetaServerIdType target,
                                       uint16_t replicaNum, PoolIdType poolId,
                                       Operator *op,
                                       CopySetInfo *selectedCopySet) {
    // find the copyset on follower and transfer leader to the target
    std::vector<CopySetInfo> candidateInfos;
    for (auto &cInfo : topo_->GetCopySetInfosInPool(poolId)) {
        // skip those copyset with the target metaserver as its leader and
        // and without the copyset
        if (cInfo.leader == target || !cInfo.ContainPeer(target)) {
            continue;
        }

        // skip copyset with configuration changing undergoing
        if (cInfo.HasCandidate()) {
            LOG(INFO) << cInfo.CopySetInfoStr() << " is on config change";
            continue;
        }

        // skip copyset with any offline metaserver
        if (CopySetHealthy(cInfo)) {
            candidateInfos.emplace_back(std::move(cInfo));
        }
    }

    if (candidateInfos.empty()) {
        LOG(INFO) << "can not find enough candidate copyset for metaserver "
                  << target << " to transfer leader in";
        return false;
    }

    thread_local static std::random_device rd;
    thread_local static std::mt19937 randomGenerator(rd());
    std::shuffle(candidateInfos.begin(), candidateInfos.end(), randomGenerator);

    for (const auto &copysetInfo : candidateInfos) {
        // fetch the leader number of the leader of the selected copyset and
        // the target
        MetaServerInfo msInfo;
        if (!topo_->GetMetaServerInfo(copysetInfo.leader, &msInfo)) {
            LOG(WARNING) << "leaderScheduler cannot get info of metaServer:"
                         << copysetInfo.leader;
            continue;
        }

        double standardLeaderNum = msInfo.copysetNum * 1.0 / replicaNum;
        double different = (msInfo.leaderNum - 1.0) - standardLeaderNum;
        if (different < -1) {
            continue;
        }

        // transfer leader to the target
        *selectedCopySet = copysetInfo;
        *op = operatorFactory.CreateTransferLeaderOperator(
            copysetInfo, target, OperatorPriority::NormalPriority);
        op->timeLimit = std::chrono::seconds(transTimeSec_);
        return true;
    }

    LOG(INFO) << "can not select metaserver for metaserver " << target
              << " to transfer leader in";
    return false;
}

bool LeaderScheduler::CoolingTimeExpired(uint64_t startUpTime) {
    if (startUpTime == 0) {
        return false;
    }

    uint64_t currentTime = ::curve::common::TimeUtility::GetTimeofDaySec();
    return currentTime - startUpTime > metaserverCoolingTimeSec_;
}

bool LeaderScheduler::CopySetHealthy(const CopySetInfo &cInfo) {
    for (auto peer : cInfo.peers) {
        MetaServerInfo msInfo;
        if (!topo_->GetMetaServerInfo(peer.id, &msInfo)) {
            LOG(ERROR) << "leaderScheduler cannot get info of metaserver:"
                       << peer.id;
            return false;
        }

        if (msInfo.IsOffline()) {
            return false;
        }
    }
    return true;
}

bool LeaderScheduler::GetRapidLeaderSchedulerFlag() {
    return FLAGS_enableRapidLeaderScheduler;
}

void LeaderScheduler::SetRapidLeaderSchedulerFlag(bool flag) {
    FLAGS_enableRapidLeaderScheduler = flag;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
