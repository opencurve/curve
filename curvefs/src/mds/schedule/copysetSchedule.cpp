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
 * @Date: 2022-02-14 14:31:39
 * @Author: chenwei
 */

#include <glog/logging.h>
#include "curvefs/src/mds/schedule/operatorFactory.h"
#include "curvefs/src/mds/schedule/scheduler.h"

using ::curve::mds::topology::UNINTIALIZE_ID;

namespace curvefs {
namespace mds {
namespace schedule {
int CopySetScheduler::Schedule() {
    LOG(INFO) << "copysetScheduler begin";

    int oneRoundGenOp = 0;
    for (auto poolId : topo_->Getpools()) {
        oneRoundGenOp += CopySetScheduleForPool(poolId);
    }

    LOG(INFO) << "copysetScheduler generate " << oneRoundGenOp
              << " operators at this round";
    return oneRoundGenOp;
}

void CopySetScheduler::GetCopySetDistribution(
    const std::vector<CopySetInfo> &copysetList,
    const std::vector<MetaServerInfo> &metaserverList,
    std::map<MetaServerIdType, std::vector<CopySetInfo>> *out) {
    // calculate the copysetlist by traversing the copyset list
    for (auto item : copysetList) {
        for (auto peer : item.peers) {
            if (out->find(peer.id) == out->end()) {
                (*out)[peer.id] = std::vector<CopySetInfo>{item};
            } else {
                (*out)[peer.id].emplace_back(item);
            }
        }
    }

    // remove offline metaserver
    for (auto item : metaserverList) {
        if (item.IsOffline()) {
            out->erase(item.info.id);
            continue;
        }
    }
}

bool CopySetScheduler::TransferCopyset(const CopySetInfo &copyset,
                                       MetaServerIdType sourceId,
                                       MetaServerIdType destId) {
    // generate operator
    Operator op = operatorFactory.CreateChangePeerOperator(
        copyset, sourceId, destId, OperatorPriority::NormalPriority);
    op.timeLimit = std::chrono::seconds(changeTimeSec_);
    LOG(INFO) << "copyset scheduler gen " << op.OpToString() << " on "
              << copyset.CopySetInfoStr();

    // add operator
    if (!opController_->AddOperator(op)) {
        LOG(INFO) << "copysetSchduler add op " << op.OpToString()
                  << " fail, copyset has already has operator"
                  << " or operator num exceeds the limit.";
        return false;
    }

    // create copyset
    if (!topo_->CreateCopySetAtMetaServer(copyset.id, destId)) {
        LOG(ERROR) << "copysetScheduler create " << copyset.CopySetInfoStr()
                   << " on metaServer: " << destId << " error, delete operator"
                   << op.OpToString();
        opController_->RemoveOperator(copyset.id);
        return false;
    }

    LOG(INFO) << "copysetScheduler create " << copyset.CopySetInfoStr()
              << "on metaServer:" << destId
              << " success. generator op: " << op.OpToString() << "success";
    return true;
}

bool CopySetScheduler::IsCopysetCanTransfer(const CopySetInfo &copyset) {
    // return false if operator exists on copyset
    Operator exist;
    if (opController_->GetOperatorById(copyset.id, &exist)) {
        return false;
    }

    // return false if copyset already has candidate
    if (copyset.HasCandidate()) {
        LOG(WARNING) << copyset.CopySetInfoStr() << " already has candidate: "
                     << copyset.candidatePeerInfo.id;
        return false;
    }

    // return false if the replica num of copyset is not standard
    uint16_t replicaNum = topo_->GetStandardReplicaNumInPool(copyset.id.first);
    if (copyset.peers.size() != replicaNum) {
        LOG(WARNING) << copyset.CopySetInfoStr()
                     << " peer num not match pool replica num: " << replicaNum;
        return false;
    }

    // some peers are offline
    if (!CopysetAllPeersOnline(copyset)) {
        LOG(WARNING) << copyset.CopySetInfoStr() << " some peers are offline";
        return false;
    }

    return true;
}

int CopySetScheduler::CopySetScheduleOverloadMetaserver(PoolIdType poolId) {
    auto copysetList = topo_->GetCopySetInfosInPool(poolId);
    auto metaserverList = topo_->GetMetaServersInPool(poolId);

    std::map<MetaServerIdType, std::vector<CopySetInfo>> distribute;
    GetCopySetDistribution(copysetList, metaserverList, &distribute);

    int genOpCount = 0;
    for (const auto &ms : metaserverList) {
        // Find if any metaserver which resource exceeds the threshold,
        // skip unhealthy metaserver
        if (!ms.IsHealthy() || !ms.IsResourceOverload()) {
            continue;
        }

        MetaServerIdType sourceId = ms.info.id;
        LOG(INFO) << "metaserver resource is overload, should transfer some "
                  << "copyset to other metaserver, metaserverId = " << sourceId;

        // TODO(cw123): get copyset resource use status,
        // and choose copyset by the cost
        for (const auto &copyset : distribute[sourceId]) {
            if (!IsCopysetCanTransfer(copyset)) {
                continue;
            }

            MetaServerIdType destId =
                SelectBestPlacementMetaServer(copyset, sourceId);
            if (destId == UNINITIALIZE_ID) {
                continue;
            }

            LOG(INFO) << "transfer copyset from metaserver " << sourceId
                      << " to metaserver " << destId;
            if (TransferCopyset(copyset, sourceId, destId)) {
                genOpCount++;
                return genOpCount;
            }
        }
    }

    return genOpCount;
}

void CopySetScheduler::FileterUnhealthyMetaserver(
    std::vector<MetaServerInfo> *vector) {
    for (auto it = vector->begin(); it != vector->end();) {
        if (!it->IsHealthy()) {
            it = vector->erase(it);
        } else {
            it++;
        }
    }

    return;
}

int CopySetScheduler::CheckAndBalanceZoneByMetaserverUsage(ZoneIdType zoneId) {
    int genOpCount = 0;
    std::vector<MetaServerInfo> metaserverVector =
        topo_->GetMetaServersInZone(zoneId);

    FileterUnhealthyMetaserver(&metaserverVector);

    if (metaserverVector.size() < 2) {
        return genOpCount;
    }

    // sort by resource usage decrease
    auto sortFunc = [](const MetaServerInfo a, const MetaServerInfo b) {
        return a.GetResourceUseRatioPercent() > b.GetResourceUseRatioPercent();
    };

    std::sort(metaserverVector.begin(), metaserverVector.end(), sortFunc);

    double ratioMax = metaserverVector.front().GetResourceUseRatioPercent();
    double rationMin = metaserverVector.back().GetResourceUseRatioPercent();

    if (ratioMax > rationMin + balanceRatioPercent_) {
        MetaServerIdType sourceId = metaserverVector.front().info.id;
        MetaServerIdType destId = metaserverVector.back().info.id;

        LOG(INFO) << "find meta usage not balance in zone = " << zoneId
                  << ", metaserver = " << sourceId << ", usage = " << ratioMax
                  << ", metaserver = " << destId << ", usage = " << rationMin;

        std::vector<CopySetInfo> copysetVector =
            topo_->GetCopySetInfosInMetaServer(sourceId);

        // TODO(chenwei): select copyset which uses less disk
        thread_local static std::random_device rd;
        thread_local static std::mt19937 randomGenerator(rd());
        std::shuffle(copysetVector.begin(), copysetVector.end(),
                     randomGenerator);
        for (const auto &copyset : copysetVector) {
            if (!IsCopysetCanTransfer(copyset)) {
                continue;
            }
            LOG(INFO) << "transfer copyset from high disk usage metaserver "
                      << sourceId << " to low usage metaserver " << destId;
            if (TransferCopyset(copyset, sourceId, destId)) {
                genOpCount++;
                return genOpCount;
            }
        }
    }

    return genOpCount;
}

int CopySetScheduler::CheckAndBalanceZoneByCopysetNum(ZoneIdType zoneId) {
    int genOpCount = 0;
    std::vector<MetaServerInfo> metaserverVector =
        topo_->GetMetaServersInZone(zoneId);

    FileterUnhealthyMetaserver(&metaserverVector);

    if (metaserverVector.size() < 2) {
        return genOpCount;
    }

    // sort by copyset num decrease
    auto sortFunc = [](const MetaServerInfo a, const MetaServerInfo b) {
        return a.copysetNum > b.copysetNum;
    };

    std::sort(metaserverVector.begin(), metaserverVector.end(), sortFunc);

    uint32_t maxCopysetNum = metaserverVector.front().copysetNum;
    uint32_t minCopysetNum = metaserverVector.back().copysetNum;

    // the resource useage of the metaserver which has the max copysets num
    double ratioFirst = metaserverVector.front().GetResourceUseRatioPercent();
    // the resource useage of the metaserver which has the min copysets num
    double rationSecond = metaserverVector.back().GetResourceUseRatioPercent();

    // If the difference between the copy nums of metaservers within a zone
    // is greater than or equal to 2, and the max copysets num metaserver'
    // resource usage is larger then the min copyset num metaserver, then
    // here transfer one copyset from max copyset num metaserver to min one
    if (maxCopysetNum >= 2 + minCopysetNum && ratioFirst >= rationSecond) {
        MetaServerIdType sourceId = metaserverVector.front().info.id;
        MetaServerIdType destId = metaserverVector.back().info.id;

        LOG(INFO) << "find meta copyset num not balance in zone = " << zoneId
                  << ", metaserver = " << sourceId
                  << ", max copysetNum = " << maxCopysetNum
                  << ", metaserver = " << destId
                  << ", min copysetNum = " << minCopysetNum;

        std::vector<CopySetInfo> copysetVector =
            topo_->GetCopySetInfosInMetaServer(sourceId);

        // TODO(chenwei): select copyset which uses less disk
        thread_local static std::random_device rd;
        thread_local static std::mt19937 randomGenerator(rd());
        std::shuffle(copysetVector.begin(), copysetVector.end(),
                     randomGenerator);
        for (const auto &copyset : copysetVector) {
            if (!IsCopysetCanTransfer(copyset)) {
                continue;
            }
            LOG(INFO) << "transfer copyset from more copyset metaserver "
                      << sourceId << " to less copyset metaserver " << destId
                      << ", copyset (" << copyset.id.first << ", "
                      << copyset.id.second << ")";
            if (TransferCopyset(copyset, sourceId, destId)) {
                genOpCount++;
                return genOpCount;
            }
        }
    }

    return genOpCount;
}

int CopySetScheduler::CopySetScheduleNormalMetaserver(PoolIdType poolId) {
    std::list<ZoneIdType> zoneList = topo_->GetZoneInPool(poolId);
    for (ZoneIdType zoneId : zoneList) {
        int ret = CheckAndBalanceZoneByMetaserverUsage(zoneId);
        if (ret > 0) {
            return ret;
        }
    }

    for (ZoneIdType zoneId : zoneList) {
        int ret = CheckAndBalanceZoneByCopysetNum(zoneId);
        if (ret > 0) {
            return ret;
        }
    }

    return 0;
}

int CopySetScheduler::CopySetScheduleForPool(PoolIdType poolId) {
    int genOpCount = CopySetScheduleOverloadMetaserver(poolId);
    if (genOpCount > 0) {
        return genOpCount;
    }

    genOpCount = CopySetScheduleNormalMetaserver(poolId);
    return genOpCount;
}

int64_t CopySetScheduler::GetRunningInterval() const { return runInterval_; }

}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
