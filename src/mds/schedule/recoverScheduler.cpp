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
 * Created Date: Mon Nov 19 2018
 * Author: lixiaocui
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

    // if over certain amount of chunkserver are downed on a server, these
    // chunkservers will be collected to the set excludes.
    std::set<ChunkServerIdType> excludes;
    CalculateExcludesChunkServer(&excludes);

    for (auto copysetInfo : topo_->GetCopySetInfos()) {
        // skip the copyset under configuration change
        Operator op;
        if (opController_->GetOperatorById(copysetInfo.id, &op)) {
            continue;
        }

        if (copysetInfo.HasCandidate()) {
            LOG(WARNING) << copysetInfo.CopySetInfoStr()
                         << " already has candidate: "
                         << copysetInfo.candidatePeerInfo.id;
            continue;
        }

        std::set<ChunkServerIdType> offlinelists;
        // check if there's any offline replica
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
            }
        }

        // do nothing if all replicas are offline
        if (offlinelists.size() == 0) {
            continue;
        }

        // alarm if over half of the replicas are offline
        int deadBound =
            copysetInfo.peers.size() - (copysetInfo.peers.size()/2 + 1);
        if (offlinelists.size() > deadBound) {
            LOG(ERROR) << "recoverSchdeuler find "
                       << copysetInfo.CopySetInfoStr()
                       << " has " << offlinelists.size()
                       << " replica offline, cannot repair, please check";
            continue;
        }

        // offline replicas in excludes will not be recovered
        for (auto it = offlinelists.begin(); it != offlinelists.end();) {
            if (excludes.count(*it) > 0) {
                LOG(ERROR) << "can not recover offline chunkserver " << *it
                          << " on " << copysetInfo.CopySetInfoStr()
                          << ", because it's server has more than "
                          << chunkserverFailureTolerance_
                          << " offline chunkservers";
                it = offlinelists.erase(it);
            } else {
                ++it;
            }
        }

        if (offlinelists.size() <= 0) {
            continue;
        }

        // recover onr of the offline replica
        Operator fixRes;
        ChunkServerIdType target;
        // failed to recover the replica
        if (!FixOfflinePeer(
                copysetInfo, *offlinelists.begin(), &fixRes, &target)) {
            continue;
        // succeeded but failed to add the operator to the controller
        } else if (!opController_->AddOperator(fixRes)) {
            LOG(WARNING) << "recover scheduler add operator "
                       << fixRes.OpToString() << " on "
                       << copysetInfo.CopySetInfoStr() << " fail";
            continue;
        // succeeded in recovering replica and adding it to the controller
        } else {
            LOG(INFO) << "recoverScheduler generate operator:"
                        << fixRes.OpToString() << " for "
                        << copysetInfo.CopySetInfoStr()
                        << ", remove offlinePeer: "
                        << *offlinelists.begin();
            // if the target returned has the initial value, that means offline
            // replicas are removed directly.
            if (target == UNINTIALIZE_ID) {
                oneRoundGenOp++;
                continue;
            }

            // if the target didn't return the initial value, that means copyset
            // should be generated on target. If failed to generate, delete the
            // operator.
            if (!topo_->CreateCopySetAtChunkServer(copysetInfo.id, target)) {
                LOG(WARNING) << "recoverScheduler create "
                           << copysetInfo.CopySetInfoStr()
                           << " on chunkServer: " << target
                           << " error, delete operator" << fixRes.OpToString();
                opController_->RemoveOperator(copysetInfo.id);
                continue;
            }
            oneRoundGenOp++;
        }
    }
    LOG(INFO) << "recoverScheduler generate " << oneRoundGenOp
              << " operators at this round";
    return 1;
}

int64_t RecoverScheduler::GetRunningInterval() {
    return runInterval_;
}

bool RecoverScheduler::FixOfflinePeer(
    const CopySetInfo &info, ChunkServerIdType peerId,
    Operator *op, ChunkServerIdType *target) {
    assert(op != nullptr);
    // check the standard number of replicas first
    auto standardReplicaNum =
        topo_->GetStandardReplicaNumInLogicalPool(info.id.first);
    if (standardReplicaNum <= 0) {
        LOG(WARNING) << "RecoverScheduler find logical pool "
                     << info.id.first << " standard num "
                     << standardReplicaNum << " invalid";
        return false;
    }
    if (info.peers.size() > standardReplicaNum) {
        // remove the offline replica
        *op = operatorFactory.CreateRemovePeerOperator(
            info, peerId, OperatorPriority::HighPriority);
        op->timeLimit = std::chrono::seconds(removeTimeSec_);
        *target = UNINTIALIZE_ID;
        return true;
    }

    // select peers to add
    auto csId = SelectBestPlacementChunkServer(info, peerId);
    if (csId == UNINTIALIZE_ID) {
        LOG(WARNING) << "recoverScheduler can not select chunkServer to "
                        "repair " << info.CopySetInfoStr()
                     << ", which replica: " << peerId << " is offline";
        return false;
    } else {
        *op = operatorFactory.CreateChangePeerOperator(
            info, peerId, csId, OperatorPriority::HighPriority);
        op->timeLimit = std::chrono::seconds(addTimeSec_);
        *target = csId;
        return true;
    }
}

void RecoverScheduler::CalculateExcludesChunkServer(
    std::set<ChunkServerIdType> *excludes) {
    // calculate the number of offline or pending chunkserver on a server
    std::map<ServerIdType, std::vector<ChunkServerIdType>> unhealthyStateCS;
    std::set<ChunkServerIdType> pendingCS;
    for (auto cs : topo_->GetChunkServerInfos()) {
        // calculate number of pending chunkservers
        if (cs.IsPendding()) {
            LOG(INFO) << "chunkserver " << cs.info.id << " is set pendding";
            pendingCS.emplace(cs.info.id);
        }

        if (cs.IsOnline()) {
            continue;
        }

        if (unhealthyStateCS.count(cs.info.serverId) <= 0) {
            unhealthyStateCS[cs.info.serverId] =
                std::vector<ChunkServerIdType>{cs.info.id};
        } else {
            unhealthyStateCS[cs.info.serverId].emplace_back(cs.info.id);
        }
    }

    // check whether the number offline or missed chunkserver has exceed the
    // tolerance threshold. If it does, the chunkservers on this server will not
    // be recovered
    for (auto item : unhealthyStateCS) {
        if (item.second.size() < chunkserverFailureTolerance_) {
            continue;
        }
        LOG(ERROR) << "server " << item.first << " has "
                    << item.second.size() << " offline chunkservers";
        for (auto cs : item.second) {
            excludes->emplace(cs);
        }
    }

    // if the chunkserver is in pending status, it will be considered recoverable //NOLINT
    for (auto it : pendingCS) {
        excludes->erase(it);
    }
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve

