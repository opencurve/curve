/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * @Date: 2021-11-8 11:01:48
 * @Author: chenwei
 */

#include <glog/logging.h>
#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/src/mds/schedule/operatorFactory.h"
#include "curvefs/src/mds/schedule/scheduler.h"

using ::curvefs::mds::topology::UNINITIALIZE_ID;

namespace curvefs {
namespace mds {
namespace schedule {
int RecoverScheduler::Schedule() {
    LOG(INFO) << "recoverScheduler begin.";
    int oneRoundGenOp = 0;

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

        std::set<MetaServerIdType> offlinelists;
        // check if there's any offline replica
        for (auto peer : copysetInfo.peers) {
            MetaServerInfo msInfo;
            if (!topo_->GetMetaServerInfo(peer.id, &msInfo)) {
                LOG(WARNING) << "recover scheduler: can not get " << peer.id
                             << " from topology" << std::endl;
                continue;
            }

            if (msInfo.IsOffline()) {
                offlinelists.emplace(peer.id);
            }
        }

        // do nothing if all replicas are online
        if (offlinelists.size() == 0) {
            continue;
        }

        // alarm if over half of the replicas are offline
        int deadBound =
            copysetInfo.peers.size() - (copysetInfo.peers.size() / 2 + 1);
        if (offlinelists.size() > deadBound) {
            LOG(ERROR) << "recoverSchdeuler find "
                       << copysetInfo.CopySetInfoStr() << " has "
                       << offlinelists.size()
                       << " replica offline, cannot repair, please check";
            continue;
        }

        // recover one of the offline replica
        MetaServerIdType target;
        // Operator newOp;

        if (!FixOfflinePeer(copysetInfo, *offlinelists.begin(), &op,
                                                                    &target)) {
            // failed to recover the replica
            continue;
        }

        if (!opController_->AddOperator(op)) {
            // succeeded but failed to add the operator to the controller
            LOG(WARNING) << "recover scheduler add operator "
                         << op.OpToString() << " on "
                         << copysetInfo.CopySetInfoStr() << " fail";
            continue;
        }

        LOG(INFO) << "recoverScheduler generate operator:"
                  << op.OpToString() << " for "
                  << copysetInfo.CopySetInfoStr()
                  << ", remove offlinePeer: " << *offlinelists.begin();
        // if the target returned has the initial value, that means offline
        // replicas are removed directly.
        if (target == UNINITIALIZE_ID) {
            oneRoundGenOp++;
            continue;
        }

        // if the target didn't return the initial value, that means copyset
        // should be generated on target. If failed to generate, delete the
        // operator.
        if (!topo_->CreateCopySetAtMetaServer(copysetInfo.id, target)) {
            LOG(WARNING) << "recoverScheduler create "
                         << copysetInfo.CopySetInfoStr()
                         << " on metaServer: " << target
                         << " error, delete operator" << op.OpToString();
            opController_->RemoveOperator(copysetInfo.id);
            continue;
        }
        oneRoundGenOp++;
    }
    LOG(INFO) << "recoverScheduler generate " << oneRoundGenOp
              << " operators at this round";
    return oneRoundGenOp;
}

int64_t RecoverScheduler::GetRunningInterval() { return runInterval_; }

bool RecoverScheduler::FixOfflinePeer(const CopySetInfo &info,
                                      MetaServerIdType offlinePeer,
                                      Operator *op, MetaServerIdType *target) {
    assert(op != nullptr);
    // check the standard number of replicas first
    PoolIdType poolId = info.id.first;
    auto standardReplicaNum = topo_->GetStandardReplicaNumInPool(poolId);
    if (standardReplicaNum <= 0) {
        LOG(WARNING) << "RecoverScheduler find pool " << poolId
                     << " standard num " << standardReplicaNum << " invalid";
        return false;
    }

    if (info.peers.size() > standardReplicaNum) {
        // remove the offline replica
        *op = operatorFactory.CreateRemovePeerOperator(
            info, offlinePeer, OperatorPriority::HighPriority);
        op->timeLimit = std::chrono::seconds(removeTimeSec_);
        *target = UNINITIALIZE_ID;
        return true;
    }

    // select peers to add
    MetaServerIdType msId = SelectBestPlacementMetaServer(info, offlinePeer);
    if (msId == UNINITIALIZE_ID) {
        LOG(WARNING) << "recoverScheduler can not select metaServer to repair "
                     << info.CopySetInfoStr()
                     << ", which replica: " << offlinePeer << " is offline";
        return false;
    }

    *op = operatorFactory.CreateChangePeerOperator(
        info, offlinePeer, msId, OperatorPriority::HighPriority);
    op->timeLimit = std::chrono::seconds(addTimeSec_);
    *target = msId;
    return true;
}

}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
