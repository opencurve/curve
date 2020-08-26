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
#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/operatorFactory.h"

using ::curve::mds::topology::UNINTIALIZE_ID;

namespace curve {
namespace mds {
namespace schedule {
int ReplicaScheduler::Schedule() {
    LOG(INFO) << "replicaScheduelr begin.";
    // 这个是所有copyset上产生的operator总数
    int oneRoundGenOp = 0;
    // 扫描全部copyset
    for (auto info : topo_->GetCopySetInfos()) {
        // skip if there's any operator on a copyset
        Operator op;
        if (opController_->GetOperatorById(info.id, &op)) {
            continue;
        }

        // it will be skipped if there's any configuration change on a copyset.
        // this case would happen when the MDS is restarted, and the operator
        // without persistence will lost.
        // configuration change is actually happening.
        if (info.HasCandidate()) {
            LOG(WARNING) << info.CopySetInfoStr()
                         << " has candidate " << info.candidatePeerInfo.id
                         << " but operator lost";
            continue;
        }

        int standardReplicaNum =
            topo_->GetStandardReplicaNumInLogicalPool(info.id.first);
        int copysetReplicaNum = info.peers.size();

        if (copysetReplicaNum == standardReplicaNum) {
            // replica number is equal to the standard
            continue;
        } else if (copysetReplicaNum < standardReplicaNum) {
            // add one replica a time when the replica number is smaller than
            // the standard.
            LOG(ERROR) << "replicaScheduler find "
                       << info.CopySetInfoStr()
                       << " replicaNum:" << copysetReplicaNum
                       << " smaller than standardReplicaNum:"
                       << standardReplicaNum;

            ChunkServerIdType csId =
                SelectBestPlacementChunkServer(info, UNINTIALIZE_ID);
            // can't find a suitable chunkserver for the new replica
            if (csId == UNINTIALIZE_ID) {
                LOG(WARNING) << "replicaScheduler can not select chunkServer"
                             "to repair "
                           << info.CopySetInfoStr() << ", witch only has "
                           << copysetReplicaNum << " but statandard is "
                           << standardReplicaNum;
                continue;
            }

            Operator op = operatorFactory.CreateAddPeerOperator(
                    info, csId, OperatorPriority::HighPriority);
            op.timeLimit = std::chrono::seconds(addTimeSec_);
            if (!opController_->AddOperator(op)) {
                LOG(WARNING) << "replicaScheduler find "
                             << info.CopySetInfoStr()
                             << ") replicaNum:" << copysetReplicaNum
                             << " smaller than standardReplicaNum:"
                             << standardReplicaNum << " but cannot apply"
                             "operator right now";
                continue;
            // create copyset on target chunkserver
            } else if (!topo_->CreateCopySetAtChunkServer(info.id, csId)) {
                LOG(WARNING) << "replicaScheduler create "
                               << info.CopySetInfoStr()
                               << ") on chunkServer: " << csId << " error";
                opController_->RemoveOperator(info.id);
                continue;
            }
            LOG(INFO) << "replicaScheduler create "
                      << info.CopySetInfoStr()
                      << ") on chunkServer: " << csId
                      << " success and generate operator: "
                      << op.OpToString();
            oneRoundGenOp += 1;
        } else {
            // remove one replica a time when the replica number is larger than
            // the standard.
            LOG(WARNING) << "replicaScheduler find " << info.CopySetInfoStr()
                       << " replicaNum:" << copysetReplicaNum
                       << " larger than standardReplicaNum:"
                       << standardReplicaNum;

            ChunkServerIdType csId =
                SelectRedundantReplicaToRemove(info);
            if (csId == UNINTIALIZE_ID) {
                LOG(WARNING) << "replicaScheduler can not select redundent "
                             "replica to remove on "
                             << info.CopySetInfoStr() << "), witch has "
                             << copysetReplicaNum << " but standard is "
                             << standardReplicaNum;
                continue;
            }

            Operator op = operatorFactory.CreateRemovePeerOperator(
                    info, csId, OperatorPriority::HighPriority);
            op.timeLimit = std::chrono::seconds(removeTimeSec_);
            if (opController_->AddOperator(op)) {
                LOG(INFO) << "replicaScheduler generate operator "
                          << op.OpToString() << " on " << info.CopySetInfoStr();
                oneRoundGenOp += 1;
            }
        }
    }
    LOG(INFO) << "replicaScheduelr generate "
              << oneRoundGenOp << " at this round";
    return 1;
}

int64_t ReplicaScheduler::GetRunningInterval() {
    return this->runInterval_;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
