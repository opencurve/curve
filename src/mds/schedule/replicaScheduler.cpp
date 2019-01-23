/*
 * Project: curve
 * Created Date: Fri Dec 21 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/operatorFactory.h"

namespace curve {
namespace mds {
namespace schedule {
int ReplicaScheduler::Schedule(const std::shared_ptr<TopoAdapter> &topo) {
    LOG(INFO) << "replicaScheduelr begin.";
    int oneRoundGenOp = 0;
    for (auto copysetInfo : topo->GetCopySetInfos()) {
        // 如果copyset上面已经有operator,跳过
        Operator op;
        if (opController_->GetOperatorById(copysetInfo.id, &op)) {
            continue;
        }

        // 如果copyset有配置变更的信息(增加副本，减少副本，leader变更)，跳过
        // 这种情况发生在mds重启的时候, operator不做持久化会丢失，
        // 实际正在进行配置变更
        if (copysetInfo.configChangeInfo.IsInitialized()) {
            LOG(WARNING) << "copySet(" << copysetInfo.id.first
                         << "," << copysetInfo.id.second
                         << ") configchangeInfo has been initialized but "
                         "operator lost";
            continue;
        }

        int standardReplicaNum =
            topo->GetStandardReplicaNumInLogicalPool(copysetInfo.id.first);
        int copysetReplicaNum = copysetInfo.peers.size();

        if (copysetReplicaNum == standardReplicaNum) {
            // 副本数量等于标准值
            continue;
        } else if (copysetReplicaNum < standardReplicaNum) {
            // 副本数量小于标准值， 一次增加一个副本
            LOG(ERROR) << "replicaScheduler find copyset("
                       << copysetInfo.id.first << "," << copysetInfo.id.second
                       << ") replicaNum:" << copysetReplicaNum
                       << " smaller than standardReplicaNum:"
                       << standardReplicaNum;

            ChunkServerIdType csId =
                    topo->SelectBestPlacementChunkServer(copysetInfo, -1);
            if (csId == ::curve::mds::topology::UNINTIALIZE_ID) {
                LOG(ERROR) << "replicaScheduler can not select chunkServer"
                             "to repair copySet(logicalPoolId: "
                           << copysetInfo.id.first << ",copySetId: "
                           << copysetInfo.id.second << "), witch only has "
                           << copysetReplicaNum << " but statandard is "
                           << standardReplicaNum;
                continue;
            }

            Operator op = operatorFactory.CreateAddPeerOperator(
                    copysetInfo, csId, OperatorPriority::HighPriority);
            op.timeLimit = std::chrono::seconds(GetAddPeerTimeLimitSec());
            if (opController_->AddOperator(op)) {
                LOG(WARNING) << "replicaScheduler find copyset("
                             << copysetInfo.id.first << ","
                             << copysetInfo.id.second
                             << ") replicaNum:" << copysetReplicaNum
                             << " smaller than standardReplicaNum:"
                             << standardReplicaNum << "but cannot apply"
                             "operator right now";
                oneRoundGenOp += 1;
            }
        } else {
            // 副本数量大于标准值， 一次移除一个副本
            LOG(ERROR) << "replicaScheduler find copyset("
                       << copysetInfo.id.first << "," << copysetInfo.id.second
                       << ") replicaNum:" << copysetReplicaNum
                       << " larger than standardReplicaNum:"
                       << standardReplicaNum;

            ChunkServerIdType csId =
                    topo->SelectRedundantReplicaToRemove(copysetInfo);
            if (csId == ::curve::mds::topology::UNINTIALIZE_ID) {
                LOG(WARNING) << "replicaScheduler can not select redundent "
                             "replica to remove on copySet(logicalPoolId: "
                             << copysetInfo.id.first << ",copySetId: "
                             << copysetInfo.id.second << "), witch has "
                             << copysetReplicaNum << " but standard is "
                             << standardReplicaNum;
                continue;
            }

            Operator op = operatorFactory.CreateRemovePeerOperator(
                    copysetInfo, csId, OperatorPriority::HighPriority);
            op.timeLimit = std::chrono::seconds(GetRemovePeerTimeLimitSec());
            if (opController_->AddOperator(op)) {
                oneRoundGenOp += 1;
            }
        }
    }
    return oneRoundGenOp;
}

int64_t ReplicaScheduler::GetRunningInterval() {
    return this->runInterval_;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
