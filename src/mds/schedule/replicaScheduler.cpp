/*
 * Project: curve
 * Created Date: Fri Dec 21 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
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
    int oneRoundGenOp = 0;
    for (auto info : topo_->GetCopySetInfos()) {
        // 如果copyset上面已经有operator,跳过
        Operator op;
        if (opController_->GetOperatorById(info.id, &op)) {
            continue;
        }

        // 如果copyset有配置变更的信息(增加副本，减少副本，leader变更)，跳过
        // 这种情况发生在mds重启的时候, operator不做持久化会丢失，
        // 实际正在进行配置变更
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
            // 副本数量等于标准值
            continue;
        } else if (copysetReplicaNum < standardReplicaNum) {
            // 副本数量小于标准值， 一次增加一个副本
            LOG(ERROR) << "replicaScheduler find "
                       << info.CopySetInfoStr()
                       << " replicaNum:" << copysetReplicaNum
                       << " smaller than standardReplicaNum:"
                       << standardReplicaNum;

            ChunkServerIdType csId =
                SelectBestPlacementChunkServer(info, UNINTIALIZE_ID);
            // 未能找到合适的目标节点
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
            // 在目标节点上创建copyset
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
            // 副本数量大于标准值， 一次移除一个副本
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
