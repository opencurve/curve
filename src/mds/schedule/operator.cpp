/*
 * Project: curve
 * Created Date: Mon Nov 18 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <string>
#include "src/mds/schedule/operator.h"

namespace curve {
namespace mds {
namespace schedule {
Operator::Operator(EpochType startEpoch,
                   const CopySetKey &id,
                   OperatorPriority pri,
                   const steady_clock::time_point &timeLimit,
                   std::shared_ptr<OperatorStep> step) {
    this->startEpoch = startEpoch;
    this->copsetID.first = id.first;
    this->copsetID.second = id.second;
    this->createTime = steady_clock::now();
    this->priority = pri;
    this->step = step;
}

ApplyStatus Operator::Apply(const CopySetInfo &originInfo,
                            CopySetConf *newInfo) {
    if (!originInfo.configChangeInfo.IsInitialized() &&
        originInfo.epoch > this->startEpoch) {
        LOG(ERROR) << "Operator on copySet(logicalPoolId: "
                   << originInfo.id.first
                   << ",copySetId: " << originInfo.id.second
                   << ") is stale, latest epoch: " << originInfo.epoch
                   << ", operator start epoch: " << this->startEpoch;
        return ApplyStatus::Failed;
    }
    return step->Apply(originInfo, newInfo);
}

std::vector<ChunkServerIdType> Operator::AffectedChunkServers() const {
    std::vector<ChunkServerIdType> affects;
    auto instance = dynamic_cast<AddPeer *>(step.get());
    if (instance != nullptr) {
        affects.emplace_back(instance->GetTargetPeer());
    }
    return affects;
}

bool Operator::IsTimeout() {
    return steady_clock::now() - createTime > timeLimit;
}

std::string Operator::OpToString() {
    return "[startEpoch: " + std::to_string(startEpoch)
        + ", copysetID: (" + std::to_string(copsetID.first) + ","
        + std::to_string(copsetID.second) + ", priority: "
        + std::to_string(priority) + "]";
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
