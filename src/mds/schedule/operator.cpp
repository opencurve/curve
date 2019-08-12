/*
 * Project: curve
 * Created Date: Mon Nov 18 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <string>
#include <memory>
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
    this->copysetID.first = id.first;
    this->copysetID.second = id.second;
    this->createTime = steady_clock::now();
    this->priority = pri;
    this->step = step;
}

ApplyStatus Operator::Apply(const CopySetInfo &originInfo,
                            CopySetConf *newInfo) {
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

std::string Operator::OpToString() const {
    return "[startEpoch: " + std::to_string(startEpoch)
        + ", copysetID: (" + std::to_string(copysetID.first) + ","
        + std::to_string(copysetID.second) + "), priority: "
        + std::to_string(priority) + ", step: "
        + step->OperatorStepToString() + "]";
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
