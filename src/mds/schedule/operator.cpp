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
 * Created Date: Mon Nov 18 2018
 * Author: lixiaocui
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

    auto cinstance = dynamic_cast<ChangePeer *>(step.get());
    if (cinstance != nullptr) {
        affects.emplace_back(cinstance->GetTargetPeer());
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
