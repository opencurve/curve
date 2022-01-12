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
 * Created Date: Sun Nov 17 2018
 * Author: lixiaocui
 */

#ifndef SRC_MDS_SCHEDULE_OPERATORTEMPLATE_H_
#define SRC_MDS_SCHEDULE_OPERATORTEMPLATE_H_

#include <memory>
#include <string>
#include <vector>
#include "src/mds/common/mds_define.h"
#include "src/mds/schedule/operatorStepTemplate.h"
#include "src/mds/topology/topology_item.h"

namespace curve {
namespace mds {
namespace schedule {
using ::curve::mds::topology::EpochType;
using ::curve::mds::topology::CopySetKey;
using ::std::chrono::steady_clock;

enum OperatorPriority { LowPriority, NormalPriority, HighPriority };

template <class IdType, class CopySetInfoT, class CopySetConfT>
class OperatorT {
 public:
    using OperatorStep =
        OperatorStepT<IdType, CopySetInfoT, CopySetConfT>;
    using AddPeer = AddPeerT<IdType, CopySetInfoT, CopySetConfT>;
    using ChangePeer = ChangePeerT<IdType, CopySetInfoT, CopySetConfT>;

 public:
    OperatorT() = default;
    OperatorT(EpochType startEpoch, const CopySetKey &id,
                 OperatorPriority pri,
                 const steady_clock::time_point &createTime,
                 std::shared_ptr<OperatorStep> step);
    /**
     * @brief execute operator
     *
     * @param originInfo copyset info reported by the chunkserver
     * @param newInfo the configuration change (also a copyset info)
     *                that the scheduler generate
     *
     * @return ApplyStatus, showing the status of the execution
     *         (finish or not / fail or success)
     */
    ApplyStatus Apply(const CopySetInfoT &originInfo,
                      CopySetConfT *newInfo);

    /**
     * @brief list of the chunkserver affected by the operation. The overhead of
     *        TransferLeader and RemovePeer is rather small, so we don't
     * consider
     *        the chunkservers involved. But for AddPeer operation, data copying
     *        is required, and thus chunkservers involved are considered
     * affected
     *
     * @return set of affected chunkServers
     */
    std::vector<IdType> AffectedChunkServers() const;

    bool IsTimeout();

    std::string OpToString() const;

 public:
    EpochType startEpoch;
    // CopySetKey is a pair, first-logicalPoolId, second-copysetId
    CopySetKey copysetID;
    steady_clock::time_point createTime;
    OperatorPriority priority;
    // TODO(lixiaocui): use template instead
    std::shared_ptr<OperatorStep> step;
    steady_clock::duration timeLimit;
};

template <class IdType, class CopySetInfoT, class CopySetConfT>
OperatorT<IdType, CopySetInfoT, CopySetConfT>::OperatorT(
    EpochType startEpoch, const CopySetKey &id, OperatorPriority pri,
    const steady_clock::time_point &timeLimit,
    std::shared_ptr<OperatorStep> step) {
    this->startEpoch = startEpoch;
    this->copysetID.first = id.first;
    this->copysetID.second = id.second;
    this->createTime = steady_clock::now();
    this->priority = pri;
    this->step = step;
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
ApplyStatus OperatorT<IdType, CopySetInfoT, CopySetConfT>::Apply(
    const CopySetInfoT &originInfo, CopySetConfT *newInfo) {
    return step->Apply(originInfo, newInfo);
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
std::vector<IdType> OperatorT<
    IdType, CopySetInfoT, CopySetConfT>::AffectedChunkServers() const {
    std::vector<IdType> affects;
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

template <class IdType, class CopySetInfoT, class CopySetConfT>
bool OperatorT<IdType, CopySetInfoT, CopySetConfT>::IsTimeout() {
    return steady_clock::now() - createTime > timeLimit;
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
std::string OperatorT<IdType, CopySetInfoT, CopySetConfT>::OpToString()
    const {
    return "[startEpoch: " + std::to_string(startEpoch) + ", copysetID: (" +
           std::to_string(copysetID.first) + "," +
           std::to_string(copysetID.second) + "), priority: " +
           std::to_string(priority) + ", step: " +
           step->OperatorStepToString() + "]";
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
#endif  // SRC_MDS_SCHEDULE_OPERATORTEMPLATE_H_
