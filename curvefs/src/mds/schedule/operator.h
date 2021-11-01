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

#ifndef CURVEFS_SRC_MDS_SCHEDULE_OPERATOR_H_
#define CURVEFS_SRC_MDS_SCHEDULE_OPERATOR_H_

#include <memory>
#include <string>
#include <vector>
#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/src/mds/schedule/operatorStep.h"
#include "curvefs/src/mds/schedule/topoAdapter.h"
#include "curvefs/src/mds/topology/topology_item.h"

using ::curvefs::mds::topology::EpochType;
using ::curvefs::mds::topology::CopySetKey;
using ::std::chrono::steady_clock;

namespace curvefs {
namespace mds {
namespace schedule {

enum OperatorPriority { LowPriority, NormalPriority, HighPriority };
// TODO(chenwei) : reuse curvebs code
class Operator {
 public:
    Operator() = default;
    Operator(EpochType startEpoch, const CopySetKey &id, OperatorPriority pri,
             const steady_clock::time_point &createTime,
             std::shared_ptr<OperatorStep> step);
    /**
     * @brief execute operator
     *
     * @param originInfo copyset info reported by the metaserver
     * @param newInfo the configuration change (also a copyset info)
     *                that the scheduler generate
     *
     * @return ApplyStatus, showing the status of the execution
     *         (finish or not / fail or success)
     */
    ApplyStatus Apply(const CopySetInfo &originInfo, CopySetConf *newInfo);

    /**
     * @brief list of the metaserver affected by the operation. The overhead of
     *        TransferLeader and RemovePeer is rather small, so we don't
     * consider
     *        the metaservers involved. But for AddPeer operation, data copying
     *        is required, and thus metaservers involved are considered affected
     *
     * @return set of affected metaServers
     */
    std::vector<MetaServerIdType> AffectedMetaServers() const;

    bool IsTimeout();

    std::string OpToString() const;

 public:
    EpochType startEpoch;
    // CopySetKey is a pair, first-poolId, second-copysetId
    CopySetKey copysetID;
    steady_clock::time_point createTime;
    OperatorPriority priority;
    std::shared_ptr<OperatorStep> step;
    steady_clock::duration timeLimit;
};
}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
#endif  // CURVEFS_SRC_MDS_SCHEDULE_OPERATOR_H_
