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


#ifndef SRC_MDS_SCHEDULE_OPERATOR_H_
#define SRC_MDS_SCHEDULE_OPERATOR_H_

#include <vector>
#include <string>
#include <memory>
#include "src/mds/schedule/topoAdapter.h"
#include "src/mds/schedule/operatorStep.h"
#include "src/mds/topology/topology_item.h"
#include "src/mds/common/mds_define.h"

using ::curve::mds::topology::EpochType;
using ::curve::mds::topology::CopySetKey;
using ::std::chrono::steady_clock;

namespace curve {
namespace mds {
namespace schedule {

enum OperatorPriority {
  LowPriority,
  NormalPriority,
  HighPriority
};

class Operator {
 public:
  Operator() = default;
  Operator(EpochType startEpoch, const CopySetKey &id, OperatorPriority pri,
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
  ApplyStatus Apply(const CopySetInfo &originInfo, CopySetConf *newInfo);

  /**
   * @brief list of the chunkserver affected by the operation. The overhead of
   *        TransferLeader and RemovePeer is rather small, so we don't consider
   *        the chunkservers involved. But for AddPeer operation, data copying
   *        is required, and thus chunkservers involved are considered affected
   *
   * @return set of affected chunkServers
   */
  std::vector<ChunkServerIdType> AffectedChunkServers() const;

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
}  // namespace schedule
}  // namespace mds
}  // namespace curve
#endif  // SRC_MDS_SCHEDULE_OPERATOR_H_
