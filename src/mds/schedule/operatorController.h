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
 * Created Date: Thu Nov 15 2018
 * Author: lixiaocui
 */


#ifndef SRC_MDS_SCHEDULE_OPERATORCONTROLLER_H_
#define SRC_MDS_SCHEDULE_OPERATORCONTROLLER_H_

#include <map>
#include <vector>
#include <memory>
#include "src/mds/schedule/operator.h"
#include "src/mds/schedule/topoAdapter.h"
#include "src/mds/topology/topology.h"

namespace curve {
namespace mds {
namespace schedule {
class ScheduleMetrics;

class OperatorController {
 public:
    OperatorController() = default;
    explicit OperatorController(
        int concurent, std::shared_ptr<ScheduleMetrics> metric);
    ~OperatorController() = default;

    bool AddOperator(const Operator &op);

    void RemoveOperator(const CopySetKey &key);

    bool GetOperatorById(const CopySetKey &id, Operator *op);

    std::vector<Operator> GetOperators();

    /**
     * @brief execute operator
     *
     * @param op the operator to be executed
     * @param originInfo copyset info reported by chunkserver
     * @param newConf configuration generated for copyset
     *
     * @return if newConf is assigned return true else return false
     */
    bool ApplyOperator(const CopySetInfo &originInfo,
                        CopySetConf *newConf);

    /**
     * @brief ChunkServerExceed Check whether the number operator on chunkserver
     *                          has reach the concurrency limit
     *
     * @param[in] id ID of chunkserv specified
     *
     * @return true if reach the limit, false if not
     */
    bool ChunkServerExceed(ChunkServerIdType id);

 private:
    /**
     * @brief update influence of replacing operator
     */
    void UpdateReplaceOpInfluenceLocked(const Operator &oldOp,
                                        const Operator &newOp);

    /**
     * @brief update the influence of adding an operator
     */
    void UpdateAddOpInfluenceLocked(const Operator &op);

    /**
     * @brief update influence about remove operator
     */
    void UpdateRemoveOpInfluenceLocked(const Operator &op);

    /**
     * @brief judge the operator will exceed concurrency if replace
     */
    bool ReplaceOpInfluencePreJudgeLocked(const Operator &oldOp,
                                            const Operator &newOp);

    /**
     * @brief judge whether the operator will
     *        exceed concurrency limit if replace
     */
    bool AddOpInfluencePreJudgeLocked(const Operator &op);

    void RemoveOperatorLocked(const CopySetKey &key);

 private:
    int operatorConcurrent_;
    std::map<CopySetKey, Operator> operators_;
    std::map<ChunkServerIdType, int> opInfluence_;
    std::mutex mutex_;

    std::shared_ptr<ScheduleMetrics> metrics_;
};

}  // namespace schedule
}  // namespace mds
}  // namespace curve


#endif  // SRC_MDS_SCHEDULE_OPERATORCONTROLLER_H_
