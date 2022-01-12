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

#ifndef SRC_MDS_SCHEDULE_OPERATORCONTROLLERTEMPLATE_H_
#define SRC_MDS_SCHEDULE_OPERATORCONTROLLERTEMPLATE_H_

#include <map>
#include <memory>
#include <vector>
#include "src/mds/schedule/operatorTemplate.h"
#include "src/mds/schedule/scheduleMetricsTemplate.h"
#include "src/mds/topology/topology.h"

namespace curve {
namespace mds {
namespace schedule {
template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
class OperatorControllerT {
 public:
    using Operator = OperatorT<IdType, CopySetInfoT, CopySetConfT>;

    using ScheduleMetrics =
        ScheduleMetricsT<IdType, CopySetInfoT, CopySetConfT, TopologyT,
                            TopoCopySetInfoT>;

 public:
    OperatorControllerT() = default;
    explicit OperatorControllerT(int concurent,
                                    std::shared_ptr<ScheduleMetrics> metric);
    ~OperatorControllerT() = default;

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
    bool ApplyOperator(const CopySetInfoT &originInfo, CopySetConfT *newConf);

    /**
     * @brief Exceed Check whether the number of operator on
     *                          chunkserver has reach the concurrency limit
     *
     * @param[in] id ID of chunkserv specified
     *
     * @return true if reach the limit, false if not
     */
    bool Exceed(IdType id);

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
    std::map<IdType, int> opInfluence_;
    std::mutex mutex_;

    std::shared_ptr<ScheduleMetrics> metrics_;
};

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
OperatorControllerT<IdType, CopySetInfoT, CopySetConfT, TopologyT,
                       TopoCopySetInfoT>::
    OperatorControllerT(int concurrent,
                           std::shared_ptr<ScheduleMetrics> metrics) {
    this->operatorConcurrent_ = concurrent;
    this->metrics_ = metrics;
}

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
bool OperatorControllerT<IdType, CopySetInfoT, CopySetConfT, TopologyT,
                            TopoCopySetInfoT>::AddOperator(const Operator &op) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto exist = operators_.find(op.copysetID);
    // no operator exist
    if (exist == operators_.end()) {
        // concurrency exceed
        if (!AddOpInfluencePreJudgeLocked(op)) {
            LOG(INFO) << "add operator " << op.OpToString()
                      << " fail because of oncurrency exceed";
            return false;
        }
        operators_[op.copysetID] = op;
        UpdateAddOpInfluenceLocked(op);
        metrics_->UpdateAddMetric(op);
        return true;
    }

    // operator priority is higher, just replace the low-pri operator
    // this is safe, because if the low-pri operator is executing,
    // the replaced-high-pri operator will be failed and removed, otherwise the
    // replaced-high-pri operator will be executed.
    if (exist->second.priority < op.priority) {
        if (!ReplaceOpInfluencePreJudgeLocked(exist->second, op)) {
            LOG(ERROR) << "replace operator on copyset(" << op.copysetID.first
                       << "," << op.copysetID.second
                       << ") fail because do new operator "
                          "do not satisfy opInfluence condition";
            return false;
        } else {
            operators_[op.copysetID] = op;
            UpdateReplaceOpInfluenceLocked(exist->second, op);
            metrics_->UpdateAddMetric(op);
            return true;
        }
    }

    return false;
}

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
void OperatorControllerT<IdType, CopySetInfoT, CopySetConfT, TopologyT,
                            TopoCopySetInfoT>::RemoveOperator(const CopySetKey
                                                                  &key) {
    std::lock_guard<std::mutex> guard(mutex_);
    RemoveOperatorLocked(key);
}

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
void OperatorControllerT<
    IdType, CopySetInfoT, CopySetConfT, TopologyT,
    TopoCopySetInfoT>::RemoveOperatorLocked(const CopySetKey &key) {
    auto exist = operators_.find(key);
    if (exist == operators_.end()) {
        return;
    }
    UpdateRemoveOpInfluenceLocked(exist->second);
    metrics_->UpdateRemoveMetric(exist->second);
    operators_.erase(key);
}

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
std::vector<OperatorT<IdType, CopySetInfoT, CopySetConfT>>
OperatorControllerT<IdType, CopySetInfoT, CopySetConfT, TopologyT,
                       TopoCopySetInfoT>::GetOperators() {
    std::lock_guard<std::mutex> guard(mutex_);
    std::vector<Operator> ops;
    for (auto &op : operators_) {
        ops.emplace_back(op.second);
    }
    return ops;
}

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
bool OperatorControllerT<IdType, CopySetInfoT, CopySetConfT, TopologyT,
                            TopoCopySetInfoT>::GetOperatorById(const CopySetKey
                                                                   &id,
                                                               Operator *op) {
    assert(op != nullptr);

    std::lock_guard<std::mutex> guard(mutex_);
    if (operators_.find(id) == operators_.end()) {
        return false;
    }
    *op = operators_[id];
    return true;
}

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
bool OperatorControllerT<IdType, CopySetInfoT, CopySetConfT, TopologyT,
                            TopoCopySetInfoT>::Exceed(IdType id) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (opInfluence_.find(id) == opInfluence_.end()) {
        return false;
    }
    return opInfluence_[id] >= operatorConcurrent_;
}

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
bool OperatorControllerT<
    IdType, CopySetInfoT, CopySetConfT, TopologyT,
    TopoCopySetInfoT>::ApplyOperator(const CopySetInfoT &originInfo,
                                     CopySetConfT *newConf) {
    assert(newConf != nullptr);

    std::lock_guard<std::mutex> guard(mutex_);
    // no operator
    if (operators_.find(originInfo.id) == operators_.end()) {
        return false;
    }

    // operator timeout or finish
    if (operators_[originInfo.id].IsTimeout()) {
        LOG(WARNING) << "apply operator: "
                     << operators_[originInfo.id].OpToString() << " on "
                     << originInfo.CopySetInfoStr()
                     << " fail, operator is timeout";
        RemoveOperatorLocked(originInfo.id);
        return false;
    } else {
        auto res = operators_[originInfo.id].Apply(originInfo, newConf);
        switch (res) {
            case ApplyStatus::Failed:
                LOG(WARNING) << "apply operator"
                             << operators_[originInfo.id].OpToString() << " on "
                             << originInfo.CopySetInfoStr() << " failed";
                RemoveOperatorLocked(originInfo.id);
                return false;
            case ApplyStatus::Finished:
                LOG(INFO) << "apply operator "
                          << operators_[originInfo.id].OpToString() << " on "
                          << originInfo.CopySetInfoStr() << " ok";
                RemoveOperatorLocked(originInfo.id);
                return false;
            case ApplyStatus::Ordered:
                return true;
            case ApplyStatus::OnGoing:
                return false;
        }
    }

    return false;
}

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
void OperatorControllerT<
    IdType, CopySetInfoT, CopySetConfT, TopologyT,
    TopoCopySetInfoT>::UpdateReplaceOpInfluenceLocked(const Operator &oldOp,
                                                      const Operator &newOp) {
    UpdateRemoveOpInfluenceLocked(oldOp);
    UpdateAddOpInfluenceLocked(newOp);
}

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
void OperatorControllerT<
    IdType, CopySetInfoT, CopySetConfT, TopologyT,
    TopoCopySetInfoT>::UpdateAddOpInfluenceLocked(const Operator &op) {
    for (auto csId : op.AffectedChunkServers()) {
        opInfluence_[csId]++;
    }
}

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
void OperatorControllerT<
    IdType, CopySetInfoT, CopySetConfT, TopologyT,
    TopoCopySetInfoT>::UpdateRemoveOpInfluenceLocked(const Operator &op) {
    for (auto csId : op.AffectedChunkServers()) {
        opInfluence_[csId]--;
    }
}

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
bool OperatorControllerT<
    IdType, CopySetInfoT, CopySetConfT, TopologyT,
    TopoCopySetInfoT>::ReplaceOpInfluencePreJudgeLocked(const Operator &oldOp,
                                                        const Operator &newOp) {
    std::map<IdType, int> influenceList;
    for (auto csId : oldOp.AffectedChunkServers()) {
        if (influenceList.find(csId) == influenceList.end()) {
            influenceList[csId] = -1;
        } else {
            influenceList[csId] -= 1;
        }
    }

    for (auto csId : newOp.AffectedChunkServers()) {
        influenceList[csId]++;
    }

    for (auto csId : influenceList) {
        if (opInfluence_[csId.first] + csId.second > operatorConcurrent_) {
            return false;
        }
    }
    return true;
}

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
bool OperatorControllerT<
    IdType, CopySetInfoT, CopySetConfT, TopologyT,
    TopoCopySetInfoT>::AddOpInfluencePreJudgeLocked(const Operator &op) {
    std::map<IdType, int> influenceList;
    for (auto csId : op.AffectedChunkServers()) {
        influenceList[csId]++;
    }
    for (auto csId : influenceList) {
        if (opInfluence_[csId.first] + csId.second > operatorConcurrent_) {
            return false;
        }
    }
    return true;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_OPERATORCONTROLLERTEMPLATE_H_
