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
 * Created Date: 20190704
 * Author: lixiaocui
 */

#ifndef SRC_MDS_SCHEDULE_SCHEDULEMETRICSTEMPLATE_H_
#define SRC_MDS_SCHEDULE_SCHEDULEMETRICSTEMPLATE_H_

#include <bvar/bvar.h>
#include <map>
#include <memory>
#include <set>
#include <string>
#include "src/common/stringstatus.h"
#include "src/mds/schedule/operatorStepTemplate.h"
#include "src/mds/schedule/operatorTemplate.h"

namespace curve {
namespace mds {
namespace schedule {
using ::curve::mds::heartbeat::ConfigChangeType;
using ::curve::common::StringStatus;

const char ADDPEER[] = "AddPeer";
const char REMOVEPEER[] = "RemovePeer";
const char TRANSFERLEADER[] = "TransferLeader";
const char CHANGEPEER[] = "ChangePeer";
const char NORMAL[] = "Normal";
const char HIGH[] = "High";

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
class ScheduleMetricsT {
 public:
    using Operator = OperatorT<IdType, CopySetInfoT, CopySetConfT>;
    using RemovePeer = RemovePeerT<IdType, CopySetInfoT, CopySetConfT>;
    using AddPeer = AddPeerT<IdType, CopySetInfoT, CopySetConfT>;
    using TransferLeader = TransferLeaderT<IdType, CopySetInfoT, CopySetConfT>;
    using ChangePeer = ChangePeerT<IdType, CopySetInfoT, CopySetConfT>;

 public:
    explicit ScheduleMetricsT(std::shared_ptr<TopologyT> topo)
        : operatorNum(ScheduleMetricsPrefix, "operator_num"),
          addOpNum(ScheduleMetricsPrefix, "addPeer_num"),
          removeOpNum(ScheduleMetricsPrefix, "removePeer_num"),
          transferOpNum(ScheduleMetricsPrefix, "transferLeader_num"),
          changeOpNum(ScheduleMetricsPrefix, "changePeer_num"),
          normalOpNum(ScheduleMetricsPrefix, "normal_operator_num"),
          highOpNum(ScheduleMetricsPrefix, "high_operator_num"),
          topo_(topo) {}

    /**
     * @brief UpdateAddMetric Interface exposed to operatorContoller for
     *                        updating metric when adding operator
     *
     * @param[in] op Specific operator
     */
    void UpdateAddMetric(const Operator &op);

    /**
     * @brief UpdateMetric Interface exposed to operatorContoller for updating
     *                     metric when deleting operator
     *
     * @param[in] op Specific operator
     */
    void UpdateRemoveMetric(const Operator &op);

 private:
    /**
     * @brief GetOpPriorityStr Get the name of the priority level in string
     *
     * @param[in] pri Priority (OperatorPriority, which is an enum)
     *
     * @return corresponding level in string
     */
    std::string GetOpPriorityStr(OperatorPriority pri);

    /**
     * @brief RemoveUpdateOperatorsMap Update operator map when removing one of
     *                                 them
     *
     * @param[in] op Specified operator
     * @param[in] type Operator type, including
     * AddPeer/RemovePeer/TransferLeader //NOLINT
     * @param[in] target Target chunkserver to change
     */
    void RemoveUpdateOperatorsMap(const Operator &op, std::string type,
                                  IdType target);

    /**
     * @brief AddUpdateOperatorsMap Update operator map when adding a new one
     *
     * @param[in] op Specified operator
     * @param[in] type Operator type, including
     * AddPeer/RemovePeer/TransferLeader //NOLINT
     * @param[in] target Target chunkserver to change
     */
    void AddUpdateOperatorsMap(const Operator &op, std::string type,
                               IdType target);

    /**
     * @brief UpdateOperatorsMap Construct operator map for exporting and
     *                           transfer to JSON format
     *
     * @param[in] op Specified operator
     * @param[in] type Operator type, including
     * AddPeer/RemovePeer/TransferLeader //NOLINT
     * @param[in] target Target chunkserver
     */
    void UpdateOperatorsMap(const Operator &op, std::string type,
                            IdType target);

 public:
    const std::string ScheduleMetricsPrefix = "mds_scheduler_metric_";
    const std::string ScheduleMetricsCopySetOpPrefix =
        "mds_scheduler_metric_copyset_";

    // number of operator under execution
    bvar::Adder<uint32_t> operatorNum;
    // xxxNUM: number of operator xxx under execution
    bvar::Adder<uint32_t> addOpNum;
    bvar::Adder<uint32_t> removeOpNum;
    bvar::Adder<uint32_t> transferOpNum;
    bvar::Adder<uint32_t> changeOpNum;
    bvar::Adder<uint32_t> normalOpNum;
    bvar::Adder<uint32_t> highOpNum;
    // specific operator under execution
    std::map<CopySetKey, StringStatus> operators;

 private:
    std::shared_ptr<TopologyT> topo_;
};

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
void ScheduleMetricsT<IdType, CopySetInfoT, CopySetConfT, TopologyT,
                      TopoCopySetInfoT>::UpdateAddMetric(const Operator &op) {
    // operator num
    operatorNum << 1;

    // high operator
    if (op.priority == OperatorPriority::HighPriority) {
        highOpNum << 1;
    }

    // normal operator
    if (op.priority == OperatorPriority::NormalPriority) {
        normalOpNum << 1;
    }

    // add operator
    if (dynamic_cast<AddPeer *>(op.step.get()) != nullptr) {
        // update the counter
        addOpNum << 1;
        // update operators map
        AddUpdateOperatorsMap(op, ADDPEER, op.step->GetTargetPeer());
    }

    // remove operator
    if (dynamic_cast<RemovePeer *>(op.step.get()) != nullptr) {
        removeOpNum << 1;
        AddUpdateOperatorsMap(op, REMOVEPEER, op.step->GetTargetPeer());
    }

    // transfer leader operator
    if (dynamic_cast<TransferLeader *>(op.step.get()) != nullptr) {
        transferOpNum << 1;
        AddUpdateOperatorsMap(op, TRANSFERLEADER, op.step->GetTargetPeer());
    }

    // change peer operator
    if (dynamic_cast<ChangePeer *>(op.step.get()) != nullptr) {
        changeOpNum << 1;
        AddUpdateOperatorsMap(op, CHANGEPEER, op.step->GetTargetPeer());
    }
}

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
void ScheduleMetricsT<IdType, CopySetInfoT, CopySetConfT, TopologyT,
                      TopoCopySetInfoT>::UpdateRemoveMetric(const Operator
                                                                &op) {
    // operator num
    operatorNum << -1;

    // high operator
    if (op.priority == OperatorPriority::HighPriority) {
        highOpNum << -1;
    }

    // normal operator
    if (op.priority == OperatorPriority::NormalPriority) {
        normalOpNum << -1;
    }

    // add operator
    if (dynamic_cast<AddPeer *>(op.step.get()) != nullptr) {
        addOpNum << -1;
        RemoveUpdateOperatorsMap(op, ADDPEER, op.step->GetTargetPeer());
    }

    // remove operator
    if (dynamic_cast<RemovePeer *>(op.step.get()) != nullptr) {
        removeOpNum << -1;
        RemoveUpdateOperatorsMap(op, REMOVEPEER, op.step->GetTargetPeer());
    }

    // transfer leader operator
    if (dynamic_cast<TransferLeader *>(op.step.get()) != nullptr) {
        transferOpNum << -1;
        RemoveUpdateOperatorsMap(op, TRANSFERLEADER, op.step->GetTargetPeer());
    }

    // change peer operator
    if (dynamic_cast<ChangePeer *>(op.step.get()) != nullptr) {
        changeOpNum << -1;
        RemoveUpdateOperatorsMap(op, CHANGEPEER, op.step->GetTargetPeer());
    }
}

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
void ScheduleMetricsT<
    IdType, CopySetInfoT, CopySetConfT, TopologyT,
    TopoCopySetInfoT>::RemoveUpdateOperatorsMap(const Operator &op,
                                                std::string type,
                                                IdType target) {
    (void)type;
    (void)target;

    auto findOp = operators.find(op.copysetID);
    if (findOp == operators.end()) {
        return;
    }

    operators.erase(findOp);
}

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
void ScheduleMetricsT<IdType, CopySetInfoT, CopySetConfT, TopologyT,
                      TopoCopySetInfoT>::AddUpdateOperatorsMap(const Operator
                                                                   &op,
                                                               std::string type,
                                                               IdType target) {
    auto findOp = operators.find(op.copysetID);
    if (findOp != operators.end()) {
        return;
    }

    // add operator to the map
    UpdateOperatorsMap(op, type, target);
}

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
void ScheduleMetricsT<IdType, CopySetInfoT, CopySetConfT, TopologyT,
                      TopoCopySetInfoT>::UpdateOperatorsMap(const Operator &op,
                                                            std::string type,
                                                            IdType target) {
    operators[op.copysetID].ExposeAs(ScheduleMetricsCopySetOpPrefix,
                                     std::to_string(op.copysetID.first) + "_" +
                                         std::to_string(op.copysetID.second));

    // set logicalpoolId
    operators[op.copysetID].Set("logicalPoolId",
                                std::to_string(op.copysetID.first));
    // set copysetId
    operators[op.copysetID].Set("copySetId",
                                std::to_string(op.copysetID.second));
    // set operator startepoch
    operators[op.copysetID].Set("startEpoch", std::to_string(op.startEpoch));

    TopoCopySetInfoT out;
    if (!topo_->GetCopySet(op.copysetID, &out)) {
        LOG(INFO) << "get copyset(" << op.copysetID.first << ","
                  << op.copysetID.second << ") info error";
        return;
    }

    // set copyset epoch
    operators[op.copysetID].Set("copySetEpoch", std::to_string(out.GetEpoch()));

    // set copysetPeers
    std::string copysetLeaderPeer;
    std::string copysetPeers;
    std::set<IdType> members = out.GetCopySetMembers();
    int count = 0;
    for (auto peerId : members) {
        count++;
        std::string hostPort = topo_->GetHostNameAndPortById(peerId);
        if (peerId == out.GetLeader()) {
            copysetLeaderPeer = hostPort;
        }
        if (count == static_cast<int>(members.size())) {
            copysetPeers += hostPort;
        } else {
            copysetPeers += hostPort + ",";
        }
    }
    operators[op.copysetID].Set("copySetPeers", copysetPeers);

    // set leader
    if (copysetLeaderPeer.empty()) {
        operators[op.copysetID].Set("copySetLeader", "UNINTIALIZE_ID");
    } else {
        operators[op.copysetID].Set("copySetLeader", copysetLeaderPeer);
    }

    // set operator priority
    operators[op.copysetID].Set("opPriority", GetOpPriorityStr(op.priority));

    // set operator type and item
    operators[op.copysetID].Set("opItem",
                                topo_->GetHostNameAndPortById(target));
    operators[op.copysetID].Set("opType", type);

    // update
    operators[op.copysetID].Update();
}

template <class IdType, class CopySetInfoT, class CopySetConfT, class TopologyT,
          class TopoCopySetInfoT>
std::string
ScheduleMetricsT<IdType, CopySetInfoT, CopySetConfT, TopologyT,
                 TopoCopySetInfoT>::GetOpPriorityStr(OperatorPriority pri) {
    switch (pri) {
        case OperatorPriority::HighPriority:
            return HIGH;
        case OperatorPriority::NormalPriority:
            return NORMAL;
        default:
            return "";
    }
}

}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_SCHEDULEMETRICSTEMPLATE_H_
