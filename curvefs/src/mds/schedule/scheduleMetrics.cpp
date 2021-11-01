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

#include <set>
#include "curvefs/src/mds/schedule/scheduleMetrics.h"
#include "curvefs/src/mds/topology/topology.h"

namespace curvefs {
namespace mds {
namespace schedule {
const char ADDPEER[] = "AddPeer";
const char REMOVEPEER[] = "RemovePeer";
const char TRANSFERLEADER[] = "TransferLeader";
const char CHANGEPEER[] = "ChangePeer";
const char NORMAL[] = "Normal";
const char HIGH[] = "High";

void ScheduleMetrics::UpdateAddMetric(const Operator &op) {
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

void ScheduleMetrics::UpdateRemoveMetric(const Operator &op) {
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
        RemoveUpdateOperatorsMap(
            op, TRANSFERLEADER, op.step->GetTargetPeer());
    }

    // change peer operator
    if (dynamic_cast<ChangePeer *>(op.step.get()) != nullptr) {
        changeOpNum << -1;
        RemoveUpdateOperatorsMap(
            op, CHANGEPEER, op.step->GetTargetPeer());
    }
}

void ScheduleMetrics::RemoveUpdateOperatorsMap(
    const Operator &op, std::string type, MetaServerIdType target) {
    auto findOp = operators.find(op.copysetID);
    if (findOp == operators.end()) {
        return;
    }

    operators.erase(findOp);
}

void ScheduleMetrics::AddUpdateOperatorsMap(
    const Operator &op, std::string type, MetaServerIdType target) {
    auto findOp = operators.find(op.copysetID);
    if (findOp != operators.end()) {
        return;
    }

    // add operator to the map
    UpdateOperatorsMap(op, type, target);
}

void ScheduleMetrics::UpdateOperatorsMap(
    const Operator &op, std::string type, MetaServerIdType target) {

    operators[op.copysetID].ExposeAs(ScheduleMetricsCopySetOpPrefix,
                                 std::to_string(op.copysetID.first) +
                                 "_" + std::to_string(op.copysetID.second));

    // set poolId
    operators[op.copysetID].Set(
        "poolId", std::to_string(op.copysetID.first));
    // set copysetId
    operators[op.copysetID].Set(
        "copySetId", std::to_string(op.copysetID.second));
    // set operator startepoch
    operators[op.copysetID].Set(
        "startEpoch", std::to_string(op.startEpoch));

    ::curvefs::mds::topology::CopySetInfo out;
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
    std::set<MetaServerIdType> members = out.GetCopySetMembers();
    int count = 0;
    for (auto peerId : members) {
        count++;
        std::string hostPort = GetHostNameAndPortById(peerId);
        if (peerId == out.GetLeader()) {
            copysetLeaderPeer = hostPort;
        }
        if (count == members.size()) {
            copysetPeers += hostPort;
        } else {
            copysetPeers += hostPort + ",";
        }
    }
    operators[op.copysetID].Set("copySetPeers", copysetPeers);

    // set leader
    if (copysetLeaderPeer.empty()) {
        operators[op.copysetID].Set("copySetLeader", "UNINITIALIZE_ID");
    } else {
        operators[op.copysetID].Set("copySetLeader", copysetLeaderPeer);
    }

    // set operator priority
    operators[op.copysetID].Set("opPriority", GetOpPriorityStr(op.priority));

    // set operator type and item
    operators[op.copysetID].Set("opItem", GetHostNameAndPortById(target));
    operators[op.copysetID].Set("opType", type);

    // update
    operators[op.copysetID].Update();
}

std::string ScheduleMetrics::GetHostNameAndPortById(MetaServerIdType csid) {
    // get target metaserver
    ::curvefs::mds::topology::MetaServer ms;
    if (!topo_->GetMetaServer(csid, &ms)) {
        LOG(INFO) << "get metaserver " << csid << " err";
        return "";
    }

    // get the server of the target metaserver
    ::curvefs::mds::topology::Server server;
    if (!topo_->GetServer(ms.GetServerId(), &server)) {
        LOG(INFO) << "get server " << ms.GetServerId() << " err";
        return "";
    }

    // get hostName of the metaserver
    return server.GetHostName() + ":" + std::to_string(ms.GetInternalPort());
}

std::string ScheduleMetrics::GetOpPriorityStr(OperatorPriority pri) {
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
}  // namespace curvefs

