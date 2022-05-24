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
 * Project: curve
 * Created Date: 2021-09-16
 * Author: chenwei
 */

#include "curvefs/src/mds/heartbeat/topo_updater.h"

#include <glog/logging.h>

namespace curvefs {
namespace mds {
namespace heartbeat {
using curvefs::mds::topology::TopoStatusCode;

void TopoUpdater::UpdateCopysetTopo(
    const ::curvefs::mds::topology::CopySetInfo& reportCopySetInfo) {
    curvefs::mds::topology::CopySetInfo recordCopySetInfo;
    if (!topo_->GetCopySet(reportCopySetInfo.GetCopySetKey(),
                           &recordCopySetInfo)) {
        LOG(ERROR) << "topoUpdater receive copyset("
                   << reportCopySetInfo.GetPoolId() << ","
                   << reportCopySetInfo.GetId()
                   << ") information, but can not get info from topology";
        return;
    }

    // here we compare epoch number reported by heartbeat and stored in mds
    // record, and there're three possible cases:
    // 1. report epoch > mds record epoch
    //    after every configuration change, raft will increase the epoch of
    //    copyset, which makes epoch of mds fall behind.
    //    update on mds of epoch and replicas relationship is required
    // 2. report epoch == mds record epoch
    //    there's no finished or undergoing configuration changes,
    //    recording candidate to mds may be needed
    // 3. report epoch < mds epoch
    //    this case should not occurs normally since epoch number in raft is
    //    always the most up-to-date. this case may caused by bugs,
    //    alarm is neened.

    // mds epoch fall behind, update needed:
    bool needUpdate = false;
    if (recordCopySetInfo.GetEpoch() < reportCopySetInfo.GetEpoch()) {
        LOG(INFO) << "topoUpdater find report copyset("
                  << reportCopySetInfo.GetPoolId() << ","
                  << reportCopySetInfo.GetId()
                  << ") epoch:" << reportCopySetInfo.GetEpoch()
                  << " > recordEpoch:" << recordCopySetInfo.GetEpoch()
                  << " need to update";
        needUpdate = true;
    } else if (recordCopySetInfo.GetEpoch() == reportCopySetInfo.GetEpoch()) {
        // epoch reported is equal to epoch stored in mds

        // leader reported and recorded in topology are different
        if (reportCopySetInfo.GetLeader() != recordCopySetInfo.GetLeader()) {
            needUpdate = true;
        }

        // heartbeat report and mds record provide a different member list.
        // this should trigger an alarm since it should not happend in normal
        // cases, since metaserver always report configurations that already
        // applied and when member list are different the epoch
        // should be different
        if (reportCopySetInfo.GetCopySetMembers() !=
            recordCopySetInfo.GetCopySetMembers()) {
            LOG(ERROR) << "topoUpdater find report copyset("
                       << reportCopySetInfo.GetPoolId() << ","
                       << reportCopySetInfo.GetId() << ") member list: "
                       << reportCopySetInfo.GetCopySetMembersStr()
                       << " is not same as record one: "
                       << recordCopySetInfo.GetCopySetMembersStr()
                       << ", but epoch is same: "
                       << recordCopySetInfo.GetEpoch();
            return;
        }

        // no configuration changes in heartbeat report (no candidate)
        if (!reportCopySetInfo.HasCandidate()) {
            // configuration changes on mds (has candidate)
            if (recordCopySetInfo.HasCandidate()) {
                LOG(WARNING) << "metasever " << reportCopySetInfo.GetLeader()
                        << " heartbeat,topoUpdater find report copyset("
                        << reportCopySetInfo.GetPoolId()
                        << "," << reportCopySetInfo.GetId()
                        << ") no candidate but record has candidate: "
                        << recordCopySetInfo.GetCandidate();
                needUpdate = true;
            }
        } else if (!recordCopySetInfo.HasCandidate()) {
            // configuration changes reported but not on mds
            needUpdate = true;
        } else if (reportCopySetInfo.GetCandidate() !=
                   recordCopySetInfo.GetCandidate()) {
            // reported data and mds record have different configuration changes
            LOG(WARNING) << "metaserver " << reportCopySetInfo.GetLeader()
                    << " heartbeat, topoUpdater find report candidate "
                    << reportCopySetInfo.GetCandidate()
                    << ", record candidate: "
                    << recordCopySetInfo.GetCandidate()
                    << " on copyset("
                    << reportCopySetInfo.GetPoolId()
                    << "," << reportCopySetInfo.GetId()
                    << ") not same";
            needUpdate = true;
        }
    } else if (recordCopySetInfo.GetEpoch() > reportCopySetInfo.GetEpoch()) {
        // this case will trigger an alarm since epoch of copyset leader should
        // always larger or equal than the epoch of mds record
        LOG(ERROR) << "topoUpdater find copyset("
                   << reportCopySetInfo.GetPoolId() << ","
                   << reportCopySetInfo.GetId()
                   << "), record epoch:" << recordCopySetInfo.GetEpoch()
                   << " bigger than report epoch:"
                   << reportCopySetInfo.GetEpoch();
        return;
    }

    // update changes to database and RAM
    if (needUpdate) {
        LOG(INFO) << "topoUpdater find copyset("
                  << reportCopySetInfo.GetPoolId() << ","
                  << reportCopySetInfo.GetId() << ") need to update";

        int updateCode = topo_->UpdateCopySetTopo(reportCopySetInfo);
        if (::curvefs::mds::topology::TopoStatusCode::TOPO_OK != updateCode) {
            LOG(ERROR) << "topoUpdater update copyset("
                       << reportCopySetInfo.GetPoolId() << ","
                       << reportCopySetInfo.GetId()
                       << ") got error code: " << updateCode;
            return;
        }
    }
}

bool TopoUpdater::CanPartitionStatusChange(PartitionStatus statusInTopo,
                                           PartitionStatus statusInHeartbeat) {
    bool statusCanChange = true;
    switch (statusInHeartbeat) {
        case PartitionStatus::READWRITE:
            if (statusInTopo != PartitionStatus::READWRITE) {
                LOG(WARNING) << "partition cann't changes from other "
                                "status to READWRITE.";
                statusCanChange = false;
            }
            break;
        case PartitionStatus::READONLY:
            if (statusInTopo == PartitionStatus::DELETING) {
                LOG(WARNING) << "partition cann't changes from DELETING "
                                "status to READONLY.";
                statusCanChange = false;
            }
            break;
        case PartitionStatus::DELETING:
        default:
            break;
    }
    return statusCanChange;
}

void TopoUpdater::UpdatePartitionTopo(
    CopySetIdType copySetId,
    const std::list<::curvefs::mds::topology::Partition>& partitionList) {
    std::list<::curvefs::mds::topology::Partition> topoPartitionList =
        topo_->GetPartitionInfosInCopyset(copySetId);
    // partition in topology, not in heartbeat
    for (auto itTopo = topoPartitionList.begin();
         itTopo != topoPartitionList.end();) {
        bool isFound = false;
        for (const auto& itHeartbeat : partitionList) {
            if (itTopo->GetPartitionId() == itHeartbeat.GetPartitionId()) {
                isFound = true;
                break;
            }
        }
        // if partition in topology and not in heartbeat,
        // and partition is deleting status,
        // delete this partition in topo
        if (!isFound && itTopo->GetStatus() == PartitionStatus::DELETING) {
            LOG(INFO) << "partition in topology and not in heartbeat, and"
                      << " partition status is DELETING, delete this partiton "
                      << "in topology, copysetId = " << copySetId
                      << ", partitionId = " << itTopo->GetPartitionId();
            TopoStatusCode ret =
                topo_->RemovePartition(itTopo->GetPartitionId());
            if (ret == TopoStatusCode::TOPO_OK ||
                ret == TopoStatusCode::TOPO_PARTITION_NOT_FOUND) {
                itTopo = topoPartitionList.erase(itTopo);
                continue;
            } else {
                LOG(WARNING)
                    << "removePartition fail, copysetId = " << copySetId
                    << ", partitionId = " << itTopo->GetPartitionId()
                    << ", status = " << TopoStatusCode_Name(ret);
            }
        }
        itTopo++;
    }

    for (const auto& it : partitionList) {
        // partition in heartbeat, not in topology, abnormal
        ::curvefs::mds::topology::Partition partitionInTopo;
        bool isFound =
            topo_->GetPartition(it.GetPartitionId(), &partitionInTopo);
        if (!isFound) {
            LOG(WARNING) << "hearbeat report partition which is not in topo"
                         << ", copysetId = " << copySetId
                         << ", partitionId = " << it.GetPartitionId();
            continue;
        }

        // partition both in heartbeat and in topology
        bool statusCanChange = CanPartitionStatusChange(
            partitionInTopo.GetStatus(), it.GetStatus());

        bool statisticChange =
            partitionInTopo.GetStatus() != it.GetStatus() ||
            partitionInTopo.GetInodeNum() != it.GetInodeNum() ||
            partitionInTopo.GetDentryNum() != it.GetDentryNum();
        if (statusCanChange && statisticChange) {
            ::curvefs::mds::topology::PartitionStatistic statistic;
            statistic.status = it.GetStatus();
            statistic.inodeNum = it.GetInodeNum();
            statistic.dentryNum = it.GetDentryNum();
            statistic.fileType2InodeNum = it.GetFileType2InodeNum();
            topo_->UpdatePartitionStatistic(it.GetPartitionId(), statistic);
        }
    }
    return;
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curvefs
