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
 * Created Date: Mon Mar 25 2019
 * Author: lixiaocui
 */

#include <glog/logging.h>
#include "src/mds/heartbeat/topo_updater.h"

namespace curve {
namespace mds {
namespace heartbeat {
void TopoUpdater::UpdateTopo(const CopySetInfo &reportCopySetInfo) {
    CopySetInfo recordCopySetInfo;
    // decide whether a copyset has corresponding info in topology module
    if (!topo_->GetCopySet(
        reportCopySetInfo.GetCopySetKey(), &recordCopySetInfo)) {
        LOG(ERROR) << "topoUpdater receive copyset("
                   << reportCopySetInfo.GetLogicalPoolId()
                   << "," << reportCopySetInfo.GetId()
                   << ") information, but can not get info from topology";
        return;
    }
    // here we compare epoch number reported by heartbeat and stored in mds
    // record, and there're three possible cases:
    // 1. report epoch > mds record epoch
    //    after every configuration change, raft will increase the epoch of
    //    copyset, which makes epoch of mds fall behind.
    //    update on mds of epoch and copy relationship is required
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
                  << reportCopySetInfo.GetLogicalPoolId()
                  << "," << reportCopySetInfo.GetId()
                  << ") epoch:" << reportCopySetInfo.GetEpoch()
                  << " > recordEpoch:"
                  << recordCopySetInfo.GetEpoch() << " need to update";
        needUpdate = true;
    } else if (recordCopySetInfo.GetEpoch() == reportCopySetInfo.GetEpoch()) {
        // epoch reported is equal to epoch stored in mds

        // leader reported and recorded in topology are different
        if (reportCopySetInfo.GetLeader() != recordCopySetInfo.GetLeader()) {
            needUpdate = true;
        }

        // heartbeat report and mds record provide a different member list.
        // this should trigger an alarm since it should not happend in normal
        // cases, since chunkserver always report configurations that already
        // applied and when member list are different the epoch
        // should be different
        if (reportCopySetInfo.GetCopySetMembers() !=
            recordCopySetInfo.GetCopySetMembers()) {
            LOG(ERROR) << "topoUpdater find report copyset("
                       << reportCopySetInfo.GetLogicalPoolId()
                       << "," << reportCopySetInfo.GetId()
                       << ") member list: "
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
                LOG(WARNING) << "topoUpdater find report"
                             " copyset("
                             << reportCopySetInfo.GetLogicalPoolId()
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
            LOG(WARNING) << "topoUpdater find report candidate "
                         << reportCopySetInfo.GetCandidate()
                         << ", record candidate: "
                         << recordCopySetInfo.GetCandidate()
                         << " on copyset("
                         << reportCopySetInfo.GetLogicalPoolId()
                         << "," << reportCopySetInfo.GetId()
                         << ") not same";
            needUpdate = true;
        }
    } else if (recordCopySetInfo.GetEpoch() > reportCopySetInfo.GetEpoch()) {
        // this case will trigger an alarm since epoch of copyset leader should
        // always larger or equal than the epoch of mds record
        LOG(ERROR) << "topoUpdater find copyset("
                   << reportCopySetInfo.GetLogicalPoolId()
                   << "," << reportCopySetInfo.GetId()
                   << "), record epoch:" << recordCopySetInfo.GetEpoch()
                   << " bigger than report epoch:"
                   << reportCopySetInfo.GetEpoch();
        return;
    }

    // update changes to database and RAM
    if (needUpdate) {
        LOG(INFO) << "topoUpdater find copyset("
                  << reportCopySetInfo.GetLogicalPoolId() << ","
                  << reportCopySetInfo.GetId() << ") need to update";

        int updateCode =
            topo_->UpdateCopySetTopo(reportCopySetInfo);
        if (::curve::mds::topology::kTopoErrCodeSuccess != updateCode) {
            LOG(ERROR) << "topoUpdater update copyset("
                       << reportCopySetInfo.GetLogicalPoolId()
                       << "," << reportCopySetInfo.GetId()
                       << ") got error code: " << updateCode;
            return;
        }
    }
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve
