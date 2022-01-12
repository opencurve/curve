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
 * Created Date: Apr Dec 7 2021
 * Author: huyao
 */

#include <glog/logging.h>
#include <algorithm>
#include "src/common/timeutility.h"
#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/operatorFactory.h"
#include "src/mds/schedule/operatorStep.h"

namespace curve {
namespace mds {
namespace schedule {

int ScanScheduler::Schedule() {
    LOG(INFO) << "ScanScheduler begin.";

    auto currentHour = ::curve::common::TimeUtility::GetCurrentHour();
    bool duringScanTime = currentHour >= scanStartHour_ &&
                          currentHour <= scanEndHour_;

    auto count = 0;
    ::curve::mds::topology::LogicalPool lpool;
    auto logicPoolIds = topo_->GetLogicalpools();
    for (const auto& lpid : logicPoolIds) {
        CopySetInfos copysets2start, copysets2cancel;
        auto copysetInfos = topo_->GetCopySetInfosInLogicalPool(lpid);
        topo_->GetLogicalPool(lpid, &lpool);
        if (!duringScanTime || !lpool.ScanEnable()) {
            for (const auto& copysetInfo : copysetInfos) {
                if (StartOrReadyToScan(copysetInfo)) {
                    copysets2cancel.push_back(copysetInfo);
                }
            }
        } else {
            SelectCopysetsForScan(
                copysetInfos, &copysets2start, &copysets2cancel);
        }

        count += GenScanOperator(copysets2start,
                                 ConfigChangeType::START_SCAN_PEER);
        count += GenScanOperator(copysets2cancel,
                                 ConfigChangeType::CANCEL_SCAN_PEER);
    }

    LOG(INFO) << "ScanScheduelr generate "
              << count << " operators at this round";
    return 1;
}

bool ScanScheduler::StartOrReadyToScan(const CopySetInfo& copysetInfo) {
    Operator op;
    if (copysetInfo.scaning) {
        return true;
    } else if (opController_->GetOperatorById(copysetInfo.id, &op)) {
        auto step = dynamic_cast<ScanPeer*>(op.step.get());
        return nullptr != step && step->IsStartScanOp();
    } else if (copysetInfo.HasCandidate()) {
        return copysetInfo.configChangeInfo.type() ==
               ConfigChangeType::START_SCAN_PEER;
    }

    return false;
}

void ScanScheduler::SelectCopysetsToStartScan(CopySetInfos* copysetInfos,
                                              int count,
                                              Selected* selected,
                                              CopySetInfos* copysets2start) {
    std::sort(copysetInfos->begin(), copysetInfos->end(),
              [](const CopySetInfo& a, const CopySetInfo& b) {
                  if (a.lastScanSec == b.lastScanSec) {
                      return a.id < b.id;
                  }
                  return a.lastScanSec < b.lastScanSec;
              });

    auto nowSec = ::curve::common::TimeUtility::GetTimeofDaySec();
    for (const auto& copysetInfo : *copysetInfos) {
        if (nowSec - copysetInfo.lastScanSec < scanIntervalSec_ || count <= 0) {
            return;
        }

        bool succ = true;
        for (const auto& peer : copysetInfo.peers) {
            if ((*selected)[peer.id] >= scanConcurrentPerChunkserver_) {
                succ = false;
                break;
            }
        }

        // The copyset can be select
        if (succ) {
            count--;
            copysets2start->push_back(copysetInfo);
            for (const auto& peer : copysetInfo.peers) {
                (*selected)[peer.id]++;
            }
        }
    }
}

void ScanScheduler::SelectCopysetsToCancelScan(CopySetInfos* copysetInfos,
                                               int count,
                                               CopySetInfos* copysets2cancel) {
    std::sort(copysetInfos->begin(), copysetInfos->end(),
              [](const CopySetInfo& a, const CopySetInfo& b) {
                  if (a.scaning == b.scaning) {
                      return a.id < b.id;
                  }
                  return a.scaning == false;
              });

    for (const auto& copysetInfo : *copysetInfos) {
        if (count-- <= 0) {
            return;
        }
        copysets2cancel->push_back(copysetInfo);
    }
}

void ScanScheduler::SelectCopysetsForScan(const CopySetInfos& copysetInfos,
                                          CopySetInfos* copysets2start,
                                          CopySetInfos* copysets2cancel) {
    CopySetInfos scaning, nonScan;
    Selected selected;  // scaning chunk server
    for (const auto& copysetInfo : copysetInfos) {
        if (StartOrReadyToScan(copysetInfo)) {
            scaning.push_back(copysetInfo);
            for (const auto& peer : copysetInfo.peers) {
                selected[peer.id]++;
            }
            LOG(INFO) << "Copyset is on scaning: "
                      << copysetInfo.CopySetInfoStr();
        } else {
            nonScan.push_back(copysetInfo);
        }
    }

    auto nScaning = scaning.size();
    if (nScaning < scanConcurrentPerPool_) {
        int count = scanConcurrentPerPool_ - nScaning;
        copysets2start->reserve(count);
        SelectCopysetsToStartScan(&nonScan, count, &selected, copysets2start);
    } else if (nScaning > scanConcurrentPerPool_) {
        int count = nScaning - scanConcurrentPerPool_;
        copysets2cancel->reserve(count);
        SelectCopysetsToCancelScan(&scaning, count, copysets2cancel);
    }
}

int ScanScheduler::GenScanOperator(const CopySetInfos& copysetInfos,
                                   ConfigChangeType opType) {
    auto count = 0;
    bool ready2start = (opType == ConfigChangeType::START_SCAN_PEER);
    for (auto& copysetInfo : copysetInfos) {
        auto priority = ready2start ? OperatorPriority::LowPriority
                                    : OperatorPriority::HighPriority;

        auto op = operatorFactory.CreateScanPeerOperator(
            copysetInfo, copysetInfo.leader, priority, opType);
        op.timeLimit = std::chrono::seconds(scanTimeSec_);

        auto succ = opController_->AddOperator(op);
        count += succ ? 1 : 0;
        LOG(INFO) << "Generate operator " << op.OpToString()
                  << " for " << copysetInfo.CopySetInfoStr()
                  << (succ ? " success" : " fail");
    }

    return count;
}

int64_t ScanScheduler::GetRunningInterval() {
    return runInterval_;
}

}  // namespace schedule
}  // namespace mds
}  // namespace curve
