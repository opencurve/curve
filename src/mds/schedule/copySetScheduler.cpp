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
 * Created Date: Fri Dec 21 2018
 * Author: lixiaocui
 */

#include <glog/logging.h>
#include <cmath>
#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/operatorFactory.h"
#include "src/mds/schedule/scheduler_helper.h"

using ::curve::mds::topology::UNINTIALIZE_ID;

namespace curve {
namespace mds {
namespace schedule {
int CopySetScheduler::Schedule() {
    LOG(INFO) << "copysetScheduler begin";

    int res = 0;
    for (auto lid : topo_->GetLogicalpools()) {
        res = DoCopySetSchedule(lid);
    }
    return res;
}

int CopySetScheduler::DoCopySetSchedule(PoolIdType lid) {
    // 1. collect the chunkserver list and copyset list of the cluster, then
    //    collect copyset on every online chunkserver
    auto copysetList = topo_->GetCopySetInfosInLogicalPool(lid);
    auto chunkserverList = topo_->GetChunkServersInLogicalPool(lid);
    std::map<ChunkServerIdType, std::vector<CopySetInfo>> distribute;
    SchedulerHelper::CopySetDistributionInOnlineChunkServer(
        copysetList, chunkserverList, &distribute);
    if (distribute.empty()) {
        LOG(WARNING) << "no not-retired chunkserver in topology";
        return UNINTIALIZE_ID;
    }

    // 2. measure the average, range and standard deviation of number of copyset
    //    on chunkservers
    float avg;
    int range;
    float stdvariance;
    StatsCopysetDistribute(distribute, &avg, &range, &stdvariance);
    /**
     * 3. Set migration condition
     *    condition: range and average volume fall into a certain percentage
     *    defined, selection of source and target should be based on the number
     *    of copyset on chunkserver.
     *
     * consider a scenario:
     * - transfer a copyset from the chunkserver with more copysets to a
     *   chunkserver with less copysets. The scatter-width changes of peers
     *   in this copyset should satisfy following requirements:
     *   * if (scatter-width < minValue) of any peers, the change should not
     *     make the scatter-width smaller
     *   * if (scatter-width > maxValue) of any peers, the change should not
     *     make the scatter-width larger
     *   * if (minValue <= scatter-with <= maxValue) of any peers, the change
     *     should be fine
     * - the definition of minValue and maxValue:
     *   * the minValue is from the configuration
     *   * the maxValue is the number larger than minValue by a certain
     *     percentage, and this makes the range falls into a certain scope
     **/
    ChunkServerIdType source = UNINTIALIZE_ID;
    if (range <= avg * copysetNumRangePercent_) {
        return source;
    }

    Operator op;
    ChunkServerIdType target = UNINTIALIZE_ID;
    CopySetInfo choose;
    // this function call will select the source, target and the copyset
    if (CopySetMigration(distribute, &op, &source, &target, &choose)) {
        // add operator
        if (!opController_->AddOperator(op)) {
            LOG(INFO) << "copysetSchduler add op " << op.OpToString()
                      << " fail, copyset has already has operator";
        }

        // create copyset
        if (!topo_->CreateCopySetAtChunkServer(choose.id, target)) {
            LOG(ERROR) << "copysetScheduler create " << choose.CopySetInfoStr()
                       << " on chunkServer: " << target
                       << " error, delete operator" << op.OpToString();
            opController_->RemoveOperator(choose.id);
        } else {
            LOG(INFO) << "copysetScheduler create " << choose.CopySetInfoStr()
                      << "on chunkserver:" << target
                      << " success. generator op: "
                      << op.OpToString() << "success";
        }
    }

    LOG_EVERY_N(INFO, 20) << "copysetScheduler is continually adjusting";
    LOG(INFO) << "copysetScheduler end.";
    return static_cast<int>(source);
}

void CopySetScheduler::StatsCopysetDistribute(
    const std::map<ChunkServerIdType, std::vector<CopySetInfo>> &distribute,
    float *avg, int *range, float *stdvariance) {
    int num = 0;
    int max = -1;
    int min = -1;
    ChunkServerIdType maxcsId = UNINTIALIZE_ID;
    ChunkServerIdType mincsId = UNINTIALIZE_ID;
    float variance = 0;
    for (auto &item : distribute) {
        num += item.second.size();

        if (max == -1 || item.second.size() > max) {
            max = item.second.size();
            maxcsId = item.first;
        }

        if (min == -1 || item.second.size() < min) {
            min = item.second.size();
            mincsId = item.first;
        }
    }

    // average
    *avg = static_cast<float>(num) / distribute.size();

    // variance
    for (auto &item : distribute) {
        variance += std::pow(static_cast<int>(item.second.size()) - *avg, 2);
    }
    // range
    *range = max - min;

    // average variance
    variance /= distribute.size();
    *stdvariance = std::sqrt(variance);
    LOG(INFO) << "copyset scheduler stats copyset distribute (avg:"
              << *avg << ", {max:" << max << ",maxCsId:" << maxcsId
              << "}, {min:" << min << ",minCsId:" << mincsId
              << "}, range:" << *range << ", stdvariance:" << *stdvariance
              << ", variance:" << variance << ")";
}
/**
 * Migration Procedure:
 *      Purpose:
 *      for a chunkserver list (ranked chunkserver by the number of copyset they have).
 *      example: {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}[chunkserver-1 has the least copyset].
 *      we are now trying to find chunkserver-n (2<=n<=11) from chunkservers above,
 *      then choose copyset-m on chunkserver-n, transfer copyset-m from
 *      chunkserver-n to chunkserver-1: operator(copyset-m, +chunkserver-1, -chunkserver-n)
 *      Question:
 *      1. how to decide the chunkserver-n?
 *      2. how to decide the copyset-m after finding chunkserver-n?
 *      Steps in pseudocode:
 *      for chunkserver-n in {11, 10, 9, 8, 7, 6, 5, 4, 3, 2} (pick from the chunkserver with most copysets) //NOLINT
 *          for copyset-m in chunkserver-n
 *               copyset-m peers (a, b, n)
 *               1. operator(+1, -n) will effect the scatter-width map of chunkserver{a, b, n, 1}            //NOLINT
 *                  for the definition and example of scatter-width map, pls refer to scheduler.cpp          //NOLINT
 *               2. for chunkserver 1 (mark as the target of the copyset migration):                         //NOLINT
 *                  value of key{a,b} in the scatter-width map will increase 1
 *                  for chunkserver n (mark as the source of the copyset migration):                         //NOLINT
 *                  value of key{a,b} in the scatter-width map will decrease 1
 *                  for chunkserver a and b (mark as the other):
 *                  value of key{1} will increase 1, and key{n} will decrease 1
 *               3. condition:
 *                  if the peers of copyset-m become (a, b, 1) after the migration,                          //NOLINT
 *                  and the scatter-width of {a, b, 1, n} satisfy the condition,
 *                  generate Operator(copyset-m, +chunkserver-1) and wait for distribution.                  //NOLINT
 *                  after the execution of this operator, replicaScheduler will
 *                  select a replica to migrate the copyset out according to
 *                  similar rule. In a stable condition, the replicaScheduler
 *                  will migrate will pick chunkserver-n
 *          done
 *      done
 **/
bool CopySetScheduler::CopySetMigration(
    const std::map<ChunkServerIdType, std::vector<CopySetInfo>> &distribute,
    Operator *op, ChunkServerIdType *source, ChunkServerIdType *target,
    CopySetInfo *choose) {
    if (distribute.size() <= 1) {
        return false;
    }

    // sort for distribute
    std::vector<std::pair<ChunkServerIdType, std::vector<CopySetInfo>>> desc;
    SchedulerHelper::SortDistribute(distribute, &desc);
    // select the chunkserver with the least number of copysets as the target
    LOG(INFO) << "copyset scheduler after sort (max:" << desc[0].second.size()
        << ",maxCsId:" << desc[0].first
        << "), (min:" << desc[desc.size() - 1].second.size()
        << ",minCsId:" << desc[desc.size() - 1].first << ")";
    *target = desc[desc.size() - 1].first;
    int copysetNumInTarget = desc[desc.size() - 1].second.size();
    if (opController_->ChunkServerExceed(*target)) {
        LOG(INFO) << "copysetScheduler found target:"
                  << *target << " operator exceed";
        return false;
    }

    // select copyset and source
    *source = UNINTIALIZE_ID;
    for (auto it = desc.begin(); it != desc.end()--; it++) {
        // there shouldn't be any migration if the difference of copyset number
        // on possible source and target is less than 1
        ChunkServerIdType possibleSource = it->first;
        int copysetNumInPossible = it->second.size();
        if (copysetNumInPossible - copysetNumInTarget <= 1) {
            continue;
        }

        for (auto info : it->second) {
            // does not meet the basic conditions
            if (!CopySetSatisfiyBasicMigrationCond(info)) {
                continue;
            }

            // determine whether the scatter-width of every replica in the
            // copyset fulfill the requirement after +target, -source
            if (!SchedulerHelper::SatisfyZoneAndScatterWidthLimit(
                    topo_, *target, possibleSource, info,
                    GetMinScatterWidth(info.id.first),
                    scatterWidthRangePerent_)) {
                continue;
            }

            *source = possibleSource;
            *choose = info;
            break;
        }

        if (*source != UNINTIALIZE_ID) {
            break;
        }
    }

    if (*source != UNINTIALIZE_ID) {
        *op = operatorFactory.CreateChangePeerOperator(
            *choose, *source, *target, OperatorPriority::NormalPriority);
        op->timeLimit = std::chrono::seconds(changeTimeSec_);
        LOG(INFO) << "copyset scheduler gen " << op->OpToString() << " on "
                  << choose->CopySetInfoStr();
        return true;
    }
    return false;
}

bool CopySetScheduler::CopySetSatisfiyBasicMigrationCond(
    const CopySetInfo &info) {
    // operator exists on copyset
    Operator exist;
    if (opController_->GetOperatorById(info.id, &exist)) {
        return false;
    }
    if (info.HasCandidate()) {
        LOG(WARNING) << info.CopySetInfoStr()
            << " already has candidate: " << info.candidatePeerInfo.id;
        return false;
    }

    // the replica num of copyset is not standard
    if (info.peers.size() !=
        topo_->GetStandardReplicaNumInLogicalPool(info.id.first)) {
        return false;
    }

    // scatter-width has not be set on topology
    int minScatterWidth = GetMinScatterWidth(info.id.first);
    if (minScatterWidth <= 0) {
        LOG(WARNING) << "minScatterWith in logical pool "
                        << info.id.first << " is not initialized";
        return false;
    }

    // some peers are offline
    if (!CopysetAllPeersOnline(info)) {
        return false;
    }

    return true;
}

int64_t CopySetScheduler::GetRunningInterval() {
    return runInterval_;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
