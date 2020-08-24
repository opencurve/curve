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
 * Created Date: Thur July 06th 2019
 * Author: lixiaocui
 */

#ifndef SRC_MDS_SCHEDULE_SCHEDULER_HELPER_H_
#define SRC_MDS_SCHEDULE_SCHEDULER_HELPER_H_

#include <map>
#include <vector>
#include <memory>
#include <utility>
#include "src/mds/common/mds_define.h"
#include "src/mds/schedule/topoAdapter.h"

using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::UNINTIALIZE_ID;

namespace curve {
namespace mds {
namespace schedule {
class SchedulerHelper {
 public:
    /**
     * @brief SatisfyScatterWidth Determine whether the scatter-width satisfy
     *                            the condition after the changing
     *
     * @param[in] target Whether the chunkserver is the target
     * @param[in] oldValue Scatter-width before changing
     * @param[in] newValue Scatter-width after changing
     * @param[in] minScatterWidth Minimum value of scatter-width
     * @param[in] scatterWidthRangePerent Scatter-width should nor exceed
     *            (1 + scatterWidthRangePerent) * minScatterWidth
     *
     * @return false when scatter-width condition is not satisfied
     *         after the migration，true of it is
     */
    static bool SatisfyScatterWidth(bool target, int oldValue, int newValue,
        int minScatterWidth, float scatterWidthRangePerent);

    /**
     * @brief SatisfyLimit Determine whether zone and scatter-width condition
     *                     is satisfied after migrating candidate from source
     *                     to target
     *
     * @param[in] topo Pointer to an instance of topology module
     * @param[in] target Target node, chunkserver that the replica migrate to
     * @param[in] targetZone Zone that the target node belongs to
     * @param[in] source Source of the migration
     * @param[in] candidate Copyset selected
     * @param[in] minScatterWidth Minimum value of scatter-width
     * @param[in] scatterWidthRangePerent scatter-width should not exceed
     *            (1 + scatterWidthRangePerent) * minScatterWidth
     *
     * @return true if limit is met false if not
     */
    static bool SatisfyZoneAndScatterWidthLimit(
        const std::shared_ptr<TopoAdapter> &topo, ChunkServerIdType target,
        ChunkServerIdType source, const CopySetInfo &candidate,
        int minScatterWidth, float scatterWidthRangePerent);

    /**
     * @brief CalculateAffectOfMigration calculate the scatter-width of
     *        {A,B,source,target} before and after the migration from source
     *        to target
     *
     * @param[in] copySetInfo Copyset selected
     * @param[in] source Chunkserver to migrate out, which is still a replica in
     *                   current copyset
     *                   only new replica is added if source == UNINTIALIZE_ID
     * @param[in] target Chunkserver that thereplica will migrate to.
     *                   target==UINITIALIZED if only remove replica from
     * @param[in] topo   For topology info
     * @param[out] scatterWidth The scatter-width result of chunkserver {A,B,
     *                          source,target} before and after the execution of
     *                          (+target, -source).
     *                          the key of the map is the chunkserver ID
     *                          value.first is the scatter-width before the
     *                          migration, and value.second is the value after
     */
    static void CalculateAffectOfMigration(
        const CopySetInfo &copySetInfo, ChunkServerIdType source,
        ChunkServerIdType target, const std::shared_ptr<TopoAdapter> &topo,
        std::map<ChunkServerIdType, std::pair<int, int>> *scatterWidth);

    /**
     * @brief InvovledReplicasSatisfyScatterWidthAfterMigration
     *        for copyset(A,B,source) and operation (+taget, -source),
     *        calculate whether the scatter-width of {A,B,source,target}
     *        satisfy the limitation.
     *
     * @param[in] copySetInfo Specified copyset
     * @param[in] source Chunkserver to migrate out, which is still a replica in
     *                   current copyset
     *                   only new replica is added if source == UNINTIALIZE_ID
     * @param[in] target Chunkserver that thereplica will migrate to.
     *                   target==UINITIALIZED if only remove replica from
     * @param[in] ignore For ignoring the changing of the scatter-width, this is
     *                   usually when the source is offline
     * @param[in] topo For topology info
     * @param[in] minScatterWidth The minimum value of scatter-width
     * @param[in] scatterWidthRangePerent Scatter-width should not exceed value
     *            (1 + scatterWidthRangePerent) * minScatterWidth
     * @param[out] affected The sum of the scatter-width changing of replica
     *                      {A,B,source,target} caused by the migration
     *
     * @return true if every scatter-width of {A,B,source,target} satisfy the
     *         requirement , false if any of them doesn't
     */
    static bool InvovledReplicasSatisfyScatterWidthAfterMigration(
        const CopySetInfo &copySetInfo, ChunkServerIdType source,
        ChunkServerIdType target, ChunkServerIdType ignore,
        const std::shared_ptr<TopoAdapter> &topo,
        int minScatterWidth, float scatterWidthRangePerent, int *affected);

    /**
     * @brief SortDistribute Sort chunkservers by copyset number in descending order //NOLINT
     *
     * @param[in] distribute The distribution for sorting
     * @param[out] desc The result in descending order
     */
    static void SortDistribute(
        const std::map<ChunkServerIdType, std::vector<CopySetInfo>> &distribute,
        std::vector<std::pair<ChunkServerIdType,
                              std::vector<CopySetInfo>>> *desc);

    /**
     * @brief SortScatterWitAffected Sort the input by the sum of the
     *                               scatter-width difference (the value of the pair of the input) //NOLINT
     *
     * @param[in] candidates
     */
    static void SortScatterWitAffected(
        std::vector<std::pair<ChunkServerIdType, int>> *candidates);

    /**
     * @brief SortChunkServerByCopySetNumAsc Rank chunkserver by the number of the copyset //NOLINT
     *
     * @param[in/out] chunkserverList The chunkserver to be ranked
     * @param[in] topo Topology info
     */
    static void SortChunkServerByCopySetNumAsc(
        std::vector<ChunkServerInfo> *chunkserverList,
        const std::shared_ptr<TopoAdapter> &topo);

    /**
     * @brief CopySetDistribution Calculate the copyset number on
     *                            chunkserver in online status
     *
     * @param[in] copysetList Copyset list in topo info
     * @param[in] chunkserverList Chunkserver list in topo info
     * @param[out] out List of copyset on chunkserver
     */
    static void CopySetDistributionInOnlineChunkServer(
        const std::vector<CopySetInfo> &copysetList,
        const std::vector<ChunkServerInfo> &chunkserverList,
        std::map<ChunkServerIdType, std::vector<CopySetInfo>> *out);
};
}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_SCHEDULER_HELPER_H_
