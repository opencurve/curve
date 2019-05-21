/*
 * Project: curve
 * Created Date: Thur July 06th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
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
     * @brief SatisfyScatterWidth 变更之后的scatter-width是否满足条件
     *
     * @param[in] target 是否为迁入节点
     * @param[in] oldValue 变更之前的scatter-width
     * @param[in] newValue 变更之后的scatter-width
     * @param[in] minScatterWidth scatter-width最小值
     * @param[in] scatterWidthRangePerent scatter-width不能超过
     *            (1 + scatterWidthRangePerent) * minScatterWidth
     *
     * @return false-迁移之后不满足scatter-width条件，true-迁移过后满足条件
     */
    static bool SatisfyScatterWidth(bool target, int oldValue, int newValue,
        int minScatterWidth, float scatterWidthRangePerent);

    /**
     * @brief SatisfyLimit 判断candidate的从source迁移到target是否满足zone和
     *                     scatter-width的条件
     *
     * @param[in] topo 拓扑逻辑
     * @param[in] target 迁移目标节点，即迁入节点
     * @param[in] targetZone 目标节点所在的zone
     * @param[in] source 迁移源节点，即迁出节点
     * @param[in] candidate 选中的copyset
     * @param[in] minScatterWidth scatter-width最小值
     * @param[in] scatterWidthRangePerent scatter-width不能超过
     *            (1 + scatterWidthRangePerent) * minScatterWidth
     *
     * @return false-满足条件; false-不满足条件
     */
    static bool SatisfyZoneAndScatterWidthLimit(
        const std::shared_ptr<TopoAdapter> &topo, ChunkServerIdType target,
        ChunkServerIdType source, const CopySetInfo &candidate,
        int minScatterWidth, float scatterWidthRangePerent);

    /**
     * @brief CalculateAffectOfMigration 计算copyset(A,B,source)从source迁移到
     *        target这一操作前 和 操作后{A,B,source,target}上的scatter-width
     *
     * @param[in] copySetInfo 选定的copyset
     * @param[in] source 迁出节点，是当前copyset的一个副本.
     *                   如果source==UNINTIALIZE_ID, 说明只新增节点
     * @param[in] target 迁入节点，如果target==UINITIALIZED, 说明只移除节点
     * @param[in] topo 用来获取topology信息
     * @param[out] scatterWidth copyset执行(+target, -source)前后{A,B,source,target} //NOLINT
     *                          上的scatter-width. key表示涉及到的chunkserver,
     *                          value.first表示迁移之前的scatterWidth, value.second //NOLINT
     *                          表示迁移之后的scatterWidth
     */
    static void CalculateAffectOfMigration(
        const CopySetInfo &copySetInfo, ChunkServerIdType source,
        ChunkServerIdType target, const std::shared_ptr<TopoAdapter> &topo,
        std::map<ChunkServerIdType, std::pair<int, int>> *scatterWidth);

    /**
     * @brief InvovledReplicasSatisfyScatterWidthAfterMigration
     *        copyset(A,B,source) +taget, -source
     *        计算copyset副本迁移操作后{A,B,source,target}上scatter-width是否
     *        都符合条件, 全部符合返回true, 否则返回false
     *
     * @param[in] copySetInfo 指定copyset
     * @param[in] source 迁出节点，是当前copyset的一个副本.
     *            如果source==UNINTIALIZE_ID, 说明只新增节点
     * @param[in] target 迁入节点，如果target==UNINTIALIZE_ID, 说明只移除节点
     * @param[in] ignore 忽略该scatter-width的变化, 一般是source为offline状态的时候 //NOLINT
     * @param[in] topo 获取toplogy相关信息
     * @param[in] minScatterWidth scatter-width最小值
     * @param[in] scatterWidthRangePerent scatter-width不能超过
     *            (1 + scatterWidthRangePerent) * minScatterWidth
     * @param[out] affected 该迁移操作对{A,B,source,target}scatter-width影响之和
     *
     * @return true-迁移之后{A,B,source,target}都满足scatter-width, false-有部分
     *         chunkserver不满足
     */
    static bool InvovledReplicasSatisfyScatterWidthAfterMigration(
        const CopySetInfo &copySetInfo, ChunkServerIdType source,
        ChunkServerIdType target, ChunkServerIdType ignore,
        const std::shared_ptr<TopoAdapter> &topo,
        int minScatterWidth, float scatterWidthRangePerent, int *affected);

    /**
     * @brief SortDistribute 对copyset的数量分布降序排序
     *
     * @param[in] distribute 为排序的分布
     * @param[out] desc 降序
     */
    static void SortDistribute(
        const std::map<ChunkServerIDType, std::vector<CopySetInfo>> &distribute,
        std::vector<std::pair<ChunkServerIDType,
                              std::vector<CopySetInfo>>> *desc);

    /**
     * @brief SortScatterWitAffected 选择不同chunkserver作为source或者target，对
     *        copyset各副本的scatter-width影响之和进行排序
     *
     * @param[in] candidates
     */
    static void SortScatterWitAffected(
        std::vector<std::pair<ChunkServerIdType, int>> *candidates);
};
}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_SCHEDULER_HELPER_H_
