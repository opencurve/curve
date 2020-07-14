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
 * Created Date: Fri Oct 12 2018
 * Author: xuchaojie
 */

#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_CHUNK_ALLOCATOR_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_CHUNK_ALLOCATOR_H_

#include <vector>
#include <memory>
#include <functional>
#include <map>

#include "src/mds/topology/topology.h"
#include "proto/nameserver2.pb.h"
#include "src/common/concurrent/concurrent.h"
#include "src/mds/topology/topology_item.h"
#include "src/mds/nameserver2/allocstatistic/alloc_statistic.h"

namespace curve {
namespace mds {
namespace topology {

enum class ChoosePoolPolicy {
    // 随机选pool
    kRandom = 0,
    // 剩余容量作为权值选pool
    kWeight,
};

class TopologyChunkAllocator {
 public:
    TopologyChunkAllocator() {}
    virtual ~TopologyChunkAllocator() {}
    virtual bool AllocateChunkRandomInSingleLogicalPool(
        ::curve::mds::FileType fileType,
        uint32_t chunkNumer,
        ChunkSizeType chunkSize,
        std::vector<CopysetIdInfo> *infos) = 0;
    virtual bool AllocateChunkRoundRobinInSingleLogicalPool(
        ::curve::mds::FileType fileType,
        uint32_t chunkNumer,
        ChunkSizeType chunkSize,
        std::vector<CopysetIdInfo> *infos) = 0;
};

class TopologyChunkAllocatorImpl : public TopologyChunkAllocator {
 public:
    TopologyChunkAllocatorImpl(std::shared_ptr<Topology> topology,
        std::shared_ptr<AllocStatistic> allocStatistic,
        const TopologyOption &option)
        : topology_(topology),
        allocStatistic_(allocStatistic),
        poolUsagePercentLimit_(option.PoolUsagePercentLimit),
        policy_(static_cast<ChoosePoolPolicy>(option.choosePoolPolicy)) {
        std::srand(std::time(nullptr));
    }
    ~TopologyChunkAllocatorImpl() {}


    /**
     * @brief 在单个逻辑池中随机分配若干个chunk
     *
     * @param fileType 文件类型
     * @param chunkNumber 分配chunk数
     * @param chunkSize chunk的大小
     * @param infos 分配到的copyset列表
     *
     * @retval true 分配成功
     * @retval false 分配失败
     */
    bool AllocateChunkRandomInSingleLogicalPool(
        curve::mds::FileType fileType,
        uint32_t chunkNumber,
        ChunkSizeType chunkSize,
        std::vector<CopysetIdInfo> *infos) override;

    /**
     * @brief 在单个逻辑池中以RoundRobin分配若干个chunk
     *
     * @param fileType 文件类型
     * @param chunkNumber 分配chunk数
     * @param chunkSize chunk的大小
     * @param infos 分配到的copyset列表
     *
     * @retval true 分配成功
     * @retval false 分配失败
     */
    bool AllocateChunkRoundRobinInSingleLogicalPool(
        curve::mds::FileType fileType,
        uint32_t chunkNumber,
        ChunkSizeType chunkSize,
        std::vector<CopysetIdInfo> *infos) override;

 private:
    /**
     * @brief 从集群中选择一个逻辑池
     *
     * @param fileType 文件类型
     * @param[out] poolOut 选择的逻辑池
     *
     * @retval true 分配成功
     * @retval false 分配失败
     */
    bool ChooseSingleLogicalPool(curve::mds::FileType fileType,
        PoolIdType *poolOut);

 private:
    std::shared_ptr<Topology> topology_;

    // 分配统计模块
    std::shared_ptr<AllocStatistic> allocStatistic_;

    // pool使用百分比上限
    uint32_t poolUsagePercentLimit_;

    /**
     * @brief RoundRobin各逻辑池起始点map
     */
    std::map<PoolIdType, uint32_t> nextIndexMap_;
    /**
     * @brief 保护上述map的锁
     */
    ::curve::common::Mutex nextIndexMapLock_;
    // 选pool策略
    ChoosePoolPolicy policy_;
};

/**
 * @brief chunk分配策略
 */
class AllocateChunkPolicy {
 public:
    AllocateChunkPolicy() {}
    /**
     * @brief  在单个逻辑池中随机分配若干个chunk
     *
     * @param copySetIds 指定逻辑池内的copysetId列表
     * @param logicalPoolId 逻辑池Id
     * @param chunkNumber 分配chunk数
     * @param infos 分配到的copyset列表
     *
     * @retval true 分配成功
     * @retval false 分配失败
     */
    static bool AllocateChunkRandomInSingleLogicalPool(
        std::vector<CopySetIdType> copySetIds,
        PoolIdType logicalPoolId,
        uint32_t chunkNumber,
        std::vector<CopysetIdInfo> *infos);


    /**
     * @brief 在单个逻辑池中使用RoundRobin的方式分配若干个chunk
     *
     * @param copySetIds 指定逻辑池内的copysetId列表
     * @param logicalPoolId 逻辑池Id
     * @param[in][out] nextIndex 分配起始位置,分配完毕后返回下一个位置
     * @param chunkNumber 分配chunk数
     * @param infos 分配到的copyset列表
     *
     * @retval true 分配成功
     * @retval false 分配失败
     */
    static bool AllocateChunkRoundRobinInSingleLogicalPool(
        std::vector<CopySetIdType> copySetIds,
        PoolIdType logicalPoolId,
        uint32_t *nextIndex,
        uint32_t chunkNumber,
        std::vector<CopysetIdInfo> *infos);

    /**
     * @brief 根据权值选择单个逻辑池
     *
     * @param poolWeightMap 逻辑池的权值表
     * @param[out] poolIdOut 选择的poolId
     *
     * @retval true 成功
     * @retval false 失败
     */
    static bool ChooseSingleLogicalPoolByWeight(
        const std::map<PoolIdType, double> &poolWeightMap,
        PoolIdType *poolIdOut);

    /**
     * @brief 随机选择单个逻辑池
     *
     * @param pools 逻辑池列表
     * @param[out] poolIdOut 选择的poolId
     *
     * @retval true 成功
     * @retval false 失败
     */
    static bool ChooseSingleLogicalPoolRandom(
        const std::vector<PoolIdType> &pools,
        PoolIdType *poolIdOut);
};



}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_CHUNK_ALLOCATOR_H_
