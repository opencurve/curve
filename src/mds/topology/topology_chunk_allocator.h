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
#include <string>
#include <map>

#include "src/mds/topology/topology.h"
#include "proto/nameserver2.pb.h"
#include "src/common/concurrent/concurrent.h"
#include "src/mds/topology/topology_item.h"
#include "src/mds/topology/topology_stat.h"
#include "src/mds/nameserver2/allocstatistic/alloc_statistic.h"

namespace curve {
namespace mds {
namespace topology {

enum class ChoosePoolPolicy {
    // choose pools randomly
    kRandom = 0,
    // use available capacity as the weight for choosing pools
    kWeight,
};

class ChunkFilePoolAllocHelp {
 public:
    ChunkFilePoolAllocHelp()
        : useChunkFilepool(false), useChunkFilePoolAsWalPool(false),
          ChunkFilePoolPoolWalReserve(0) {}
    ~ChunkFilePoolAllocHelp() {}
    void
    UpdateChunkFilePoolAllocConfig(bool useChunkFilepool_,
                                   bool useChunkFilePoolAsWalPool_,
                                   uint32_t useChunkFilePoolAsWalPoolReserve_) {
        useChunkFilepool.store(useChunkFilepool_, std::memory_order_release);
        useChunkFilePoolAsWalPool.store(useChunkFilePoolAsWalPool_,
                                        std::memory_order_release);
        ChunkFilePoolPoolWalReserve.store(useChunkFilePoolAsWalPoolReserve_,
                                          std::memory_order_release);
    }
    bool GetUseChunkFilepool() {
        return useChunkFilepool.load(std::memory_order_acquire);
    }
    // After removing the reserved space, the remaining percentage
    uint32_t GetAvailable() {
        if (useChunkFilePoolAsWalPool.load(std::memory_order_acquire)) {
            return 100 -
                   ChunkFilePoolPoolWalReserve.load(std::memory_order_acquire);
        } else {
            return 100;
        }
    }

 private:
    // use chunkfile as allocation condition
    std::atomic<bool> useChunkFilepool;
    std::atomic<bool> useChunkFilePoolAsWalPool;
    // Reserve extra space for walpool
    std::atomic<uint32_t> ChunkFilePoolPoolWalReserve;
};

class TopologyChunkAllocator {
 public:
    TopologyChunkAllocator() {}
    virtual ~TopologyChunkAllocator() {}
    virtual bool AllocateChunkRandomInSingleLogicalPool(
        ::curve::mds::FileType fileType,
        const std::string& pstName,
        uint32_t chunkNumer,
        ChunkSizeType chunkSize,
        std::vector<CopysetIdInfo> *infos) = 0;
    virtual bool AllocateChunkRoundRobinInSingleLogicalPool(
        ::curve::mds::FileType fileType,
        const std::string &pstName,
        uint32_t chunkNumer,
        ChunkSizeType chunkSize,
        std::vector<CopysetIdInfo> *infos) = 0;
    virtual void GetRemainingSpaceInLogicalPool(
        const std::vector<PoolIdType>& logicalPools,
        std::map<PoolIdType, double>* remianingSpace,
        const std::string& pstName) = 0;

    virtual void UpdateChunkFilePoolAllocConfig(
        bool useChunkFilepool_, bool useChunkFilePoolAsWalPool_,
        uint32_t useChunkFilePoolAsWalPoolReserve_) = 0;
};

class TopologyChunkAllocatorImpl : public TopologyChunkAllocator {
 public:
    TopologyChunkAllocatorImpl(
        std::shared_ptr<Topology> topology,
        std::shared_ptr<AllocStatistic> allocStatistic,
        std::shared_ptr<TopologyStat> topologyStat,
        std::shared_ptr<ChunkFilePoolAllocHelp> ChunkFilePoolAllocHelp,
        const TopologyOption &option)
        : topology_(topology), allocStatistic_(allocStatistic),
          available_(option.PoolUsagePercentLimit),
          topoStat_(topologyStat),
          chunkFilePoolAllocHelp_(ChunkFilePoolAllocHelp),
          policy_(static_cast<ChoosePoolPolicy>(option.choosePoolPolicy)),
          enableLogicalPoolStatus_(option.enableLogicalPoolStatus),
          csAvailable_(option.ChunkServerUsagePercentLimit), {
        std::srand(std::time(nullptr));
    }
    ~TopologyChunkAllocatorImpl() {}


    /**
     * @brief allocate chunks randomly in a single logical pool
     *
     * @param fileType file type
     * @param chunkNumber number of chunks to allocate
     * @param chunkSize size of a chunk
     * @param infos copyset list that chunks allocated to
     *
     * @retval true if succeeded
     * @retval false if failed
     */
    bool AllocateChunkRandomInSingleLogicalPool(
        curve::mds::FileType fileType,
        const std::string& pstName,
        uint32_t chunkNumber,
        ChunkSizeType chunkSize,
        std::vector<CopysetIdInfo> *infos) override;

    /**
     * @brief allocate chunks by round robin in a single logical pool
     *
     * @param fileType file type
     * @param chunkNumber number of chunks to allocate
     * @param chunkSize size of a chunk
     * @param infos copyset list that chunks allocated to
     *
     * @retval true if succeeded
     * @retval false if failed
     */
    bool AllocateChunkRoundRobinInSingleLogicalPool(
        curve::mds::FileType fileType,
        const std::string &pstName,
        uint32_t chunkNumber,
        ChunkSizeType chunkSize,
        std::vector<CopysetIdInfo> *infos) override;
    void GetRemainingSpaceInLogicalPool(
        const std::vector<PoolIdType>& logicalPools,
        std::map<PoolIdType, double>* remianingSpace,
        const std::string& pstName) override;

    void UpdateChunkFilePoolAllocConfig(bool useChunkFilepool_,
            bool useChunkFilePoolAsWalPool_,
            uint32_t useChunkFilePoolAsWalPoolReserve_) override {
              chunkFilePoolAllocHelp_->UpdateChunkFilePoolAllocConfig(
                useChunkFilepool_, useChunkFilePoolAsWalPool_,
                useChunkFilePoolAsWalPoolReserve_);
        }

 private:
    /**
     * @brief select a logical pool in the cluster
     *
     * @param fileType file type
     * @param[out] poolOut logical pool chosen
     *
     * @retval true if succeeded
     * @retval false if failed
     */
    bool ChooseSingleLogicalPool(curve::mds::FileType fileType,
        const std::string& pstName,
        PoolIdType *poolOut);

 private:
    std::shared_ptr<Topology> topology_;

    // allocation statistic module
    std::shared_ptr<AllocStatistic> allocStatistic_;

    // usage limit percentage of pool
    uint32_t available_;

    /**
     * @brief topology statistic module
     */
    std::shared_ptr<TopologyStat> topoStat_;
    /**
     * @brief chunkfilepool help with capacity allocation
     */
    std::shared_ptr<ChunkFilePoolAllocHelp> chunkFilePoolAllocHelp_;
    /**
     * @brief starting point of round robin for (copysets of) every logical pool
     */
    std::map<PoolIdType, uint32_t> nextIndexMap_;
    /**
     * @brief mutex for nextIndexMap_
     */
    ::curve::common::Mutex nextIndexMapLock_;
    // policy for choosing pool
    ChoosePoolPolicy policy_;
    // enableLogicalPoolStatus
    bool enableLogicalPoolStatus_;
    // usage limit percentage of cs capacity
    uint32_t csAvailable_;
};

/**
 * @brief chunk allocation policies
 */
class AllocateChunkPolicy {
 public:
    AllocateChunkPolicy() {}
    /**
     * @brief  allocate chunks in a single logical pool
     *
     * @param copySetIds copyset id list in designated logical pool
     * @param logicalPoolId logical pool id
     * @param chunkNumber number of chunks to allocate
     * @param infos copyset list that chunks allocated to
     *
     * @retval true if succeeded
     * @retval false if failed
     */
    static bool AllocateChunkRandomInSingleLogicalPool(
        std::vector<CopySetIdType> copySetIds, PoolIdType logicalPoolId,
        uint32_t chunkNumber, std::vector<CopysetIdInfo> *infos);

    /**
     * @brief allocate chunks by round robin in a single logical pool
     *
     * @param copySetIds copyset id list in designated logical pool
     * @param logicalPoolId target logical pool id
     * @param[in][out] nextIndex the starting index for the copysets in a
     *                           logical pool in round robin implementation.
     *                           It should be mentioned that this value will be
     *                           updated (to become the index of next copyset
     *                           of the last chosen copyset of that round)
     *                           after each round of chunk allocation.
     * @param chunkNumber number of chunks to allocate
     * @param infos copyset list that chunks allocated to
     *
     * @retval true if succeeded
     * @retval false if failed
     */
    static bool AllocateChunkRoundRobinInSingleLogicalPool(
        std::vector<CopySetIdType> copySetIds, PoolIdType logicalPoolId,
        uint32_t *nextIndex, uint32_t chunkNumber,
        std::vector<CopysetIdInfo> *infos);

    /**
     * @brief choose a logical pool according to their weight
     *
     * @param poolWeightMap map of the weight of every logical pool
     * @param[out] poolIdOut id of the chosen pool
     *
     * @retval true if succeeded
     * @retval false if failed
     */
    static bool ChooseSingleLogicalPoolByWeight(
        const std::map<PoolIdType, double> &poolWeightMap,
        PoolIdType *poolIdOut);

    /**
     * @brief choose a logical pool randomly
     *
     * @param pools logical pool list
     * @param[out] poolIdOut id of the chosen pool
     *
     * @retval true if succeeded
     * @retval false if failed
     */
    static bool
    ChooseSingleLogicalPoolRandom(const std::vector<PoolIdType> &pools,
                                  PoolIdType *poolIdOut);
};


}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_CHUNK_ALLOCATOR_H_
