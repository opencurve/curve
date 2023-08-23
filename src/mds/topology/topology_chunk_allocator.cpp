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

#include "src/mds/topology/topology_chunk_allocator.h"

#include <glog/logging.h>

#include <cstdlib>
#include <ctime>
#include <vector>
#include <list>
#include <random>


namespace curve {
namespace mds {
namespace topology {

// logical pool is not designated when calling this function. When executing,
// a logical will be chosen following the policy (randomly or weighted)
bool TopologyChunkAllocatorImpl::AllocateChunkRandomInSingleLogicalPool(
    curve::mds::FileType fileType, const std::string& pstName,
    uint32_t chunkNumber, ChunkSizeType chunkSize,
    std::vector<CopysetIdInfo> *infos) {
    (void)chunkSize;
    if (fileType != INODE_PAGEFILE) {
        LOG(ERROR) << "Invalid FileType, fileType = " << fileType;
        return false;
    }
    PoolIdType logicalPoolChosenId = 0;
    bool ret = ChooseSingleLogicalPool(fileType, pstName, &logicalPoolChosenId);
    if (!ret) {
        LOG(ERROR) << "ChooseSingleLogicalPool fail, ret =  " << ret;
        return false;
    }

    CopySetFilter filter = [](const CopySetInfo &copyset) {
        return copyset.IsAvailable();
    };
    std::vector<CopySetIdType> copySetIds =
        topology_->GetCopySetsInLogicalPool(logicalPoolChosenId, filter);

    if (0 == copySetIds.size()) {
        LOG(ERROR) << "[AllocateChunkRandomInSingleLogicalPool]:"
                   << " Does not have any available copySets,"
                   << " logicalPoolId = " << logicalPoolChosenId;
        return false;
    }
    ret = AllocateChunkPolicy::AllocateChunkRandomInSingleLogicalPool(
        copySetIds, logicalPoolChosenId, chunkNumber, infos);
    return ret;
}

bool TopologyChunkAllocatorImpl::AllocateChunkRoundRobinInSingleLogicalPool(
    curve::mds::FileType fileType, const std::string& pstName,
    uint32_t chunkNumber, ChunkSizeType chunkSize,
    std::vector<CopysetIdInfo> *infos) {
    (void)chunkSize;
    if (fileType != INODE_PAGEFILE) {
        LOG(ERROR) << "Invalid FileType, fileType = " << fileType;
        return false;
    }
    PoolIdType logicalPoolChosenId = 0;
    bool ret = ChooseSingleLogicalPool(fileType, pstName, &logicalPoolChosenId);
    if (!ret) {
        LOG(ERROR) << "ChooseSingleLogicalPool fail, ret = false.";
        return false;
    }

    CopySetFilter filter = [](const CopySetInfo &copyset) {
        return copyset.IsAvailable();
    };
    std::vector<CopySetIdType> copySetIds =
        topology_->GetCopySetsInLogicalPool(logicalPoolChosenId, filter);
    // exclude copySets with insufficient capacity node.
    double csAvailable=csAvailable_;
    topology_->FilterCopySetsPeersInsufficientCapacityNodes(logicalPoolChosenId,copySetIds,csAvailable);
    if (0 == copySetIds.size()) {
        LOG(ERROR) << "[AllocateChunkRoundRobinInSingleLogicalPool]:"
                   << " Does not have any available copySets,"
                   << " logicalPoolId = " << logicalPoolChosenId;
        return false;
    }

    uint32_t nextIndex = 0;

    ::curve::common::LockGuard guard(nextIndexMapLock_);
    auto it = nextIndexMap_.find(logicalPoolChosenId);
    if (it != nextIndexMap_.end()) {
        nextIndex = it->second;
    } else {
        // TODO(xuchaojie): used capacity as the standard for nextIndex
        std::random_device rd;   // generating seed for random number engine
        std::mt19937 gen(rd());  // engin used: mersenne_twister_engine
        std::uniform_int_distribution<> dis(0, copySetIds.size() - 1);
        nextIndex = dis(gen);
        nextIndexMap_.emplace(logicalPoolChosenId, nextIndex);
    }

    ret = AllocateChunkPolicy::AllocateChunkRoundRobinInSingleLogicalPool(
        copySetIds, logicalPoolChosenId, &nextIndex, chunkNumber, infos);
    if (ret) {
        nextIndexMap_[logicalPoolChosenId] = nextIndex;
    }
    return ret;
}

bool TopologyChunkAllocatorImpl::ChooseSingleLogicalPool(
    curve::mds::FileType fileType, const std::string& pstName,
    PoolIdType *poolOut) {
    std::vector<PoolIdType> logicalPools;

    LogicalPoolType poolType;
    switch (fileType) {
    case INODE_PAGEFILE: {
        poolType = LogicalPoolType::PAGEFILE;
        break;
    }
    case INODE_APPENDFILE:
    case INODE_APPENDECFILE:
    default:
        return false;
        break;
    }

    auto logicalPoolFilter = [poolType, this](const LogicalPool &pool) {
        return pool.GetLogicalPoolAvaliableFlag() &&
               (!this->enableLogicalPoolStatus_ ||
                AllocateStatus::ALLOW == pool.GetStatus()) &&
               pool.GetLogicalPoolType() == poolType;
    };

    logicalPools = topology_->GetLogicalPoolInCluster(logicalPoolFilter);
    if (0 == logicalPools.size()) {
        LOG(ERROR) << "[ChooseSingleLogicalPool]:"
                   << " Does not have any available logicalPools.";
        return false;
    }

    std::map<PoolIdType, double> poolWeightMap;
    std::vector<PoolIdType> poolToChoose;
    std::map<PoolIdType, double> poolsEnough;
    GetRemainingSpaceInLogicalPool(logicalPools, &poolsEnough, pstName);
    for (auto pool : poolsEnough) {
        // choose logical pool according to its weight
        if (ChoosePoolPolicy::kWeight == policy_) {
            // record capacity remaining as the weight of this logicalpool
            poolWeightMap.emplace(pool.first, pool.second);
        } else {
            poolToChoose.push_back(pool.first);
        }
    }
    if (ChoosePoolPolicy::kWeight == policy_) {
        return AllocateChunkPolicy::ChooseSingleLogicalPoolByWeight(
            poolWeightMap, poolOut);
    } else {
        return AllocateChunkPolicy::ChooseSingleLogicalPoolRandom(poolToChoose,
                                                                  poolOut);
    }
}

void TopologyChunkAllocatorImpl::GetRemainingSpaceInLogicalPool(
    const std::vector<PoolIdType> &logicalPools,
    std::map<PoolIdType, double> *enoughSpacePools,
    const std::string& pstName) {
    for (auto pid : logicalPools) {
        LogicalPool lPool;
        if (!topology_->GetLogicalPool(pid, &lPool)) {
            continue;
        }
        PhysicalPool pPool;
        if (!topology_->GetPhysicalPool(lPool.GetPhysicalPoolId(), &pPool)) {
            continue;
        }

        PoolsetIdType poolsetId = pPool.GetPoolsetId();
        Poolset poolset;
        if (!topology_->GetPoolset(poolsetId, &poolset)) {
            LOG(WARNING) << "Get poolset fail , poolset is null";
            continue;
        }
        if (pstName != poolset.GetName()) {
            continue;
        }

        uint64_t diskCapacity = 0;
        double available = available_;
        if (chunkFilePoolAllocHelp_->GetUseChunkFilepool()) {
            topoStat_->GetChunkPoolSize(lPool.GetPhysicalPoolId(),
                                        &diskCapacity);
            available =
                available * chunkFilePoolAllocHelp_->GetAvailable() / 100;
            diskCapacity = diskCapacity * available / 100;
        } else {
            diskCapacity = pPool.GetDiskCapacity();
            // calculate actual capacity available
            diskCapacity = diskCapacity * available / 100;
        }

        // TODO(xuchaojie): if create more than one logical pools is supported,
        //                  the logic here need to be fixed
        int64_t alloc = 0;
        allocStatistic_->GetAllocByLogicalPool(pid, &alloc);

        // multipled by replica number
        alloc *= lPool.GetReplicaNum();

        // calculate remaining capacity
        uint64_t diskRemainning = (static_cast<int64_t>(diskCapacity) > alloc)
                                      ? diskCapacity - alloc
                                      : 0;

        LOG(INFO) << "ChooseSingleLogicalPool find pool {"
                  << "diskCapacity:" << diskCapacity << ", diskAlloc:" << alloc
                  << ", diskRemainning:" << diskRemainning << "}";
        if (diskRemainning > 0) {
            (*enoughSpacePools)[pid] = diskRemainning;
        }
    }
}
bool AllocateChunkPolicy::AllocateChunkRandomInSingleLogicalPool(
    std::vector<CopySetIdType> copySetIds, PoolIdType logicalPoolId,
    uint32_t chunkNumber, std::vector<CopysetIdInfo> *infos) {
    infos->clear();

    static std::random_device rd;   // generating seed for random number engine
    static std::mt19937 gen(rd());  // engin used: mersenne_twister_engine
    std::uniform_int_distribution<> dis(0, copySetIds.size() - 1);

    for (uint32_t i = 0; i < chunkNumber; i++) {
        int randomCopySetIndex = dis(gen);
        CopysetIdInfo idInfo;
        idInfo.logicalPoolId = logicalPoolId;
        idInfo.copySetId = copySetIds[randomCopySetIndex];
        infos->push_back(idInfo);
    }
    return true;
}

bool AllocateChunkPolicy::AllocateChunkRoundRobinInSingleLogicalPool(
    std::vector<CopySetIdType> copySetIds, PoolIdType logicalPoolId,
    uint32_t *nextIndex, uint32_t chunkNumber,
    std::vector<CopysetIdInfo> *infos) {
    if (copySetIds.empty()) {
        return false;
    }
    infos->clear();
    uint32_t size = copySetIds.size();
    // copysets will be chosen in rounds
    for (uint32_t i = 0; i < chunkNumber; i++) {
        uint32_t index = (*nextIndex + i) % size;
        CopysetIdInfo idInfo;
        idInfo.logicalPoolId = logicalPoolId;
        idInfo.copySetId = copySetIds[index];
        infos->push_back(idInfo);
    }
    *nextIndex = (*nextIndex + chunkNumber) % size;
    return true;
}

bool AllocateChunkPolicy::ChooseSingleLogicalPoolByWeight(
    const std::map<PoolIdType, double> &poolWeightMap, PoolIdType *poolIdOut) {
    if (poolWeightMap.empty()) {
        LOG(ERROR) << "ChooseSingleLogicalPoolByWeight, "
                   << "poolWeightMap is empty.";
        return false;
    }
    // sum up the weight of every logical pool, thus every logical pool has its
    // own sector [sum of weight before, sum of weight before + its own weight).
    // Then we generate a random number within (0, sum).
    // Here we decide which logical pool to choose by figuring out which sector
    // the random number falls at. The heavier the weight is (higher capacity),
    // the more likely that this logical pool to be chosen.
    // In this way we can balance the load between different logical pools.
    std::map<double, PoolIdType> distributionMap;
    double sum = 0;
    for (auto &v : poolWeightMap) {
        if (v.second != 0) {
            sum += v.second;
            distributionMap.emplace(sum, v.first);
        }
    }
    if (distributionMap.size() != 0 && sum > 0) {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        std::uniform_real_distribution<> dis(0, sum);
        double randomValue = dis(gen);
        auto it = distributionMap.upper_bound(randomValue);
        *poolIdOut = it->second;
        return true;
    } else {
        LOG(ERROR) << "distributionMap does not have any available pool.";
        return false;
    }
}

bool AllocateChunkPolicy::ChooseSingleLogicalPoolRandom(
    const std::vector<PoolIdType> &pools, PoolIdType *poolIdOut) {
    if (pools.empty()) {
        LOG(ERROR) << "ChooseSingleLogicalPoolRandom, "
                   << "pools is empty.";
        return false;
    }
    static std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, pools.size() - 1);
    int randomValue = dis(gen);
    *poolIdOut = pools[randomValue];
    return true;
}

}  // namespace topology
}  // namespace mds
}  // namespace curve
