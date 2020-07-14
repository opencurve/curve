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

bool TopologyChunkAllocatorImpl::AllocateChunkRandomInSingleLogicalPool(
    curve::mds::FileType fileType,
    uint32_t chunkNumber,
    ChunkSizeType chunkSize,
    std::vector<CopysetIdInfo> *infos) {
    if (fileType != INODE_PAGEFILE) {
        LOG(ERROR) << "Invalid FileType, fileType = "
                   << fileType;
        return false;
    }
    PoolIdType logicalPoolChosenId = 0;
    bool ret = ChooseSingleLogicalPool(fileType, &logicalPoolChosenId);
    if (!ret) {
        LOG(ERROR) << "ChooseSingleLogicalPool fail, ret =  " << ret;
        return false;
    }

    std::vector<CopySetIdType> copySetIds =
        topology_->GetCopySetsInLogicalPool(logicalPoolChosenId);

    if (0 == copySetIds.size()) {
        LOG(ERROR) << "[AllocateChunkRandomInSingleLogicalPool]:"
                   << " Does not have any available copySets,"
                   << " logicalPoolId = " << logicalPoolChosenId;
        return false;
    }
    ret = AllocateChunkPolicy::AllocateChunkRandomInSingleLogicalPool(
               copySetIds,
               logicalPoolChosenId,
               chunkNumber,
               infos);
    return ret;
}

bool TopologyChunkAllocatorImpl::AllocateChunkRoundRobinInSingleLogicalPool(
    curve::mds::FileType fileType,
    uint32_t chunkNumber,
    ChunkSizeType chunkSize,
    std::vector<CopysetIdInfo> *infos) {
    if (fileType != INODE_PAGEFILE) {
        LOG(ERROR) << "Invalid FileType, fileType = "
                   << fileType;
        return false;
    }
    PoolIdType logicalPoolChosenId = 0;
    bool ret = ChooseSingleLogicalPool(fileType, &logicalPoolChosenId);
    if (!ret) {
        LOG(ERROR) << "ChooseSingleLogicalPool fail, ret = false.";
        return false;
    }

    std::vector<CopySetIdType> copySetIds =
        topology_->GetCopySetsInLogicalPool(logicalPoolChosenId);

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
        // TODO(xuchaojie): 后续可以使用剩余容量最大的作为起始。
        std::random_device rd;  // 将用于为随机数引擎获得种子
        std::mt19937 gen(rd());  // 以播种标准 mersenne_twister_engine
        std::uniform_int_distribution<> dis(0, copySetIds.size() - 1);
        nextIndex = dis(gen);
        nextIndexMap_.emplace(logicalPoolChosenId, nextIndex);
    }

    ret = AllocateChunkPolicy::AllocateChunkRoundRobinInSingleLogicalPool(
               copySetIds,
               logicalPoolChosenId,
               &nextIndex,
               chunkNumber,
               infos);
    if (ret) {
        nextIndexMap_[logicalPoolChosenId] = nextIndex;
    }
    return ret;
}

bool TopologyChunkAllocatorImpl::ChooseSingleLogicalPool(
    curve::mds::FileType fileType,
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

    auto logicalPoolFilter =
    [poolType] (const LogicalPool &pool) {
        return pool.GetLogicalPoolAvaliableFlag() &&
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
    for (auto pid : logicalPools) {
        LogicalPool lPool;
        if (!topology_->GetLogicalPool(pid, &lPool)) {
            continue;
        }
        PhysicalPool pPool;
        if (!topology_->GetPhysicalPool(lPool.GetPhysicalPoolId(), &pPool)) {
            continue;
        }
        uint64_t diskCapacity = pPool.GetDiskCapacity();
        // 除去预留
        diskCapacity = diskCapacity * poolUsagePercentLimit_ / 100;

        // TODO(xuchaojie): 后续若支持同一物理池创建多个逻辑池，此处需修正
        int64_t alloc = 0;
        allocStatistic_->GetAllocByLogicalPool(pid, &alloc);

        // 乘以副本数
        alloc *= lPool.GetReplicaNum();

        // 减去已使用
        uint64_t diskRemainning =
            (diskCapacity > alloc) ? diskCapacity - alloc : 0;

        LOG(INFO) << "ChooseSingleLogicalPool find pool {"
                  << "diskCapacity:" << diskCapacity
                  << ", diskAlloc:" << alloc
                  << ", diskRemainning:" << diskRemainning
                  << "}";

        if (ChoosePoolPolicy::kWeight == policy_) {
            // 以剩余空间作为权值
            poolWeightMap.emplace(pid, diskRemainning);
        } else {
            if (diskRemainning > 0) {
                poolToChoose.push_back(pid);
            }
        }
    }
    if (ChoosePoolPolicy::kWeight == policy_) {
        return AllocateChunkPolicy::ChooseSingleLogicalPoolByWeight(
            poolWeightMap, poolOut);
    } else {
        return AllocateChunkPolicy::ChooseSingleLogicalPoolRandom(
            poolToChoose, poolOut);
    }
}

bool AllocateChunkPolicy::AllocateChunkRandomInSingleLogicalPool(
    std::vector<CopySetIdType> copySetIds,
    PoolIdType logicalPoolId,
    uint32_t chunkNumber,
    std::vector<CopysetIdInfo> *infos) {
    infos->clear();

    static std::random_device rd;  // 将用于为随机数引擎获得种子
    static std::mt19937 gen(rd());  // 以播种标准 mersenne_twister_engine
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
    std::vector<CopySetIdType> copySetIds,
    PoolIdType logicalPoolId,
    uint32_t *nextIndex,
    uint32_t chunkNumber,
    std::vector<CopysetIdInfo> *infos) {
    if (copySetIds.empty()) {
        return false;
    }
    infos->clear();
    uint32_t size = copySetIds.size();
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
    const std::map<PoolIdType, double> &poolWeightMap,
    PoolIdType *poolIdOut) {
    if (poolWeightMap.empty()) {
        LOG(ERROR) << "ChooseSingleLogicalPoolByWeight, "
                   << "poolWeightMap is empty.";
        return false;
    }
    // 将权值累加，在(0,sum)这段区间内使用标准均匀分布取随机值，
    // 由于权值越大的区间越大，权值越小的区间越小，
    // 从而获得加权的随机值，将不同pool的分布慢慢拉平。
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
    const std::vector<PoolIdType> &pools,
    PoolIdType *poolIdOut) {
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

















