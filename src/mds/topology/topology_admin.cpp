/*
 * Project: curve
 * Created Date: Fri Oct 12 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/mds/topology/topology_admin.h"

#include <glog/logging.h>

#include <cstdlib>
#include <ctime>
#include <vector>
#include <list>
#include <random>


namespace curve {
namespace mds {
namespace topology {

bool TopologyAdminImpl::AllocateChunkRandomInSingleLogicalPool(
    curve::mds::FileType fileType,
    uint32_t chunkNumber,
    std::vector<CopysetIdInfo> *infos) {
    PoolIdType logicalPoolChosenId = 0;
    bool ret = ChooseSingleLogicalPool(fileType, &logicalPoolChosenId);
    if (!ret) {
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
    return AllocateChunkPolicy::AllocateChunkRandomInSingleLogicalPool(
               copySetIds,
               logicalPoolChosenId,
               chunkNumber,
               infos);
}

bool TopologyAdminImpl::AllocateChunkRoundRobinInSingleLogicalPool(
    curve::mds::FileType fileType,
    uint32_t chunkNumber,
    std::vector<CopysetIdInfo> *infos) {
    PoolIdType logicalPoolChosenId = 0;
    bool ret = ChooseSingleLogicalPool(fileType, &logicalPoolChosenId);
    if (!ret) {
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

    static std::map<PoolIdType, uint32_t> nextIndexMap;

    uint32_t nextIndex = 0;
    auto it = nextIndexMap.find(logicalPoolChosenId);
    if (it != nextIndexMap.end()) {
        nextIndex = it->second;
    } else {
        // TODO(xuchaojie): 后续可以使用剩余容量最大的作为起始。
        std::random_device rd;  // 将用于为随机数引擎获得种子
        std::mt19937 gen(rd());  // 以播种标准 mersenne_twister_engine
        std::uniform_int_distribution<> dis(0, copySetIds.size() - 1);
        nextIndex = dis(gen);
        nextIndexMap.emplace(logicalPoolChosenId, nextIndex);
    }

    ret = AllocateChunkPolicy::AllocateChunkRoundRobinInSingleLogicalPool(
               copySetIds,
               logicalPoolChosenId,
               &nextIndex,
               chunkNumber,
               infos);
    if (ret) {
        nextIndexMap[logicalPoolChosenId] = nextIndex;
    }
    return ret;
}

bool TopologyAdminImpl::ChooseSingleLogicalPool(curve::mds::FileType fileType,
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

    // TODO(xuchaojie): 还需过滤容量不足的pool
    auto logicalPoolFilter =
    [poolType] (const LogicalPool &pool) {
        return pool.GetLogicalPoolType() == poolType;
    };

    logicalPools = topology_->GetLogicalPoolInCluster(logicalPoolFilter);
    if (0 == logicalPools.size()) {
        LOG(ERROR) << "[ChooseSingleLogicalPool]:"
                   << " Does not have any available logicalPools.";
        return false;
    }

    // TODO(xuchaojie): 后续以pool剩余容量作为权值随机
    std::map<PoolIdType, double> poolWeightMap;
    for (auto pid : logicalPools) {
        poolWeightMap.emplace(pid, 1);
    }

    return AllocateChunkPolicy::ChooseSingleLogicalPoolByWeight(
        poolWeightMap, poolOut);
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
        return false;
    }
    // 将权值累加，在(0,sum)这段区间内使用标准均匀分布取随机值，
    // 由于权值越大的区间越大，权值越小的区间越小，
    // 从而获得加权的随机值，将不同pool的分布慢慢拉平。
    std::map<double, PoolIdType> distributionMap;
    double sum = 0;
    for (auto &v : poolWeightMap) {
        sum += v.second;
        distributionMap.emplace(sum, v.first);
    }
    static std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0, sum);

    double randomValue = dis(gen);
    auto it = distributionMap.upper_bound(randomValue);
    *poolIdOut = it->second;
    return true;
}


}  // namespace topology
}  // namespace mds
}  // namespace curve

















