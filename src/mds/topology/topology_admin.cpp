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

    std::vector<PoolIdType> logicalPools;

    LogicalPoolType poolType;
    switch (fileType) {
        case INODE_PAGEFILE: {
            poolType = LogicalPoolType::PAGEFILE;
            break;
        }
        case INODE_APPENDFILE: {
            poolType = LogicalPoolType::APPENDFILE;
            break;
        }
        case INODE_APPENDECFILE: {
            poolType = LogicalPoolType::APPENDECFILE;
            break;
        }
        default:
            return false;
            break;
    }

    auto logicalPoolFilter =
        [poolType] (const LogicalPool &pool) {
            return pool.GetLogicalPoolType() == poolType;
        };

    logicalPools = topology_->GetLogicalPoolInCluster(logicalPoolFilter);
    if (0 == logicalPools.size()) {
        LOG(ERROR) << "[AllocateChunkRandomInSingleLogicalPool]:"
                   << " Does not have any logicalPool needed.";
        return false;
    }

    int randomIndex = std::rand() % logicalPools.size();

    PoolIdType logicalPoolChosenId = logicalPools[randomIndex];

    std::vector<CopySetIdType> copySetIds =
        topology_->GetCopySetsInLogicalPool(logicalPoolChosenId);

    if (0 == copySetIds.size()) {
        LOG(ERROR) << "[AllocateChunkRandomInSingleLogicalPool]:"
                   << " Does not have any copySetIds needed,"
                   << " logicalPoolId = " << logicalPoolChosenId;
        return false;
    }
    return AllocateChunkPolicy::AllocateChunkRandomInSingleLogicalPool(
        copySetIds,
        logicalPoolChosenId,
        chunkNumber,
        infos);
}

// TODO(xuchaojie): 后续增加多种chunk分配策略

bool AllocateChunkPolicy::AllocateChunkRandomInSingleLogicalPool(
    std::vector<CopySetIdType> copySetIds,
    PoolIdType logicalPoolId,
    uint32_t chunkNumber,
    std::vector<CopysetIdInfo> *infos) {
    infos->clear();

    std::random_device rd;  // 将用于为随机数引擎获得种子
    std::mt19937 gen(rd());  // 以播种标准 mersenne_twister_engine
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

}  // namespace topology
}  // namespace mds
}  // namespace curve

















