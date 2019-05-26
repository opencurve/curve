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

    FilterLogicalPool(logicalPoolFilter, &logicalPools);
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

    infos->clear();
    for (uint32_t i = 0; i < chunkNumber; i++) {
        int randomCopySetIndex = std::rand() % copySetIds.size();
        CopysetIdInfo idInfo;
        idInfo.logicalPoolId = logicalPoolChosenId;
        idInfo.copySetId = copySetIds[randomCopySetIndex];
        infos->push_back(idInfo);
    }
    return true;
}

// TODO(xuchaojie): 后续增加多种chunk分配策略

void TopologyAdminImpl::FilterLogicalPool(LogicalPoolFilter filter,
    std::vector<PoolIdType> *logicalPoolIdsOut) {
    logicalPoolIdsOut->clear();

    std::list<PoolIdType> logicalPoolList =
        topology_->GetLogicalPoolInCluster();

    for (PoolIdType id : logicalPoolList) {
        LogicalPool pool;
        if (topology_->GetLogicalPool(id, &pool)) {
            if (filter(pool)) {
                logicalPoolIdsOut->push_back(id);
            }
        }
    }
}

}  // namespace topology
}  // namespace mds
}  // namespace curve

















