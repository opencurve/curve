/*
 * Project:
 * Created Date: Wed Oct 10 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <algorithm>

#include "src/mds/copyset/copyset_manager.h"

namespace curve {
namespace mds {
namespace copyset {

bool CopysetManager::Init(const CopysetConstrait &constrait) {
    constrait_ = constrait;
    if ((3 == constrait.zoneChoseNum) &&
        (3 == constrait.replicaNum)) {
        policy_ = std::make_shared<CopysetZoneShufflePolicy>(
               std::make_shared<CopysetPermutationPolicyN33>());
        return true;
    } else {
        LOG(ERROR) << "current constrait is not supported"
                   << ", zoneNum = " << constrait.zoneNum
                   << ", zoneChoseNum = " << constrait.zoneChoseNum
                   << ", replicaNum = " << constrait.replicaNum;
        return false;
    }
}

bool CopysetManager::GenCopyset(const ClusterInfo& cluster,
    int numCopysets,
    int scatterWidth,
    std::vector<Copyset>* out) {
    if (nullptr == policy_) {
        return false;
    }

    if (0 == numCopysets && 0 == scatterWidth) {
        return false;
    }

    int numChunkServers = cluster.GetClusterSize();
    if (scatterWidth >= (numChunkServers - 1)) {
        // scatterWidth大于上限不可能达到
        return false;
    }

    if (numCopysets != 0) {
        if (GenCopyset(cluster, numCopysets, out)) {
            if (validator_->ValidateScatterWidth(scatterWidth, *out)) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    } else {
        policy_->GetMinCopySetFromScatterWidth(
            numChunkServers,
            scatterWidth,
            constrait_.replicaNum,
            &numCopysets);
        // 设置while循环上限防止死循环,
        // 每轮permutation 产生 N/R个copyset,
        // 假设最多能容忍每10次permutation产生1个scatter-width,
        // 那么需要P=10S次permutation， 产生10SN/R个copyset。
        int maxRetryNum =
            10 * scatterWidth * numChunkServers / constrait_.replicaNum;
        while (numCopysets <= maxRetryNum) {
            if (GenCopyset(cluster, numCopysets, out)) {
                if (validator_->ValidateScatterWidth(scatterWidth, *out)) {
                    return true;
                } else {
                    numCopysets++;
                }
            } else {
                return false;
            }
        }
    }
    return false;
}

bool CopysetManager::GenCopyset(const ClusterInfo& cluster,
    int numCopysets,
    std::vector<Copyset>* out) {
    int retry = 0;
    while (retry < option_.copysetRetryTimes) {
        out->clear();
        if (!policy_->GenCopyset(cluster, numCopysets, out)) {
            return false;
        }
        if (validator_->Validate(*out)) {
            return true;
        }
        retry++;
    }
    return false;
}

}  // namespace copyset
}  // namespace mds
}  // namespace curve
