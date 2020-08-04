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
 * Project:
 * Created Date: Wed Oct 10 2018
 * Author: xuchaojie
 */

#include <glog/logging.h>
#include <algorithm>

#include "src/mds/copyset/copyset_manager.h"

namespace curve {
namespace mds {
namespace copyset {

bool CopysetManager::Init(const CopysetConstrait &constrait) {
    constrait_ = constrait;
    if (constrait.zoneChoseNum == constrait.replicaNum) {
        policy_ = std::make_shared<CopysetZoneShufflePolicy>(
               std::make_shared<CopysetPermutationPolicyNXX>(constrait));
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
    uint32_t *scatterWidth,
    std::vector<Copyset>* out) {
    if (nullptr == policy_) {
        return false;
    }

    if (0 == numCopysets && 0 == *scatterWidth) {
        return false;
    }

    int numChunkServers = cluster.GetClusterSize();
    if (*scatterWidth >= (numChunkServers - 1)) {
        // it's impossible that scatter width is lager than cluster size
        return false;
    }

    uint32_t targetScatterWidth = *scatterWidth;

    if (numCopysets != 0) {
        if (GenCopyset(cluster, numCopysets, out)) {
            if (validator_->ValidateScatterWidth(targetScatterWidth,
                scatterWidth, *out)) {
                return true;
            } else {
                LOG(ERROR) << "GenCopyset ValidateScatterWidth failed, "
                           << "numCopysets not enough, "
                           << "numCopysets = " << numCopysets
                           << " , scatterWidth = " << targetScatterWidth;
                return false;
            }
        } else {
            return false;
        }
    } else {
        policy_->GetMinCopySetFromScatterWidth(
            numChunkServers,
            targetScatterWidth,
            constrait_.replicaNum,
            &numCopysets);
        // set an upper limit here to avoid infinite loop,
        // for every permutation N/R copysets are generated,
        // assume that we can tolerate generating only one scatter-width for every 10 permutation,
        // we will need P=10S permutations for 10SN/R copysets.
        // P: permutations S: scatter width N: number of chunkservers R: number of replicas
        
        int maxRetryNum =
            10 * targetScatterWidth * numChunkServers / constrait_.replicaNum;
        while (numCopysets <= maxRetryNum) {
            if (GenCopyset(cluster, numCopysets, out)) {
                if (validator_->ValidateScatterWidth(targetScatterWidth,
                    scatterWidth, *out)) {
                    return true;
                } else {
                    numCopysets++;
                }
            } else {
                LOG(ERROR) << "GenCopyset by scatterWidth failed, "
                           << "scatterWidth can not reach, scatterWidth = "
                           << targetScatterWidth;
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
            LOG(ERROR) << "GenCopyset policy failed.";
            return false;
        }
        if (validator_->Validate(*out)) {
            return true;
        }
        LOG(WARNING) << "Validate copyset metric failed, retry = "
                   << retry;
        retry++;
    }
    LOG(ERROR) << "GenCopyset retry times exceed, times = "
               << option_.copysetRetryTimes;
    return false;
}

}  // namespace copyset
}  // namespace mds
}  // namespace curve
