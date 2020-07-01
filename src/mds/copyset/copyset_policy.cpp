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
 * Created Date: Thu Oct 11 2018
 * Author: xuchaojie
 */

#include "src/mds/copyset/copyset_policy.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <iostream>
#include <sstream>
#include <iterator>
#include <set>
#include <unordered_set>
#include <random>
#include <utility>
#include <map>


namespace curve {
namespace mds {
namespace copyset {

using ::curve::mds::topology::ChunkServerIdType;

bool operator<(const Copyset& lhs, const Copyset& rhs) {
    std::ostringstream buf1;
    std::copy(lhs.replicas.begin(),
        lhs.replicas.end(),
        std::ostream_iterator<ChunkServerIdType>(buf1, "_"));
    std::ostringstream buf2;
    std::copy(rhs.replicas.begin(),
        rhs.replicas.end(),
        std::ostream_iterator<ChunkServerIdType>(buf2, "_"));
    return buf1.str() < buf2.str();
}

std::ostream& operator<<(std::ostream& out, const Copyset& rhs) {
    out << "{copyset: ";
    std::copy(rhs.replicas.begin(),
        rhs.replicas.end(),
        std::ostream_iterator<ChunkServerIdType>(out, " "));
    return out << "}";
}


std::ostream& operator<<(std::ostream& out, const ChunkServerInfo& rhs) {
    return out << "{server:"
               << rhs.id
               << ",zone:"
               << rhs.location.zoneId
               << "}";
}

bool CopysetZoneShufflePolicy::GenCopyset(const ClusterInfo& cluster,
    int numCopysets,
    std::vector<Copyset>* out) {

    std::vector<ChunkServerInfo> chunkServers = cluster.GetChunkServerInfo();
    uint32_t numReplicas = permutationPolicy_->GetReplicaNum();
    int copyseyNum = numCopysets;

    std::vector<ChunkServerInfo> replicas(numReplicas);
    int maxNum = GetMaxPermutationNum(copyseyNum,
        chunkServers.size(),
        numReplicas);

    for (int count = 0; count < maxNum; count++) {
        std::vector<ChunkServerInfo> csList;
        if (!permutationPolicy_->permutation(chunkServers, &csList)) {
            return false;
        }

        for (uint32_t i = 0; i < csList.size(); i += numReplicas) {
            if (i + numReplicas > csList.size()) {
                break;
            }
            std::copy(csList.begin() + i,
                csList.begin() + i + numReplicas,
                replicas.begin());
            Copyset copyset;
            for (auto& replica : replicas) {
                copyset.replicas.insert(replica.id);
            }
            out->emplace_back(copyset);
            if (--copyseyNum == 0) {
                LOG(INFO) << "Generate copyset success"
                          << ", numCopysets = " << numCopysets;
                return true;
            }
        }
    }
    return false;
}

void CopysetZoneShufflePolicy::GetMinCopySetFromScatterWidth(
    int numChunkServers,
    int scatterWidth,
    int numReplicas,
    int *min) {
    // scatter-width S 所需的copyset数估算:
    // 1. 每轮permutation 产生 N/R个copyset
    // 2. 每次permutation最多增加R-1个scatter-width
    // 那么至少需要P = S/(R—1) 次permutation，
    // 至少需要产生(S/(R-1))(N/R)个copyset。
    // 因此，copyset的下限是(S/(R-1))(N/R)
    // 其中，N为chunkserver num， R为replica num。
    *min = scatterWidth * numChunkServers / numReplicas / (numReplicas - 1);
}

int CopysetZoneShufflePolicy::GetMaxPermutationNum(int numCopysets,
    int numChunkServers,
    int numReplicas) {
    return numCopysets;
}

/**
 * @brief  N个zone中选择3个zone放置3副本的随机排列算法
 *
 *  1. 首先对所有zone的server各自进行随机排列，
 *  2. 之后按照如下顺序依次选择：
 *
 *  zone1    zone2    zone3    zone4   ...  zoneN
 *   1        2        3         4     ...    N
 *  N+1      N+2      N+3       N+4    ...   2N
 *  ...      ...      ...       ...    ...   ...
 *
 * @param chunkServers  all chunkserver in cluster.
 *
 * @reval true   exec success.
 * @reval false  exec fail.
 */
bool CopysetPermutationPolicyN33::permutation(
    const std::vector<ChunkServerInfo> &serversIn,
    std::vector<ChunkServerInfo> *serversOut) {
    std::map<curve::mds::topology::ZoneIdType,
        std::vector<ChunkServerInfo> > csMap;
    for (const ChunkServerInfo& sv : serversIn) {
        curve::mds::topology::ZoneIdType zid = sv.location.zoneId;
        if (csMap.find(zid) != csMap.end()) {
            csMap[zid].push_back(sv);
        } else {
            std::vector<ChunkServerInfo> temp;
            temp.push_back(sv);
            csMap[zid] = temp;
        }
    }

    if (csMap.size() < GetZoneChosenNum()) {
        LOG(ERROR) << "[CopysetPermutationPolicyN33::permutation]:"
                   << "error, cluster must has more than 3 zones";
        return false;
    }

    std::random_device rd;
    std::mt19937 g(rd());

    serversOut->clear();
    if (!csMap.empty()) {
        auto minSize = csMap.begin()->second.size();
        for (auto& it : csMap) {
            std::shuffle(it.second.begin(), it.second.end(), g);
            if (it.second.size() <  minSize) {
                minSize = it.second.size();
            }
        }
        for (decltype(minSize) i = 0; i < minSize; i++) {
            for (auto& it : csMap) {
                serversOut->push_back(it.second[i]);
            }
        }
    }
    return true;
}

}  // namespace copyset
}  // namespace mds
}  // namespace curve

