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

#ifndef SRC_MDS_COPYSET_COPYSET_POLICY_H_
#define SRC_MDS_COPYSET_COPYSET_POLICY_H_


#include <set>
#include <vector>
#include <memory>
#include <iostream>
#include <cstdint>
#include <iterator>
#include <utility>

#include "src/mds/common/mds_define.h"
#include "src/mds/copyset/copyset_structure.h"


namespace curve {
namespace mds {
namespace copyset {

class CopysetPolicy {
 public:
    CopysetPolicy() {}
    virtual ~CopysetPolicy() {}

    // GenCopyset generate some copysets for a cluster
    // return: if succeed return true
    virtual bool GenCopyset(const ClusterInfo& cluster,
        int numCopysets,
        std::vector<Copyset>* out) = 0;


    virtual void GetMinCopySetFromScatterWidth(
        int numChunkServers,
        int scatterWidth,
        int numReplicas,
        int *min) = 0;
};

class CopysetPermutationPolicy {
 public:
    CopysetPermutationPolicy()
    : zoneNum_(0),
    zoneChoseNum_(0),
    replicaNum_(0) {}
    CopysetPermutationPolicy(uint32_t zoneNum,
        uint32_t zoneChoseNum,
        uint32_t replicaNum)
    : zoneNum_(zoneNum),
    zoneChoseNum_(zoneChoseNum),
    replicaNum_(replicaNum) {}
    virtual ~CopysetPermutationPolicy() {}
    virtual bool permutation(
            const std::vector<ChunkServerInfo> &serversIn,
            std::vector<ChunkServerInfo> *serversOut) = 0;

    uint32_t GetZoneNum() const {
        return zoneNum_;
    }

    uint32_t GetZoneChosenNum() const {
        return zoneChoseNum_;
    }

    uint32_t GetReplicaNum() const {
        return replicaNum_;
    }

 protected:
    const uint32_t zoneNum_;
    const uint32_t zoneChoseNum_;
    const uint32_t replicaNum_;
};


class CopysetZoneShufflePolicy : public CopysetPolicy {
 public:
    CopysetZoneShufflePolicy(
        std::shared_ptr<CopysetPermutationPolicy> permutationPolicy)
        : permutationPolicy_(permutationPolicy) {}

    virtual ~CopysetZoneShufflePolicy() {}

    // GenCopyset generate some copysets for a cluster
    // return: if succeed return true
    bool GenCopyset(const ClusterInfo& cluster,
        int numCopysets,
        std::vector<Copyset>* out) override;

    void GetMinCopySetFromScatterWidth(
        int numChunkServers,
        int scatterWidth,
        int numReplicas,
        int *min) override;

 private:
    // Get Max permutation num,  make sure the permutation will stop finally
    int GetMaxPermutationNum(int numCopysets,
        int numChunkServers,
        int numReplicas);

 private:
    std::shared_ptr<CopysetPermutationPolicy> permutationPolicy_;
};

class CopysetPermutationPolicyN33 : public CopysetPermutationPolicy {
 public:
    CopysetPermutationPolicyN33()
        : CopysetPermutationPolicy(CopysetConstrait::NUM_ANY, 3, 3) {}

    ~CopysetPermutationPolicyN33() {}

    bool permutation(const std::vector<ChunkServerInfo> &serversIn,
            std::vector<ChunkServerInfo> *serversOut) override;
};

}  // namespace copyset
}  // namespace mds
}  // namespace curve


#endif  // SRC_MDS_COPYSET_COPYSET_POLICY_H_
