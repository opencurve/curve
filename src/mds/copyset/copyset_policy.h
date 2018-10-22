/*
 * Project: curve
 * Created Date: Thu Oct 11 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_MDS_COPYSET_COPYSET_POLICY_H_
#define CURVE_SRC_MDS_COPYSET_COPYSET_POLICY_H_


#include <set>
#include <vector>
#include <memory>
#include <iostream>
#include <cstdint>
#include <iterator>
#include <utility>

#include "src/mds/common/topology_define.h"


namespace curve {
namespace mds {
namespace copyset {


struct Copyset {
    std::set<curve::mds::topology::ChunkServerIdType> replicas;
};

bool operator<(const Copyset& lhs, const Copyset& rhs);
std::ostream& operator<<(std::ostream& out, const Copyset& rhs);

struct ChunkServerLocation {
    curve::mds::topology::ZoneIdType zoneId;
    curve::mds::topology::PoolIdType logicalPoolId;
};

// ChunkServerInfo represents a chunkserver
struct ChunkServerInfo {
    curve::mds::topology::ChunkServerIdType id;
    ChunkServerLocation location;
};

// for logging
std::ostream& operator<<(std::ostream& out, const ChunkServerInfo& rhs);

class ClusterInfo {
 public:
    ClusterInfo() {}
    virtual ~ClusterInfo() {}

    bool GetChunkServerInfo(curve::mds::topology::ChunkServerIdType id,
        ChunkServerInfo* out) const {
        for (auto& server : csInfo_) {
            if (server.id == id) {
                *out = server;
                return true;
            }
        }
        return false;
    }

    void AddChunkServerInfo(const ChunkServerInfo &info) {
        csInfo_.push_back(info);
    }
    std::vector<ChunkServerInfo> GetChunkServerInfo() const {
        return csInfo_;
    }

 protected:
    std::vector<ChunkServerInfo> csInfo_;
};



class CopysetPolicy {
 public:
    CopysetPolicy() {}
    virtual ~CopysetPolicy() {}

    // GenCopyset generate some copysets for a cluster
    // return: if succeed return true
    virtual bool GenCopyset(const ClusterInfo& cluster,
        int numCopysets,
        std::vector<Copyset>* out) = 0;

    // new node
    virtual bool AddNode(const ClusterInfo& cluster,
        const ChunkServerInfo& node,
        int scatterWidth,
        std::vector<Copyset>* out) = 0;


    // RemoveNode when node offline, change all copysets cover this node,
    // replace origin node with a random node
    virtual bool RemoveNode(const ClusterInfo& cluster,
        const ChunkServerInfo& node,
        const std::vector<Copyset>& css,
        std::vector<std::pair<Copyset, Copyset>>* out) = 0;

    // Get Max permutation num,  make sure the permutation will stop finally
    virtual int GetMaxPermutationNum(int numCopysets,
        int num_servers,
        int num_replicas) = 0;
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
    virtual bool permutation(std::vector<ChunkServerInfo> *servers) = 0;

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
    virtual bool GenCopyset(const ClusterInfo& cluster,
        int numCopysets,
        std::vector<Copyset>* out);

    // new node
    virtual bool AddNode(const ClusterInfo& cluster,
        const ChunkServerInfo& node,
        int scatterWidth,
        std::vector<Copyset>* out);


    // RemoveNode when node offline, change all copysets cover this node,
    // replace origin node with a random node
    virtual bool RemoveNode(const ClusterInfo& cluster,
        const ChunkServerInfo& node,
        const std::vector<Copyset>& css,
        std::vector<std::pair<Copyset, Copyset>>* out);

    // Get Max permutation num,  make sure the permutation will stop finally
    int GetMaxPermutationNum(int numCopysets,
        int num_servers,
        int num_replicas) override;

 private:
    std::shared_ptr<CopysetPermutationPolicy> permutationPolicy_;
};


class CopysetPermutationPolicy333 : public CopysetPermutationPolicy {
 public:
    CopysetPermutationPolicy333()
        : CopysetPermutationPolicy(3, 3, 3) {}

    ~CopysetPermutationPolicy333() {}

    virtual bool permutation(std::vector<ChunkServerInfo> *servers);
};

}  // namespace copyset
}  // namespace mds
}  // namespace curve


#endif  // CURVE_SRC_MDS_COPYSET_COPYSET_POLICY_H_
