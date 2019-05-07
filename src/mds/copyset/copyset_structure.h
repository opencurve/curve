/*
 * Project: curve
 * Created Date: Wed May 08 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#ifndef SRC_MDS_COPYSET_COPYSET_STRUCTURE_H_
#define SRC_MDS_COPYSET_COPYSET_STRUCTURE_H_

#include <set>
#include <vector>
#include <memory>

#include "src/mds/common/mds_define.h"

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

    ClusterInfo(const ClusterInfo&) = default;
    ClusterInfo(ClusterInfo&&) = default;
    ClusterInfo& operator=(const ClusterInfo&) = default;
    ClusterInfo& operator=(ClusterInfo&&) = default;

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

    uint32_t GetClusterSize() const {
        return csInfo_.size();
    }

 protected:
    std::vector<ChunkServerInfo> csInfo_;
};

struct CopysetConstrait {
    uint32_t zoneNum;
    uint32_t zoneChoseNum;
    uint32_t replicaNum;

    CopysetConstrait()
    : zoneNum(0),
      zoneChoseNum(0),
      replicaNum(0) {}

    static const uint32_t NUM_ANY = 0;
};

}  // namespace copyset
}  // namespace mds
}  // namespace curve


#endif  // SRC_MDS_COPYSET_COPYSET_STRUCTURE_H_
