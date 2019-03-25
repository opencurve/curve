/*
 * Project: curve
 * Created Date: Thu Nov 22 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_STAT_H_
#define CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_STAT_H_

#include <unordered_map>
#include <vector>

#include "src/mds/common/mds_define.h"
#include "src/common/concurrent/rw_lock.h"

namespace curve {
namespace mds {
namespace topology {

class CopysetStat {
 public:
    PoolIdType logicalPoolId;
    CopySetIdType copysetId;
    ChunkServerIdType leader;
    uint32_t readRate;
    uint32_t writeRate;
    uint32_t readIOPS;
    uint32_t writeIOPS;
};


class ChunkServerStat {
 public:
    uint32_t leaderCount;
    uint32_t copysetCount;
    uint32_t readRate;
    uint32_t writeRate;
    uint32_t readIOPS;
    uint32_t writeIOPS;
    std::vector<CopysetStat> copysetStats;
    uint64_t updateTime;
};

class TopologyStat {
 public:
    TopologyStat() {}
    ~TopologyStat() {}

    virtual void UpdateChunkServerStat(ChunkServerIdType csId,
        const ChunkServerStat &stat) = 0;
    virtual void GetChunkServerStat(ChunkServerIdType csId,
        ChunkServerStat *stat) const = 0;

    virtual void GetAllChunkServerStat(
        std::unordered_map<ChunkServerIdType,
            ChunkServerStat> *chunkServerStats) const = 0;
};

class TopologyStatImpl : public TopologyStat {
 public:
    TopologyStatImpl() {}
    ~TopologyStatImpl() {}

    void UpdateChunkServerStat(ChunkServerIdType csId,
        const ChunkServerStat &stat) override;
    void GetChunkServerStat(ChunkServerIdType csId,
        ChunkServerStat *stat) const override;

    void GetAllChunkServerStat(
        std::unordered_map<ChunkServerIdType,
            ChunkServerStat> *chunkServerStats) const override;

 private:
    std::unordered_map<ChunkServerIdType, ChunkServerStat>  chunkServerStats_;
    mutable curve::common::RWLock CsStatsLock_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_STAT_H_
