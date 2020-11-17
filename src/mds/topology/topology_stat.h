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
 * Created Date: Thu Nov 22 2018
 * Author: xuchaojie
 */

#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_STAT_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_STAT_H_


#include <vector>
#include <map>
#include <string>
#include <memory>

#include "src/mds/common/mds_define.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_item.h"
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace mds {
namespace topology {

struct CopysetStat {
    // logical pool id
    PoolIdType logicalPoolId;
    // Copyset id
    CopySetIdType copysetId;
    // Leader id
    ChunkServerIdType leader;
    // Reading bandwidth
    uint32_t readRate;
    // Writing bandwidth
    uint32_t writeRate;
    // Reading IOPS
    uint32_t readIOPS;
    // Writing IOPS
    uint32_t writeIOPS;
    CopysetStat() :
        logicalPoolId(UNINTIALIZE_ID),
        copysetId(UNINTIALIZE_ID),
        leader(UNINTIALIZE_ID),
        readRate(0),
        writeRate(0),
        readIOPS(0),
        writeIOPS(0) {}
};

struct ChunkServerStat {
    // Leader number the heartbeat reported
    uint32_t leaderCount;
    // Copyset number the heartbeat reported
    uint32_t copysetCount;
    // Reading Bandwidth
    uint32_t readRate;
    // Writing bandwidth
    uint32_t writeRate;
    // Reading IOPS
    uint32_t readIOPS;
    // Writing IOPS
    uint32_t writeIOPS;
    // Size of chunks already used
    uint64_t chunkSizeUsedBytes;
    // Size of chunks unused
    uint64_t chunkSizeLeftBytes;
    // Size of chunks in recycle bin
    uint64_t chunkSizeTrashedBytes;

    // Copyset statistic
    std::vector<CopysetStat> copysetStats;

    ChunkServerStat() :
        leaderCount(0),
        copysetCount(0),
        readRate(0),
        writeRate(0),
        readIOPS(0),
        writeIOPS(0) {}
};

/**
 * @brief Topology statistic module for managing its stats
 */
class TopologyStat {
 public:
    TopologyStat() {}
    ~TopologyStat() {}

    /**
     * @brief Update the statistic of the chunkservers that sent heartbeat
     *
     * @param csId chunkserverId
     * @param stat statistic brought by the heartbeat
     */
    virtual void UpdateChunkServerStat(ChunkServerIdType csId,
        const ChunkServerStat &stat) = 0;
    /**
     * @brief fetch the statistic information of chunkservers that sent by heartbeat
     *
     * @param csId chunkserverId
     * @param[out] stat statistic of the chunkserver
     *
     * @retval true if succeeded
     * @retval false if failed
     */
    virtual bool GetChunkServerStat(ChunkServerIdType csId,
        ChunkServerStat *stat) = 0;
};

class TopologyStatImpl : public TopologyStat {
 public:
    explicit TopologyStatImpl(std::shared_ptr<Topology> topo)
        : topo_(topo) {}

    int Init();

    void UpdateChunkServerStat(ChunkServerIdType csId,
        const ChunkServerStat &stat) override;
    bool GetChunkServerStat(ChunkServerIdType csId,
        ChunkServerStat *stat) override;

 private:
    /**
     * @brief chunkserver statistic
     */
    std::map<ChunkServerIdType, ChunkServerStat>  chunkServerStats_;
    /**
     * @brief the lock for protecting concurrent visit of chunkServerStats_
     */
    mutable curve::common::RWLock statsLock_;

    /**
     * @brief topology module
     */
    std::shared_ptr<Topology> topo_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_STAT_H_
