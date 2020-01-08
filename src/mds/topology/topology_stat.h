/*
 * Project: curve
 * Created Date: Thu Nov 22 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
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
    // 逻辑池id
    PoolIdType logicalPoolId;
    // copysetid
    CopySetIdType copysetId;
    // leader id
    ChunkServerIdType leader;
    // 读带宽
    uint32_t readRate;
    // 写带宽
    uint32_t writeRate;
    // 读iops
    uint32_t readIOPS;
    // 写iops
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
    // 心跳上报的leader数量
    uint32_t leaderCount;
    // 心跳上报的copyset数量
    uint32_t copysetCount;
    // 读带宽
    uint32_t readRate;
    // 写带宽
    uint32_t writeRate;
    // 读iops
    uint32_t readIOPS;
    // 写iops
    uint32_t writeIOPS;
    // 已使用的chunk占用的磁盘空间
    uint64_t chunkSizeUsedBytes;
    // chunkfilepool中未使用的chunk占用的磁盘空间
    uint64_t chunkSizeLeftBytes;
    // 回收站中chunk占用的磁盘空间
    uint64_t chunkSizeTrashedBytes;

    // copyset数据
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
 * @brief Topology统计模块，负责处理各种统计数据
 */
class TopologyStat {
 public:
    TopologyStat() {}
    ~TopologyStat() {}

    // 心跳来源

    /**
     * @brief 更新心跳来源的chunkserver统计数据
     *
     * @param csId chunkserverId
     * @param stat 心跳传来的chunkserver统计数据
     */
    virtual void UpdateChunkServerStat(ChunkServerIdType csId,
        const ChunkServerStat &stat) = 0;
    /**
     * @brief 获取心跳来源的chunkserver统计数据
     *
     * @param csId chunkserverId
     * @param[out] stat 心跳传来的chunkserver统计数据
     *
     * @retval true 获取成功
     * @retval false 未找到
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
     * @brief 心跳来源的chunkserver统计数据
     */
    std::map<ChunkServerIdType, ChunkServerStat>  chunkServerStats_;
    /**
     * @brief 保护chunkServerStats_并发访问的锁
     */
    mutable curve::common::RWLock statsLock_;

    /**
     * @brief topology模块
     */
    std::shared_ptr<Topology> topo_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_STAT_H_
