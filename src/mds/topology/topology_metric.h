/*
 * Project: curve
 * Created Date: Thu Jun 27 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_METRIC_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_METRIC_H_

#include <bvar/bvar.h>

#include <vector>
#include <map>
#include <string>
#include <memory>
#include <utility>

#include "src/mds/topology/topology_stat.h"

namespace curve {
namespace mds {
namespace topology {

struct ChunkServerMetric {
    const std::string kTopologyChunkServerMetricPrefix =
        "topology_metric_chunkserver_Id_";
    // scatterWidth
    bvar::Status<uint32_t> scatterWidth;
    // copyset数量
    bvar::Status<uint32_t> copysetNum;
    // leader数量
    bvar::Status<uint32_t> leaderNum;

    // 磁盘容量
    bvar::Status<uint64_t> diskCapacity;
    // 磁盘使用量
    bvar::Status<uint64_t> diskUsed;

    // 心跳上报的leader数量
    bvar::Status<uint32_t> leaderCount;
    // 心跳上报的copyset数量
    bvar::Status<uint32_t> copysetCount;
    // 读带宽
    bvar::Status<uint32_t> readRate;
    // 写带宽
    bvar::Status<uint32_t> writeRate;
    // 读iops
    bvar::Status<uint32_t> readIOPS;
    // 写iops
    bvar::Status<uint32_t> writeIOPS;

    explicit ChunkServerMetric(ChunkServerIdType csId) :
        scatterWidth(kTopologyChunkServerMetricPrefix,
            std::to_string(csId) + "_scatterwidth", 0),
        copysetNum(kTopologyChunkServerMetricPrefix,
            std::to_string(csId) + "_copysetnum", 0),
        leaderNum(kTopologyChunkServerMetricPrefix,
            std::to_string(csId) + "_leadernum", 0),
        diskCapacity(kTopologyChunkServerMetricPrefix,
            std::to_string(csId) + "_diskCapacity", 0),
        diskUsed(kTopologyChunkServerMetricPrefix,
            std::to_string(csId) + "_diskUsed", 0),
        leaderCount(kTopologyChunkServerMetricPrefix,
            std::to_string(csId) + "_leaderCount_from_heartbeat", 0),
        copysetCount(kTopologyChunkServerMetricPrefix,
            std::to_string(csId) + "_copysetCount_from_heartbeat", 0),
        readRate(kTopologyChunkServerMetricPrefix,
            std::to_string(csId) + "_readRate", 0),
        writeRate(kTopologyChunkServerMetricPrefix,
            std::to_string(csId) + "_writeRate", 0),
        readIOPS(kTopologyChunkServerMetricPrefix,
            std::to_string(csId) + "_readIOPS", 0),
        writeIOPS(kTopologyChunkServerMetricPrefix,
            std::to_string(csId) + "_writeIOPS", 0) {}
};
using ChunkServerMetricPtr = std::unique_ptr<ChunkServerMetric>;

struct LogicalPoolMetric {
    const std::string kTopologyLogicalPoolMetricPrefix =
        "topology_metric_logicalPool_";
    // chunkserver数量
    bvar::Status<uint32_t> chunkServerNum;
    // copyset数量
    bvar::Status<uint32_t> copysetNum;
    // scatterWidth平均值
    bvar::Status<double> scatterWidthAvg;
    // scatterWidth方差
    bvar::Status<double> scatterWidthVariance;
    // scatterWidth标准差
    bvar::Status<double> scatterWidthStandardDeviation;
    // scatterWidth极差
    bvar::Status<double> scatterWidthRange;
    // scatterWidth最小值
    bvar::Status<double> scatterWidthMin;
    // scatterWidth最大值
    bvar::Status<double> scatterWidthMax;
    // copyset数量平均值
    bvar::Status<double> copysetNumAvg;
    // copyset数量方差
    bvar::Status<double> copysetNumVariance;
    // copyset数量标准差
    bvar::Status<double> copysetNumStandardDeviation;
    // copyset数量极差
    bvar::Status<double> copysetNumRange;
    // copyset数量最小值
    bvar::Status<double> copysetNumMin;
    // copyset数量最大值
    bvar::Status<double> copysetNumMax;
    // leader数量平均值
    bvar::Status<double> leaderNumAvg;
    // leader数量方差
    bvar::Status<double> leaderNumVariance;
    // leader数量标准差
    bvar::Status<double> leaderNumStandardDeviation;
    // leader数量极差
    bvar::Status<double> leaderNumRange;
    // leader数量最小值
    bvar::Status<double> leaderNumMin;
    // leader数量最大值
    bvar::Status<double> leaderNumMax;

    // 整个逻辑池的容量
    bvar::Status<uint64_t> diskCapacity;
    // 整个逻辑池的使用量
    bvar::Status<uint64_t> diskUsed;

    explicit LogicalPoolMetric(const std::string &logicalPoolName) :
        chunkServerNum(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_chunkserver_num", 0),
        copysetNum(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_copyset_num", 0),
        scatterWidthAvg(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_scatterwidth_avg", 0),
        scatterWidthVariance(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_scatterwidth_variance", 0),
        scatterWidthStandardDeviation(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_scatterwidth_standard_deviation", 0),
        scatterWidthRange(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_scatterwidth_range", 0),
        scatterWidthMin(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_scatterwidth_min", 0),
        scatterWidthMax(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_scatterwidth_max", 0),
        copysetNumAvg(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_copysetnum_avg", 0),
        copysetNumVariance(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_copysetnum_variance", 0),
        copysetNumStandardDeviation(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_copysetnum_standard_deviation", 0),
        copysetNumRange(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_copysetnum_range", 0),
        copysetNumMin(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_copysetnum_min", 0),
        copysetNumMax(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_copysetnum_max", 0),
        leaderNumAvg(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_leadernum_avg", 0),
        leaderNumVariance(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_leadernum_variance", 0),
        leaderNumStandardDeviation(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_leadernum_standard_deviation", 0),
        leaderNumRange(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_leadernum_range", 0),
        leaderNumMin(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_leadernum_min", 0),
        leaderNumMax(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_leadernum_max", 0),
        diskCapacity(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_diskCapacity", 0),
        diskUsed(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_diskUsed", 0) {}
};
using LogicalPoolMetricPtr = std::unique_ptr<LogicalPoolMetric>;

extern std::map<PoolIdType, LogicalPoolMetricPtr> gLogicalPoolMetrics;
extern std::map<ChunkServerIdType, ChunkServerMetricPtr> gChunkServerMetrics;

class TopologyMetricService {
 public:
    struct ChunkServerMetricInfo {
        // scatterWidth
        uint32_t scatterWidth;
        // copyset Num
        uint32_t copysetNum;
        // leader Num
        uint32_t leaderNum;
        ChunkServerMetricInfo() :
            scatterWidth(0),
            copysetNum(0),
            leaderNum(0) {}
    };

    struct LogicalPoolMetricInfo {
        // scatterWidth平均值
        double scatterWidthAvg;
        // scatterWidth方差
        double scatterWidthVariance;
        // scatterWidth标准差
        double scatterWidthStandardDeviation;
        // scatterWidth极差
        double scatterWidthRange;
        // scatterWidth最小值
        double scatterWidthMin;
        // scatterWidth最大值
        double scatterWidthMax;
        // copyset数量平均值
        double copysetNumAvg;
        // copyset数量方差
        double copysetNumVariance;
        // copyset数量标准差
        double copysetNumStandardDeviation;
        // copyset数量极差
        double copysetNumRange;
        // copyset数量最小值
        double copysetNumMin;
        // copyset数量最大值
        double copysetNumMax;
        // leader数量平均值
        double leaderNumAvg;
        // leader数量方差
        double leaderNumVariance;
        // leader数量标准差
        double leaderNumStandardDeviation;
        // leader数量极差
        double leaderNumRange;
        // leader数量最小值
        double leaderNumMin;
        // leader数量最大值
        double leaderNumMax;
        LogicalPoolMetricInfo() :
            scatterWidthAvg(0.0),
            scatterWidthVariance(0.0),
            scatterWidthStandardDeviation(0.0),
            scatterWidthRange(0.0),
            scatterWidthMin(0.0),
            scatterWidthMax(0.0),
            copysetNumAvg(0.0),
            copysetNumVariance(0.0),
            copysetNumStandardDeviation(0.0),
            copysetNumRange(0.0),
            copysetNumMin(0.0),
            copysetNumMax(0.0),
            leaderNumAvg(0.0),
            leaderNumVariance(0.0),
            leaderNumStandardDeviation(0.0),
            leaderNumRange(0.0),
            leaderNumMin(0.0),
            leaderNumMax(0.0) {}
    };

 public:
     TopologyMetricService(std::shared_ptr<Topology> topo,
        std::shared_ptr<TopologyStat> topoStat)
        : topo_(topo),
          topoStat_(topoStat),
          isStop_(true) {}
    ~TopologyMetricService() {
        Stop();
    }

    int Init(const TopologyOption &option);

    int Run();

    int Stop();

    void UpdateTopologyMetrics();

    /**
     * @brief 计算chunkserver的metric数据
     *
     * @param copysets CopySetInfo列表
     * @param[out] csMetricInfoMap metric数据
     */
    void CalcChunkServerMetrics(const std::vector<CopySetInfo> &copysets,
        std::map<ChunkServerIdType, ChunkServerMetricInfo> *csMetricInfoMap);

    /**
     * @brief 计算逻辑池的metric数据
     *
     * @param csMetricInfoMap chunkserver的metric数据
     * @param[out] poolMetricInfo 逻辑池的metric数据
     */
    void CalcLogicalPoolMetrics(
        const std::map<ChunkServerIdType,
            ChunkServerMetricInfo> &csMetricInfoMap,
        LogicalPoolMetricInfo *poolMetricInfo);

 private:
    /**
     * @brief 后来线程执行函数，定期执行UpdateTopologyMetrics
     */
    void BackEndFunc();

 private:
    /**
     * @brief topology模块
     */
    std::shared_ptr<Topology> topo_;
    /**
     * @brief topology统计模块
     */
    std::shared_ptr<TopologyStat> topoStat_;
    /**
     * @brief 后台线程
     */
    curve::common::Thread backEndThread_;
    /**
     * @brief 是否停止后台线程的标准位
     */
    curve::common::Atomic<bool> isStop_;

    /**
     * @brief 拓扑配置项
     */
    TopologyOption option_;
};


}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_METRIC_H_
