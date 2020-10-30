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
 * Created Date: Thu Jun 27 2019
 * Author: xuchaojie
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
#include "src/mds/nameserver2/allocstatistic/alloc_statistic.h"
#include "src/common/interruptible_sleeper.h"

using ::curve::common::InterruptibleSleeper;

namespace curve {
namespace mds {
namespace topology {

struct ChunkServerMetric {
    const std::string kTopologyChunkServerMetricPrefix =
        "topology_metric_chunkserver_Id_";
    // scatterWidth
    bvar::Status<uint32_t> scatterWidth;
    // copyset numbers
    bvar::Status<uint32_t> copysetNum;
    // leader numbers
    bvar::Status<uint32_t> leaderNum;

    // disk capacity
    bvar::Status<uint64_t> diskCapacity;
    // disk utilization
    bvar::Status<uint64_t> diskUsed;

    // number of leaders reported by heartbeat
    bvar::Status<uint32_t> leaderCount;
    // number of copyset reported by heartbeat
    bvar::Status<uint32_t> copysetCount;
    // bandwidth of reading
    bvar::Status<uint32_t> readRate;
    // bandwidth of writing
    bvar::Status<uint32_t> writeRate;
    // IOPS of reading
    bvar::Status<uint32_t> readIOPS;
    // IOPS of writing
    bvar::Status<uint32_t> writeIOPS;
    // disk capacity of chunkfilepool occupied by used chunks
    bvar::Status<uint64_t> chunkSizeUsedBytes;
    // disk capacity of chunkfilepool occupied by unused chunks
    bvar::Status<uint64_t> chunkSizeLeftBytes;
    // disk capacity of recycle bin occupied by chunks
    bvar::Status<uint64_t> chunkSizeTrashedBytes;
    // total capacity
    bvar::Status<uint64_t> chunkSizeTotalBytes;

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
            std::to_string(csId) + "_writeIOPS", 0),
        chunkSizeUsedBytes(kTopologyChunkServerMetricPrefix,
            std::to_string(csId) + "_chunkSizeUsedBytes", 0),
        chunkSizeLeftBytes(kTopologyChunkServerMetricPrefix,
            std::to_string(csId) + "_chunkSizeLeftBytes", 0),
        chunkSizeTrashedBytes(kTopologyChunkServerMetricPrefix,
            std::to_string(csId) + "_chunkSizeTrashedBytes", 0),
        chunkSizeTotalBytes(kTopologyChunkServerMetricPrefix,
            std::to_string(csId) + "_chunkSizeTotalBytes", 0) {}
};
using ChunkServerMetricPtr = std::unique_ptr<ChunkServerMetric>;

struct LogicalPoolMetric {
    const std::string kTopologyLogicalPoolMetricPrefix =
        "topology_metric_logicalPool_";
    // chunkserver number
    bvar::Status<uint32_t> chunkServerNum;
    // copyset number
    bvar::Status<uint32_t> copysetNum;
    // average scatterWidth
    bvar::Status<double> scatterWidthAvg;
    // variance of scatterWidth
    bvar::Status<double> scatterWidthVariance;
    // standard deviation of scatterWidth
    bvar::Status<double> scatterWidthStandardDeviation;
    // range of scatterWidth
    bvar::Status<double> scatterWidthRange;
    // minimum scatterWidth
    bvar::Status<double> scatterWidthMin;
    // maximum scatterWidth
    bvar::Status<double> scatterWidthMax;
    // average copyset number
    bvar::Status<double> copysetNumAvg;
    // variance of copyset number
    bvar::Status<double> copysetNumVariance;
    // standard deviation of copyset number
    bvar::Status<double> copysetNumStandardDeviation;
    // range of copyset number
    bvar::Status<double> copysetNumRange;
    // minimum copyset number
    bvar::Status<double> copysetNumMin;
    // maximum copyset number
    bvar::Status<double> copysetNumMax;
    // average value of leader number
    bvar::Status<double> leaderNumAvg;
    // variance of leader number
    bvar::Status<double> leaderNumVariance;
    // standard deviation of leader number
    bvar::Status<double> leaderNumStandardDeviation;
    // range of leader number
    bvar::Status<double> leaderNumRange;
    // minimum leader number
    bvar::Status<double> leaderNumMin;
    // maximum leader number
    bvar::Status<double> leaderNumMax;

    // capacity of the whole logical pool, which is also
    // the capacity of the whole physical pool
    bvar::Status<uint64_t> diskCapacity;
    // capacity allocated of the entire logical pool / physical pool
    bvar::Status<uint64_t> diskAlloc;
    // utilization of the entire logical pool / physical pool
    bvar::Status<uint64_t> diskUsed;

    // disk capacity occupied by used chunks
    bvar::Status<uint64_t> chunkSizeUsedBytes;
    // disk capacity occupied by unused chunks in chunkfilepool
    bvar::Status<uint64_t> chunkSizeLeftBytes;
    // capacity occupied by chunks in recycle bin
    bvar::Status<uint64_t> chunkSizeTrashedBytes;
    // total capacity
    bvar::Status<uint64_t> chunkSizeTotalBytes;
    // logical total capacity
    bvar::Status<uint64_t> logicalCapacity;
    // bytes have alloced
    bvar::Status<uint64_t> logicalAlloc;

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
        diskAlloc(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_diskAlloc", 0),
        diskUsed(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_diskUsed", 0),
        chunkSizeUsedBytes(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_chunkSizeUsedBytes", 0),
        chunkSizeLeftBytes(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_chunkSizeLeftBytes", 0),
        chunkSizeTrashedBytes(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_chunkSizeTrashedBytes", 0),
        chunkSizeTotalBytes(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_chunkSizeTotalBytes", 0),
        logicalCapacity(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_logicalCapacity", 0),
        logicalAlloc(kTopologyLogicalPoolMetricPrefix,
            logicalPoolName + "_logicalAlloc", 0) {}
};
using LogicalPoolMetricPtr = std::unique_ptr<LogicalPoolMetric>;

extern std::map<PoolIdType, LogicalPoolMetricPtr> gLogicalPoolMetrics;
extern std::map<ChunkServerIdType, ChunkServerMetricPtr> gChunkServerMetrics;

class TopologyMetricService {
 public:
    struct ChunkServerMetricInfo {
        // scatterWidth
        uint32_t scatterWidth;
        // copyset number
        uint32_t copysetNum;
        // leader number
        uint32_t leaderNum;

        ChunkServerMetricInfo() :
            scatterWidth(0),
            copysetNum(0),
            leaderNum(0) {}
    };

    struct LogicalPoolMetricInfo {
        // average scatterWidth
        double scatterWidthAvg;
        // variance of scatterWidth
        double scatterWidthVariance;
        // standard deviation of scatterWidth
        double scatterWidthStandardDeviation;
        // range of scatterWidth
        double scatterWidthRange;
        // minimum scatterWidth
        double scatterWidthMin;
        // maximum scatterWidth
        double scatterWidthMax;
        // average value of copyset number
        double copysetNumAvg;
        // variance of copyset number
        double copysetNumVariance;
        // standard deviation of copyset
        double copysetNumStandardDeviation;
        // range if copyset number
        double copysetNumRange;
        // minumum copyset number
        double copysetNumMin;
        // maximum copyset number
        double copysetNumMax;
        // average leader number
        double leaderNumAvg;
        // variance of leader number
        double leaderNumVariance;
        // standard deviation of leader number
        double leaderNumStandardDeviation;
        // range of leader number
        double leaderNumRange;
        // minimum leader number
        double leaderNumMin;
        // maximum leader number
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
        std::shared_ptr<TopologyStat> topoStat,
        std::shared_ptr<AllocStatistic> allocStatistic)
        : topo_(topo),
          topoStat_(topoStat),
          allocStatistic_(allocStatistic),
          isStop_(true) {}
    ~TopologyMetricService() {
        Stop();
    }

    int Init(const TopologyOption &option);

    int Run();

    int Stop();

    void UpdateTopologyMetrics();

    /**
     * @brief calculate metric data of chunkserver
     *
     * @param copysets CopySetInfo list
     * @param[out] csMetricInfoMap metric data
     */
    void CalcChunkServerMetrics(const std::vector<CopySetInfo> &copysets,
        std::map<ChunkServerIdType, ChunkServerMetricInfo> *csMetricInfoMap);

    /**
     * @brief calculate metric data of logical pool
     *
     * @param csMetricInfoMap metric data of chunkserver
     * @param[out] poolMetricInfo metric data of logical pool
     */
    void CalcLogicalPoolMetrics(
        const std::map<ChunkServerIdType,
            ChunkServerMetricInfo> &csMetricInfoMap,
        LogicalPoolMetricInfo *poolMetricInfo);

 private:
    /**
     * @brief backend function that executes UpdateTopologyMetrics regularly
     */
    void BackEndFunc();

 private:
    /**
     * @brief topology module
     */
    std::shared_ptr<Topology> topo_;
    /**
     * @brief topology statistic module
     */
    std::shared_ptr<TopologyStat> topoStat_;
    /**
     * @brief allocation statistic module
     */
    std::shared_ptr<AllocStatistic> allocStatistic_;
    /**
     * @brief backend thread
     */
    curve::common::Thread backEndThread_;
    /**
     * @brief flag marking out stopping the backend thread or not
     */
    curve::common::Atomic<bool> isStop_;

    InterruptibleSleeper sleeper_;

    /**
     * @brief topology options
     */
    TopologyOption option_;
};


}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_METRIC_H_
