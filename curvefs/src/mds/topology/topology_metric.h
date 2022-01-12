/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Project: curvefs
 * Created Date: 2021/11/1
 * Author: chenwei
 */

#ifndef CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_METRIC_H_
#define CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_METRIC_H_

#include <bvar/bvar.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/src/mds/topology/topology.h"
#include "src/common/interruptible_sleeper.h"

using ::curve::common::InterruptibleSleeper;

namespace curvefs {
namespace mds {
namespace topology {

struct MetaServerMetric {
    const std::string kTopologyMetaServerMetricPrefix =
        "topology_metric_metaserver_id_";
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
    // memory utilization
    bvar::Status<uint64_t> memoryUsed;
    // partition numbers
    bvar::Status<uint32_t> partitionNum;

    explicit MetaServerMetric(MetaServerIdType msId)
        : scatterWidth(kTopologyMetaServerMetricPrefix,
                       std::to_string(msId) + "_scatterwidth", 0),
          copysetNum(kTopologyMetaServerMetricPrefix,
                     std::to_string(msId) + "_copyset_num", 0),
          leaderNum(kTopologyMetaServerMetricPrefix,
                    std::to_string(msId) + "_leader_num", 0),
          diskCapacity(kTopologyMetaServerMetricPrefix,
                       std::to_string(msId) + "_disk_capacity", 0),
          diskUsed(kTopologyMetaServerMetricPrefix,
                   std::to_string(msId) + "_disk_used", 0),
          memoryUsed(kTopologyMetaServerMetricPrefix,
                     std::to_string(msId) + "_memory_used", 0),
          partitionNum(kTopologyMetaServerMetricPrefix,
                       std::to_string(msId) + "_partition_num", 0) {}
};

using MetaServerMetricPtr = std::unique_ptr<MetaServerMetric>;

struct PoolMetric {
    const std::string kTopologyPoolMetricPrefix = "topology_metric_pool_";
    bvar::Status<uint32_t> metaServerNum;
    bvar::Status<uint32_t> copysetNum;
    bvar::Status<uint64_t> diskCapacity;
    bvar::Status<uint64_t> diskUsed;
    bvar::Status<uint64_t> inodeNum;
    bvar::Status<uint64_t> dentryNum;
    bvar::Status<uint64_t> partitionNum;

    explicit PoolMetric(const std::string &poolName)
        : metaServerNum(kTopologyPoolMetricPrefix, poolName + "_metaserver_num",
                        0),
          copysetNum(kTopologyPoolMetricPrefix, poolName + "_copyset_num", 0),

          diskCapacity(kTopologyPoolMetricPrefix, poolName + "_disk_capacity",
                       0),
          diskUsed(kTopologyPoolMetricPrefix, poolName + "_disk_used", 0),
          inodeNum(kTopologyPoolMetricPrefix, poolName + "_inode_num", 0),
          dentryNum(kTopologyPoolMetricPrefix, poolName + "_dentry_num", 0),
          partitionNum(kTopologyPoolMetricPrefix, poolName + "_partition_num",
                       0) {}
};
using PoolMetricPtr = std::unique_ptr<PoolMetric>;

extern std::map<PoolIdType, PoolMetricPtr> gPoolMetrics;
extern std::map<MetaServerIdType, MetaServerMetricPtr> gMetaServerMetrics;

class TopologyMetricService {
 public:
    struct MetaServerMetricInfo {
        uint32_t scatterWidth;
        uint32_t copysetNum;
        uint32_t leaderNum;
        uint32_t partitionNum;

        MetaServerMetricInfo()
            : scatterWidth(0), copysetNum(0), leaderNum(0), partitionNum(0) {}
    };

 public:
    explicit TopologyMetricService(std::shared_ptr<Topology> topo)
        : topo_(topo), isStop_(true) {}
    ~TopologyMetricService() { Stop(); }

    void Init(const TopologyOption &option);

    void Run();

    void Stop();

    void UpdateTopologyMetrics();

    /**
     * @brief calculate metric data of metaserver
     *
     * @param copysets CopySetInfo list
     * @param[out] msMetricInfoMap metric data
     */
    void CalcMetaServerMetrics(
        const std::vector<CopySetInfo> &copysets,
        std::map<MetaServerIdType, MetaServerMetricInfo> *msMetricInfoMap);

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
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_METRIC_H_
