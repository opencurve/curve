/*
 * Project: curve
 * File Created: 2019-12-04
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <bvar/bvar.h>

#include <string>

#ifndef SRC_TOOLS_METRIC_NAME_H_
#define SRC_TOOLS_METRIC_NAME_H_


namespace curve {
namespace tool {

const char kOperatorNum[] = "mds_scheduler_metric_operator_num";
const char kLogicalPoolMetricPrefix[] = "topology_metric_logicalPool_";
const char kChunkServerMetricPrefix[] = "topology_metric_chunkserver_Id_";

inline std::string GetPoolTotalBytesName(
                            const std::string& poolName) {
    std::string tmpName = kLogicalPoolMetricPrefix +
                                poolName + "_chunkSizeTotalBytes";
    std::string metricName;
    bvar::to_underscored_name(&metricName, tmpName);
    return metricName;
}

inline std::string GetPoolUsedBytesName(
                            const std::string& poolName) {
    std::string tmpName = kLogicalPoolMetricPrefix +
                            poolName + "_chunkSizeUsedBytes";
    std::string metricName;
    bvar::to_underscored_name(&metricName, tmpName);
    return metricName;
}

inline std::string GetPoolDiskAllocName(
                            const std::string& poolName) {
    std::string tmpName = kLogicalPoolMetricPrefix +
                            poolName + "_diskAlloc";
    std::string metricName;
    bvar::to_underscored_name(&metricName, tmpName);
    return metricName;
}

inline std::string GetCSLeftBytesName(uint64_t id) {
    std::string tmpName = kChunkServerMetricPrefix +
                        std::to_string(id) + "_chunkSizeLeftBytes";
    std::string metricName;
    bvar::to_underscored_name(&metricName, tmpName);
    return metricName;
}
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_METRIC_NAME_H_
