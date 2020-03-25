/*
 * Project: curve
 * File Created: 2019-12-04
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <bvar/bvar.h>

#include <string>
#include <map>

#ifndef SRC_TOOLS_METRIC_NAME_H_
#define SRC_TOOLS_METRIC_NAME_H_


namespace curve {
namespace tool {

const char kLogicalPoolMetricPrefix[] = "topology_metric_logicalPool_";
const char kChunkServerMetricPrefix[] = "chunkserver_";
const char kOperatorNumMetricName[] = "mds_scheduler_metric_operator_num";
const char kMdsListenAddrMetricName[] = "mds_config_mds_listen_addr";
const char kCurveVersionMetricName[] = "curve_version";
const char kSechduleOpMetricpPrefix[] = "mds_scheduler_metric_";

// operator名称
const char kTotalOpName[] = "operator";
const char kChangeOpName[] = "change_peer";
const char kAddOpName[] = "add_peer";
const char kRemoveOpName[] = "remove_peer";
const char kTransferOpName[] = "transfer_leader";

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

inline std::string GetCSLeftChunkName(const std::string& csAddr) {
    std::string tmpName = kChunkServerMetricPrefix +
                        csAddr + "_chunkfilepool_left";
    std::string metricName;
    bvar::to_underscored_name(&metricName, tmpName);
    return metricName;
}

inline std::string GetOpNumMetricName(const std::string& opName) {
    std::string tmpName = kSechduleOpMetricpPrefix +
                                opName + "_num";
    std::string metricName;
    bvar::to_underscored_name(&metricName, tmpName);
    return metricName;
}

inline bool SupportOpName(const std::string& opName) {
    return opName == kTotalOpName || opName == kChangeOpName
                || opName == kAddOpName || opName == kRemoveOpName
                || opName == kTransferOpName;
}

inline void PrintSupportOpName() {
    std::cout << kTotalOpName << ", " << kChangeOpName
              << ", " << kAddOpName << ", " << kRemoveOpName
              << ", " << kTransferOpName << std::endl;
}

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_METRIC_NAME_H_
