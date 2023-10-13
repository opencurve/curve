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
 * File Created: 2019-12-04
 * Author: charisu
 */

#include <bvar/bvar.h>

#include <map>
#include <string>

#ifndef SRC_TOOLS_METRIC_NAME_H_
#define SRC_TOOLS_METRIC_NAME_H_

namespace curve
{
    namespace tool
    {

        // common metric name
        const char kCurveVersionMetricName[] = "curve_version";

        // snapshot clone server metric name
        const char kSnapshotCloneConfMetricName[] =
            "snapshot_clone_server_config_server_address";
        const char kSnapshotCloneStatusMetricName[] = "snapshotcloneserver_status";
        const char kSnapshotCloneStatusActive[] = "active";

        // mds metric name
        const char kLogicalPoolMetricPrefix[] = "topology_metric_logicalPool_";
        const char kChunkServerMetricPrefix[] = "chunkserver_";
        const char kOperatorNumMetricName[] = "mds_scheduler_metric_operator_num";
        const char kProcessCmdLineMetricName[] = "process_cmdline";
        const char kSechduleOpMetricpPrefix[] = "mds_scheduler_metric_";
        const char kMdsListenAddrMetricName[] = "mds_config_mds_listen_addr";
        const char kMdsStatusMetricName[] = "mds_status";
        const char kMdsStatusLeader[] = "leader";
        // operator Name
        const char kTotalOpName[] = "operator";
        const char kChangeOpName[] = "change_peer";
        const char kAddOpName[] = "add_peer";
        const char kRemoveOpName[] = "remove_peer";
        const char kTransferOpName[] = "transfer_leader";

        inline std::string GetPoolTotalChunkSizeName(const std::string &poolName)
        {
            std::string tmpName =
                kLogicalPoolMetricPrefix + poolName + "_chunkSizeTotalBytes";
            std::string metricName;
            bvar::to_underscored_name(&metricName, tmpName);
            return metricName;
        }

        inline std::string GetPoolUsedChunkSizeName(const std::string &poolName)
        {
            std::string tmpName =
                kLogicalPoolMetricPrefix + poolName + "_chunkSizeUsedBytes";
            std::string metricName;
            bvar::to_underscored_name(&metricName, tmpName);
            return metricName;
        }

        inline std::string GetPoolLogicalCapacityName(const std::string &poolName)
        {
            std::string tmpName =
                kLogicalPoolMetricPrefix + poolName + "_logicalCapacity";
            std::string metricName;
            bvar::to_underscored_name(&metricName, tmpName);
            return metricName;
        }

        inline std::string GetPoolLogicalAllocName(const std::string &poolName)
        {
            std::string tmpName = kLogicalPoolMetricPrefix + poolName + "_logicalAlloc";
            std::string metricName;
            bvar::to_underscored_name(&metricName, tmpName);
            return metricName;
        }

        inline std::string GetCSLeftChunkName(const std::string &csAddr)
        {
            std::string tmpName =
                kChunkServerMetricPrefix + csAddr + "_chunkfilepool_left";
            std::string metricName;
            bvar::to_underscored_name(&metricName, tmpName);
            return metricName;
        }

        inline std::string GetCSLeftWalSegmentName(const std::string &csAddr)
        {
            std::string tmpName =
                kChunkServerMetricPrefix + csAddr + "_walfilepool_left";
            std::string metricName;
            bvar::to_underscored_name(&metricName, tmpName);
            return metricName;
        }

        inline std::string GetUseWalPoolName(const std::string &csAddr)
        {
            std::string tmpName =
                kChunkServerMetricPrefix + csAddr + "_config_copyset_raft_log_uri";
            std::string metricName;
            bvar::to_underscored_name(&metricName, tmpName);
            return metricName;
        }

        inline std::string GetUseChunkFilePoolAsWalPoolName(const std::string &csAddr)
        {
            std::string tmpName = kChunkServerMetricPrefix + csAddr +
                                  "_config_walfilepool_use_chunk_file_pool";
            std::string metricName;
            bvar::to_underscored_name(&metricName, tmpName);
            return metricName;
        }

        inline std::string GetOpNumMetricName(const std::string &opName)
        {
            std::string tmpName = kSechduleOpMetricpPrefix + opName + "_num";
            std::string metricName;
            bvar::to_underscored_name(&metricName, tmpName);
            return metricName;
        }

        inline bool SupportOpName(const std::string &opName)
        {
            return opName == kTotalOpName || opName == kChangeOpName ||
                   opName == kAddOpName || opName == kRemoveOpName ||
                   opName == kTransferOpName;
        }

        inline void PrintSupportOpName()
        {
            std::cout << kTotalOpName << ", " << kChangeOpName << ", " << kAddOpName
                      << ", " << kRemoveOpName << ", " << kTransferOpName << std::endl;
        }

    } // namespace tool
} // namespace curve

#endif // SRC_TOOLS_METRIC_NAME_H_
