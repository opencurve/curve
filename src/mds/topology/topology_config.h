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
 * Created Date: Wed May 08 2019
 * Author: xuchaojie
 */

#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_CONFIG_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_CONFIG_H_

#include <string>

namespace curve {
namespace mds {
namespace topology {

struct TopologyOption {
    // time interval that topology data updated to storage
    uint32_t TopologyUpdateToRepoSec;
    // timeout peroid of RPC for copyset creation (in ms)
    uint32_t CreateCopysetRpcTimeoutMs;
    // retry times after timeout of RPC for copyset creation
    uint32_t CreateCopysetRpcRetryTimes;
    // time interval of retries (in ms)
    uint32_t CreateCopysetRpcRetrySleepTimeMs;
    // time interval for updating topology metric
    uint32_t UpdateMetricIntervalSec;
    // threshold of maximum usage of a physical pool
    uint32_t PoolUsagePercentLimit;
    // policy of pool choosing
    int choosePoolPolicy;
    // enable LogicalPool ALLOW/DENY status
    bool enableLogicalPoolStatus;
    // threshold of maximum usage of a chunkserver capacity
    uint32_t ChunkServerUsagePercentLimit;

    TopologyOption()
        : TopologyUpdateToRepoSec(0),
          CreateCopysetRpcTimeoutMs(500),
          CreateCopysetRpcRetryTimes(3),
          CreateCopysetRpcRetrySleepTimeMs(500),
          UpdateMetricIntervalSec(0),
          choosePoolPolicy(0),
          enableLogicalPoolStatus(false) {}
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_CONFIG_H_
