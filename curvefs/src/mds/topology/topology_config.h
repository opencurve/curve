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
 * Project: curve
 * Created Date: 2021-08-24
 * Author: wanghai01
 */

#ifndef CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_CONFIG_H_
#define CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_CONFIG_H_

#include <string>

namespace curvefs {
namespace mds {
namespace topology {

struct TopologyOption {
    // time interval that topology data updated to storage
    uint32_t topologyUpdateToRepoSec;
    // partition number in each copyset
    uint64_t maxPartitionNumberInCopyset;
    // id number in each partition
    uint64_t idNumberInPartition;
    // initial create copyset number
    uint32_t initialCopysetNumber;
    // min available copyset num
    uint32_t minAvailableCopysetNum;
    // create partition number
    uint32_t createPartitionNumber;
    // max copyset num in metaserver
    uint32_t maxCopysetNumInMetaserver;
    // time interval for updating topology metric
    uint32_t UpdateMetricIntervalSec;

    TopologyOption()
        : topologyUpdateToRepoSec(0),
          maxPartitionNumberInCopyset(256),
          idNumberInPartition(16777216),
          initialCopysetNumber(10),
          minAvailableCopysetNum(10),
          createPartitionNumber(3),
          maxCopysetNumInMetaserver(100),
          UpdateMetricIntervalSec(60) {}
};

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_CONFIG_H_
