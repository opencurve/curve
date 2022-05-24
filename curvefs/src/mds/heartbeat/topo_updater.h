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
 * Created Date: 2021-09-16
 * Author: chenwei
 */

#ifndef CURVEFS_SRC_MDS_HEARTBEAT_TOPO_UPDATER_H_
#define CURVEFS_SRC_MDS_HEARTBEAT_TOPO_UPDATER_H_

#include <list>
#include <memory>
#include "curvefs/src/mds/topology/topology.h"
#include "curvefs/src/mds/topology/topology_item.h"

using ::curvefs::mds::topology::CopySetInfo;
using ::curvefs::mds::topology::Topology;

namespace curvefs {
namespace mds {
namespace heartbeat {
using curvefs::mds::topology::CopySetIdType;
class TopoUpdater {
 public:
    explicit TopoUpdater(const std::shared_ptr<Topology> &topo) : topo_(topo) {}
    ~TopoUpdater() {}

    /*
     * @brief UpdateCopysetTopo this function will be called by leader copyset
     *                   for updating copyset epoch, replicas relationship and
     *                   statistical data according to reportCopySetInfo
     * @param[in] reportCopySetInfo copyset info reported by metaserver
     */
    void UpdateCopysetTopo(
        const ::curvefs::mds::topology::CopySetInfo &reportCopySetInfo);

    /*
     * @brief UpdatePartitionTopo this function will be called by leader copyset
     *                   for updating partition info
     * @param[in] partitionList partition info list reported by metaserver
     */
    void UpdatePartitionTopo(
        CopySetIdType copySetId,
        const std::list<::curvefs::mds::topology::Partition> &partitionList);

 public:
    // public for test
    bool CanPartitionStatusChange(PartitionStatus statusInTopo,
                                  PartitionStatus statusInHeartbeat);

 private:
    std::shared_ptr<Topology> topo_;
};
}  // namespace heartbeat
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_HEARTBEAT_TOPO_UPDATER_H_
