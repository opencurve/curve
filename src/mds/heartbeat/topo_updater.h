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
 * Created Date: Mon Mar 25 2019
 * Author: lixiaocui
 */

#ifndef SRC_MDS_HEARTBEAT_TOPO_UPDATER_H_
#define SRC_MDS_HEARTBEAT_TOPO_UPDATER_H_

#include <memory>
#include "src/mds/topology/topology_item.h"
#include "src/mds/topology/topology.h"

using ::curve::mds::topology::CopySetInfo;
using ::curve::mds::topology::Topology;

namespace curve {
namespace mds {
namespace heartbeat {
class TopoUpdater {
 public:
    explicit TopoUpdater(std::shared_ptr<Topology> topo) : topo_(topo) {}
    ~TopoUpdater() {}

   /*
    * @brief UpdateTopo this function will be called by leader copyset
    *                  for updating copyset epoch, copy relationship and statistical
    *                  data according to reportCopySetInfo 
    * @param[in] reportCopySetInfo copyset info reported by chunkserver
    */
    void UpdateTopo(const CopySetInfo &reportCopySetInfo);

 private:
    std::shared_ptr<Topology> topo_;
};
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_HEARTBEAT_TOPO_UPDATER_H_
