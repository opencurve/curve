/*
 * Project: curve
 * Created Date: Mon Mar 25 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_MDS_HEARTBEAT_TOPO_UPDATER_H_
#define CURVE_SRC_MDS_HEARTBEAT_TOPO_UPDATER_H_

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
    * @brief UpdateTopo 根据reportCopySetInfo的信息，更新topology中copyset的
    *                    epoch, 副本关系， 统计信息等; leader copyset调用
    * @param[in] reportCopySetInfo chunkserver上报的copyset信息
    */
    void UpdateTopo(const CopySetInfo &reportCopySetInfo);

 private:
    std::shared_ptr<Topology> topo_;
};
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve

#endif  // CURVE_SRC_MDS_HEARTBEAT_TOPO_UPDATER_H_
