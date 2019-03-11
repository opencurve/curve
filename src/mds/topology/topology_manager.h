/*
 * Project:
 * Created Date: Thu Aug 23 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_MANAGER_H_
#define CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_MANAGER_H_

#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_id_generator.h"
#include "src/mds/topology/topology_token_generator.h"
#include "src/mds/topology/topology_storge.h"
#include "src/mds/topology/topology_service_manager.h"
#include "src/mds/topology/singleton.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/copyset/copyset_manager.h"
#include "src/mds/topology/topology_admin.h"


namespace curve {
namespace mds {
namespace topology {

class TopologyManager : public Singleton<TopologyManager> {
    friend class Singleton<TopologyManager>;
 public:
    virtual ~TopologyManager() {}

    std::shared_ptr<TopologyServiceManager> GetServiceManager() {
        return serviceManager_;
    }

    std::shared_ptr<TopologyAdmin> GetTopologyAdmin() {
        return topologyAdmin_;
    }

    std::shared_ptr<Topology> GetTopology() {
        return topology_;
    }

 private:
    TopologyManager();

    std::shared_ptr<Topology> topology_;
    std::shared_ptr<TopologyServiceManager> serviceManager_;
    std::shared_ptr<TopologyAdmin> topologyAdmin_;
};
}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_MANAGER_H_
