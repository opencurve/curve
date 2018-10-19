/*
 * Project: curve
 * Created Date: Mon Aug 27 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_MANAGER_H_
#define CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_MANAGER_H_

#include "src/mds/topology/topology.h"
#include "src/mds/copyset/copyset_manager.h"

#include <string>

namespace curve {
namespace mds {
namespace topology {

class TopologyServiceManager {
 public:
    TopologyServiceManager(std::shared_ptr<Topology> topology,
            std::shared_ptr<curve::mds::copyset::CopysetManager> copysetManager)
    : topology_(topology),
    copysetManager_(copysetManager) {}

    ~TopologyServiceManager() {}

    virtual void RegistChunkServer(const ChunkServerRegistRequest* request,
                      ChunkServerRegistResponse* response);

    virtual void ListChunkServer(const ListChunkServerRequest* request,
                      ListChunkServerResponse* response);

    virtual void GetChunkServer(const GetChunkServerInfoRequest* request,
                      GetChunkServerInfoResponse* response);

    virtual void DeleteChunkServer(const DeleteChunkServerRequest* request,
                      DeleteChunkServerResponse* response);

    virtual void SetChunkServer(const SetChunkServerStatusRequest* request,
                      SetChunkServerStatusResponse* response);

    virtual void RegistServer(const ServerRegistRequest* request,
                      ServerRegistResponse* response);

    virtual void GetServer(const GetServerRequest* request,
                      GetServerResponse* response);

    virtual void DeleteServer(const DeleteServerRequest* request,
                      DeleteServerResponse* response);

    virtual void ListZoneServer(const ListZoneServerRequest* request,
                      ListZoneServerResponse* response);

    virtual void CreateZone(const ZoneRequest* request,
                      ZoneResponse* response);

    virtual void DeleteZone(const ZoneRequest* request,
                      ZoneResponse* response);

    virtual void GetZone(const ZoneRequest* request,
                      ZoneResponse* response);

    virtual void ListPoolZone(const ListPoolZoneRequest* request,
                      ListPoolZoneResponse* response);

    virtual void CreatePhysicalPool(const PhysicalPoolRequest* request,
                      PhysicalPoolResponse* response);

    virtual void DeletePhysicalPool(const PhysicalPoolRequest* request,
                      PhysicalPoolResponse* response);

    virtual void GetPhysicalPool(const PhysicalPoolRequest* request,
                      PhysicalPoolResponse* response);

    virtual void ListPhysicalPool(const ListPhysicalPoolRequest* request,
                      ListPhysicalPoolResponse* response);

    virtual void CreateLogicalPool(const CreateLogicalPoolRequest* request,
                      CreateLogicalPoolResponse* response);

    virtual void DeleteLogicalPool(const DeleteLogicalPoolRequest* request,
                      DeleteLogicalPoolResponse* response);

    virtual void GetLogicalPool(const GetLogicalPoolRequest* request,
                      GetLogicalPoolResponse* response);

    virtual void ListLogicalPool(const ListLogicalPoolRequest* request,
                      ListLogicalPoolResponse* response);

    virtual void GetChunkServerListInCopySets(
                      const GetChunkServerListInCopySetsRequest* request,
                      GetChunkServerListInCopySetsResponse* response);

 private:
    std::shared_ptr<Topology> topology_;
    std::shared_ptr<curve::mds::copyset::CopysetManager> copysetManager_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_MANAGER_H_
