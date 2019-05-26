/*
 * Project: curve
 * Created Date: Mon Aug 27 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_MANAGER_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_MANAGER_H_

#include <string>
#include <vector>

#include "src/mds/topology/topology.h"
#include "src/mds/copyset/copyset_manager.h"
#include "src/mds/copyset/copyset_policy.h"


namespace curve {
namespace mds {
namespace topology {


class TopologyServiceManager {
 public:
  TopologyServiceManager(
      std::shared_ptr<Topology> topology,
      std::shared_ptr<curve::mds::copyset::CopysetManager> copysetManager)
      : topology_(topology),
        copysetManager_(copysetManager) {}

  virtual ~TopologyServiceManager() {}

  virtual void Init(const TopologyOption &option) {
      option_ = option;
  }

  virtual void RegistChunkServer(const ChunkServerRegistRequest *request,
                                 ChunkServerRegistResponse *response);

  virtual void ListChunkServer(const ListChunkServerRequest *request,
                               ListChunkServerResponse *response);

  virtual void GetChunkServer(const GetChunkServerInfoRequest *request,
                              GetChunkServerInfoResponse *response);

  virtual void DeleteChunkServer(const DeleteChunkServerRequest *request,
                                 DeleteChunkServerResponse *response);

  virtual void SetChunkServer(const SetChunkServerStatusRequest *request,
                              SetChunkServerStatusResponse *response);

  virtual void RegistServer(const ServerRegistRequest *request,
                            ServerRegistResponse *response);

  virtual void GetServer(const GetServerRequest *request,
                         GetServerResponse *response);

  virtual void DeleteServer(const DeleteServerRequest *request,
                            DeleteServerResponse *response);

  virtual void ListZoneServer(const ListZoneServerRequest *request,
                              ListZoneServerResponse *response);

  virtual void CreateZone(const ZoneRequest *request,
                          ZoneResponse *response);

  virtual void DeleteZone(const ZoneRequest *request,
                          ZoneResponse *response);

  virtual void GetZone(const ZoneRequest *request,
                       ZoneResponse *response);

  virtual void ListPoolZone(const ListPoolZoneRequest *request,
                            ListPoolZoneResponse *response);

  virtual void CreatePhysicalPool(const PhysicalPoolRequest *request,
                                  PhysicalPoolResponse *response);

  virtual void DeletePhysicalPool(const PhysicalPoolRequest *request,
                                  PhysicalPoolResponse *response);

  virtual void GetPhysicalPool(const PhysicalPoolRequest *request,
                               PhysicalPoolResponse *response);

  virtual void ListPhysicalPool(const ListPhysicalPoolRequest *request,
                                ListPhysicalPoolResponse *response);

  virtual void CreateLogicalPool(const CreateLogicalPoolRequest *request,
                                 CreateLogicalPoolResponse *response);

  virtual void DeleteLogicalPool(const DeleteLogicalPoolRequest *request,
                                 DeleteLogicalPoolResponse *response);

  virtual void GetLogicalPool(const GetLogicalPoolRequest *request,
                              GetLogicalPoolResponse *response);

  virtual void ListLogicalPool(const ListLogicalPoolRequest *request,
                               ListLogicalPoolResponse *response);

  virtual void GetChunkServerListInCopySets(
      const GetChunkServerListInCopySetsRequest *request,
      GetChunkServerListInCopySetsResponse *response);

  virtual bool CreateCopysetAtChunkServer(
      const CopySetInfo &info, ChunkServerIdType id);

 private:
    int CreateCopysetForLogicalPool(
        const LogicalPool &lPool,
        uint32_t scatterWidth,
        std::vector<CopySetInfo> *copysetInfos);

    int GenCopysetForPageFilePool(
        const LogicalPool &lPool,
        uint32_t scatterWidth,
        std::vector<CopySetInfo> *copysetInfos);

    int CreateCopysetOnChunkServer(
        const std::vector<CopySetInfo> *copysetInfos);

    int RemoveErrLogicalPoolAndCopyset(const LogicalPool &pool,
        const std::vector<CopySetInfo> *copysetInfos);

 private:
    std::shared_ptr<Topology> topology_;
    std::shared_ptr<curve::mds::copyset::CopysetManager> copysetManager_;
    TopologyOption option_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_MANAGER_H_
