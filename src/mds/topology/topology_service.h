/*
 * Project: curve
 * Created Date: Mon Aug 20 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_H_
#define CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_H_

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/server.h>

#include "proto/topology.pb.h"
#include "src/mds/topology/topology_manager.h"
#include "src/google/protobuf/service.h"
#include "src/google/protobuf/stubs/callback.h"

namespace curve {
namespace mds {
namespace topology {

class TopologyServiceImpl : public TopologyService {
 public:
  TopologyServiceImpl() {
    topology_ = TopologyManager::GetInstance()->GetServiceManager();
  }

  explicit TopologyServiceImpl(std::shared_ptr<TopologyServiceManager> topology)
      : topology_(topology) {
  }

  virtual ~TopologyServiceImpl() {}

  virtual void RegistChunkServer(google::protobuf::RpcController *cntl_base,
                                 const ChunkServerRegistRequest *request,
                                 ChunkServerRegistResponse *response,
                                 google::protobuf::Closure *done);

  virtual void ListChunkServer(google::protobuf::RpcController *cntl_base,
                               const ListChunkServerRequest *request,
                               ListChunkServerResponse *response,
                               google::protobuf::Closure *done);

  virtual void GetChunkServer(google::protobuf::RpcController *cntl_base,
                              const GetChunkServerInfoRequest *request,
                              GetChunkServerInfoResponse *response,
                              google::protobuf::Closure *done);

  virtual void DeleteChunkServer(google::protobuf::RpcController *cntl_base,
                                 const DeleteChunkServerRequest *request,
                                 DeleteChunkServerResponse *response,
                                 google::protobuf::Closure *done);

  virtual void SetChunkServer(google::protobuf::RpcController *cntl_base,
                              const SetChunkServerStatusRequest *request,
                              SetChunkServerStatusResponse *response,
                              google::protobuf::Closure *done);

  virtual void RegistServer(google::protobuf::RpcController *cntl_base,
                            const ServerRegistRequest *request,
                            ServerRegistResponse *response,
                            google::protobuf::Closure *done);

  virtual void GetServer(google::protobuf::RpcController *cntl_base,
                         const GetServerRequest *request,
                         GetServerResponse *response,
                         google::protobuf::Closure *done);

  virtual void DeleteServer(google::protobuf::RpcController *cntl_base,
                            const DeleteServerRequest *request,
                            DeleteServerResponse *response,
                            google::protobuf::Closure *done);

  virtual void ListZoneServer(google::protobuf::RpcController *cntl_base,
                              const ListZoneServerRequest *request,
                              ListZoneServerResponse *response,
                              google::protobuf::Closure *done);

  virtual void CreateZone(google::protobuf::RpcController *cntl_base,
                          const ZoneRequest *request,
                          ZoneResponse *response,
                          google::protobuf::Closure *done);

  virtual void DeleteZone(google::protobuf::RpcController *cntl_base,
                          const ZoneRequest *request,
                          ZoneResponse *response,
                          google::protobuf::Closure *done);

  virtual void GetZone(google::protobuf::RpcController *cntl_base,
                       const ZoneRequest *request,
                       ZoneResponse *response,
                       google::protobuf::Closure *done);

  virtual void ListPoolZone(google::protobuf::RpcController *cntl_base,
                            const ListPoolZoneRequest *request,
                            ListPoolZoneResponse *response,
                            google::protobuf::Closure *done);

  virtual void CreatePhysicalPool(google::protobuf::RpcController *cntl_base,
                                  const PhysicalPoolRequest *request,
                                  PhysicalPoolResponse *response,
                                  google::protobuf::Closure *done);

  virtual void DeletePhysicalPool(google::protobuf::RpcController *cntl_base,
                                  const PhysicalPoolRequest *request,
                                  PhysicalPoolResponse *response,
                                  google::protobuf::Closure *done);

  virtual void GetPhysicalPool(google::protobuf::RpcController *cntl_base,
                               const PhysicalPoolRequest *request,
                               PhysicalPoolResponse *response,
                               google::protobuf::Closure *done);

  virtual void ListPhysicalPool(google::protobuf::RpcController *cntl_base,
                                const ListPhysicalPoolRequest *request,
                                ListPhysicalPoolResponse *response,
                                google::protobuf::Closure *done);

  virtual void CreateLogicalPool(google::protobuf::RpcController *cntl_base,
                                 const CreateLogicalPoolRequest *request,
                                 CreateLogicalPoolResponse *response,
                                 google::protobuf::Closure *done);

  virtual void DeleteLogicalPool(google::protobuf::RpcController *cntl_base,
                                 const DeleteLogicalPoolRequest *request,
                                 DeleteLogicalPoolResponse *response,
                                 google::protobuf::Closure *done);

  virtual void GetLogicalPool(google::protobuf::RpcController *cntl_base,
                              const GetLogicalPoolRequest *request,
                              GetLogicalPoolResponse *response,
                              google::protobuf::Closure *done);

  virtual void ListLogicalPool(google::protobuf::RpcController *cntl_base,
                               const ListLogicalPoolRequest *request,
                               ListLogicalPoolResponse *response,
                               google::protobuf::Closure *done);

  virtual void GetChunkServerListInCopySets(
      google::protobuf::RpcController *cntl_base,
      const GetChunkServerListInCopySetsRequest *request,
      GetChunkServerListInCopySetsResponse *response,
      google::protobuf::Closure *done);

 private:
  std::shared_ptr<TopologyServiceManager> topology_{};
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_H_
