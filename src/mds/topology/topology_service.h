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
 * Created Date: Mon Aug 20 2018
 * Author: xuchaojie
 */

#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_H_

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/server.h>
#include <memory>

#include "proto/topology.pb.h"
#include "src/mds/topology/topology_service_manager.h"

namespace curve {
namespace mds {
namespace topology {

//这个TopologyService又是RPC调用相关的结构

class TopologyServiceImpl : public TopologyService {
 public:
    explicit TopologyServiceImpl(
        std::shared_ptr<TopologyServiceManager> topology)
        : topology_(topology) {
    }

    virtual ~TopologyServiceImpl() {}

    virtual void RegistChunkServer(google::protobuf::RpcController* cntl_base,
                      const ChunkServerRegistRequest* request,
                      ChunkServerRegistResponse* response,
                      google::protobuf::Closure* done);

    virtual void ListChunkServer(google::protobuf::RpcController* cntl_base,
                      const ListChunkServerRequest* request,
                      ListChunkServerResponse* response,
                      google::protobuf::Closure* done);

    virtual void GetChunkServer(google::protobuf::RpcController* cntl_base,
                      const GetChunkServerInfoRequest* request,
                      GetChunkServerInfoResponse* response,
                      google::protobuf::Closure* done);

    virtual void DeleteChunkServer(google::protobuf::RpcController* cntl_base,
                      const DeleteChunkServerRequest* request,
                      DeleteChunkServerResponse* response,
                      google::protobuf::Closure* done);

    virtual void SetChunkServer(google::protobuf::RpcController* cntl_base,
                      const SetChunkServerStatusRequest* request,
                      SetChunkServerStatusResponse* response,
                      google::protobuf::Closure* done);

    virtual void RegistServer(google::protobuf::RpcController* cntl_base,
                      const ServerRegistRequest* request,
                      ServerRegistResponse* response,
                      google::protobuf::Closure* done);

    virtual void GetServer(google::protobuf::RpcController* cntl_base,
                      const GetServerRequest* request,
                      GetServerResponse* response,
                      google::protobuf::Closure* done);

    virtual void DeleteServer(google::protobuf::RpcController* cntl_base,
                      const DeleteServerRequest* request,
                      DeleteServerResponse* response,
                      google::protobuf::Closure* done);

    virtual void ListZoneServer(google::protobuf::RpcController* cntl_base,
                      const ListZoneServerRequest* request,
                      ListZoneServerResponse* response,
                      google::protobuf::Closure* done);

    virtual void CreateZone(google::protobuf::RpcController* cntl_base,
                      const ZoneRequest* request,
                      ZoneResponse* response,
                      google::protobuf::Closure* done);

    virtual void DeleteZone(google::protobuf::RpcController* cntl_base,
                      const ZoneRequest* request,
                      ZoneResponse* response,
                      google::protobuf::Closure* done);

    virtual void GetZone(google::protobuf::RpcController* cntl_base,
                      const ZoneRequest* request,
                      ZoneResponse* response,
                      google::protobuf::Closure* done);

    virtual void ListPoolZone(google::protobuf::RpcController* cntl_base,
                      const ListPoolZoneRequest* request,
                      ListPoolZoneResponse* response,
                      google::protobuf::Closure* done);

    virtual void CreatePhysicalPool(google::protobuf::RpcController* cntl_base,
                      const PhysicalPoolRequest* request,
                      PhysicalPoolResponse* response,
                      google::protobuf::Closure* done);

    virtual void DeletePhysicalPool(google::protobuf::RpcController* cntl_base,
                      const PhysicalPoolRequest* request,
                      PhysicalPoolResponse* response,
                      google::protobuf::Closure* done);

    virtual void GetPhysicalPool(google::protobuf::RpcController* cntl_base,
                      const PhysicalPoolRequest* request,
                      PhysicalPoolResponse* response,
                      google::protobuf::Closure* done);

    virtual void ListPhysicalPool(google::protobuf::RpcController* cntl_base,
                      const ListPhysicalPoolRequest* request,
                      ListPhysicalPoolResponse* response,
                      google::protobuf::Closure* done);

    virtual void CreateLogicalPool(google::protobuf::RpcController* cntl_base,
                      const CreateLogicalPoolRequest* request,
                      CreateLogicalPoolResponse* response,
                      google::protobuf::Closure* done);

    virtual void DeleteLogicalPool(google::protobuf::RpcController* cntl_base,
                      const DeleteLogicalPoolRequest* request,
                      DeleteLogicalPoolResponse* response,
                      google::protobuf::Closure* done);

    virtual void GetLogicalPool(google::protobuf::RpcController* cntl_base,
                      const GetLogicalPoolRequest* request,
                      GetLogicalPoolResponse* response,
                      google::protobuf::Closure* done);

    virtual void ListLogicalPool(google::protobuf::RpcController* cntl_base,
                      const ListLogicalPoolRequest* request,
                      ListLogicalPoolResponse* response,
                      google::protobuf::Closure* done);

    virtual void GetChunkServerListInCopySets(
                      google::protobuf::RpcController* cntl_base,
                      const GetChunkServerListInCopySetsRequest* request,
                      GetChunkServerListInCopySetsResponse* response,
                      google::protobuf::Closure* done);

    virtual void GetCopySetsInChunkServer(
                      google::protobuf::RpcController* cntl_base,
                      const GetCopySetsInChunkServerRequest* request,
                      GetCopySetsInChunkServerResponse* response,
                      google::protobuf::Closure* done);

    virtual void GetClusterInfo(
                      google::protobuf::RpcController* cntl_base,
                      const GetClusterInfoRequest* request,
                      GetClusterInfoResponse* response,
                      google::protobuf::Closure* done);

 private:
    std::shared_ptr<TopologyServiceManager> topology_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_H_
