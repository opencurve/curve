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
 * Created Date: 2021-08-26
 * Author: wanghai01
 */

#ifndef CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_H_
#define CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_H_

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/server.h>
#include <google/protobuf/service.h>
#include <google/protobuf/stubs/callback.h>
#include <memory>

#include "curvefs/src/mds/topology/topology_manager.h"

namespace curvefs {
namespace mds {
namespace topology {

class TopologyServiceImpl : public TopologyService {
 public:
    explicit TopologyServiceImpl(
        std::shared_ptr<TopologyManager> topology)
        : topologyManager_(topology) {
    }

    virtual ~TopologyServiceImpl() {}

    virtual void RegistMetaServer(google::protobuf::RpcController* cntl_base,
                                  const MetaServerRegistRequest* request,
                                  MetaServerRegistResponse* response,
                                  google::protobuf::Closure* done);

    virtual void ListMetaServer(google::protobuf::RpcController* cntl_base,
                                const ListMetaServerRequest* request,
                                ListMetaServerResponse* response,
                                google::protobuf::Closure* done);

    virtual void GetMetaServer(google::protobuf::RpcController* cntl_base,
                              const GetMetaServerInfoRequest* request,
                              GetMetaServerInfoResponse* response,
                              google::protobuf::Closure* done);

    virtual void DeleteMetaServer(google::protobuf::RpcController* cntl_base,
                                  const DeleteMetaServerRequest* request,
                                  DeleteMetaServerResponse* response,
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
                            const CreateZoneRequest* request,
                            CreateZoneResponse* response,
                            google::protobuf::Closure* done);

    virtual void DeleteZone(google::protobuf::RpcController* cntl_base,
                            const DeleteZoneRequest* request,
                            DeleteZoneResponse* response,
                            google::protobuf::Closure* done);

    virtual void GetZone(google::protobuf::RpcController* cntl_base,
                         const GetZoneRequest* request,
                         GetZoneResponse* response,
                         google::protobuf::Closure* done);

    virtual void ListPoolZone(google::protobuf::RpcController* cntl_base,
                              const ListPoolZoneRequest* request,
                              ListPoolZoneResponse* response,
                              google::protobuf::Closure* done);

    virtual void CreatePool(google::protobuf::RpcController* cntl_base,
                            const CreatePoolRequest* request,
                            CreatePoolResponse* response,
                            google::protobuf::Closure* done);

    virtual void DeletePool(google::protobuf::RpcController* cntl_base,
                            const DeletePoolRequest* request,
                            DeletePoolResponse* response,
                            google::protobuf::Closure* done);

    virtual void GetPool(google::protobuf::RpcController* cntl_base,
                         const GetPoolRequest* request,
                         GetPoolResponse* response,
                         google::protobuf::Closure* done);

    virtual void ListPool(google::protobuf::RpcController* cntl_base,
                          const ListPoolRequest* request,
                          ListPoolResponse* response,
                          google::protobuf::Closure* done);

    virtual void GetMetaServerListInCopysets(
            google::protobuf::RpcController* cntl_base,
            const GetMetaServerListInCopySetsRequest* request,
            GetMetaServerListInCopySetsResponse* response,
            google::protobuf::Closure* done);

    virtual void CreatePartition(::google::protobuf::RpcController* cntl_base,
                         const CreatePartitionRequest* request,
                         CreatePartitionResponse* response,
                         ::google::protobuf::Closure* done);

    virtual void DeletePartition(::google::protobuf::RpcController* cntl_base,
                                const DeletePartitionRequest* request,
                                DeletePartitionResponse* response,
                                ::google::protobuf::Closure* done);

    virtual void CommitTx(::google::protobuf::RpcController* cntl_base,
                  const CommitTxRequest* request,
                  CommitTxResponse* response,
                  ::google::protobuf::Closure* done);

    virtual void ListPartition(::google::protobuf::RpcController* cntl_base,
                         const ListPartitionRequest* request,
                         ListPartitionResponse* response,
                         ::google::protobuf::Closure* done);

    virtual void GetCopysetOfPartition(
            ::google::protobuf::RpcController* cntl_base,
            const GetCopysetOfPartitionRequest* request,
            GetCopysetOfPartitionResponse* response,
            ::google::protobuf::Closure* done);

    virtual void GetCopysetsInfo(::google::protobuf::RpcController* cntl_base,
                                 const GetCopysetsInfoRequest* request,
                                 GetCopysetsInfoResponse* response,
                                 ::google::protobuf::Closure* done);

    virtual void ListCopysetInfo(::google::protobuf::RpcController* cntl_base,
                                 const ListCopysetInfoRequest* request,
                                 ListCopysetInfoResponse* response,
                                 ::google::protobuf::Closure* done);

    virtual void StatMetadataUsage(
        ::google::protobuf::RpcController* controller,
        const ::curvefs::mds::topology::StatMetadataUsageRequest* request,
        ::curvefs::mds::topology::StatMetadataUsageResponse* response,
        ::google::protobuf::Closure* done);

    virtual void ListTopology(::google::protobuf::RpcController* controller,
                              const ListTopologyRequest* request,
                              ListTopologyResponse* response,
                              ::google::protobuf::Closure* done);

    /**
     * @brief
     *
     * @param controller
     * @param request
     * @param response
            statusCode:
                1. TOPO_OK : success
                2. TOPO_INVALID_PARAM: no servers in request
                3. TOPO_IP_PORT_DUPLICATED: The server to be registered has
     already been registered
                4. TOPO_ALLOCATE_ID_FAIL: Failed to assign to cluster id
                5. TOPO_STORGE_FAIL: Fail to storage Cluster to etcd(or other
     thing)
     * @param done
     */
    virtual void RegistMemcacheCluster(
        ::google::protobuf::RpcController* controller,
        const RegistMemcacheClusterRequest* request,
        RegistMemcacheClusterResponse* response,
        ::google::protobuf::Closure* done);

    /**
     * @brief
     *
     * @param controller
     * @param request
     * @param response
            statusCode:
                1. TOPO_OK : success
                2. TOPO_MEMCACHECLUSTER_NOT_FOUND: no memcacheCluster
     * @param done
     */
    virtual void ListMemcacheCluster(
        ::google::protobuf::RpcController* controller,
        const ListMemcacheClusterRequest* request,
        ListMemcacheClusterResponse* response,
        ::google::protobuf::Closure* done);

    /**
     * @brief Get or Alloc one Memcache Cluster
     *
     * @param controller
     * @param request
     * @param response
            statusCode：
                1. TOPO_OK: success
                2. TOPO_MEMCACHECLUSTER_NOT_FOUND: no memcacheCluster
     * @param done 
     */
    virtual void AllocOrGetMemcacheCluster(
        ::google::protobuf::RpcController* controller,
        const AllocOrGetMemcacheClusterRequest* request,
        AllocOrGetMemcacheClusterResponse* response,
        ::google::protobuf::Closure* done);

 private:
    std::shared_ptr<TopologyManager> topologyManager_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_H_
