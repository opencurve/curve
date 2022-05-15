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

#ifndef CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_MANAGER_H_
#define CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_MANAGER_H_

#include <memory>
#include <set>
#include <string>
#include <vector>
#include <list>

#include "curvefs/proto/copyset.pb.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/mds/metaserverclient/metaserver_client.h"
#include "curvefs/src/mds/topology/topology.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/name_lock.h"
#include "src/common/timeutility.h"

namespace curvefs {
namespace mds {
namespace topology {

using std::string;
using curve::common::NameLock;
using curve::common::NameLockGuard;
using curve::common::TimeUtility;
using curvefs::mds::MetaserverClient;

class TopologyManager {
 public:
    TopologyManager(const std::shared_ptr<Topology> &topology,
                    std::shared_ptr<MetaserverClient> metaserverClient)
        : topology_(topology), metaserverClient_(metaserverClient) {}

    virtual ~TopologyManager() {}

    virtual void Init(const TopologyOption &option);

    virtual void RegistMetaServer(const MetaServerRegistRequest *request,
                                  MetaServerRegistResponse *response);

    virtual void ListMetaServer(const ListMetaServerRequest *request,
                                ListMetaServerResponse *response);

    virtual void GetMetaServer(const GetMetaServerInfoRequest *request,
                               GetMetaServerInfoResponse *response);

    virtual void DeleteMetaServer(const DeleteMetaServerRequest *request,
                                  DeleteMetaServerResponse *response);

    virtual void RegistServer(const ServerRegistRequest *request,
                              ServerRegistResponse *response);

    virtual void GetServer(const GetServerRequest *request,
                           GetServerResponse *response);

    virtual void DeleteServer(const DeleteServerRequest *request,
                              DeleteServerResponse *response);

    virtual void ListZoneServer(const ListZoneServerRequest *request,
                                ListZoneServerResponse *response);

    virtual void CreateZone(const CreateZoneRequest *request,
                            CreateZoneResponse *response);

    virtual void DeleteZone(const DeleteZoneRequest *request,
                            DeleteZoneResponse *response);

    virtual void GetZone(const GetZoneRequest *request,
                         GetZoneResponse *response);

    virtual void ListPoolZone(const ListPoolZoneRequest *request,
                              ListPoolZoneResponse *response);

    virtual void CreatePool(const CreatePoolRequest *request,
                            CreatePoolResponse *response);

    virtual void DeletePool(const DeletePoolRequest *request,
                            DeletePoolResponse *response);

    virtual void GetPool(const GetPoolRequest *request,
                         GetPoolResponse *response);

    virtual void ListPool(const ListPoolRequest *request,
                          ListPoolResponse *response);

    virtual void CreatePartitions(const CreatePartitionRequest *request,
                                  CreatePartitionResponse *response);

    virtual TopoStatusCode CreatePartitionsAndGetMinPartition(
        FsIdType fsId, PartitionInfo *partition);

    virtual TopoStatusCode CommitTxId(const std::vector<PartitionTxId>& txIds);

    virtual void CommitTx(const CommitTxRequest *request,
                          CommitTxResponse *response);

    virtual void GetMetaServerListInCopysets(
        const GetMetaServerListInCopySetsRequest *request,
        GetMetaServerListInCopySetsResponse *response);

    virtual void ListPartition(const ListPartitionRequest *request,
                               ListPartitionResponse *response);

    virtual void ListPartitionOfFs(FsIdType fsId,
                                   std::list<PartitionInfo>* list);

    virtual void
    GetLatestPartitionsTxId(const std::vector<PartitionTxId> &txIds,
                            std::vector<PartitionTxId> *needUpdate);

    virtual TopoStatusCode UpdatePartitionStatus(PartitionIdType partitionId,
                                                 PartitionStatus status);

    virtual void GetCopysetOfPartition(
        const GetCopysetOfPartitionRequest *request,
        GetCopysetOfPartitionResponse *response);

    virtual TopoStatusCode GetCopysetMembers(const PoolIdType poolId,
                                             const CopySetIdType copysetId,
                                             std::set<std::string> *addrs);

    virtual bool CreateCopysetNodeOnMetaServer(PoolIdType poolId,
                                               CopySetIdType copysetId,
                                               MetaServerIdType metaServerId);

    virtual void GetCopysetsInfo(const GetCopysetsInfoRequest* request,
                                 GetCopysetsInfoResponse* response);

    virtual void ListCopysetsInfo(ListCopysetInfoResponse* response);

    virtual void GetMetaServersSpace(
        ::google::protobuf::RepeatedPtrField<
            curvefs::mds::topology::MetadataUsage>* spaces);

    virtual void GetTopology(ListTopologyResponse* response);

    virtual void ListZone(ListZoneResponse* response);

    virtual void ListServer(ListServerResponse* response);

    virtual void ListMetaserverOfCluster(ListMetaServerResponse* response);

 private:
    TopoStatusCode CreateCopyset();

    virtual void GetCopysetInfo(const uint32_t& poolId,
                                const uint32_t& copysetId,
                                CopysetValue* copysetValue);


    virtual void ClearCopysetCreating(PoolIdType poolId,
        const std::set<CopySetIdType> &copysets);

 private:
    std::shared_ptr<Topology> topology_;
    std::shared_ptr<MetaserverClient> metaserverClient_;

    /**
     * @brief register mutex for metaserver, preventing duplicate registration
     *        in concurrent scenario
     */
    NameLock registMsMutex;

    /**
     * @brief topology options
     */
    TopologyOption option_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_MANAGER_H_
