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
 * Created Date: Mon Aug 27 2018
 * Author: xuchaojie
 */

#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_MANAGER_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_MANAGER_H_

#include <string>
#include <vector>
#include <memory>

#include "src/mds/topology/topology.h"
#include "src/mds/copyset/copyset_manager.h"
#include "src/mds/copyset/copyset_policy.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/name_lock.h"
#include "src/mds/topology/topology_stat.h"
#include "src/mds/topology/topology_chunk_allocator.h"

using curve::mds::copyset::CopysetManager;

namespace curve {
namespace mds {
namespace topology {

struct ChunkServerRegistInfoBuilder {
    ChunkServerRegistInfoBuilder() {}
    virtual ~ChunkServerRegistInfoBuilder() {}

    virtual int BuildEpochMap(::google::protobuf::Map<
        ::google::protobuf::uint64, ::google::protobuf::uint64> *epochMap) = 0;
};

class TopologyServiceManager {
 public:
    TopologyServiceManager(
        const std::shared_ptr<Topology> &topology,
        const std::shared_ptr<TopologyStat> &topologyStat,
        const std::shared_ptr<TopologyChunkAllocator> &topologychunkallocator,
        const std::shared_ptr<CopysetManager> &copysetManager,
        const std::shared_ptr<ChunkServerRegistInfoBuilder> &registInfoBuilder)
        : topology_(topology),
          topoStat_(topologyStat),
          topologyChunkAllocator_(topologychunkallocator),
          copysetManager_(copysetManager),
          registInfoBuilder_(registInfoBuilder) {}

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

    virtual void GetChunkServerInCluster(
        const GetChunkServerInClusterRequest *request,
        GetChunkServerInClusterResponse *response);

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

    virtual void SetLogicalPool(const SetLogicalPoolRequest *request,
                                SetLogicalPoolResponse *response);

    virtual void SetLogicalPoolScanState(
        const SetLogicalPoolScanStateRequest* request,
        SetLogicalPoolScanStateResponse* response);

    virtual void GetChunkServerListInCopySets(
        const GetChunkServerListInCopySetsRequest *request,
        GetChunkServerListInCopySetsResponse *response);

    virtual void GetCopySetsInChunkServer(
                      const GetCopySetsInChunkServerRequest* request,
                      GetCopySetsInChunkServerResponse* response);

    virtual void GetCopySetsInCluster(
        const GetCopySetsInClusterRequest* request,
        GetCopySetsInClusterResponse* response);

    virtual void GetCopyset(const GetCopysetRequest* request,
                            GetCopysetResponse* response);

    virtual void GetClusterInfo(
          const GetClusterInfoRequest* request,
          GetClusterInfoResponse* response);

    virtual void SetCopysetsAvailFlag(
          const SetCopysetsAvailFlagRequest* request,
          SetCopysetsAvailFlagResponse* response);

    virtual void ListUnAvailCopySets(
          const ListUnAvailCopySetsRequest* request,
          ListUnAvailCopySetsResponse* response);

    /**
     * @brief RPC calling for creating copysetnode on chunkserver
     *
     * @param id target chunkserver
     * @param copysetInfos copysets going to be created on target chunkserver
     *
     * @return error code
     */
    virtual bool CreateCopysetNodeOnChunkServer(
        ChunkServerIdType id,
        const std::vector<CopySetInfo> &copysetInfos);

 private:
    /**
    * @brief create copyset for logical pool
    *
    * @param lPool target logical pool
    * @param[in][out] scatterWidth target scatterWidth as input,
    *                              actual scatterWidth as output
    * @param[out] copysetInfos Info of copyset to be created
    *
    * @return error code
    */
    int CreateCopysetForLogicalPool(
        const LogicalPool &lPool,
        uint32_t *scatterWidth,
        std::vector<CopySetInfo> *copysetInfos);

    /**
    * @brief create copyset for PageFilePool
    *
    * @param lPool target logical pool
    * @param[in][out] scatterWidth target scatterWidth as input,
    *                              actual scatterWidth as output
    * @param[out] copysetInfos Info of copyset to be created
    *
    * @return error code
    */
    int GenCopysetForPageFilePool(
        const LogicalPool &lPool,
        uint32_t *scatterWidth,
        std::vector<CopySetInfo> *copysetInfos);

    /**
     * @brief create copysetnode on chunkserver for
     *        every copyset in the copyset list.
     *
     * @param copysetInfos copyset list
     *
     * @return error code
     */
    int CreateCopysetNodeOnChunkServer(
        const std::vector<CopySetInfo> &copysetInfos);

    /**
     * @brief remove target logical pool and corresponding copyset
     *
     * @param pool target logical pool
     * @param copysetInfos copyset list
     *
     * @return error code
     */
    int RemoveErrLogicalPoolAndCopyset(const LogicalPool &pool,
        const std::vector<CopySetInfo> *copysetInfos);

    /**
     * @brief Convert topology CopySetInfo to proto CopysetInfo
     * @param[in] in the topology CopySetInfo
     * @param[in] out the proto CopysetInfo
     */
    void ConvertCopyset(const CopySetInfo& in,
                        ::curve::common::CopysetInfo* out);

 private:
    /**
     * @brief topology module
     */
    std::shared_ptr<Topology> topology_;

    /**
     * @brief topology statistic module
     */
    std::shared_ptr<TopologyStat> topoStat_;

    /**
     * @brief topology allocation module
     */
    std::shared_ptr<TopologyChunkAllocator> topologyChunkAllocator_;

    /**
     * @brief copyset manager module
     */
    std::shared_ptr<curve::mds::copyset::CopysetManager> copysetManager_;


    /**
     * @brief chunkserver register info builder
     */
    std::shared_ptr<ChunkServerRegistInfoBuilder> registInfoBuilder_;

    /**
     * @brief register mutex for chunkserver, preventing duplicate registration
     *        in concurrent scenario
     */
    ::curve::common::NameLock registCsMutex;

    /**
     * @brief topology options
     */
    TopologyOption option_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_MANAGER_H_
