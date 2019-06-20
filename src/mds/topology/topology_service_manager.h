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
#include <memory>

#include "src/mds/topology/topology.h"
#include "src/mds/copyset/copyset_manager.h"
#include "src/mds/copyset/copyset_policy.h"
#include "src/common/concurrent/concurrent.h"


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

    /**
     * @brief 调用rpc接口在chunkserver上创建copysetnode
     *
     * @param id 目标chunkserver
     * @param copysetInfos 需在该chunkserver上创建的copyset列表
     *
     * @return 错误码
     */
    virtual bool CreateCopysetNodeOnChunkServer(
        ChunkServerIdType id,
        const std::vector<CopySetInfo> &copysetInfos);

 private:
    /**
    * @brief 为logicalpool创建copyset
    *
    * @param lPool 逻辑池
    * @param[in][out] scatterWidth 入参为目标scatterWidth,
    *   出参返回实际scatterWidth
    * @param[out] copysetInfos 创建的copyset信息
    *
    * @return 错误码
    */
    int CreateCopysetForLogicalPool(
        const LogicalPool &lPool,
        uint32_t *scatterWidth,
        std::vector<CopySetInfo> *copysetInfos);

    /**
    * @brief 为PageFilePool创建copyset
    *
    * @param lPool 逻辑池
    * @param[in][out] scatterWidth 入参为目标scatterWidth,
    *   出参返回实际scatterWidth
    * @param[out] copysetInfos 创建的copyset信息
    *
    * @return 错误码
    */
    int GenCopysetForPageFilePool(
        const LogicalPool &lPool,
        uint32_t *scatterWidth,
        std::vector<CopySetInfo> *copysetInfos);

    /**
     * @brief 在copyset列表中的所有copyset为其在chunkserver上创建copysetnode
     *
     * @param copysetInfos copyset列表
     *
     * @return 错误码
     */
    int CreateCopysetNodeOnChunkServer(
        const std::vector<CopySetInfo> &copysetInfos);

    /**
     * @brief 移除logicalpool及其相关copyset
     *
     * @param pool 逻辑池
     * @param copysetInfos copyset列表
     *
     * @return 错误码
     */
    int RemoveErrLogicalPoolAndCopyset(const LogicalPool &pool,
        const std::vector<CopySetInfo> *copysetInfos);

 private:
    /**
     * @brief 拓扑模块
     */
    std::shared_ptr<Topology> topology_;
    /**
     * @brief copyset管理模块
     */
    std::shared_ptr<curve::mds::copyset::CopysetManager> copysetManager_;

    /**
     * @brief 注册chunkserver保护锁，防止并发注册chunkserver造成重复注册
     */
    ::curve::common::Mutex registCsMutex;

    /**
     * @brief 拓扑配置项
     */
    TopologyOption option_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_SERVICE_MANAGER_H_
