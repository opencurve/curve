/*
 * Project: curve
 * Created Date: Fri Aug 17 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */
#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_H_

#include <unordered_map>
#include <string>
#include <list>
#include <memory>
#include <vector>
#include <map>

#include "proto/topology.pb.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/topology/topology_item.h"
#include "src/mds/topology/topology_id_generator.h"
#include "src/mds/topology/topology_token_generator.h"
#include "src/mds/topology/topology_storge.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/concurrent/concurrent.h"

using ::curve::common::RWLock;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;

namespace curve {
namespace mds {
namespace topology {

using ChunkServerFilter = std::function<bool(const ChunkServer&)>;
using ServerFilter = std::function<bool (const Server&)>;
using ZoneFilter = std::function<bool (const Zone&)>;
using PhysicalPoolFilter = std::function<bool (const PhysicalPool&)>;
using LogicalPoolFilter = std::function<bool(const LogicalPool&)>;
using CopySetFilter = std::function<bool (const CopySetInfo&)>;

class Topology {
 public:
    Topology() {}
    virtual ~Topology() {}

    virtual PoolIdType AllocateLogicalPoolId() = 0;
    virtual PoolIdType AllocatePhysicalPoolId() = 0;
    virtual ZoneIdType AllocateZoneId() = 0;
    virtual ServerIdType AllocateServerId() = 0;
    virtual ChunkServerIdType AllocateChunkServerId() = 0;
    virtual CopySetIdType AllocateCopySetId(PoolIdType logicalPoolId) = 0;

    virtual std::string AllocateToken() = 0;

    virtual int AddLogicalPool(const LogicalPool &data) = 0;
    virtual int AddPhysicalPool(const PhysicalPool &data) = 0;
    virtual int AddZone(const Zone &data) = 0;
    virtual int AddServer(const Server &data) = 0;
    virtual int AddChunkServer(const ChunkServer &data) = 0;
    virtual int AddCopySet(const CopySetInfo &data) = 0;

    virtual int RemoveLogicalPool(PoolIdType id) = 0;
    virtual int RemovePhysicalPool(PoolIdType id) = 0;
    virtual int RemoveZone(ZoneIdType id) = 0;
    virtual int RemoveServer(ServerIdType id) = 0;
    virtual int RemoveChunkServer(ChunkServerIdType id) = 0;
    virtual int RemoveCopySet(CopySetKey key) = 0;

    virtual int UpdateLogicalPool(const LogicalPool &data) = 0;
    virtual int UpdatePhysicalPool(const PhysicalPool &data) = 0;
    virtual int UpdateZone(const Zone &data) = 0;
    virtual int UpdateServer(const Server &data) = 0;
    /**
     * @brief 更新chunkserver Topo信息
     * - 仅更新topo部分数据
     * - 先更新数据库，再更新内存，若更新数据库失败，则更新失败
     *
     * @param data chunkserver数据
     *
     * @return 错误码
     *
     */
    virtual int UpdateChunkServerTopo(const ChunkServer &data) = 0;

    /**
     * @brief 更新chunkserver 读写状态
     * - 仅更新读写状态
     * - 先更新数据库，再更新内存，若更新数据库失败，则更新失败
     *
     * @param rwState 读写或retired状态
     * @param id chunkserverid
     *
     * @return 错误码
     */
    virtual int UpdateChunkServerRwState(const ChunkServerStatus &rwState,
                                  ChunkServerIdType id) = 0;
    /**
     * @brief 更新chunkserver online状态
     * - 仅更新online/offline状态
     * - 仅更新内存，后台定期刷入数据库
     *
     * @param onlineState online/offline状态
     * @param id chunkseverid
     *
     * @return 错误码
     */
    virtual int UpdateChunkServerOnlineState(const OnlineState &onlineState,
                                  ChunkServerIdType id) = 0;
    /**
     * @brief 更新chunkserver 磁盘状态
     * - 仅更新disk state
     * - 仅更新内存，后台定期刷入数据库
     *
     * @param state 磁盘状态
     * @param id chunkserverid
     *
     * @return 错误码
     */
    virtual int UpdateChunkServerDiskStatus(const ChunkServerState &state,
                                       ChunkServerIdType id) = 0;

    /**
     * @brief 更新copyset 拓扑信息
     * @detail
     * - 用于更新epoch，leader等心跳周期性上报的copyset数据
     * - 仅更新内存，后台定期刷入数据库
     *
     * @param data copyset数据
     *
     * @return 错误码
     */
    virtual int UpdateCopySetTopo(const CopySetInfo &data) = 0;

    virtual PoolIdType
        FindLogicalPool(const std::string &logicalPoolName,
                        const std::string &physicalPoolName) const = 0;
    virtual PoolIdType FindPhysicalPool(
        const std::string &physicalPoolName) const = 0;
    virtual ZoneIdType FindZone(const std::string &zoneName,
                                const std::string &physicalPoolName) const = 0;
    virtual ZoneIdType FindZone(const std::string &zoneName,
                                PoolIdType physicalpoolid) const = 0;
    virtual ServerIdType FindServerByHostName(
        const std::string &hostName) const = 0;
    virtual ServerIdType FindServerByHostIpPort(
        const std::string &hostIp, uint32_t port) const = 0;
    virtual ChunkServerIdType FindChunkServerNotRetired(
        const std::string &hostIp,
        uint32_t port) const = 0;

    virtual bool GetLogicalPool(PoolIdType poolId,
                                LogicalPool *out) const = 0;
    virtual bool GetPhysicalPool(PoolIdType poolId,
                                 PhysicalPool *out) const = 0;
    virtual bool GetZone(ZoneIdType zoneId,
                         Zone *out) const = 0;
    virtual bool GetServer(ServerIdType serverId,
                           Server *out) const = 0;
    virtual bool GetChunkServer(ChunkServerIdType chunkserverId,
                                ChunkServer *out) const = 0;

    virtual bool GetCopySet(CopySetKey key, CopySetInfo *out) const = 0;

    virtual bool GetLogicalPool(const std::string &logicalPoolName,
                                const std::string &physicalPoolName,
                                LogicalPool *out) const = 0;

    virtual bool GetPhysicalPool(const std::string &physicalPoolName,
                                 PhysicalPool *out) const = 0;

    virtual bool GetZone(const std::string &zoneName,
                         const std::string &physicalPoolName,
                         Zone *out) const = 0;

    virtual bool GetZone(const std::string &zoneName,
                         PoolIdType physicalPoolId,
                         Zone *out) const = 0;

    virtual bool GetServerByHostName(const std::string &hostName,
                                     Server *out) const = 0;

    virtual bool GetServerByHostIpPort(const std::string &hostIp,
                                   uint32_t port,
                                   Server *out) const = 0;

    virtual bool GetChunkServerNotRetired(const std::string &hostIp,
                                uint32_t port,
                                ChunkServer *out) const = 0;


    virtual std::vector<ChunkServerIdType> GetChunkServerInCluster(
        ChunkServerFilter filter = [](const ChunkServer&) {
            return true;}) const = 0;

    virtual std::vector<ServerIdType> GetServerInCluster(
        ServerFilter filter = [](const Server&) {
            return true;}) const = 0;

    virtual std::vector<ZoneIdType> GetZoneInCluster(
        ZoneFilter filter = [](const Zone&) {
            return true;}) const = 0;

    virtual std::vector<PoolIdType> GetPhysicalPoolInCluster(
        PhysicalPoolFilter filter = [](const PhysicalPool&) {
            return true;}) const = 0;

    virtual std::vector<PoolIdType> GetLogicalPoolInCluster(
        LogicalPoolFilter filter = [](const LogicalPool&) {
            return true;}) const = 0;

    virtual std::vector<CopySetKey> GetCopySetsInCluster(
        CopySetFilter filter = [](const CopySetInfo&) {
            return true;}) const = 0;

    // get chunkserver list
    virtual std::list<ChunkServerIdType> GetChunkServerInServer(
        ServerIdType id,
        ChunkServerFilter filter = [](const ChunkServer&) {
            return true;}) const = 0;
    virtual std::list<ChunkServerIdType> GetChunkServerInZone(
        ZoneIdType id,
        ChunkServerFilter filter = [](const ChunkServer&) {
            return true;}) const = 0;
    virtual std::list<ChunkServerIdType> GetChunkServerInPhysicalPool(
        PoolIdType id,
        ChunkServerFilter filter = [](const ChunkServer&) {
            return true;}) const = 0;
    virtual std::list<ChunkServerIdType> GetChunkServerInLogicalPool(
        PoolIdType id,
        ChunkServerFilter filter = [](const ChunkServer&) {
            return true;}) const = 0;

    // get server list
    virtual std::list<ServerIdType> GetServerInZone(ZoneIdType id,
            ServerFilter filter = [](const Server&) {
                return true;}) const = 0;
    virtual std::list<ServerIdType> GetServerInPhysicalPool(
        PoolIdType id,
        ServerFilter filter = [](const Server&) {
            return true;}) const = 0;
    virtual std::list<ServerIdType> GetServerInLogicalPool(
        PoolIdType id,
        ServerFilter filter = [](const Server&) {
            return true;}) const = 0;

    // get zone list
    virtual std::list<ZoneIdType> GetZoneInPhysicalPool(
        PoolIdType id,
        ZoneFilter filter = [](const Zone&) {
            return true;}) const = 0;
    virtual std::list<ZoneIdType> GetZoneInLogicalPool(
        PoolIdType id,
        ZoneFilter filter = [](const Zone&) {
            return true;}) const = 0;

    // get logicalpool list
    virtual std::list<PoolIdType> GetLogicalPoolInPhysicalPool(
        PoolIdType id,
        LogicalPoolFilter filter = [](const LogicalPool&) {
            return true;}) const = 0;

    // get copyset list
    virtual std::vector<CopySetIdType> GetCopySetsInLogicalPool(
        PoolIdType logicalPoolId,
        CopySetFilter filter = [](const CopySetInfo&) {
            return true;}) const = 0;

    virtual std::vector<CopySetInfo> GetCopySetInfosInLogicalPool(
        PoolIdType logicalPoolId,
        CopySetFilter filter = [](const CopySetInfo&) {
            return true;}) const = 0;

    virtual std::vector<CopySetKey>
        GetCopySetsInChunkServer(ChunkServerIdType id,
        CopySetFilter filter = [](const CopySetInfo&) {
            return true;}) const = 0;
};

class TopologyImpl : public Topology {
 public:
    TopologyImpl(std::shared_ptr<TopologyIdGenerator> idGenerator,
                 std::shared_ptr<TopologyTokenGenerator> tokenGenerator,
                 std::shared_ptr<TopologyStorage> storage)
        : idGenerator_(idGenerator),
          tokenGenerator_(tokenGenerator),
          storage_(storage),
          isStop_(true) {
    }

    ~TopologyImpl() {
        Stop();
    }

    int init(const TopologyOption &option);

    int Run();
    int Stop();

    PoolIdType AllocateLogicalPoolId() override;
    PoolIdType AllocatePhysicalPoolId() override;
    ZoneIdType AllocateZoneId() override;
    ServerIdType AllocateServerId() override;
    ChunkServerIdType AllocateChunkServerId() override;
    CopySetIdType AllocateCopySetId(PoolIdType logicalPoolId) override;

    std::string AllocateToken() override;

    int AddLogicalPool(const LogicalPool &data) override;
    int AddPhysicalPool(const PhysicalPool &data) override;
    int AddZone(const Zone &data) override;
    int AddServer(const Server &data) override;
    int AddChunkServer(const ChunkServer &data) override;
    int AddCopySet(const CopySetInfo &data) override;

    int RemoveLogicalPool(PoolIdType id) override;
    int RemovePhysicalPool(PoolIdType id) override;
    int RemoveZone(ZoneIdType id) override;
    int RemoveServer(ServerIdType id) override;
    int RemoveChunkServer(ChunkServerIdType id) override;
    int RemoveCopySet(CopySetKey key) override;

    int UpdateLogicalPool(const LogicalPool &data) override;
    int UpdatePhysicalPool(const PhysicalPool &data) override;
    int UpdateZone(const Zone &data) override;
    int UpdateServer(const Server &data) override;

    int UpdateChunkServerTopo(const ChunkServer &data) override;
    int UpdateChunkServerRwState(const ChunkServerStatus &rwState,
                                  ChunkServerIdType id) override;
    int UpdateChunkServerOnlineState(const OnlineState &onlineState,
                          ChunkServerIdType id) override;
    int UpdateChunkServerDiskStatus(const ChunkServerState &state,
                         ChunkServerIdType id) override;

    int UpdateCopySetTopo(const CopySetInfo &data) override;

    PoolIdType FindLogicalPool(const std::string &logicalPoolName,
        const std::string &physicalPoolName) const override;
    PoolIdType FindPhysicalPool(
        const std::string &physicalPoolName) const override;
    ZoneIdType FindZone(const std::string &zoneName,
        const std::string &physicalPoolName) const override;
    ZoneIdType FindZone(const std::string &zoneName,
        PoolIdType physicalpoolid) const override;
    ServerIdType FindServerByHostName(
        const std::string &hostName) const override;
    ServerIdType FindServerByHostIpPort(
        const std::string &hostIp, uint32_t port) const override;
    ChunkServerIdType FindChunkServerNotRetired(const std::string &hostIp,
                                      uint32_t port) const override;

    bool GetLogicalPool(PoolIdType poolId, LogicalPool *out) const override;
    bool GetPhysicalPool(PoolIdType poolId, PhysicalPool *out) const override;
    bool GetZone(ZoneIdType zoneId, Zone *out) const override;
    bool GetServer(ServerIdType serverId, Server *out) const override;
    bool GetChunkServer(ChunkServerIdType chunkserverId,
                        ChunkServer *out) const override;

    bool GetCopySet(CopySetKey key, CopySetInfo *out) const override;

    bool GetLogicalPool(const std::string &logicalPoolName,
                        const std::string &physicalPoolName,
                        LogicalPool *out) const override {
        return GetLogicalPool(
            FindLogicalPool(logicalPoolName, physicalPoolName), out);
    }
    bool GetPhysicalPool(const std::string &physicalPoolName,
                         PhysicalPool *out) const override {
        return GetPhysicalPool(FindPhysicalPool(physicalPoolName), out);
    }
    bool GetZone(const std::string &zoneName,
                 const std::string &physicalPoolName,
                 Zone *out) const override {
        return GetZone(FindZone(zoneName, physicalPoolName), out);
    }
    bool GetZone(const std::string &zoneName,
                 PoolIdType physicalPoolId,
                 Zone *out) const override {
        return GetZone(FindZone(zoneName, physicalPoolId), out);
    }
    bool GetServerByHostName(const std::string &hostName,
                             Server *out) const override {
        return GetServer(FindServerByHostName(hostName), out);
    }
    bool GetServerByHostIpPort(const std::string &hostIp,
                           uint32_t port,
                           Server *out) const override {
        return GetServer(FindServerByHostIpPort(hostIp, port), out);
    }
    bool GetChunkServerNotRetired(const std::string &hostIp,
                        uint32_t port,
                        ChunkServer *out) const override {
        return GetChunkServer(FindChunkServerNotRetired(hostIp, port), out);
    }

    std::vector<ChunkServerIdType> GetChunkServerInCluster(
        ChunkServerFilter filter = [](const ChunkServer&) {
            return true;}) const override;

    std::vector<ServerIdType> GetServerInCluster(
        ServerFilter filter = [](const Server&) {
            return true;}) const override;

    std::vector<ZoneIdType> GetZoneInCluster(
        ZoneFilter filter = [](const Zone&) {
            return true;}) const override;

    std::vector<PoolIdType> GetPhysicalPoolInCluster(
        PhysicalPoolFilter filter = [](const PhysicalPool&) {
            return true;}) const override;

    std::vector<PoolIdType> GetLogicalPoolInCluster(
        LogicalPoolFilter filter = [](const LogicalPool&) {
            return true;}) const override;

    std::vector<CopySetKey> GetCopySetsInCluster(
        CopySetFilter filter = [](const CopySetInfo&) {
            return true;}) const override;

    // get chunksever list
    std::list<ChunkServerIdType>
        GetChunkServerInServer(ServerIdType id,
            ChunkServerFilter filter = [](const ChunkServer&) {
                return true;}) const override;
    std::list<ChunkServerIdType>
        GetChunkServerInZone(ZoneIdType id,
            ChunkServerFilter filter = [](const ChunkServer&) {
                return true;}) const override;
    std::list<ChunkServerIdType>
        GetChunkServerInPhysicalPool(PoolIdType id,
            ChunkServerFilter filter = [](const ChunkServer&) {
                return true;}) const override;
    std::list<ChunkServerIdType>
        GetChunkServerInLogicalPool(PoolIdType id,
            ChunkServerFilter filter = [](const ChunkServer&) {
                return true;}) const override;

    // get server list
    std::list<ServerIdType>
        GetServerInZone(ZoneIdType id,
            ServerFilter filter = [](const Server&) {
                return true;}) const override;
    std::list<ServerIdType>
        GetServerInPhysicalPool(PoolIdType id,
            ServerFilter filter = [](const Server&) {
                return true;}) const override;
    std::list<ServerIdType>
        GetServerInLogicalPool(PoolIdType id,
            ServerFilter filter = [](const Server&) {
                return true;}) const override;

    // get zone list
    std::list<ZoneIdType>
        GetZoneInPhysicalPool(PoolIdType id,
            ZoneFilter filter = [](const Zone&) {
                return true;}) const override;
    std::list<ZoneIdType>
        GetZoneInLogicalPool(PoolIdType id,
            ZoneFilter filter = [](const Zone&) {
                return true;}) const override;

    // get logicalpool list
    std::list<PoolIdType>
        GetLogicalPoolInPhysicalPool(PoolIdType id,
            LogicalPoolFilter filter = [](const LogicalPool&) {
                return true;}) const override;

    // get copyset list
    std::vector<CopySetIdType> GetCopySetsInLogicalPool(
        PoolIdType logicalPoolId,
        CopySetFilter filter = [](const CopySetInfo&) {
            return true;}) const override;

    std::vector<CopySetInfo> GetCopySetInfosInLogicalPool(
        PoolIdType logicalPoolId,
        CopySetFilter filter = [](const CopySetInfo&) {
            return true;}) const override;

    std::vector<CopySetKey> GetCopySetsInChunkServer(
        ChunkServerIdType id,
        CopySetFilter filter = [](const CopySetInfo&) {
            return true;}) const override;

 private:
    int CleanInvalidLogicalPoolAndCopyset();

    void BackEndFunc();

    void FlushCopySetToStorage();

    void FlushChunkServerToStorage();

 private:
    std::unordered_map<PoolIdType, LogicalPool> logicalPoolMap_;
    std::unordered_map<PoolIdType, PhysicalPool> physicalPoolMap_;
    std::unordered_map<ZoneIdType, Zone> zoneMap_;
    std::unordered_map<ServerIdType, Server> serverMap_;
    std::unordered_map<ChunkServerIdType, ChunkServer> chunkServerMap_;

    std::map<CopySetKey, CopySetInfo> copySetMap_;

    std::shared_ptr<TopologyIdGenerator> idGenerator_;
    std::shared_ptr<TopologyTokenGenerator> tokenGenerator_;
    std::shared_ptr<TopologyStorage> storage_;

    //以如下声明的顺序获取锁，防止死锁
    mutable curve::common::RWLock logicalPoolMutex_;
    mutable curve::common::RWLock physicalPoolMutex_;
    mutable curve::common::RWLock zoneMutex_;
    mutable curve::common::RWLock serverMutex_;
    mutable curve::common::RWLock chunkServerMutex_;
    mutable curve::common::RWLock copySetMutex_;

    TopologyOption option_;
    curve::common::Thread backEndThread_;
    curve::common::Atomic<bool> isStop_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curve


#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_H_
