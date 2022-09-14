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
 * Created Date: 2019-06-11
 * Author: lixiaocui
 */

#ifndef TEST_INTEGRATION_HEARTBEAT_COMMON_H_
#define TEST_INTEGRATION_HEARTBEAT_COMMON_H_

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <brpc/channel.h>
#include <brpc/server.h>

#include <memory>
#include <thread>    //NOLINT
#include <chrono>    //NOLINT
#include <unordered_map>
#include <map>
#include <string>
#include <vector>
#include <set>

#include "src/common/configuration.h"
#include "src/mds/topology/topology_config.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/topology/topology_item.h"
#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_token_generator.h"
#include "src/mds/topology/topology_service_manager.h"
#include "src/mds/topology/topology_storge.h"
#include "src/mds/nameserver2/idgenerator/chunk_id_generator.h"
#include "src/mds/heartbeat/heartbeat_manager.h"
#include "src/mds/heartbeat/heartbeat_service.h"
#include "src/mds/heartbeat/chunkserver_healthy_checker.h"
#include "src/mds/schedule/topoAdapter.h"
#include "src/mds/schedule/operator.h"
#include "src/mds/copyset/copyset_manager.h"
#include "src/mds/copyset/copyset_config.h"
#include "src/mds/schedule/scheduleMetrics.h"
#include "proto/topology.pb.h"
#include "proto/heartbeat.pb.h"
#include "proto/common.pb.h"
#include "src/common/timeutility.h"

using ::curve::common::Configuration;
using std::string;

using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::ChunkServerState;
using ::curve::mds::topology::CopySetIdType;
using ::curve::mds::topology::DefaultIdGenerator;
using ::curve::mds::topology::DefaultTokenGenerator;
using ::curve::mds::topology::kTopoErrCodeSuccess;
using ::curve::mds::topology::LogicalPool;
using ::curve::mds::topology::LogicalPoolType;
using ::curve::mds::topology::Poolset;
using ::curve::mds::topology::PhysicalPool;
using ::curve::mds::topology::PoolsetIdType;
using ::curve::mds::topology::PoolIdType;
using ::curve::mds::topology::ServerIdType;
using ::curve::mds::topology::TopologyImpl;
using ::curve::mds::topology::TopologyOption;
using ::curve::mds::topology::TopologyServiceManager;
using ::curve::mds::topology::TopologyStatImpl;
using ::curve::mds::topology::UNINTIALIZE_ID;
using ::curve::mds::topology::Zone;
using ::curve::mds::topology::ZoneIdType;
using ::curve::mds::topology::Server;
using ::curve::mds::topology::CopySetKey;

using ::curve::mds::heartbeat::ChunkServerHeartbeatRequest;
using ::curve::mds::heartbeat::ChunkServerHeartbeatResponse;
using ::curve::mds::heartbeat::ChunkServerStatisticInfo;
using ::curve::mds::heartbeat::ConfigChangeType;
using ::curve::mds::heartbeat::CopySetConf;
using ::curve::mds::heartbeat::HeartbeatManager;
using ::curve::mds::heartbeat::HeartbeatOption;
using ::curve::mds::heartbeat::HeartbeatService_Stub;
using ::curve::mds::heartbeat::HeartbeatServiceImpl;

using ::curve::mds::copyset::CopysetManager;
using ::curve::mds::copyset::CopysetOption;

using ::curve::mds::schedule::AddPeer;
using ::curve::mds::schedule::ChangePeer;
using ::curve::mds::schedule::Operator;
using ::curve::mds::schedule::OperatorPriority;
using ::curve::mds::schedule::OperatorStep;
using ::curve::mds::schedule::RemovePeer;
using ::curve::mds::schedule::ScheduleMetrics;
using ::curve::mds::schedule::ScheduleOption;
using ::curve::mds::schedule::TopoAdapterImpl;
using ::curve::mds::schedule::TransferLeader;

namespace curve {
namespace mds {

#define SENDHBOK false
#define SENDHBFAIL true

namespace topology {
class FakeTopologyStorage : public TopologyStorage {
 public:
    FakeTopologyStorage() {}

    bool
    LoadPoolset(std::unordered_map<PoolsetIdType, Poolset> *PoolsetMap,
                    PoolsetIdType *maxPoolsetId) {
        return true;
    }
    bool
    LoadLogicalPool(std::unordered_map<PoolIdType, LogicalPool> *logicalPoolMap,
                    PoolIdType *maxLogicalPoolId) {
        return true;
    }
    bool LoadPhysicalPool(
        std::unordered_map<PoolIdType, PhysicalPool> *physicalPoolMap,
        PoolIdType *maxPhysicalPoolId) {
        return true;
    }
    bool LoadZone(std::unordered_map<ZoneIdType, Zone> *zoneMap,
                  ZoneIdType *maxZoneId) {
        return true;
    }
    bool LoadServer(std::unordered_map<ServerIdType, Server> *serverMap,
                    ServerIdType *maxServerId) {
        return true;
    }
    bool LoadChunkServer(
        std::unordered_map<ChunkServerIdType, ChunkServer> *chunkServerMap,
        ChunkServerIdType *maxChunkServerId) {
        return true;
    }
    bool LoadCopySet(std::map<CopySetKey, CopySetInfo> *copySetMap,
                     std::map<PoolIdType, CopySetIdType> *copySetIdMaxMap) {
        return true;
    }

    bool StoragePoolset(const Poolset &data) {
        return true;
    }
    bool StorageLogicalPool(const LogicalPool &data) {
        return true;
    }
    bool StoragePhysicalPool(const PhysicalPool &data) {
        return true;
    }
    bool StorageZone(const Zone &data) {
        return true;
    }
    bool StorageServer(const Server &data) {
        return true;
    }
    bool StorageChunkServer(const ChunkServer &data) {
        return true;
    }
    bool StorageCopySet(const CopySetInfo &data) {
        return true;
    }

    bool DeletePoolset(PoolsetIdType id) {
        return true;
    }
    bool DeleteLogicalPool(PoolIdType id) {
        return true;
    }
    bool DeletePhysicalPool(PoolIdType id) {
        return true;
    }
    bool DeleteZone(ZoneIdType id) {
        return true;
    }
    bool DeleteServer(ServerIdType id) {
        return true;
    }
    bool DeleteChunkServer(ChunkServerIdType id) {
        return true;
    }
    bool DeleteCopySet(CopySetKey key) {
        return true;
    }

    bool UpdateLogicalPool(const LogicalPool &data) {
        return true;
    }
    bool UpdatePhysicalPool(const PhysicalPool &data) {
        return true;
    }
    bool UpdateZone(const Zone &data) {
        return true;
    }
    bool UpdateServer(const Server &data) {
        return true;
    }
    bool UpdateChunkServer(const ChunkServer &data) {
        return true;
    }
    bool UpdateCopySet(const CopySetInfo &data) {
        return true;
    }

    bool LoadClusterInfo(std::vector<ClusterInformation> *info) {
        return true;
    }
    bool StorageClusterInfo(const ClusterInformation &info) {
        return true;
    }
};
}  // namespace topology

class HeartbeatIntegrationCommon {
 public:
    /* HeartbeatIntegrationCommon 构造函数
     *
     * @param[in] conf 配置信息
     */
    explicit HeartbeatIntegrationCommon(const Configuration &conf) {
        conf_ = conf;
    }

    /* PrepareAddPoolset 在集群中添加物理池集合
     *
     * @param[in] poolset 物理池集合（池组）
     */
    void PrepareAddPoolset(const Poolset &poolset);

    /* PrepareAddLogicalPool 在集群中添加逻辑池
     *
     * @param[in] lpool 逻辑池
     */
    void PrepareAddLogicalPool(const LogicalPool &lpool);

    /* PrepareAddPhysicalPool 在集群中添加物理池
     *
     * @param[in] ppool 物理池
     */
    void PrepareAddPhysicalPool(const PhysicalPool &ppool);

    /* PrepareAddZone 在集群中添加zone
     *
     * @param[in] zone
     */
    void PrepareAddZone(const Zone &zone);

    /* PrepareAddServer 在集群中添加server
     *
     * @param[in] server
     */
    void PrepareAddServer(const Server &server);

    /* PrepareAddChunkServer 在集群中添加chunkserver节点
     *
     * @param[in] chunkserver
     */
    void PrepareAddChunkServer(const ChunkServer &chunkserver);

    /* PrepareAddCopySet 在集群中添加copyset
     *
     * @param[in] copysetId copyset id
     * @param[in] logicalPoolId 逻辑池id
     * @param[in] members copyset成员
     */
    void PrepareAddCopySet(CopySetIdType copysetId, PoolIdType logicalPoolId,
                           const std::set<ChunkServerIdType> &members);

    /* UpdateCopysetTopo 更新topology中copyset的状态
     *
     * @param[in] copysetId copyset的id
     * @param[in] logicalPoolId 逻辑池id
     * @param[in] epoch copyset的epoch
     * @param[in] leader copyset的leader
     * @param[in] members copyset的成员
     * @param[in] candidate copyset的candidate信息
     */
    void UpdateCopysetTopo(CopySetIdType copysetId, PoolIdType logicalPoolId,
                           uint64_t epoch, ChunkServerIdType leader,
                           const std::set<ChunkServerIdType> &members,
                           ChunkServerIdType candidate = UNINTIALIZE_ID);

    /* SendHeartbeat 发送心跳
     *
     * @param[in] req
     * @param[in] expectedFailed 为true表示希望发送成功，为false表示希望发送失败
     * @param[out] response
     */
    void SendHeartbeat(const ChunkServerHeartbeatRequest &request,
                       bool expectFailed,
                       ChunkServerHeartbeatResponse *response);

    /* BuildBasicChunkServerRequest 构建最基本的request
     *
     * @param[in] id chunkserver的id
     * @param[out] req 构造好的指定id的request
     */
    void BuildBasicChunkServerRequest(ChunkServerIdType id,
                                      ChunkServerHeartbeatRequest *req);

    /* AddCopySetToRequest 向request中添加copyset
     *
     * @param[in] req
     * @param[in] csInfo copyset信息
     * @param[in] type copyset当前变更类型
     */
    void AddCopySetToRequest(ChunkServerHeartbeatRequest *req,
                             const CopySetInfo &csInfo,
                             ConfigChangeType type = ConfigChangeType::NONE);

    /* AddOperatorToOpController 向调度模块添加op
     *
     * @param[in] op
     */
    void AddOperatorToOpController(const Operator &op);

    /* RemoveOperatorFromOpController 从调度模块移除指定copyset上的op
     *
     * @param[in] id 需要移除op的copysetId
     */
    void RemoveOperatorFromOpController(const CopySetKey &id);

    /*
     * PrepareBasicCluseter 在topology中构建最基本的拓扑结构
     * 一个物理池，一个逻辑池，三个zone，每个zone一个chunkserver,
     * 集群中有一个copyset
     */
    void PrepareBasicCluseter();

    /**
     * InitHeartbeatOption 初始化heartbeatOption
     *
     * @param[in] conf 配置模块
     * @param[out] heartbeatOption 赋值完成的心跳option
     */
    void InitHeartbeatOption(Configuration *conf,
                             HeartbeatOption *heartbeatOption);

    /**
     * InitSchedulerOption 初始化scheduleOption
     *
     * @param[in] conf 配置模块
     * @param[out] heartbeatOption 赋值完成的调度option
     */
    void InitSchedulerOption(Configuration *conf,
                             ScheduleOption *scheduleOption);

    /**
     * BuildBasicCluster 运行heartbeat/topology/scheduler模块
     */
    void BuildBasicCluster();

 public:
    Configuration conf_;
    std::string listenAddr_;
    brpc::Server server_;

    std::shared_ptr<TopologyImpl> topology_;
    std::shared_ptr<TopologyStatImpl> topologyStat_;

    std::shared_ptr<HeartbeatManager> heartbeatManager_;
    std::shared_ptr<HeartbeatServiceImpl> heartbeatService_;
    std::shared_ptr<Coordinator> coordinator_;
};
}  // namespace mds
}  // namespace curve
#endif  // TEST_INTEGRATION_HEARTBEAT_COMMON_H_
