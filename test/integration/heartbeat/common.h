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

#include <brpc/channel.h>
#include <brpc/server.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>  //NOLINT
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>  //NOLINT
#include <unordered_map>
#include <vector>

#include "proto/common.pb.h"
#include "proto/heartbeat.pb.h"
#include "proto/topology.pb.h"
#include "src/common/configuration.h"
#include "src/common/timeutility.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/copyset/copyset_config.h"
#include "src/mds/copyset/copyset_manager.h"
#include "src/mds/heartbeat/chunkserver_healthy_checker.h"
#include "src/mds/heartbeat/heartbeat_manager.h"
#include "src/mds/heartbeat/heartbeat_service.h"
#include "src/mds/nameserver2/idgenerator/chunk_id_generator.h"
#include "src/mds/schedule/operator.h"
#include "src/mds/schedule/scheduleMetrics.h"
#include "src/mds/schedule/topoAdapter.h"
#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_config.h"
#include "src/mds/topology/topology_item.h"
#include "src/mds/topology/topology_service_manager.h"
#include "src/mds/topology/topology_storge.h"
#include "src/mds/topology/topology_token_generator.h"

using ::curve::common::Configuration;
using std::string;

using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::ChunkServerState;
using ::curve::mds::topology::CopySetIdType;
using ::curve::mds::topology::CopySetKey;
using ::curve::mds::topology::DefaultIdGenerator;
using ::curve::mds::topology::DefaultTokenGenerator;
using ::curve::mds::topology::kTopoErrCodeSuccess;
using ::curve::mds::topology::LogicalPool;
using ::curve::mds::topology::LogicalPoolType;
using ::curve::mds::topology::PhysicalPool;
using ::curve::mds::topology::PoolIdType;
using ::curve::mds::topology::Poolset;
using ::curve::mds::topology::PoolsetIdType;
using ::curve::mds::topology::Server;
using ::curve::mds::topology::ServerIdType;
using ::curve::mds::topology::TopologyImpl;
using ::curve::mds::topology::TopologyOption;
using ::curve::mds::topology::TopologyServiceManager;
using ::curve::mds::topology::TopologyStatImpl;
using ::curve::mds::topology::UNINTIALIZE_ID;
using ::curve::mds::topology::Zone;
using ::curve::mds::topology::ZoneIdType;

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

    bool LoadPoolset(std::unordered_map<PoolsetIdType, Poolset>* PoolsetMap,
                     PoolsetIdType* maxPoolsetId) {
        return true;
    }
    bool LoadLogicalPool(
        std::unordered_map<PoolIdType, LogicalPool>* logicalPoolMap,
        PoolIdType* maxLogicalPoolId) {
        return true;
    }
    bool LoadPhysicalPool(
        std::unordered_map<PoolIdType, PhysicalPool>* physicalPoolMap,
        PoolIdType* maxPhysicalPoolId) {
        return true;
    }
    bool LoadZone(std::unordered_map<ZoneIdType, Zone>* zoneMap,
                  ZoneIdType* maxZoneId) {
        return true;
    }
    bool LoadServer(std::unordered_map<ServerIdType, Server>* serverMap,
                    ServerIdType* maxServerId) {
        return true;
    }
    bool LoadChunkServer(
        std::unordered_map<ChunkServerIdType, ChunkServer>* chunkServerMap,
        ChunkServerIdType* maxChunkServerId) {
        return true;
    }
    bool LoadCopySet(std::map<CopySetKey, CopySetInfo>* copySetMap,
                     std::map<PoolIdType, CopySetIdType>* copySetIdMaxMap) {
        return true;
    }

    bool StoragePoolset(const Poolset& data) { return true; }
    bool StorageLogicalPool(const LogicalPool& data) { return true; }
    bool StoragePhysicalPool(const PhysicalPool& data) { return true; }
    bool StorageZone(const Zone& data) { return true; }
    bool StorageServer(const Server& data) { return true; }
    bool StorageChunkServer(const ChunkServer& data) { return true; }
    bool StorageCopySet(const CopySetInfo& data) { return true; }

    bool DeletePoolset(PoolsetIdType id) { return true; }
    bool DeleteLogicalPool(PoolIdType id) { return true; }
    bool DeletePhysicalPool(PoolIdType id) { return true; }
    bool DeleteZone(ZoneIdType id) { return true; }
    bool DeleteServer(ServerIdType id) { return true; }
    bool DeleteChunkServer(ChunkServerIdType id) { return true; }
    bool DeleteCopySet(CopySetKey key) { return true; }

    bool UpdateLogicalPool(const LogicalPool& data) { return true; }
    bool UpdatePhysicalPool(const PhysicalPool& data) { return true; }
    bool UpdateZone(const Zone& data) { return true; }
    bool UpdateServer(const Server& data) { return true; }
    bool UpdateChunkServer(const ChunkServer& data) { return true; }
    bool UpdateCopySet(const CopySetInfo& data) { return true; }

    bool LoadClusterInfo(std::vector<ClusterInformation>* info) { return true; }
    bool StorageClusterInfo(const ClusterInformation& info) { return true; }
};
}  // namespace topology

class HeartbeatIntegrationCommon {
 public:
    /* HeartbeatIntegrationCommon constructor
     *
     * @param[in] conf configuration information
     */
    explicit HeartbeatIntegrationCommon(const Configuration& conf) {
        conf_ = conf;
    }

    /* PrepareAddPoolset adds a physical pool collection to the cluster
     *
     * @param[in] poolset Physical pool set (pool group)
     */
    void PrepareAddPoolset(const Poolset& poolset);

    /* PrepareAddLogicalPool Adding a Logical Pool to a Cluster
     *
     * @param[in] lpool logical pool
     */
    void PrepareAddLogicalPool(const LogicalPool& lpool);

    /* PrepareAddPhysicalPool Adding a Physical Pool to a Cluster
     *
     * @param[in] ppool physical pool
     */
    void PrepareAddPhysicalPool(const PhysicalPool& ppool);

    /* PrepareAddZone adds a zone to the cluster
     *
     * @param[in] zone
     */
    void PrepareAddZone(const Zone& zone);

    /* PrepareAddServer Adding a server to a Cluster
     *
     * @param[in] server
     */
    void PrepareAddServer(const Server& server);

    /* PrepareAddChunkServer adds chunkserver nodes to the cluster
     *
     * @param[in] chunkserver
     */
    void PrepareAddChunkServer(const ChunkServer& chunkserver);

    /* PrepareAddCopySet Adding a copyset to a cluster
     *
     * @param[in] copysetId copyset ID
     * @param[in] logicalPoolId Logical Pool ID
     * @param[in] members copyset members
     */
    void PrepareAddCopySet(CopySetIdType copysetId, PoolIdType logicalPoolId,
                           const std::set<ChunkServerIdType>& members);

    /* UpdateCopysetTopo updates the status of copyset in topology
     *
     * @param[in] copysetId The ID of the copyset
     * @param[in] logicalPoolId Logical Pool ID
     * @param[in] epoch epoch of copyset
     * @param[in] leader copyset's leader
     * @param[in] members  members of copyset
     * @param[in] candidate copyset's candidate information
     */
    void UpdateCopysetTopo(CopySetIdType copysetId, PoolIdType logicalPoolId,
                           uint64_t epoch, ChunkServerIdType leader,
                           const std::set<ChunkServerIdType>& members,
                           ChunkServerIdType candidate = UNINTIALIZE_ID);

    /* SendHeartbeat sends a heartbeat
     *
     * @param[in] req
     * @param[in] expectedFailed true: to indicate that the transmission is
     * expected to succeed, false: indicate that the transmission is expected to
     * fail
     * @param[out] response
     */
    void SendHeartbeat(const ChunkServerHeartbeatRequest& request,
                       bool expectFailed,
                       ChunkServerHeartbeatResponse* response);

    /* BuildBasicChunkServerRequest Build the most basic request
     *
     * @param[in] id chunkserver ID
     * @param[out] req Constructed request with specified id
     */
    void BuildBasicChunkServerRequest(ChunkServerIdType id,
                                      ChunkServerHeartbeatRequest* req);

    /* AddCopySetToRequest adds a copyset to the request
     *
     * @param[in] req
     * @param[in] csInfo copyset information
     * @param[in] type copyset Current change type
     */
    void AddCopySetToRequest(ChunkServerHeartbeatRequest* req,
                             const CopySetInfo& csInfo,
                             ConfigChangeType type = ConfigChangeType::NONE);

    /* AddOperatorToOpController adds op to the scheduling module
     *
     * @param[in] op
     */
    void AddOperatorToOpController(const Operator& op);

    /* RemoveOperatorFromOpController removes the op on the specified copyset
     * from the scheduling module
     *
     * @param[in] id needs to remove the copysetId of op
     */
    void RemoveOperatorFromOpController(const CopySetKey& id);

    /*
     * PrepareBasicCluseter builds the most basic topology structure in topology
     * One physical pool, one logical pool, three zones, and one chunkserver for
     * each zone, There is a copyset in the cluster
     */
    void PrepareBasicCluseter();

    /**
     * InitHeartbeatOption
     *
     * @param[in] conf configuration module
     * @param[out] heartbeat option assignment completed heartbeat option
     */
    void InitHeartbeatOption(Configuration* conf,
                             HeartbeatOption* heartbeatOption);

    /**
     * InitSchedulerOption initializes scheduleOption
     *
     * @param[in] conf configuration module
     * @param[out] heartbeat  Scheduling option with completed assignment of
     * option
     */
    void InitSchedulerOption(Configuration* conf,
                             ScheduleOption* scheduleOption);

    /**
     * BuildBasicCluster runs the heartbeat/topology/scheduler module
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
