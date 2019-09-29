/*
 * Project: curve
 * Created Date: Mon Apr 8th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef TEST_MDS_SCHEDULE_COPYSETSCHEDULERPOC_MOCK_TOPOLOGY_H_
#define TEST_MDS_SCHEDULE_COPYSETSCHEDULERPOC_MOCK_TOPOLOGY_H_
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <map>
#include <unordered_map>
#include <string>
#include <vector>
#include "proto/topology.pb.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_id_generator.h"
#include "src/mds/topology/topology_token_generator.h"
#include "src/mds/topology/topology_storge.h"

#include "proto/copyset.pb.h"

using ::testing::Return;
using ::testing::_;

using ::curve::mds::topology::PoolIdType;
using ::curve::mds::topology::ZoneIdType;
using ::curve::mds::topology::ServerIdType;
using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::CopySetIdType;
using ::curve::mds::topology::LogicalPool;
using ::curve::mds::topology::PhysicalPool;
using ::curve::mds::topology::Zone;
using ::curve::mds::topology::Server;
using ::curve::mds::topology::ChunkServerState;
using ::curve::mds::topology::ChunkServer;
using ::curve::mds::topology::CopySetInfo;
using ::curve::mds::topology::CopySetKey;
using ::curve::mds::topology::TopologyIdGenerator;
using ::curve::mds::topology::TopologyTokenGenerator;
using ::curve::mds::topology::TopologyStorage;
using ::curve::mds::topology::Topology;
using ::curve::mds::topology::ClusterInformation;

namespace curve {
namespace mds {
namespace schedule {
class MockIdGenerator : public TopologyIdGenerator {
 public:
    MockIdGenerator() {}
    ~MockIdGenerator() {}

    MOCK_METHOD1(initLogicalPoolIdGenerator, void(PoolIdType
        idMax));
    MOCK_METHOD1(initPhysicalPoolIdGenerator, void(PoolIdType
        idMax));
    MOCK_METHOD1(initZoneIdGenerator, void(ZoneIdType
        idMax));
    MOCK_METHOD1(initServerIdGenerator, void(ServerIdType
        idMax));
    MOCK_METHOD1(initChunkServerIdGenerator, void(ChunkServerIdType
        idMax));
    MOCK_METHOD1(initCopySetIdGenerator, void(
        const std::map<PoolIdType, CopySetIdType> &idMaxMap));

    MOCK_METHOD0(GenLogicalPoolId, PoolIdType());
    MOCK_METHOD0(GenPhysicalPoolId, PoolIdType());
    MOCK_METHOD0(GenZoneId, ZoneIdType());
    MOCK_METHOD0(GenServerId, ServerIdType());
    MOCK_METHOD0(GenChunkServerId, ChunkServerIdType());
    MOCK_METHOD1(GenCopySetId, CopySetIdType(PoolIdType
        logicalPoolId));
};

class MockTokenGenerator : public TopologyTokenGenerator {
 public:
    MockTokenGenerator() {}
    ~MockTokenGenerator() {}

    MOCK_METHOD0(GenToken, std::string());
};

class MockStorage : public TopologyStorage {
 public:
    MockStorage() {}
    ~MockStorage() {}

    MOCK_METHOD4(init, bool(
        const std::string &dbName,
        const std::string &user,
        const std::string &url,
        const std::string &password));

    MOCK_METHOD2(LoadLogicalPool,
        bool(std::unordered_map<PoolIdType, LogicalPool>
        *logicalPoolMap, PoolIdType * maxLogicalPoolId));
    MOCK_METHOD2(LoadPhysicalPool,
                bool(std::unordered_map<PoolIdType, PhysicalPool>
                    *physicalPoolMap, PoolIdType * maxPhysicalPoolId));
    MOCK_METHOD2(LoadZone, bool(std::unordered_map<ZoneIdType, Zone>
        *zoneMap, ZoneIdType * maxZoneId));
    MOCK_METHOD2(LoadServer, bool(std::unordered_map<ServerIdType, Server>
        *serverMap, ServerIdType * maxServerId));
    MOCK_METHOD2(LoadChunkServer,
                bool(std::unordered_map<ChunkServerIdType, ChunkServer>
                    *chunkServerMap, ChunkServerIdType * maxChunkServerId));
    MOCK_METHOD2(LoadCopySet, bool(
        std::map<CopySetKey, ::curve::mds::topology::CopySetInfo> *copySetMap,
        std::map<PoolIdType, CopySetIdType> * copySetIdMaxMap));

    MOCK_METHOD1(StorageLogicalPool, bool(
        const LogicalPool &data));
    MOCK_METHOD1(StoragePhysicalPool, bool(
        const PhysicalPool &data));
    MOCK_METHOD1(StorageZone, bool(
        const Zone &data));
    MOCK_METHOD1(StorageServer, bool(
        const Server &data));
    MOCK_METHOD1(StorageChunkServer, bool(
        const ChunkServer &data));
    MOCK_METHOD1(StorageCopySet, bool(
        const ::curve::mds::topology::CopySetInfo &data));

    MOCK_METHOD1(DeleteLogicalPool, bool(PoolIdType
        id));
    MOCK_METHOD1(DeletePhysicalPool, bool(PoolIdType
        id));
    MOCK_METHOD1(DeleteZone, bool(ZoneIdType
        id));
    MOCK_METHOD1(DeleteServer, bool(ServerIdType
        id));
    MOCK_METHOD1(DeleteChunkServer, bool(ChunkServerIdType
        id));
    MOCK_METHOD1(DeleteCopySet, bool(CopySetKey
        key));

    MOCK_METHOD1(UpdateLogicalPool, bool(
        const LogicalPool &data));
    MOCK_METHOD1(UpdatePhysicalPool, bool(
        const PhysicalPool &data));
    MOCK_METHOD1(UpdateZone, bool(
        const Zone &data));
    MOCK_METHOD1(UpdateServer, bool(
        const Server &data));
    MOCK_METHOD1(UpdateChunkServer, bool(
        const ChunkServer &data));
    MOCK_METHOD1(UpdateCopySet, bool(
        const ::curve::mds::topology::CopySetInfo &data));

    MOCK_METHOD1(LoadClusterInfo,
        bool(std::vector<ClusterInformation> *info));
    MOCK_METHOD1(StorageClusterInfo,
        bool(const ClusterInformation &info));
};
}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // TEST_MDS_SCHEDULE_COPYSETSCHEDULERPOC_MOCK_TOPOLOGY_H_
