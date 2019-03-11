/*
 * Project: curve
 * Created Date: Sat Jan 05 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */


#ifndef CURVE_TEST_MDS_HEARTBEAT_MOCK_TOPOLOGY_H
#define CURVE_TEST_MDS_HEARTBEAT_MOCK_TOPOLOGY_H

#include <gmock/gmock.h>
#include <string>
#include <list>
#include <vector>
#include "src/mds/schedule/topoAdapter.h"
#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_item.h"
#include "src/mds/common/mds_define.h"

using ::curve::mds::topology::PoolIdType;
using ::curve::mds::topology::ZoneIdType;
using ::curve::mds::topology::ServerIdType;
using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::UserIdType;
using ::curve::mds::topology::CopySetIdType;
using ::curve::mds::topology::EpochType;
using ::curve::mds::topology::CopySetKey;
using ::curve::mds::topology::ChunkServerState;
using ::curve::mds::topology::CopySetInfo;
using ::curve::mds::topology::Zone;
using ::curve::mds::topology::Server;
using ::curve::mds::topology::ChunkServer;
using ::curve::mds::topology::LogicalPool;
using ::curve::mds::topology::PhysicalPool;

namespace curve {
namespace mds {
namespace heartbeat {
class MockTopology : public Topology {
 public:
  MockTopology() {}
  ~MockTopology() {}
  MOCK_METHOD0(init, int());

  // allocate id & token
  MOCK_METHOD0(AllocateLogicalPoolId, PoolIdType());
  MOCK_METHOD0(AllocatePhysicalPoolId, PoolIdType());
  MOCK_METHOD0(AllocateZoneId, ZoneIdType());
  MOCK_METHOD0(AllocateServerId, ServerIdType());
  MOCK_METHOD0(AllocateChunkServerId, ChunkServerIdType());
  MOCK_METHOD1(AllocateCopySetId, CopySetIdType(PoolIdType
                                                logicalPoolId));
  MOCK_METHOD0(AllocateToken, std::string());

  // add
  MOCK_METHOD1(AddLogicalPool, int(const LogicalPool &data));
  MOCK_METHOD1(AddPhysicalPool, int(const PhysicalPool &data));
  MOCK_METHOD1(AddZone, int(const Zone &data));
  MOCK_METHOD1(AddServer, int(const Server &data));
  MOCK_METHOD1(AddChunkServer, int(const ChunkServer &data));
  MOCK_METHOD1(AddCopySet,
  int(const ::curve::mds::topology::CopySetInfo &data));
  MOCK_METHOD1(AddCopySetList,
  int(const std::vector<::curve::mds::topology::CopySetInfo>
      &copysets));

  // remove
  MOCK_METHOD1(RemoveLogicalPool, int(PoolIdType id));
  MOCK_METHOD1(RemovePhysicalPool, int(PoolIdType id));
  MOCK_METHOD1(RemoveZone, int(ZoneIdType id));
  MOCK_METHOD1(RemoveServer, int(ServerIdType id));
  MOCK_METHOD1(RemoveChunkServer, int(ChunkServerIdType id));
  MOCK_METHOD1(RemoveCopySet, int(CopySetKey key));

  // update
  MOCK_METHOD1(UpdateLogicalPool, int(const LogicalPool &data));
  MOCK_METHOD1(UpdatePhysicalPool, int(const PhysicalPool &data));
  MOCK_METHOD1(UpdateZone, int(const Zone &data));
  MOCK_METHOD1(UpdateServer, int(const Server &data));
  // 更新内存并持久化全部数据
  MOCK_METHOD1(UpdateChunkServer, int(const ChunkServer &data));
  // 更新内存，定期持久化数据
  MOCK_METHOD2(UpdateChunkServerState, int(const ChunkServerState &state,
      ChunkServerIdType id));
  MOCK_METHOD1(UpdateCopySet,
  int(const ::curve::mds::topology::CopySetInfo &data));

  // find
  MOCK_CONST_METHOD2(FindLogicalPool,
      PoolIdType(const std::string &logicalPoolName,
                 const std::string &physicalPoolName));
  MOCK_CONST_METHOD1(FindPhysicalPool,
      PoolIdType(const std::string &physicalPoolName));
  MOCK_CONST_METHOD2(FindZone,
      ZoneIdType(const std::string &zoneName,
                 const std::string &physicalPoolName));
  MOCK_CONST_METHOD2(FindZone,
      ZoneIdType(const std::string &zoneName,
                 PoolIdType physicalpoolid));
  MOCK_CONST_METHOD1(FindServerByHostName,
      ServerIdType(const std::string &hostName));
  MOCK_CONST_METHOD2(FindServerByHostIpPort,
      ServerIdType(const std::string &hostIp, uint32_t port));
  MOCK_CONST_METHOD2(FindChunkServer,
      ChunkServerIdType(const std::string &hostIp,
                        uint32_t port));
  // get
  MOCK_CONST_METHOD2(GetLogicalPool,
  bool(PoolIdType poolId, LogicalPool *out));
  MOCK_CONST_METHOD2(GetPhysicalPool,
  bool(PoolIdType poolId, PhysicalPool *out));
  MOCK_CONST_METHOD2(GetZone, bool(ZoneIdType zoneId, Zone *out));
  MOCK_CONST_METHOD2(GetServer, bool(ServerIdType serverId, Server *out));
  MOCK_CONST_METHOD2(GetChunkServer,
  bool(ChunkServerIdType chunkserverId, ChunkServer *out));

  MOCK_CONST_METHOD2(GetCopySet,
  bool(CopySetKey key, ::curve::mds::topology::CopySetInfo
      *out));

  MOCK_CONST_METHOD3(GetLogicalPool,
  bool(const std::string &logicalPoolName,
  const std::string &physicalPoolName,
      LogicalPool *out));

  MOCK_CONST_METHOD2(GetPhysicalPool,
  bool(const std::string &physicalPoolName,
      PhysicalPool *out));
  MOCK_CONST_METHOD3(GetZone,
  bool(const std::string &zoneName,
  const std::string &physicalPoolName, Zone *out));

  MOCK_CONST_METHOD3(GetZone,
  bool(const std::string &zoneName,
      PoolIdType physicalPoolId, Zone *out));

  MOCK_CONST_METHOD2(GetServerByHostName,
  bool(const std::string &hostName, Server *out));

  MOCK_CONST_METHOD3(GetServerByHostIpPort,
  bool(const std::string &hostIp, uint32_t port, Server *out));

  MOCK_CONST_METHOD3(GetChunkServer,
  bool(const std::string &hostIp,
      uint32_t port,
  ChunkServer *out));
  // getlist
  MOCK_CONST_METHOD0(GetChunkServerInCluster, std::list<ChunkServerIdType>());
  MOCK_CONST_METHOD0(GetServerInCluster, std::list<ServerIdType> ());
  MOCK_CONST_METHOD0(GetZoneInCluster, std::list<ZoneIdType> ());
  MOCK_CONST_METHOD0(GetPhysicalPoolInCluster, std::list<PoolIdType>());
  MOCK_CONST_METHOD0(GetLogicalPoolInCluster, std::list<PoolIdType>());

  MOCK_CONST_METHOD1(GetChunkServerInServer,
      std::list<ChunkServerIdType>(ServerIdType id));
  MOCK_CONST_METHOD1(GetChunkServerInZone,
      std::list<ChunkServerIdType>(ZoneIdType id));
  MOCK_CONST_METHOD1(GetChunkServerInPhysicalPool,
      std::list<ChunkServerIdType>(PoolIdType id));

  MOCK_CONST_METHOD1(GetServerInZone,
      std::list<ServerIdType>(ZoneIdType id));
  MOCK_CONST_METHOD1(GetServerInPhysicalPool,
      std::list<ServerIdType>(PoolIdType id));

  MOCK_CONST_METHOD1(GetZoneInPhysicalPool,
      std::list<ZoneIdType>(PoolIdType id));
  MOCK_CONST_METHOD1(GetLogicalPoolInPhysicalPool,
      std::list<PoolIdType>(PoolIdType id));
  MOCK_CONST_METHOD1(GetChunkServerInLogicalPool,
      std::list<ChunkServerIdType>(PoolIdType id));
  MOCK_CONST_METHOD1(GetServerInLogicalPool,
      std::list<ServerIdType>(PoolIdType id));
  MOCK_CONST_METHOD1(GetZoneInLogicalPool,
      std::list<ZoneIdType>(PoolIdType id));

  MOCK_CONST_METHOD1(GetCopySetsInLogicalPool,
      std::vector<CopySetIdType>(PoolIdType logicalPoolId));
  // TODO(chaojie-schedule): add
  MOCK_CONST_METHOD0(GetCopySetsInCluster, std::vector<CopySetKey>());
  MOCK_CONST_METHOD1(GetCopySetsInChunkServer,
      std::vector<CopySetKey>(ChunkServerIdType id));
};
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve
#endif  // CURVE_TEST_MDS_HEARTBEAT_MOCK_TOPOLOGY_H
