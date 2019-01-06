/*
 * Project: curve
 * Created Date: Tue Dec 25 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */
#ifndef CURVE_TEST_MDS_SCHEDULE_MOCK_TOPOADAPTER_H
#define CURVE_TEST_MDS_SCHEDULE_MOCK_TOPOADAPTER_H

#include <gmock/gmock.h>
#include <vector>
#include "src/mds/schedule/topoAdapter.h"

namespace curve {
namespace mds {
namespace schedule {
class MockTopoAdapter : public TopoAdapter {
 public:
  MockTopoAdapter() {}
  ~MockTopoAdapter() {}

  MOCK_METHOD2(GetCopySetInfo, bool(const CopySetKey &id, CopySetInfo *info));

  MOCK_METHOD0(GetCopySetInfos, std::vector<CopySetInfo>());

  MOCK_METHOD2(GetChunkServerInfo,
               bool(ChunkServerIdType id, ChunkServerInfo *info));

  MOCK_METHOD0(GetChunkServerInfos, std::vector<ChunkServerInfo>());

  MOCK_METHOD1(GetStandardZoneNumInLogicalPool, int(PoolIdType id));

  MOCK_METHOD1(GetStandardReplicaNumInLogicalPool, int(PoolIdType id));

  MOCK_METHOD2(SelectBestPlacementChunkServer,
               ChunkServerIdType(
                   const CopySetInfo &copySetInfo, ChunkServerIdType oldPeer));

  MOCK_METHOD2(CreateCopySetAtChunkServer,
               bool(CopySetKey id, ChunkServerIdType csID));

  MOCK_METHOD2(CopySetFromTopoToSchedule,
               bool(const ::curve::mds::topology::CopySetInfo &origin,
                   ::curve::mds::schedule::CopySetInfo *out));
  MOCK_METHOD2(ChunkServerFromTopoToSchedule,
               bool(const ::curve::mds::topology::ChunkServer &origin,
                   ::curve::mds::schedule::ChunkServerInfo *out));
};
}  // namespace schedule
}  // namespace mds
}  // namespace curve
#endif  // CURVE_TEST_MDS_SCHEDULE_MOCK_TOPOADAPTER_H
