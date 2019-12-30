/*
 * Project: curve
 * Created Date: Sat Jan 05 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef TEST_MDS_MOCK_MOCK_TOPOADAPTER_H_
#define TEST_MDS_MOCK_MOCK_TOPOADAPTER_H_

#include <gmock/gmock.h>
#include "src/mds/schedule/coordinator.h"
#include "src/mds/topology/topology.h"

namespace curve {
namespace mds {
namespace heartbeat {
class MockTopoAdapter : public ::curve::mds::schedule::TopoAdapterImpl {
 public:
  MockTopoAdapter() {}
  ~MockTopoAdapter() {}

  MOCK_METHOD2(CopySetFromTopoToSchedule,
  bool(const ::curve::mds::topology::CopySetInfo &origin,
      ::curve::mds::schedule::CopySetInfo *out));

  MOCK_METHOD2(ChunkServerFromTopoToSchedule,
  bool(const ::curve::mds::topology::ChunkServer &origin,
      ::curve::mds::schedule::ChunkServerInfo *out));
};
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve
#endif  // TEST_MDS_MOCK_MOCK_TOPOADAPTER_H_


