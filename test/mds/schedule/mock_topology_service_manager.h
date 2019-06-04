/*
 * Project: curve
 * Created Date: Thu Jan 03 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */


#ifndef TEST_MDS_SCHEDULE_MOCK_TOPOLOGY_SERVICE_MANAGER_H_
#define TEST_MDS_SCHEDULE_MOCK_TOPOLOGY_SERVICE_MANAGER_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <vector>

#include "src/mds/topology/topology_service_manager.h"

using ::curve::mds::topology::TopologyServiceManager;
using ::curve::mds::topology::Topology;
using ::curve::mds::topology::ChunkServerIdType;

namespace curve {
namespace mds {
namespace schedule {
class MockTopologyServiceManager : public TopologyServiceManager {
 public:
  MockTopologyServiceManager(std::shared_ptr<Topology> topology,
                             std::shared_ptr<curve::mds::copyset::CopysetManager> copysetManager) //NOLINT
      : TopologyServiceManager(topology, copysetManager) {}

  ~MockTopologyServiceManager() {}

  MOCK_METHOD2(CreateCopysetNodeOnChunkServer,
               bool(ChunkServerIdType id,
               const std::vector<::curve::mds::topology::CopySetInfo>  &infos));
};

}  // namespace schedule
}  // namespace mds
}  // namespace curve
#endif  // TEST_MDS_SCHEDULE_MOCK_TOPOLOGY_SERVICE_MANAGER_H_

