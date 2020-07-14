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
 * Created Date: Thu Jan 03 2019
 * Author: lixiaocui
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

