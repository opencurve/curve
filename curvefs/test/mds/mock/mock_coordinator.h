/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * @Project: curve
 * @Date: 2021-11-12 11:26:50
 * @Author: chenwei
 */

#ifndef CURVEFS_TEST_MDS_MOCK_MOCK_COORDINATOR_H_
#define CURVEFS_TEST_MDS_MOCK_MOCK_COORDINATOR_H_

#include <gmock/gmock.h>
#include <map>
#include <vector>
#include "curvefs/src/mds/schedule/coordinator.h"

namespace curvefs {
namespace mds {
using ::curvefs::mds::topology::MetaServerIdType;
using ::curvefs::mds::topology::PoolIdType;
using ::curvefs::mds::topology::CopySetKey;
class MockCoordinator : public ::curvefs::mds::schedule::Coordinator {
 public:
    MockCoordinator() {}
    ~MockCoordinator() {}

    MOCK_METHOD3(
        CopySetHeartbeat,
        MetaServerIdType(
            const ::curvefs::mds::topology::CopySetInfo &originInfo,
            const ::curvefs::mds::heartbeat::ConfigChangeInfo &configChInfo,
            ::curvefs::mds::heartbeat::CopySetConf *newConf));

    MOCK_METHOD2(MetaserverGoingToAdd, bool(MetaServerIdType, CopySetKey));

    MOCK_METHOD2(
        QueryMetaServerRecoverStatus,
        schedule::ScheduleStatusCode(const std::vector<MetaServerIdType> &,
                                  std::map<MetaServerIdType, bool> *));
};
}  // namespace mds
}  // namespace curvefs
#endif  // CURVEFS_TEST_MDS_MOCK_MOCK_COORDINATOR_H_
