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
 * @Date: 2021-11-10 11:04:24
 * @Author: chenwei
 */
#ifndef CURVEFS_TEST_MDS_SCHEDULE_MOCK_TOPOADAPTER_H_
#define CURVEFS_TEST_MDS_SCHEDULE_MOCK_TOPOADAPTER_H_

#include <gmock/gmock.h>
#include <map>
#include <set>
#include <vector>
#include "curvefs/src/mds/schedule/topoAdapter.h"

namespace curvefs {
namespace mds {
namespace schedule {
class MockTopoAdapter : public TopoAdapter {
 public:
    MockTopoAdapter() {}
    ~MockTopoAdapter() {}

    MOCK_METHOD2(GetCopySetInfo, bool(const CopySetKey &id, CopySetInfo *info));

    MOCK_METHOD0(GetCopySetInfos, std::vector<CopySetInfo>());

    MOCK_METHOD1(GetCopySetInfosInMetaServer,
                 std::vector<CopySetInfo>(MetaServerIdType));

    MOCK_METHOD2(GetMetaServerInfo,
                 bool(MetaServerIdType id, MetaServerInfo *info));

    MOCK_METHOD0(GetMetaServerInfos, std::vector<MetaServerInfo>());

    MOCK_METHOD1(GetStandardZoneNumInPool, uint16_t(PoolIdType id));

    MOCK_METHOD1(GetStandardReplicaNumInPool, uint16_t(PoolIdType id));

    MOCK_METHOD1(GetAvgScatterWidthInPool, int(PoolIdType id));

    MOCK_METHOD2(CreateCopySetAtMetaServer,
                 bool(CopySetKey id, MetaServerIdType csID));

    MOCK_METHOD2(CopySetFromTopoToSchedule,
                 bool(const ::curvefs::mds::topology::CopySetInfo &origin,
                      ::curvefs::mds::schedule::CopySetInfo *out));

    MOCK_METHOD2(MetaServerFromTopoToSchedule,
                 bool(const ::curvefs::mds::topology::MetaServer &origin,
                      ::curvefs::mds::schedule::MetaServerInfo *out));

    MOCK_METHOD0(Getpools, std::vector<PoolIdType>());

    MOCK_METHOD2(GetPool,
                 bool(PoolIdType id, ::curvefs::mds::topology::Pool *pool));

    MOCK_METHOD1(GetCopySetInfosInPool, std::vector<CopySetInfo>(PoolIdType));

    MOCK_METHOD1(GetMetaServersInPool, std::vector<MetaServerInfo>(PoolIdType));

    MOCK_METHOD3(ChooseZoneInPool,
                 bool(PoolIdType poolId, ZoneIdType *zoneId,
                      const std::set<ZoneIdType> &excludeZones));
    MOCK_METHOD3(ChooseSingleMetaServerInZone,
                 bool(ZoneIdType zoneId, MetaServerIdType *metaServerId,
                      const std::set<MetaServerIdType> &excludeMetaservers));
};
}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
#endif  // CURVEFS_TEST_MDS_SCHEDULE_MOCK_TOPOADAPTER_H_
