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
 * Created Date: Tue Dec 25 2018
 * Author: lixiaocui
 */
#ifndef TEST_MDS_SCHEDULE_MOCK_TOPOADAPTER_H_
#define TEST_MDS_SCHEDULE_MOCK_TOPOADAPTER_H_

#include <gmock/gmock.h>
#include <vector>
#include <map>
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

    MOCK_METHOD1(GetCopySetInfosInChunkServer,
        std::vector<CopySetInfo>(ChunkServerIdType));

    MOCK_METHOD2(GetChunkServerInfo,
                bool(ChunkServerIdType id, ChunkServerInfo *info));

    MOCK_METHOD0(GetChunkServerInfos, std::vector<ChunkServerInfo>());

    MOCK_METHOD1(GetStandardZoneNumInLogicalPool, int(PoolIdType id));

    MOCK_METHOD1(GetStandardReplicaNumInLogicalPool, int(PoolIdType id));

    MOCK_METHOD1(GetAvgScatterWidthInLogicalPool, int(PoolIdType id));

    MOCK_METHOD2(CreateCopySetAtChunkServer,
                bool(CopySetKey id, ChunkServerIdType csID));

    MOCK_METHOD2(CopySetFromTopoToSchedule,
                bool(const ::curve::mds::topology::CopySetInfo &origin,
                    ::curve::mds::schedule::CopySetInfo *out));

    MOCK_METHOD2(ChunkServerFromTopoToSchedule,
                bool(const ::curve::mds::topology::ChunkServer &origin,
                    ::curve::mds::schedule::ChunkServerInfo *out));

    MOCK_METHOD2(GetChunkServerScatterMap, void(const ChunkServerIdType &,
                std::map<ChunkServerIdType, int> *));

    MOCK_METHOD0(GetLogicalpools, std::vector<PoolIdType>());

    MOCK_METHOD2(
        GetLogicalPool,
        bool(PoolIdType id, ::curve::mds::topology::LogicalPool* lpool));

    MOCK_METHOD1(GetCopySetInfosInLogicalPool,
        std::vector<CopySetInfo>(PoolIdType));

    MOCK_METHOD1(GetChunkServersInLogicalPool,
        std::vector<ChunkServerInfo>(PoolIdType));
};
}  // namespace schedule
}  // namespace mds
}  // namespace curve
#endif  // TEST_MDS_SCHEDULE_MOCK_TOPOADAPTER_H_
