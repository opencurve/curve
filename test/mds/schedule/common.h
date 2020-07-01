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
 * Created Date: Mon Dec 24 2018
 * Author: lixiaocui
 */


#ifndef TEST_MDS_SCHEDULE_COMMON_H_
#define TEST_MDS_SCHEDULE_COMMON_H_

#include <vector>
#include <map>
#include <boost/shared_ptr.hpp>
#include "src/mds/schedule/operatorStep.h"
#include "src/mds/schedule/topoAdapter.h"
#include "proto/heartbeat.pb.h"

using ::curve::mds::schedule::TransferLeader;
using ::curve::mds::schedule::AddPeer;
using ::curve::mds::schedule::RemovePeer;
using ::curve::mds::schedule::PeerInfo;
using ::curve::mds::schedule::CopySetConf;
using ::curve::mds::topology::EpochType;
using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::ServerIdType;
using ::curve::mds::topology::PoolIdType;
using ::curve::mds::topology::CopySetIdType;
using ::curve::mds::topology::LogicalPoolType;
using ::curve::mds::heartbeat::CandidateError;

namespace curve {
namespace mds {
namespace schedule {
::curve::mds::schedule::CopySetInfo GetCopySetInfoForTest();
void GetCopySetInChunkServersForTest(
    std::map<ChunkServerIDType, std::vector<CopySetInfo>> *out);
::curve::mds::topology::CopySetInfo GetTopoCopySetInfoForTest();
std::vector<::curve::mds::topology::ChunkServer> GetTopoChunkServerForTest();
std::vector<::curve::mds::topology::Server> GetServerForTest();
::curve::mds::topology::LogicalPool GetPageFileLogicalPoolForTest();
::curve::mds::topology::LogicalPool GetAppendFileLogicalPoolForTest();
::curve::mds::topology::LogicalPool GetAppendECFileLogicalPoolForTest();
}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // TEST_MDS_SCHEDULE_COMMON_H_
