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
 * @Date: 2021-11-15 11:01:48
 * @Author: chenwei
 */

#ifndef CURVEFS_TEST_MDS_SCHEDULE_COMMON_H_
#define CURVEFS_TEST_MDS_SCHEDULE_COMMON_H_

#include <vector>
#include <map>
#include "curvefs/src/mds/schedule/operatorStep.h"
#include "curvefs/src/mds/schedule/topoAdapter.h"
#include "curvefs/proto/heartbeat.pb.h"

using ::curvefs::mds::schedule::TransferLeader;
using ::curvefs::mds::schedule::AddPeer;
using ::curvefs::mds::schedule::RemovePeer;
using ::curvefs::mds::schedule::PeerInfo;
using ::curvefs::mds::schedule::CopySetConf;
using ::curvefs::mds::topology::EpochType;
using ::curvefs::mds::topology::MetaServerIdType;
using ::curvefs::mds::topology::ServerIdType;
using ::curvefs::mds::topology::PoolIdType;
using ::curvefs::mds::topology::CopySetIdType;
using ::curvefs::mds::heartbeat::CandidateError;

namespace curvefs {
namespace mds {
namespace schedule {
::curvefs::mds::schedule::CopySetInfo GetCopySetInfoForTest();
void GetCopySetInMetaServersForTest(
    std::map<MetaServerIdType, std::vector<CopySetInfo>> *out);
::curvefs::mds::topology::CopySetInfo GetTopoCopySetInfoForTest();
std::vector<::curvefs::mds::topology::MetaServer> GetTopoMetaServerForTest();
std::vector<::curvefs::mds::topology::Server> GetServerForTest();
::curvefs::mds::topology::Pool GetPoolForTest();
}  // namespace schedule
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_TEST_MDS_SCHEDULE_COMMON_H_
