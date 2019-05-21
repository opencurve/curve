/*
 * Project: curve
 * Created Date: Mon Dec 24 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
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
