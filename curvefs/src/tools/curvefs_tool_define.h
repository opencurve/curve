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
 * Project: curve
 * Created Date: 2021-09-14
 * Author: chengyi01
 */

#ifndef CURVEFS_SRC_TOOLS_CURVEFS_TOOL_DEFINE_H_
#define CURVEFS_SRC_TOOLS_CURVEFS_TOOL_DEFINE_H_

#include <gflags/gflags.h>

#include <functional>
#include <sstream>
#include <string>
#include <vector>

#include "curvefs/proto/copyset.pb.h"
#include "curvefs/proto/heartbeat.pb.h"
#include "src/common/configuration.h"

namespace curvefs {
namespace tools {

/* command */
// programe name
const char kProgrameName[] = "curvefs_tool";
// version
const char kVersionCmd[] = "version";
// build-topology
const char kBuildTopologyCmd[] = "build-topology";
// umount
const char kUmountFsCmd[] = "umount-fs";
// delete
const char kDeleteFsCmd[] = "delete-fs";
// metadata-usage
const char kMetedataUsageCmd[] = "usage-metadata";
// mds-status
const char kMdsStatusCmd[] = "status-mds";
// metaserver-status
const char kMetaserverStatusCmd[] = "status-metaserver";
// etcd-status
const char kEtcdStatusCmd[] = "status-etcd";
// copyset-status
const char kCopysetStatusCmd[] = "status-copyset";
// copysets-status
// check the cluster of copyset status
const char kCopysetCheckCmd[] = "check-copyset";
// copyset-query
const char kCopysetQueryCmd[] = "query-copyset";
// partition-query
const char kPartitionQueryCmd[] = "query-partition";
// metaserver-query
const char kMetaserverQueryCmd[] = "query-metaserver";
// fs-query
const char kFsQueryCmd[] = "query-fs";
// fsinfo-list
const char kFsInfoListCmd[] = "list-fs";
// list-fs-copysetid
const char kCopysetInfoListCmd[] = "list-copysetInfo";
// no-invoke Used for commands that are not directly invoked
const char kNoInvokeCmd[] = "no-invoke";

// configure
const char kConfPathHelp[] = "[-confPath=/etc/curvefs/tools.conf]";
// kHelp
const char kHelpStr[] =
    "Usage: curvefs_tool [Command] [OPTIONS...]\n"
    "COMMANDS:\n"  // NOLINT
    "version:           show the version of curvefs_tool\n"
    "status-mds:        show the status of mds\n"
    "status-metaserver: show the status of metaserver\n"
    "status-etcd:       show the status of etcd\n"
    "status-copyset:    show the status of copyset\n"
    "list-fs:           list all fs in cluster\n"
    "list-copysetInfo:  list all copysetInfo in cluster\n"
    "build-topology:    build cluster topology based on topo.json\n"
    "umount-fs:         umount curvefs from local and cluster\n"
    "usage-metadata:    show the metadata usage of cluster\n"
    "delete-fs:         delete fs by fsName\n"
    "check-copyset:     checkout copyset status\n"
    "query-copyset:     query copyset by copysetId and poolId\n"
    "query-partition:   query copyset in partition by partitionId\n"
    "query-metaserver:  query metaserver by metaserverId or metaserverName\n"
    "query-fs:          query fs by fsId or fsName\n"
    "You can specify the config path by -confPath to avoid typing too many "
    "options\n";  // NOLINT

/* Status Host Type */
const char kHostTypeMds[] = "mds";
const char kHostTypeMetaserver[] = "metaserver";
const char kHostTypeEtcd[] = "etcd";

/* Metric */
const char kVersionUri[] = "/version";
const char kStatusUri[] = "/vars/status";
const char kMdsStatusUri[] = "/vars/curvefs_mds_status";
const char kMetaserverStatusUri[] = "/vars/pid";
const char kEtcdVersionUri[] = "/version";
const char kEtcdStatusUri[] = "/v2/stats/self";
const char kEtcdClusterVersionKey[] = "etcdcluster";
const char kMdsStatusKey[] = "curvefs_mds_status";
const char kEtcdStateKey[] = "state";
const char kHostLeaderValue[] = "leader";
const char kHostFollowerValue[] = "follower";
const char kEtcdLeaderValue[] = "StateLeader";
const char kEtcdFollowerValue[] = "StateFollower";

}  // namespace tools
}  // namespace curvefs

namespace curvefs {
namespace mds {
namespace topology {
/* topology */
const int kRetCodeCommonErr = -1;
const int kRetCodeRedirectMds = -2;
const char kPools[] = "pools";
const char kPool[] = "pool";
const char kServers[] = "servers";
const char kMetaServer[] = "metaservers";
const char kName[] = "name";
const char kInternalIp[] = "internalip";
const char kInternalPort[] = "internalport";
const char kExternalIp[] = "externalip";
const char kExternalPort[] = "externalport";
const char kZone[] = "zone";
const char kReplicasNum[] = "replicasnum";
const char kCopysetNum[] = "copysetnum";
const char kZoneNum[] = "zonenum";

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

namespace curvefs {
namespace tools {

template <class FlagInfoT>
void SetFlagInfo(curve::common::Configuration* conf,
                 google::CommandLineFlagInfo* info, const std::string& key,
                 FlagInfoT* flag);

/* update flags */
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetMdsAddr;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetRpcTimeoutMs;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetRpcRetryTimes;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetMetaserverAddr;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetEtcdAddr;

/* checkout the flag is default */
extern std::function<bool(google::CommandLineFlagInfo*)>
    CheckMetaserverIdDefault;

extern std::function<bool(google::CommandLineFlagInfo*)>
    CheckMetaserverAddrDefault;

extern std::function<bool(google::CommandLineFlagInfo*)> CheckFsNameDefault;

extern std::function<bool(google::CommandLineFlagInfo*)> CheckFsIdDefault;

/* translate to string */
std::string StrVec2Str(const std::vector<std::string>&);

std::string HeartbeatCopysetInfo2Str(const mds::heartbeat::CopySetInfo&);

std::string CommomPeer2Str(const common::Peer&);

std::string CommomPartitionInfo2Str(const common::PartitionInfo&);

std::string CopysetOpStatus2Str(const metaserver::copyset::COPYSET_OP_STATUS&);

}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_CURVEFS_TOOL_DEFINE_H_
