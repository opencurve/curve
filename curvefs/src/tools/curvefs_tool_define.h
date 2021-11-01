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
const char kProgrameName[] = "curvefs-tool";
// version
const char kVersionCmd[] = "version";
// build-topology
const char kBuildTopologyCmd[] = "build-topology";
// umount
const char kUmountCmd[] = "umountfs";
// metadata-usage
const char kMetedataUsageCmd[] = "metadata-usage";
// mds-status
const char kMdsStatusCmd[] = "mds-status";
// metaserver-status
const char kMetaserverStatusCmd[] = "metaserver-status";
// etcd-status
const char kEtcdStatusCmd[] = "etcd-status";
// copysets-status
// check the cluster of copysets status
const char kCopysetsStatusCmd[] = "copysets-status";
// copyset-query
const char kCopysetQueryCmd[] = "copyset-query";
// partition-list
const char kPartitionListCmd[] = "partition-list";
// fsinfo-list
const char kFsInfoListCmd[] = "fsinfo-list";

// no-invoke Used for commands that are not directly invoked
const char kNoInvokeCmd[] = "no-invoke";

// query-metadata
const char kMetedataUsageCmd[] = "metadata-usage";

// configure
const char kConfPathHelp[] = "[-confPath=/etc/curvefs/tools.conf]";
// kHelp
const char kHelpStr[] =
    "Usage: curvefs_tool [Command] [OPTIONS...]\n"
    "COMMANDS:\n"  // NOLINT
    "version: show the version of cluster\n"
    "build-topology: build cluster topology based on topo.json\n"
    "umountfs: umount curvefs from local and cluster\n"
    "metadata-usage: show the metadata usage of cluster\n"
    "mds-status: show the status of mds\n"
    "metaserver-status: show the status of metaserver\n"
    "mds-status: show the status of etcd\n"
    "copyset-query: query copyset by copysetId\n"
    "partition-list: list partition in fsId"
    "You can specify the config path by -confPath to avoid typing too many "
    "options\n";  // NOLINT

/* Status Host Type */
constexpr char kHostTypeMds[] = "mds";
constexpr char kHostTypeMetaserver[] = "metaserver";
constexpr char kHostTypeEtcd[] = "etcd";

/* Metric */
const char kVersionUri[] = "/version";
const char kStatusUri[] = "/vars/status";
const char kMdsStatusUri[] = "/vars/curvefs_mds_status";
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
    SetRpcTimeout;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetRpcRetryTimes;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetMetaserverAddr;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetEtcdAddr;

/* translate to string */
extern std::function<std::string(const std::vector<std::string>&)> StrVec2Str;

extern std::function<std::string(const mds::heartbeat::CopySetInfo&)>
    HeartbeatCopysetInfo2Str;

extern std::function<std::string(const common::Peer&)> CommomPeer2Str;

extern std::function<std::string(const common::PartitionInfo&)>
    CommomPartitionInfo2Str;

extern std::function<std::string(
    const metaserver::copyset::CopysetStatusResponse&)>
    MetadataserverCopysetCopysetStatusResponse2Str;

extern std::function<std::string(const metaserver::copyset::COPYSET_OP_STATUS&)>
    CopysetOpStatus2Str;

}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_CURVEFS_TOOL_DEFINE_H_
