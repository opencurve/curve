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
#include "curvefs/proto/topology.pb.h"
#include "src/common/configuration.h"

namespace curvefs {
namespace tools {

/* command */
// programe name
const char kProgrameName[] = "curvefs_tool";
// version
const char kVersionCmd[] = "version";
// create-topology
const char kCreateTopologyCmd[] = "create-topology";
// create-fs
const char kCreateFsCmd[] = "create-fs";
// umount
const char kUmountFsCmd[] = "umount-fs";
// delete-fs
const char kDeleteFsCmd[] = "delete-fs";
// delete-partition
const char kPartitionDeleteCmd[] = "delete-partition";
// metadata-usage
const char kMetedataUsageCmd[] = "usage-metadata";
// status
const char kStatusCmd[] = "status";
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
// Inode-query
const char kInodeQueryCmd[] = "query-inode";
// fsinfo-list
const char kFsInfoListCmd[] = "list-fs";
// list-fs-copysetid
const char kCopysetInfoListCmd[] = "list-copysetInfo";
// list-topology
const char kTopologyListCmd[] = "list-topology";
// list-partition
const char kPartitionListCmd[] = "list-partition";
// no-invoke Used for commands that are not directly invoked
const char kNoInvokeCmd[] = "no-invoke";

// configure
const char kConfPathHelp[] = "[-confPath=/etc/curvefs/tools.conf]";
// kHelp
const char kHelpStr[] =
    "Usage: curvefs_tool [Command] [OPTIONS...]\n"
    "COMMANDS:\n"  // NOLINT
    "version:           show the version of curvefs_tool\n"
    "status:            show the status of cluster(include mds, metaserver, "
    "etcd and copyset)\n"
    "status-mds:        show the status of mds\n"
    "status-metaserver: show the status of metaserver\n"
    "status-etcd:       show the status of etcd\n"
    "status-copyset:    show the status of copyset\n"
    "list-fs:           list all fs in cluster\n"
    "list-copysetInfo:  list all copysetInfo in cluster\n"
    "list-topology:     list cluster's topology\n"
    "list-partition:    list partition by fsId\n"
    "create-topology:   create cluster topology based on topo.json\n"
    "create-fs:         create fs\n"
    "umount-fs:         umount curvefs from local and cluster\n"
    "usage-metadata:    show the metadata usage of cluster\n"
    "delete-fs:         delete fs by fsName\n"
    "delete-partition:  delete partition by partitionId\n"
    "check-copyset:     checkout copyset status\n"
    "query-copyset:     query copyset by copysetId and poolId\n"
    "query-partition:   query copyset in partition by partitionId\n"
    "query-metaserver:  query metaserver by metaserverId or metaserverName\n"
    "query-fs:          query fs by fsId or fsName\n"
    "query-inode:       query inode\n"
    "You can specify the config path by -confPath to avoid typing too many "
    "options\n";  // NOLINT

/* Status Host Type */
const char kHostTypeMds[] = "mds";
const char kHostTypeMetaserver[] = "metaserver";
const char kHostTypeEtcd[] = "etcd";

/* Metric */
const char kVersionUri[] = "/vars/curve_version";
const char kStatusUri[] = "/vars/status";
const char kMdsStatusUri[] = "/vars/curvefs_mds_status";
const char kMetaserverStatusUri[] = "/vars/pid";
const char kEtcdVersionUri[] = "/version";
const char kEtcdStatusUri[] = "/v2/stats/self";
const char kEtcdClusterVersionKey[] = "etcdcluster";
const char kVersionKey[] = "curve_version";
const char kMdsStatusKey[] = "curvefs_mds_status";
const char kEtcdStateKey[] = "state";
const char kHostLeaderValue[] = "leader";
const char kHostFollowerValue[] = "follower";
const char kEtcdLeaderValue[] = "StateLeader";
const char kEtcdFollowerValue[] = "StateFollower";

/* fs type */
const char kFsTypeS3[] = "s3";
const char kFsTypeVolume[] = "volume";
const char kFsTypeHybrid[] = "hybrid";

/* json type */
const char kJsonTypeBuild[] = "build";
const char kJsonTypeTree[] = "tree";

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
const char kClusterId[] = "clusterid";
const char kPoolId[] = "poolid";
const char kPoolName[] = "poolname";
const char kCreateTime[] = "createtime";
const char kPolicy[] = "policy";
const char kPoollist[] = "poollist";
const char kZonelist[] = "zonelist";
const char kZoneId[] = "zoneid";
const char kZoneName[] = "zonename";
const char kServerlist[] = "serverlist";
const char kServerId[] = "serverid";
const char kHostName[] = "hostname";
const char kMetaserverList[] = "metaserverlist";
const char kMetaserverId[] = "metaserverid";
const char kHostIp[] = "hostip";
const char kPort[] = "port";
const char kOnlineState[] = "state";
const char kPartitionId[] = "partitionid";

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
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetMdsDummyAddr;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetBlockSize;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetFsType;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetVolumeBlockSize;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetVolumeName;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetVolumeUser;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetVolumePassword;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetVolumeBitmapLocation;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetVolumeAutoExtend;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetVolumeExtendFactor;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetVolumeCluster;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetS3_ak;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetS3_sk;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetS3_endpoint;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetS3_bucket_name;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetS3_blocksize;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetS3_chunksize;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetS3_objectPrefix;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetEnableSumInDir;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetRpcStreamIdleTimeoutMs;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetRpcRetryIntervalUs;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetRecycleTimeHour;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetFilterType;
extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetFilterList;

/* checkout the flag is default */
extern std::function<bool(google::CommandLineFlagInfo*)>
    CheckMetaserverIdDefault;

extern std::function<bool(google::CommandLineFlagInfo*)>
    CheckMetaserverAddrDefault;

extern std::function<bool(google::CommandLineFlagInfo*)> CheckFsNameDefault;

extern std::function<bool(google::CommandLineFlagInfo*)> CheckFsIdDefault;

extern std::function<bool(google::CommandLineFlagInfo*)> CheckPoolIdDefault;

extern std::function<bool(google::CommandLineFlagInfo*)> CheckCopysetIdDefault;

extern std::function<bool(google::CommandLineFlagInfo*)>
    CheckPartitionIdDefault;

extern std::function<bool(google::CommandLineFlagInfo*)> CheckJsonPathDefault;

extern std::function<bool(google::CommandLineFlagInfo*)> CheckInodeIdDefault;

/* translate to string */
std::string StrVec2Str(const std::vector<std::string>&);

std::string HeartbeatCopysetInfo2Str(const mds::heartbeat::CopySetInfo&);

std::string CommomPeer2Str(const common::Peer&);

std::string CommomPartitionInfo2Str(const common::PartitionInfo&);

std::string CopysetOpStatus2Str(const metaserver::copyset::COPYSET_OP_STATUS&);

std::string PoolInfo2Str(const mds::topology::PoolInfo&);

std::string ZoneInfo2Str(const mds::topology::ZoneInfo&);

std::string ServerInfo2Str(const mds::topology::ServerInfo&);

std::string MetaserverInfo2Str(const mds::topology::MetaServerInfo&);

}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_CURVEFS_TOOL_DEFINE_H_
