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
#include "curvefs/src/tools/curvefs_tool_define.h"

#include "curvefs/src/tools/list/curvefs_topology_list.h"

DEFINE_string(mdsAddr, "127.0.0.1:16700,127.0.0.1:26700", "mds addr");
DEFINE_string(mdsDummyAddr, "127.0.0.1:17700,127.0.0.1:27700",
              "mds dummy addr");
DEFINE_string(metaserverAddr, "127.0.0.1:16701,127.0.0.1:26701",
              "metaserver addr");
DEFINE_string(etcdAddr, "127.0.0.1:2379", "etcd addr");
DEFINE_bool(example, false, "print the example of usage");
DEFINE_string(confPath, "/etc/curvefs/tools.conf", "config file path of tools");
DEFINE_string(fsName, "curvefs", "fs name");
DEFINE_string(fsId, "1,2,3", "fs id");
DEFINE_string(mountpoint, "127.0.0.1:9000:/mnt/curvefs-umount-test",
              "curvefs mount in local path");
DEFINE_uint32(rpcRetryTimes, 0, "rpc retry times");
DEFINE_uint32(rpcTimeoutMs, 10000u, "rpc time out");
DEFINE_string(copysetId, "1,2,3", "copysets id");
DEFINE_string(poolId, "1,2,3", "pools id");
DEFINE_bool(detail, false, "show more infomation");
DEFINE_string(partitionId, "1,2,3", "partition id");
DEFINE_string(metaserverId, "1,2,3", "metaserver id");
DEFINE_bool(noconfirm, false, "execute command without confirmation");
DEFINE_uint64(blockSize, 1048576, "block size");
DEFINE_string(fsType, "s3", "fs type: s3 or volume");

// volume fs
DEFINE_uint64(volumeBlockSize, 4096, "volume block size");
DEFINE_string(volumeName, "volume", "volume name");
DEFINE_string(volumeUser, "user", "volume user");
DEFINE_string(volumePassword, "user", "volume user");
DEFINE_uint64(volumeBlockGroupSize,
              128ULL * 1024 * 1024,
              "volume block group size");

DEFINE_string(volumeBitmapLocation,
              "AtStart",
              "volume space bitmap location, support |AtStart| and |AtEnd|");

static constexpr uint64_t kGiB = 1ULL * 1024 * 1024 * 1024;
DEFINE_uint64(volumeSliceSize, 1 * kGiB, "volume extents slice size");

DEFINE_bool(volumeAutoExtend, false, "auto extend volume when space used up");
DEFINE_double(volumeExtendFactor, 1.5, "auto extend factor");
DEFINE_string(volumeCluster,
              "127.0.0.1:6666,127.0.0.1:6667,127.0.0.1:6668",
              "curvebs cluster");

DEFINE_string(s3_ak, "ak", "s3 ak");
DEFINE_string(s3_sk, "sk", "s3 sk");
DEFINE_string(s3_endpoint, "endpoint", "s3 endpoint");
DEFINE_string(s3_bucket_name, "bucketname", "s3 bucket name");
DEFINE_uint64(s3_blocksize, 1048576, "s3 block size");
DEFINE_uint64(s3_chunksize, 4194304, "s3 chunk size");
DEFINE_uint32(s3_objectPrefix, 0, "object prefix");
DEFINE_bool(enableSumInDir, false, "statistic info in xattr");
DEFINE_uint64(capacity, (uint64_t)0,
    "capacity of fs, unit is bytes, default 0 to disable quota");
DEFINE_string(user, "anonymous", "user of request");
DEFINE_string(inodeId, "1,2,3", "inodes id");

DEFINE_uint32(recycleTimeHour, 1, "recycle time hour");

// list-topology
DEFINE_string(jsonPath, "/tmp/topology.json", "output json path");
DEFINE_string(jsonType, "build", "output json type(build or tree)");

// topology
DEFINE_string(mds_addr, "127.0.0.1:6700",
              "mds ip and port, separated by \",\"");  // NOLINT
DEFINE_string(cluster_map, "topo_example.json", "cluster topology map.");

DEFINE_uint32(rpcStreamIdleTimeoutMs, 10000, "rpc stream idle timeout");
DEFINE_uint32(rpcRetryIntervalUs, 1000, "rpc retry interval(us)");

namespace curvefs {

namespace tools {

template <class FlagInfoT>
void SetFlagInfo(curve::common::Configuration* conf,
                 google::CommandLineFlagInfo* info, const std::string& key,
                 FlagInfoT* flag) {
    if (GetCommandLineFlagInfo(key.c_str(), info) && info->is_default) {
        conf->GetValueFatalIfFail(key, flag);
    }
}

// Used for flagname and the name in the configuration file is inconsistent
template <class FlagInfoT>
void SetDiffFlagInfo(curve::common::Configuration* conf,
                     google::CommandLineFlagInfo* info,
                     const std::string& flagName, const std::string& key,
                     FlagInfoT* flag) {
    if (GetCommandLineFlagInfo(flagName.c_str(), info) && info->is_default) {
        conf->GetValueFatalIfFail(key, flag);
    }
}

template <class FlagInfoT>
bool CheckFlagInfoDefault(google::CommandLineFlagInfo* info,
                          const std::string& key) {
    return (GetCommandLineFlagInfo(key.c_str(), info) && info->is_default);
}

/* set flag */
std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetMdsAddr = std::bind(&SetFlagInfo<fLS::clstring>, std::placeholders::_1,
                           std::placeholders::_2, "mdsAddr", &FLAGS_mdsAddr);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetRpcTimeoutMs =
        std::bind(&SetFlagInfo<uint32_t>, std::placeholders::_1,
                  std::placeholders::_2, "rpcTimeoutMs", &FLAGS_rpcTimeoutMs);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetRpcRetryTimes =
        std::bind(&SetFlagInfo<uint32_t>, std::placeholders::_1,
                  std::placeholders::_2, "rpcRetryTimes", &FLAGS_rpcRetryTimes);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetMetaserverAddr = std::bind(&SetFlagInfo<fLS::clstring>,
                                  std::placeholders::_1, std::placeholders::_2,
                                  "metaserverAddr", &FLAGS_metaserverAddr);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetEtcdAddr = std::bind(&SetFlagInfo<fLS::clstring>, std::placeholders::_1,
                            std::placeholders::_2, "etcdAddr", &FLAGS_etcdAddr);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetMdsDummyAddr =
        std::bind(&SetFlagInfo<fLS::clstring>, std::placeholders::_1,
                  std::placeholders::_2, "mdsDummyAddr", &FLAGS_mdsDummyAddr);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetBlockSize =
        std::bind(&SetFlagInfo<uint64_t>, std::placeholders::_1,
                  std::placeholders::_2, "blockSize", &FLAGS_blockSize);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetFsType = std::bind(&SetFlagInfo<fLS::clstring>, std::placeholders::_1,
                          std::placeholders::_2, "fsType", &FLAGS_fsType);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetVolumeBlockSize = std::bind(&SetFlagInfo<uint64_t>,
                                   std::placeholders::_1, std::placeholders::_2,
                                   "volumeBlockSize", &FLAGS_volumeBlockSize);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetVolumeName =
        std::bind(&SetFlagInfo<fLS::clstring>, std::placeholders::_1,
                  std::placeholders::_2, "volumeName", &FLAGS_volumeName);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetVolumeUser =
        std::bind(&SetFlagInfo<fLS::clstring>, std::placeholders::_1,
                  std::placeholders::_2, "volumeUser", &FLAGS_volumeUser);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetVolumePassword = std::bind(&SetFlagInfo<fLS::clstring>,
                                  std::placeholders::_1, std::placeholders::_2,
                                  "volumePassword", &FLAGS_volumePassword);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetVolumeBlockGroupSize = std::bind(&SetFlagInfo<uint64_t>,
                                        std::placeholders::_1,
                                        std::placeholders::_2,
                                        "volumeBlockGroupSize",
                                        &FLAGS_volumeBlockGroupSize);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetVolumeBitmapLocation = std::bind(&SetFlagInfo<fLS::clstring>,
                                        std::placeholders::_1,
                                        std::placeholders::_2,
                                        "volumeBitmapLocation",
                                        &FLAGS_volumeBitmapLocation);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetVolumeAutoExtend = std::bind(&SetFlagInfo<bool>,
                                        std::placeholders::_1,
                                        std::placeholders::_2,
                                        "autoExtend",
                                        &FLAGS_volumeAutoExtend);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetVolumeExtendFactor = std::bind(&SetFlagInfo<double>,
                                        std::placeholders::_1,
                                        std::placeholders::_2,
                                        "extendFactor",
                                        &FLAGS_volumeExtendFactor);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetVolumeCluster = std::bind(&SetFlagInfo<fLS::clstring>,
                                 std::placeholders::_1,
                                 std::placeholders::_2,
                                 "volumeCluster",
                                 &FLAGS_volumeCluster);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetS3_ak = std::bind(&SetDiffFlagInfo<fLS::clstring>, std::placeholders::_1,
                         std::placeholders::_2, "s3_ak", "s3.ak", &FLAGS_s3_ak);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetS3_sk = std::bind(&SetDiffFlagInfo<fLS::clstring>, std::placeholders::_1,
                         std::placeholders::_2, "s3_sk", "s3.sk", &FLAGS_s3_sk);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetS3_endpoint =
        std::bind(&SetDiffFlagInfo<fLS::clstring>, std::placeholders::_1,
                  std::placeholders::_2, "s3_endpoint", "s3.endpoint",
                  &FLAGS_s3_endpoint);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetS3_bucket_name =
        std::bind(&SetDiffFlagInfo<fLS::clstring>, std::placeholders::_1,
                  std::placeholders::_2, "s3_bucket_name", "s3.bucket_name",
                  &FLAGS_s3_bucket_name);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetS3_blocksize =
        std::bind(&SetDiffFlagInfo<uint64_t>, std::placeholders::_1,
                  std::placeholders::_2, "s3_blocksize", "s3.blocksize",
                  &FLAGS_s3_blocksize);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetS3_chunksize =
        std::bind(&SetDiffFlagInfo<uint64_t>, std::placeholders::_1,
                  std::placeholders::_2, "s3_chunksize", "s3.chunksize",
                  &FLAGS_s3_chunksize);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetS3_objectPrefix =
        std::bind(&SetDiffFlagInfo<uint32_t>, std::placeholders::_1,
                  std::placeholders::_2, "s3_objectPrefix", "s3.objectPrefix",
                  &FLAGS_s3_objectPrefix);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetEnableSumInDir = std::bind(&SetFlagInfo<bool>, std::placeholders::_1,
                                  std::placeholders::_2, "enableSumInDir",
                                  &FLAGS_enableSumInDir);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetRpcStreamIdleTimeoutMs = std::bind(
        &SetFlagInfo<uint32_t>, std::placeholders::_1, std::placeholders::_2,
        "rpcStreamIdleTimeoutMs", &FLAGS_rpcStreamIdleTimeoutMs);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetRpcRetryIntervalUs = std::bind(
        &SetFlagInfo<uint32_t>, std::placeholders::_1, std::placeholders::_2,
        "rpcRetryIntervalUs", &FLAGS_rpcRetryIntervalUs);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetRecycleTimeHour = std::bind(
        &SetFlagInfo<uint32_t>, std::placeholders::_1, std::placeholders::_2,
        "recycleTimeHour", &FLAGS_recycleTimeHour);

/* check flag */
std::function<bool(google::CommandLineFlagInfo*)> CheckMetaserverIdDefault =
    std::bind(&CheckFlagInfoDefault<fLS::clstring>, std::placeholders::_1,
              "metaserverId");

std::function<bool(google::CommandLineFlagInfo*)> CheckMetaserverAddrDefault =
    std::bind(&CheckFlagInfoDefault<fLS::clstring>, std::placeholders::_1,
              "metaserverAddr");

std::function<bool(google::CommandLineFlagInfo*)> CheckFsNameDefault =
    std::bind(&CheckFlagInfoDefault<fLS::clstring>, std::placeholders::_1,
              "fsName");

std::function<bool(google::CommandLineFlagInfo*)> CheckFsIdDefault = std::bind(
    &CheckFlagInfoDefault<fLS::clstring>, std::placeholders::_1, "fsId");

std::function<bool(google::CommandLineFlagInfo*)> CheckPoolIdDefault =
    std::bind(&CheckFlagInfoDefault<fLS::clstring>, std::placeholders::_1,
              "poolId");

std::function<bool(google::CommandLineFlagInfo*)> CheckCopysetIdDefault =
    std::bind(&CheckFlagInfoDefault<fLS::clstring>, std::placeholders::_1,
              "copysetId");

std::function<bool(google::CommandLineFlagInfo*)> CheckPartitionIdDefault =
    std::bind(&CheckFlagInfoDefault<fLS::clstring>, std::placeholders::_1,
              "partitionId");

std::function<bool(google::CommandLineFlagInfo*)> CheckJsonPathDefault =
    std::bind(&CheckFlagInfoDefault<fLS::clstring>, std::placeholders::_1,
              "jsonPath");

std::function<bool(google::CommandLineFlagInfo*)> CheckInodeIdDefault =
    std::bind(&CheckFlagInfoDefault<fLS::clstring>, std::placeholders::_1,
              "inodeId");

/* translate to string */

auto StrVec2Str(const std::vector<std::string>& strVec) -> std::string {
    std::stringstream ret;
    for (auto const& i : strVec) {
        ret << i << " ";
    }
    return ret.str();
};

std::string HeartbeatCopysetInfo2Str(
    const mds::heartbeat::CopySetInfo& copySetInfo) {
    std::stringstream ret;
    ret << "poolId: " << copySetInfo.poolid()
        << " copysetId: " << copySetInfo.copysetid() << " peers: [ ";
    for (auto const& i : copySetInfo.peers()) {
        ret << CommomPeer2Str(i) << " ";
    }
    ret << "] epoch: " << copySetInfo.epoch()
        << "leader peer: " << CommomPeer2Str(copySetInfo.leaderpeer())
        << " partitioninfo: [ ";
    for (auto const& i : copySetInfo.partitioninfolist()) {
        CommomPartitionInfo2Str(i);
    }
    ret << " ]";
    return ret.str();
}

std::string CommomPeer2Str(const common::Peer& peer) {
    std::stringstream ret;
    ret << "id: " << peer.id() << " address: " << peer.address();
    return ret.str();
}

std::string CommomPartitionInfo2Str(const common::PartitionInfo& partition) {
    std::stringstream ret;
    ret << "fsId: " << partition.fsid() << " poolId: " << partition.poolid()
        << " copysetId: " << partition.copysetid()
        << " partitionId: " << partition.partitionid()
        << " start: " << partition.start() << " end: " << partition.end()
        << " txId: " << partition.txid() << " nextId: " << partition.nextid()
        << " status: ";
    if (partition.status() == common::PartitionStatus::READWRITE) {
        ret << "rw ";
    } else if (partition.status() == common::PartitionStatus::READONLY) {
        ret << "r ";
    } else {
        ret << "unknown ";
    }
    ret << "inodeNum: " << partition.inodenum()
        << " dentryNum: " << partition.status();
    return ret.str();
}

std::string CopysetOpStatus2Str(
    const metaserver::copyset::COPYSET_OP_STATUS& op_status) {
    std::stringstream ret;
    switch (op_status) {
        case metaserver::copyset::COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS:
            ret << "success ";
            break;
        case metaserver::copyset::COPYSET_OP_STATUS::COPYSET_OP_STATUS_EXIST:
            ret << "exist ";
            break;
        case metaserver::copyset::COPYSET_OP_STATUS::
            COPYSET_OP_STATUS_COPYSET_NOTEXIST:
            ret << "no exist ";
            break;
        case metaserver::copyset::COPYSET_OP_STATUS::
            COPYSET_OP_STATUS_COPYSET_IS_HEALTHY:
            ret << "health ";
            break;
        case metaserver::copyset::COPYSET_OP_STATUS::
            COPYSET_OP_STATUS_PARSE_PEER_ERROR:
            ret << "parse peer error ";
            break;
        case metaserver::copyset::COPYSET_OP_STATUS::
            COPYSET_OP_STATUS_PEER_MISMATCH:
            ret << "parse mismatch ";
            break;
        case metaserver::copyset::COPYSET_OP_STATUS::
            COPYSET_OP_STATUS_FAILURE_UNKNOWN:
        default:
            ret << "failure unknown ";
            break;
    }
    return ret.str();
}

std::string PoolInfo2Str(const mds::topology::PoolInfo& poolInfo) {
    std::stringstream ret;
    ret << "poolId:" << poolInfo.poolid()
        << ", poolName:" << poolInfo.poolname()
        << ", createTime:" << poolInfo.createtime() << ", policy:{ "
        << list::PoolPolicy(poolInfo.redundanceandplacementpolicy()) << " }";
    return ret.str();
}

std::string ZoneInfo2Str(const mds::topology::ZoneInfo& zoneInfo) {
    std::stringstream ret;
    ret << "zoneId:" << zoneInfo.zoneid()
        << ", zoneName:" << zoneInfo.zonename()
        << ", poolId:" << zoneInfo.poolid() << " ";
    return ret.str();
}

std::string ServerInfo2Str(const mds::topology::ServerInfo& serverInfo) {
    std::stringstream ret;
    ret << "serverId:" << serverInfo.serverid()
        << ", hostname:" << serverInfo.hostname()
        << ", internalIp:" << serverInfo.internalip()
        << ", internalPort:" << serverInfo.internalport()
        << ", externalIp:" << serverInfo.externalip()
        << ", externalPort:" << serverInfo.externalport()
        << ", zoneId:" << serverInfo.zoneid()
        << ", poolId:" << serverInfo.poolid() << " ";
    return ret.str();
}

std::string MetaserverInfo2Str(
    const mds::topology::MetaServerInfo& metaserver) {
    std::stringstream ret;
    ret << "metaserverId:" << metaserver.metaserverid()
        << ", hostname:" << metaserver.hostname()
        << ", InternalIp:" << metaserver.internalip()
        << ", internalPort:" << metaserver.internalport()
        << ", externalIp:" << metaserver.externalip()
        << ", externalPort:" << metaserver.externalport() << ", onlineState:"
        << mds::topology::OnlineState_Name(metaserver.onlinestate())
        << ", serverId:" << metaserver.serverid();
    return ret.str();
}

}  // namespace tools
}  // namespace curvefs
