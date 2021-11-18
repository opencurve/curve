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

DEFINE_string(mdsAddr, "127.0.0.1:16700,127.0.0.1:26700", "mds addr");
DEFINE_string(metaserverAddr, "127.0.0.1:16701,127.0.0.1:26701",
              "metaserver addr");
DEFINE_string(etcdAddr, "127.0.0.1:2379", "etcd addr");
DEFINE_bool(example, false, "print the example of usage");
DEFINE_string(confPath, "/etc/curvefs/tools.conf", "config file path of tools");
DEFINE_string(fsName, "curvefs", "fs name");
DEFINE_string(fsId, "1,2,3", "fs id");
DEFINE_string(mountpoint, "127.0.0.1:/mnt/curvefs-umount-test",
              "curvefs mount in local path");
DEFINE_uint64(rpcRetryTimes, 5, "rpc retry times");
DEFINE_uint32(rpcTimeoutMs, 5000u, "rpc time out");
DEFINE_string(copysetId, "1,2,3", "copysets id");
DEFINE_string(poolId, "1,2,3", "pools id");
DEFINE_bool(detail, false, "show more infomation");

// topology
DEFINE_string(mds_addr, "127.0.0.1:6700",
              "mds ip and port, separated by \",\"");  // NOLINT
DEFINE_string(cluster_map, "topo_example.json", "cluster topology map.");

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

template <class FlagInfoT>
bool CheckFlagInfoDefault(google::CommandLineFlagInfo* info,
                          const std::string& key, FlagInfoT* flag) {
    return (GetCommandLineFlagInfo(key.c_str(), info) && info->is_default);
}

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetMdsAddr = std::bind(&SetFlagInfo<fLS::clstring>, std::placeholders::_1,
                           std::placeholders::_2, "mdsAddr", &FLAGS_mdsAddr);

std::function<bool(google::CommandLineFlagInfo*)> CheckFsNameDefault =
    std::bind(&CheckFlagInfoDefault<fLS::clstring>, std::placeholders::_1,
              "fsName", &FLAGS_fsName);

std::function<bool(google::CommandLineFlagInfo*)> CheckFsIdDefault =
    std::bind(&CheckFlagInfoDefault<fLS::clstring>, std::placeholders::_1,
              "fsId", &FLAGS_fsId);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetRpcTimeoutMs =
        std::bind(&SetFlagInfo<uint32_t>, std::placeholders::_1,
                  std::placeholders::_2, "rpcTimeoutMs", &FLAGS_rpcTimeoutMs);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetRpcRetryTimes =
        std::bind(&SetFlagInfo<uint64_t>, std::placeholders::_1,
                  std::placeholders::_2, "rpcRetryTimes", &FLAGS_rpcRetryTimes);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetMetaserverAddr = std::bind(&SetFlagInfo<fLS::clstring>,
                                  std::placeholders::_1, std::placeholders::_2,
                                  "metaserverAddr", &FLAGS_metaserverAddr);

std::function<void(curve::common::Configuration*, google::CommandLineFlagInfo*)>
    SetEtcdAddr = std::bind(&SetFlagInfo<fLS::clstring>, std::placeholders::_1,
                            std::placeholders::_2, "etcdAddr", &FLAGS_etcdAddr);

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

std::string MetadataserverCopysetCopysetStatusResponse2Str(
    const metaserver::copyset::CopysetStatusResponse& response) {
    std::stringstream ret;
    ret << "op_status: " << CopysetOpStatus2Str(response.status());
    if (response.has_copysetstatus()) {
        ret << response.copysetstatus().DebugString();
    }
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

}  // namespace tools
}  // namespace curvefs
