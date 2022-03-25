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
 * Created Date: 2021-08-24
 * Author: wanghai01
 */

#include "curvefs/src/mds/topology/topology_item.h"

#include <string>
#include <vector>

#include "json/json.h"
#include "src/common/string_util.h"

namespace curvefs {
namespace mds {
namespace topology {

bool ClusterInformation::SerializeToString(std::string *value) const {
    ClusterInfoData data;
    data.set_clusterid(clusterId);
    return data.SerializeToString(value);
}

bool ClusterInformation::ParseFromString(const std::string &value) {
    ClusterInfoData data;
    bool ret = data.ParseFromString(value);
    clusterId = data.clusterid();
    return ret;
}

bool Pool::TransRedundanceAndPlaceMentPolicyFromJsonStr(
    const std::string &jsonStr, RedundanceAndPlaceMentPolicy *rap) {
    Json::Reader reader;
    Json::Value rapJson;
    if (!reader.parse(jsonStr, rapJson)) {
        return false;
    }
    if (!rapJson["replicaNum"].isNull()) {
        rap->replicaNum = rapJson["replicaNum"].asInt();
    } else {
        return false;
    }
    if (!rapJson["copysetNum"].isNull()) {
        rap->copysetNum = rapJson["copysetNum"].asInt();
    } else {
        return false;
    }
    if (!rapJson["zoneNum"].isNull()) {
        rap->zoneNum = rapJson["zoneNum"].asInt();
    } else {
        return false;
    }

    return true;
}

bool Pool::SetRedundanceAndPlaceMentPolicyByJson(const std::string &jsonStr) {
    return Pool::TransRedundanceAndPlaceMentPolicyFromJsonStr(jsonStr, &rap_);
}

std::string Pool::GetRedundanceAndPlaceMentPolicyJsonStr() const {
    std::string rapStr;
    Json::Value rapJson;
    rapJson["replicaNum"] = rap_.replicaNum;
    rapJson["copysetNum"] = rap_.copysetNum;
    rapJson["zoneNum"] = rap_.zoneNum;
    rapStr = rapJson.toStyledString();
    return rapStr;
}

bool Pool::SerializeToString(std::string *value) const {
    PoolData data;
    data.set_poolid(id_);
    data.set_poolname(name_);
    data.set_createtime(createTime_);
    data.set_redundanceandplacementpolicy(
        this->GetRedundanceAndPlaceMentPolicyJsonStr());
    return data.SerializeToString(value);
}

bool Pool::ParseFromString(const std::string &value) {
    PoolData data;
    bool ret = data.ParseFromString(value);
    id_ = data.poolid();
    name_ = data.poolname();
    SetRedundanceAndPlaceMentPolicyByJson(data.redundanceandplacementpolicy());
    createTime_ = data.createtime();
    return ret;
}

bool Zone::SerializeToString(std::string *value) const {
    ZoneData data;
    data.set_zoneid(id_);
    data.set_zonename(name_);
    data.set_poolid(poolId_);
    return data.SerializeToString(value);
}

bool Zone::ParseFromString(const std::string &value) {
    ZoneData data;
    bool ret = data.ParseFromString(value);
    id_ = data.zoneid();
    name_ = data.zonename();
    poolId_ = data.poolid();
    return ret;
}

bool Server::SerializeToString(std::string *value) const {
    ServerData data;
    data.set_serverid(id_);
    data.set_hostname(hostName_);
    data.set_internalip(internalIp_);
    data.set_internalport(internalPort_);
    data.set_externalip(externalIp_);
    data.set_externalport(externalPort_);
    data.set_zoneid(zoneId_);
    data.set_poolid(poolId_);
    return data.SerializeToString(value);
}

bool Server::ParseFromString(const std::string &value) {
    ServerData data;
    bool ret = data.ParseFromString(value);
    id_ = data.serverid();
    hostName_ = data.hostname();
    internalIp_ = data.internalip();
    internalPort_ = data.internalport();
    externalIp_ = data.externalip();
    externalPort_ = data.externalport();
    zoneId_ = data.zoneid();
    poolId_ = data.poolid();
    return ret;
}

bool MetaServer::SerializeToString(std::string *value) const {
    MetaServerData data;
    data.set_metaserverid(id_);
    data.set_hostname(hostName_);
    data.set_token(token_);
    data.set_internalip(internalIp_);
    data.set_internalport(internalPort_);
    data.set_externalip(externalIp_);
    data.set_externalport(externalPort_);
    data.set_serverid(serverId_);
    data.mutable_spacestatus()->set_diskthresholdbyte(
        space_.GetDiskThreshold());
    data.mutable_spacestatus()->set_diskcopysetminrequirebyte(
        space_.GetDiskMinRequire());
    data.mutable_spacestatus()->set_diskusedbyte(space_.GetDiskUsed());
    data.mutable_spacestatus()->set_memorythresholdbyte(
        space_.GetMemoryThreshold());
    data.mutable_spacestatus()->set_memorycopysetminrequirebyte(
        space_.GetMemoryMinRequire());
    data.mutable_spacestatus()->set_memoryusedbyte(space_.GetMemoryUsed());
    return data.SerializeToString(value);
}

bool MetaServer::ParseFromString(const std::string &value) {
    MetaServerData data;
    bool ret = data.ParseFromString(value);
    id_ = data.metaserverid();
    hostName_ = data.hostname();
    token_ = data.token();
    serverId_ = data.serverid();
    internalIp_ = data.internalip();
    internalPort_ = data.internalport();
    externalIp_ = data.externalip();
    externalPort_ = data.externalport();
    onlineState_ = OnlineState::UNSTABLE;
    space_.SetSpaceStatus(data.spacestatus());
    return ret;
}

std::string CopySetInfo::GetCopySetMembersStr() const {
    Json::Value copysetMemJson;
    for (MetaServerIdType id : peers_) {
        copysetMemJson.append(id);
    }
    std::string metaServerListStr = copysetMemJson.toStyledString();
    return metaServerListStr;
}

bool CopySetInfo::SetCopySetMembersByJson(const std::string &jsonStr) {
    Json::Reader reader;
    Json::Value copysetMemJson;
    if (!reader.parse(jsonStr, copysetMemJson)) {
        return false;
    }
    peers_.clear();
    for (int i = 0; i < copysetMemJson.size(); i++) {
        if (copysetMemJson[i].isInt()) {
            peers_.insert(copysetMemJson[i].asInt());
        } else {
            return false;
        }
    }
    return true;
}

bool CopySetInfo::SerializeToString(std::string *value) const {
    CopysetData data;
    data.set_copysetid(copySetId_);
    data.set_poolid(poolId_);
    data.set_epoch(epoch_);
    data.set_partitionnumber(partitionNum_);
    for (MetaServerIdType msId : peers_) {
        data.add_metaserverids(msId);
    }
    data.set_availflag(available_);
    return data.SerializeToString(value);
}

bool CopySetInfo::ParseFromString(const std::string &value) {
    CopysetData data;
    bool ret = data.ParseFromString(value);
    poolId_ = data.poolid();
    copySetId_ = data.copysetid();
    epoch_ = data.epoch();
    partitionNum_ = data.partitionnumber();
    if (data.has_availflag()) {
        available_ = data.availflag();
    } else {
        available_ = true;
    }
    peers_.clear();
    for (int i = 0; i < data.metaserverids_size(); i++) {
        peers_.insert(data.metaserverids(i));
    }
    return ret;
}

bool Partition::SerializeToString(std::string *value) const {
    curvefs::common::PartitionInfo data;
    data.set_fsid(fsId_);
    data.set_poolid(poolId_);
    data.set_copysetid(copySetId_);
    data.set_partitionid(partitionId_);
    data.set_start(idStart_);
    data.set_end(idEnd_);
    data.set_txid(txId_);
    data.set_status(status_);
    // no need serialize inodenum and dentrynum
    return data.SerializeToString(value);
}

bool Partition::ParseFromString(const std::string &value) {
    curvefs::common::PartitionInfo data;
    bool ret = data.ParseFromString(value);
    fsId_ = data.fsid();
    poolId_ = data.poolid();
    copySetId_ = data.copysetid();
    partitionId_ = data.partitionid();
    idStart_ = data.start();
    idEnd_ = data.end();
    txId_ = data.txid();
    status_ = data.status();
    // no need parse inodenum and dentrynum
    return ret;
}

common::PartitionInfo Partition::ToPartitionInfo() {
    common::PartitionInfo info;
    info.set_fsid(fsId_);
    info.set_poolid(poolId_);
    info.set_copysetid(copySetId_);
    info.set_partitionid(partitionId_);
    info.set_start(idStart_);
    info.set_end(idEnd_);
    info.set_txid(txId_);
    info.set_status(status_);
    if (inodeNum_ != UNINITIALIZE_COUNT) {
        info.set_inodenum(inodeNum_);
    }

    if (dentryNum_ != UNINITIALIZE_COUNT) {
        info.set_dentrynum(dentryNum_);
    }
    return info;
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
