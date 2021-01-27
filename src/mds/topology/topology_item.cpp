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
 * Created Date: Thu Oct 25 2018
 * Author: xuchaojie
 */

#include "src/mds/topology/topology_item.h"

#include <string>
#include <vector>

#include "json/json.h"
#include "src/common/string_util.h"
#include "proto/topology.pb.h"

namespace curve {
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

bool LogicalPool::TransRedundanceAndPlaceMentPolicyFromJsonStr(
    const std::string &jsonStr,
    LogicalPoolType type,
    RedundanceAndPlaceMentPolicy *rap) {
    Json::Reader reader;
    Json::Value rapJson;
    if (!reader.parse(jsonStr, rapJson)) {
        return false;
    }

    switch (type) {
        case LogicalPoolType::PAGEFILE: {
            if (!rapJson["replicaNum"].isNull()) {
                rap->pageFileRAP.replicaNum = rapJson["replicaNum"].asInt();
            } else {
                return false;
            }
            if (!rapJson["copysetNum"].isNull()) {
                rap->pageFileRAP.copysetNum = rapJson["copysetNum"].asInt();
            } else {
                return false;
            }
            if (!rapJson["zoneNum"].isNull()) {
                rap->pageFileRAP.zoneNum = rapJson["zoneNum"].asInt();
            } else {
                return false;
            }
            break;
        }
        case LogicalPoolType::APPENDFILE: {
            // TODO(xuchaojie): it is not done.
            return false;
        }
        case LogicalPoolType::APPENDECFILE: {
            // TODO(xuchaojie): it is not done.
            return false;
        }
        default: {
            return false;
        }
    }
    return true;
}

bool LogicalPool::TransUserPolicyFromJsonStr(
    const std::string &jsonStr, LogicalPoolType type, UserPolicy *policy) {
    // TODO(xuchaojie): to finish it.
    return true;
}

bool LogicalPool::SetRedundanceAndPlaceMentPolicyByJson(
    const std::string &jsonStr) {
    return LogicalPool::TransRedundanceAndPlaceMentPolicyFromJsonStr(
               jsonStr,
               GetLogicalPoolType(),
               &rap_);
}

std::string LogicalPool::GetRedundanceAndPlaceMentPolicyJsonStr() const {
    std::string rapStr;
    Json::Value rapJson;
    switch (GetLogicalPoolType()) {
        case LogicalPoolType::PAGEFILE : {
            rapJson["replicaNum"] = rap_.pageFileRAP.replicaNum;
            rapJson["copysetNum"] = rap_.pageFileRAP.copysetNum;
            rapJson["zoneNum"] = rap_.pageFileRAP.zoneNum;
            rapStr = rapJson.toStyledString();
            break;
        }
        case LogicalPoolType::APPENDFILE : {
            // TODO(xuchaojie): fix it
            break;
        }
        case LogicalPoolType::APPENDECFILE : {
            // TODO(xuchaojie): fix it
            break;
        }
        default:
            break;
    }
    return rapStr;
}


bool LogicalPool::SetUserPolicyByJson(const std::string &jsonStr) {
    return LogicalPool::TransUserPolicyFromJsonStr(
               jsonStr,
               GetLogicalPoolType(),
               &policy_);
}

std::string LogicalPool::GetUserPolicyJsonStr() const {
    // TODO(xuchaojie): to fix it
    return "{\"policy\" : 1}";
}

uint16_t LogicalPool::GetReplicaNum() const {
    uint16_t ret = 0;
    switch (GetLogicalPoolType()) {
        case LogicalPoolType::PAGEFILE : {
            ret = rap_.pageFileRAP.replicaNum;
            break;
        }
        case LogicalPoolType::APPENDFILE : {
            // TODO(xuchaojie): fix it
            break;
        }
        case LogicalPoolType::APPENDECFILE : {
            // TODO(xuchaojie): fix it
            break;
        }
        default:
            break;
    }
    return ret;
}

bool LogicalPool::SerializeToString(std::string *value) const {
    LogicalPoolData data;
    data.set_logicalpoolid(id_);
    data.set_logicalpoolname(name_);
    data.set_physicalpoolid(physicalPoolId_);
    data.set_type(type_);
    data.set_initialscatterwidth(initialScatterWidth_);
    data.set_createtime(createTime_);
    data.set_status(status_);
    data.set_redundanceandplacementpolicy(
        this->GetRedundanceAndPlaceMentPolicyJsonStr());
    data.set_userpolicy(this->GetUserPolicyJsonStr());
    data.set_availflag(avaliable_);
    return data.SerializeToString(value);
}

bool LogicalPool::ParseFromString(const std::string &value) {
    LogicalPoolData data;
    bool ret = data.ParseFromString(value);
    id_ = data.logicalpoolid();
    name_ = data.logicalpoolname();
    physicalPoolId_ = data.physicalpoolid();
    type_ = data.type();
    SetRedundanceAndPlaceMentPolicyByJson(
        data.redundanceandplacementpolicy());
    SetUserPolicyByJson(
        data.userpolicy());
    initialScatterWidth_ = data.initialscatterwidth();
    createTime_ = data.createtime();
    status_ = data.status();
    avaliable_ = data.availflag();
    return ret;
}

bool PhysicalPool::SerializeToString(std::string *value) const {
    PhysicalPoolData data;
    data.set_physicalpoolid(id_);
    data.set_physicalpoolname(name_);
    data.set_desc(desc_);
    return data.SerializeToString(value);
}

bool PhysicalPool::ParseFromString(const std::string &value) {
    PhysicalPoolData data;
    bool ret = data.ParseFromString(value);
    id_ = data.physicalpoolid();
    name_ = data.physicalpoolname();
    desc_ = data.desc();
    return ret;
}

bool Zone::SerializeToString(std::string *value) const {
    ZoneData data;
    data.set_zoneid(id_);
    data.set_zonename(name_);
    data.set_physicalpoolid(physicalPoolId_);
    data.set_desc(desc_);
    return data.SerializeToString(value);
}

bool Zone::ParseFromString(const std::string &value) {
    ZoneData data;
    bool ret = data.ParseFromString(value);
    id_ = data.zoneid();
    name_ = data.zonename();
    physicalPoolId_ = data.physicalpoolid();
    desc_ = data.desc();
    return ret;
}

bool Server::SerializeToString(std::string *value) const {
    ServerData data;
    data.set_serverid(id_);
    data.set_hostname(hostName_);
    data.set_internalhostip(internalHostIp_);
    data.set_internalport(internalPort_);
    data.set_externalhostip(externalHostIp_);
    data.set_externalport(externalPort_);
    data.set_zoneid(zoneId_);
    data.set_physicalpoolid(physicalPoolId_);
    data.set_desc(desc_);
    return data.SerializeToString(value);
}

bool Server::ParseFromString(const std::string &value) {
    ServerData data;
    bool ret = data.ParseFromString(value);
    id_ = data.serverid();
    hostName_ = data.hostname();
    internalHostIp_ = data.internalhostip();
    internalPort_ = data.internalport();
    externalHostIp_ = data.externalhostip();
    externalPort_ = data.externalport();
    zoneId_ = data.zoneid();
    physicalPoolId_ = data.physicalpoolid();
    desc_ = data.desc();
    return ret;
}

bool ChunkServer::SerializeToString(std::string *value) const {
    ChunkServerData data;
    data.set_chunkserverid(id_);
    data.set_token(token_);
    data.set_disktype(diskType_);
    data.set_internalhostip(internalHostIp_);
    data.set_port(port_);
    data.set_serverid(serverId_);
    data.set_rwstatus(status_);
    data.set_diskstate(state_.GetDiskState());
    data.set_onlinestate(onlineState_);
    data.set_mountpoint(mountPoint_);
    data.set_diskcapacity(state_.GetDiskCapacity());
    data.set_diskused(state_.GetDiskUsed());
    return data.SerializeToString(value);
}

bool ChunkServer::ParseFromString(const std::string &value) {
    ChunkServerData data;
    bool ret = data.ParseFromString(value);
    id_ = data.chunkserverid();
    token_ = data.token();
    diskType_ = data.disktype();
    serverId_ = data.serverid();
    internalHostIp_ = data.internalhostip();
    port_ = data.port();
    mountPoint_ = data.mountpoint();
    status_ = data.rwstatus();
    onlineState_ = data.onlinestate();
    state_.SetDiskState(data.diskstate());
    state_.SetDiskCapacity(data.diskcapacity());
    state_.SetDiskUsed(data.diskused());
    return ret;
}

std::string CopySetInfo::GetCopySetMembersStr() const {
    Json::Value copysetMemJson;
    for (ChunkServerIdType id : peers_) {
        copysetMemJson.append(id);
    }
    std::string chunkServerListStr = copysetMemJson.toStyledString();
    return chunkServerListStr;
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
    data.set_logicalpoolid(logicalPoolId_);
    data.set_epoch(epoch_);
    for (ChunkServerIdType csId : peers_) {
        data.add_chunkserverids(csId);
    }
    data.set_availflag(available_);
    return data.SerializeToString(value);
}

bool CopySetInfo::ParseFromString(const std::string &value) {
    CopysetData data;
    bool ret = data.ParseFromString(value);
    logicalPoolId_ = data.logicalpoolid();
    copySetId_ = data.copysetid();
    epoch_ = data.epoch();
    if (data.has_availflag()) {
        available_ = data.availflag();
    } else {
        available_ = true;
    }
    peers_.clear();
    for (int i = 0; i < data.chunkserverids_size(); i++) {
        peers_.insert(data.chunkserverids(i));
    }
    return ret;
}

bool SplitPeerId(
    const std::string &peerId,
    std::string *ip,
    uint32_t *port,
    uint32_t *idx) {
    std::vector<std::string> items;
    curve::common::SplitString(peerId, ":", &items);
    if (3 == items.size()) {
        *ip = items[0];
        *port = std::stoul(items[1]);
        if (idx != nullptr) {
            *idx = std::stoul(items[2]);
        }
        return true;
    }
    return false;
}

}  // namespace topology
}  // namespace mds
}  // namespace curve
