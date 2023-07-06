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
 * Created Date: Fri Oct 19 2018
 * Author: xuchaojie
 */

#include <utility>
#include "tools/curvefsTool.h"
#include "src/client/auth_client.h"
#include "src/client/config_info.h"
#include "src/common/authenticator.h"
#include "src/common/namespace_define.h"

using ::curve::common::kDefaultPoolsetName;

DEFINE_string(mds_addr, "127.0.0.1:6666",
    "mds ip and port list, separated by \",\"");

DEFINE_string(op,
    "",
    "operation: create_logicalpool, "
               "create_physicalpool, "
               "set_chunkserver, "
               "set_logicalpool");

DEFINE_string(cluster_map, "/etc/curve/topo.json", "cluster topology map.");

DEFINE_int32(chunkserver_id, -1, "chunkserver id for set chunkserver status.");
DEFINE_string(chunkserver_status, "readwrite",
    "chunkserver status: readwrite, pendding.");

DEFINE_uint32(rpcTimeOutMs, 5000u, "rpc time out");
DEFINE_string(confPath, "/etc/curve/tools.conf", "config file path of tools");

DEFINE_uint32(logicalpool_id, -1, "logicalpool id for set logicalpool status.");
DEFINE_string(logicalpool_status, "allow",
    "logicalpool status: allow, deny.");

const int kRetCodeCommonErr = -1;
const int kRetCodeRedirectMds = -2;
const char kServers[] = "servers";
const char kLogicalPools[] = "logicalpools";
const char kName[] = "name";
const char kInternalIp[] = "internalip";
const char kInternalPort[] = "internalport";
const char kExternalIp[] = "externalip";
const char kExternalPort[] = "externalport";
const char kZone[] = "zone";
const char kPhysicalPool[] = "physicalpool";
const char kType[] = "type";
const char kReplicasNum[] = "replicasnum";
const char kCopysetNum[] = "copysetnum";
const char kZoneNum[] = "zonenum";
const char kScatterWidth[] = "scatterwidth";
const char kAllocStatus[] = "allocstatus";
const char kAllocStatusAllow[] = "allow";
const char kAllocStatusDeny[] = "deny";
const char kPoolsets[] = "poolsets";
const char kPoolsetName[] = "poolset";


using ::curve::common::SplitString;

namespace curve {
namespace mds {
namespace topology {

const std::string CurvefsTools::clusterMapSeprator = " ";  // NOLINT

void UpdateFlagsFromConf(curve::common::Configuration* conf) {
    // 如果配置文件不存在的话不报错，以命令行为准,这是为了不强依赖配置
    // 如果配置文件存在并且没有指定命令行的话，就以配置文件为准
    if (conf->LoadConfig()) {
        google::CommandLineFlagInfo info;
        if (GetCommandLineFlagInfo("mds_addr", &info) && info.is_default) {
            conf->GetStringValue("mdsAddr", &FLAGS_mds_addr);
            LOG(INFO) << "conf: " << FLAGS_mds_addr;
        }
    }
}

int CurvefsTools::Init() {
    curve::common::Configuration conf;
    conf.SetConfigPath(FLAGS_confPath);
    UpdateFlagsFromConf(&conf);
    SplitString(FLAGS_mds_addr, ",", &mdsAddressStr_);
    if (mdsAddressStr_.empty()) {
        LOG(ERROR) << "no available mds address.";
        return kRetCodeCommonErr;
    }

    butil::EndPoint endpt;
    for (const auto& addr : mdsAddressStr_) {
        if (butil::str2endpoint(addr.c_str(), &endpt) < 0) {
            LOG(ERROR) << "Invalid sub mds ip:port provided: " << addr;
            return kRetCodeCommonErr;
        }
    }
    mdsAddressIndex_ = -1;

    // init auth client
    curve::client::MetaServerOption metaOpt;
    metaOpt.rpcRetryOpt.addrs = mdsAddressStr_;
    curve::common::AuthClientOption authOpt;
    // InitAuthClientOption
    authOpt.Load(&conf);
    authClient_.Init(metaOpt, authOpt);
    return 0;
}

int CurvefsTools::TryAnotherMdsAddress() {
    if (mdsAddressStr_.size() == 0) {
        LOG(ERROR) << "no available mds address.";
        return kRetCodeCommonErr;
    }
    mdsAddressIndex_ = (mdsAddressIndex_ + 1) % mdsAddressStr_.size();
    std::string mdsAddress = mdsAddressStr_[mdsAddressIndex_];
    LOG(INFO) << "try mds address(" << mdsAddressIndex_
              << "): " << mdsAddress;
    int ret = channel_.Init(mdsAddress.c_str(), NULL);
    if (ret != 0) {
        LOG(ERROR) << "Fail to init channel to mdsAddress: "
                   << mdsAddress;
    }
    return ret;
}

int CurvefsTools::DealFailedRet(int ret, std::string operation) {
    if (kRetCodeRedirectMds == ret) {
        LOG(WARNING) << operation << " fail on mds: "
                   << mdsAddressStr_[mdsAddressIndex_];
    } else {
        LOG(ERROR) << operation << " fail.";
    }
    return ret;
}

int CurvefsTools::HandleCreateLogicalPool() {
    int ret = ReadClusterMap();
    if (ret < 0) {
        return DealFailedRet(ret, "read cluster map");
    }
    ret = InitLogicalPoolData();
    if (ret < 0) {
        return DealFailedRet(ret, "init logical pool data");
    }
    ret = ScanLogicalPool();
    if (ret < 0) {
        return DealFailedRet(ret, "scan logical pool");
    }
    for (const auto& lgPool : lgPoolDatas) {
        TopologyService_Stub stub(&channel_);

        CreateLogicalPoolRequest request;
        request.set_logicalpoolname(lgPool.name);
        request.set_physicalpoolname(lgPool.physicalPoolName);
        request.set_type(lgPool.type);
        std::string replicaNumStr = std::to_string(lgPool.replicasNum);
        std::string copysetNumStr = std::to_string(lgPool.copysetNum);
        std::string zoneNumStr = std::to_string(lgPool.zoneNum);

        std::string rapString = "{\"replicaNum\":" + replicaNumStr
                             + ", \"copysetNum\":" + copysetNumStr
                             + ", \"zoneNum\":" + zoneNumStr
                             + "}";

        request.set_redundanceandplacementpolicy(rapString);
        request.set_userpolicy("{\"aaa\":1}");
        request.set_scatterwidth(lgPool.scatterwidth);
        request.set_status(lgPool.status);
        if (!authClient_.GetToken(curve::common::MDS_ROLE,
            request.mutable_authtoken())) {
            LOG(ERROR) << "HandleCreateLogicalPool: GetToken failed";
            return kRetCodeCommonErr;
        }

        CreateLogicalPoolResponse response;

        brpc::Controller cntl;
        cntl.set_max_retry(0);
        cntl.set_timeout_ms(-1);
        cntl.set_log_id(1);

        LOG(INFO) << "CreateLogicalPool, second request: "
                  << request.DebugString();

        stub.CreateLogicalPool(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            LOG(WARNING) << "send rpc get cntl Failed, error context:"
                       << cntl.ErrorText();
            return kRetCodeRedirectMds;
        }
        if (response.statuscode() == kTopoErrCodeSuccess) {
            LOG(INFO) << "Received CreateLogicalPool Rpc response success, "
                      << response.DebugString();
        } else if (response.statuscode() == kTopoErrCodeLogicalPoolExist) {
            LOG(INFO) << "Logical pool already exist";
        } else {
            LOG(ERROR) << "CreateLogicalPool Rpc response fail. "
                       << "Message is :"
                       << response.DebugString();
            return response.statuscode();
        }
    }
    return 0;
}

int CurvefsTools::ScanLogicalPool() {
    // get all logicalpool and compare
    // De-duplication
    std::set<std::string> phyPools;
    for (const auto& lgPool : lgPoolDatas) {
        phyPools.insert(lgPool.physicalPoolName);
    }
    for (const auto& phyPool : phyPools) {
        std::list<LogicalPoolInfo> logicalPoolInfos;
        int ret = ListLogicalPool(phyPool, &logicalPoolInfos);
        if (ret < 0) {
            return ret;
        }
        for (auto it = logicalPoolInfos.begin();
            it != logicalPoolInfos.end();) {
            auto ix = std::find_if(lgPoolDatas.begin(),
                lgPoolDatas.end(),
                [it] (CurveLogicalPoolData& data) {
                    return data.name == it->logicalpoolname();
                });
            if (ix != lgPoolDatas.end()) {
                lgPoolDatas.erase(ix);
                it++;
            }
        }
    }
    return 0;
}

int CurvefsTools::ListLogicalPool(const std::string& phyPoolName,
        std::list<LogicalPoolInfo> *logicalPoolInfos) {
    TopologyService_Stub stub(&channel_);
    ListLogicalPoolRequest request;
    ListLogicalPoolResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
    cntl.set_log_id(1);
    request.set_physicalpoolname(phyPoolName);
    if (!authClient_.GetToken(curve::common::MDS_ROLE,
        request.mutable_authtoken())) {
        LOG(ERROR) << "ListLogicalPool: GetToken failed";
        return kRetCodeCommonErr;
    }

    LOG(INFO) << "ListLogicalPool send request: "
              << request.DebugString();
    stub.ListLogicalPool(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return kRetCodeRedirectMds;
    }
    for (int i = 0; i < response.logicalpoolinfos_size(); i++) {
        logicalPoolInfos->push_back(
            response.logicalpoolinfos(i));
    }
    return 0;
}

int CurvefsTools::HandleBuildCluster() {
    int ret = ReadClusterMap();
    if (ret < 0) {
        return DealFailedRet(ret, "read cluster map");
    }
    ret = InitPoolsetData();
    if (ret < 0) {
        return DealFailedRet(ret, "init poolset data");
    }
    ret = InitServerData();
    if (ret < 0) {
        return DealFailedRet(ret, "init server data");
    }
    ret = ScanCluster();
    if (ret < 0) {
        return DealFailedRet(ret, "scan cluster");
    }
    ret = ClearServer();
    if (ret < 0) {
        return DealFailedRet(ret, "clear server");
    }
    ret = ClearZone();
    if (ret < 0) {
        return DealFailedRet(ret, "clear zone");
    }
    ret = ClearPhysicalPool();
    if (ret < 0) {
        return DealFailedRet(ret, "clear physicalpool");
    }
    ret = ClearPoolset();
    if (ret < 0) {
        return DealFailedRet(ret, "clear poolset");
    }
    ret = CreatePoolset();
    if (ret < 0) {
        return DealFailedRet(ret, "create Poolset");
    }
    ret = CreatePhysicalPool();
    if (ret < 0) {
        return DealFailedRet(ret, "create physicalpool");
    }
    ret = CreateZone();
    if (ret < 0) {
        return DealFailedRet(ret, "create zone");
    }
    ret = CreateServer();
    if (ret < 0) {
        return DealFailedRet(ret, "create server");
    }
    return ret;
}


int CurvefsTools::ReadClusterMap() {
    std::ifstream fin(FLAGS_cluster_map);
    if (fin.is_open()) {
        Json::CharReaderBuilder reader;
        JSONCPP_STRING errs;
        bool ok = Json::parseFromStream(reader, fin, &clusterMap_, &errs);
        fin.close();
        if (!ok) {
            LOG(ERROR) << "Parse cluster map file " << FLAGS_cluster_map
                       << " fail: " << errs;
            return -1;
        }
    } else {
        LOG(ERROR) << "open cluster map file : "
                   << FLAGS_cluster_map << " fail.";
        return -1;
    }
    return 0;
}
int CurvefsTools::InitPoolsetData() {
    if (clusterMap_[kPoolsets].isNull()) {
        return 0;
    }

    for (const auto& poolset : clusterMap_[kPoolsets]) {
        CurvePoolsetData poolsetData;
        if (!poolset[kName].isString()) {
            LOG(ERROR) <<"poolset name must be string" <<  poolset[kName];
            return -1;
        }
        poolsetData.name = poolset[kName].asString();

        if (!poolset[kType].isString()) {
            LOG(ERROR) << "poolset type must be string";
            return -1;
        }
        poolsetData.type = poolset[kType].asString();
        if (poolsetData.type.empty()) {
            LOG(ERROR) << "poolset type must not empty";
            return -1;
        }

        poolsetDatas.emplace_back(std::move(poolsetData));
    }
    return 0;
}

int CurvefsTools::InitServerData() {
    if (clusterMap_[kServers].isNull()) {
        LOG(ERROR) << "No servers in cluster map";
        return -1;
    }
    for (const auto &server : clusterMap_[kServers]) {
        CurveServerData serverData;
        if (!server[kName].isString()) {
            LOG(ERROR) << "server name must be string";
            return -1;
        }
        serverData.serverName = server[kName].asString();
        if (!server[kInternalIp].isString()) {
            LOG(ERROR) << "server internal ip must be string";
            return -1;
        }
        serverData.internalIp = server[kInternalIp].asString();
        if (!server[kInternalPort].isUInt()) {
            LOG(ERROR) << "server internal port must be uint";
            return -1;
        }
        serverData.internalPort = server[kInternalPort].asUInt();
        if (!server[kExternalIp].isString()) {
            LOG(ERROR) << "server internal port must be string";
            return -1;
        }
        serverData.externalIp = server[kExternalIp].asString();
        if (!server[kExternalPort].isUInt()) {
            LOG(ERROR) << "server internal port must be string";
            return -1;
        }
        serverData.externalPort = server[kExternalPort].asUInt();
        if (!server[kZone].isString()) {
            LOG(ERROR) << "server zone must be string";
            return -1;
        }
        serverData.zoneName = server[kZone].asString();

        if (!server[kPhysicalPool].isString()) {
            LOG(ERROR) << "server physicalpool must be string";
            return -1;
        }
        serverData.physicalPoolName = server[kPhysicalPool].asString();

        if (!server.isMember(kPoolsetName)) {
            serverData.poolsetName = kDefaultPoolsetName;
        } else if (server[kPoolsetName].isString()) {
            serverData.poolsetName = server[kPoolsetName].asString();
        } else {
            LOG(ERROR) << "server poolsetName must be string, poolsetName is "
                       << server[kPoolsetName];
            return -1;
        }

        serverDatas.emplace_back(std::move(serverData));
    }
    return 0;
}

int CurvefsTools::InitLogicalPoolData() {
    if (clusterMap_[kLogicalPools].isNull()) {
        LOG(ERROR) << "No servers in cluster map";
        return -1;
    }
    for (const auto &lgPool : clusterMap_[kLogicalPools]) {
        CurveLogicalPoolData lgPoolData;
        if (!lgPool[kName].isString()) {
            LOG(ERROR) << "logicalpool name must be string";
            return -1;
        }
        lgPoolData.name = lgPool[kName].asString();
        if (!lgPool[kPhysicalPool].isString()) {
            LOG(ERROR) << "logicalpool physicalpool must be string";
            return -1;
        }
        lgPoolData.physicalPoolName = lgPool[kPhysicalPool].asString();
        if (!lgPool[kType].isInt()) {
            LOG(ERROR) << "logicalpool type must be int";
            return -1;
        }
        lgPoolData.type = static_cast<LogicalPoolType>(lgPool[kType].asInt());
        if (!lgPool[kReplicasNum].isUInt()) {
            LOG(ERROR) << "logicalpool replicasnum must be uint";
            return -1;
        }
        lgPoolData.replicasNum = lgPool[kReplicasNum].asUInt();
        if (!lgPool[kCopysetNum].isUInt64()) {
            LOG(ERROR) << "logicalpool copysetnum must be uint64";
            return -1;
        }
        lgPoolData.copysetNum = lgPool[kCopysetNum].asUInt64();
        if (!lgPool[kZoneNum].isUInt64()) {
            LOG(ERROR) << "logicalpool zonenum must be uint64";
            return -1;
        }
        lgPoolData.zoneNum = lgPool[kZoneNum].asUInt();
        if (!lgPool[kScatterWidth].isUInt()) {
            LOG(ERROR) << "logicalpool scatterwidth must be uint";
            return -1;
        }
        lgPoolData.scatterwidth = lgPool[kScatterWidth].asUInt();
        if (lgPool[kAllocStatus].isString()) {
            if (lgPool[kAllocStatus].asString() == kAllocStatusAllow) {
                lgPoolData.status = AllocateStatus::ALLOW;
            } else if (lgPool[kAllocStatus].asString() == kAllocStatusDeny) {
                lgPoolData.status = AllocateStatus::DENY;
            } else {
                LOG(ERROR) << "logicalpool status string is invalid!, which is "
                           << lgPool[kAllocStatus].asString();
                return -1;
            }
        } else {
            LOG(WARNING) << "logicalpool not set, use default allow";
            lgPoolData.status = AllocateStatus::ALLOW;
        }
        lgPoolDatas.emplace_back(lgPoolData);
    }
    return 0;
}

int CurvefsTools::ListPoolset(std::list<PoolsetInfo>* poolsetInfos) {
    TopologyService_Stub stub(&channel_);
    ListPoolsetRequest request;
    ListPoolsetResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
    cntl.set_log_id(1);
    if (!authClient_.GetToken(curve::common::MDS_ROLE,
        request.mutable_authtoken())) {
        LOG(ERROR) << "ListPoolset: GetToken failed";
        return kRetCodeCommonErr;
    }

    LOG(INFO) << "ListPoolset send request: " << request.DebugString();

    stub.ListPoolset(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        return kRetCodeRedirectMds;
    }
    if (response.statuscode() != kTopoErrCodeSuccess) {
        LOG(ERROR) << "ListPoolset Rpc response fail. "
                   << "Message is :"
                   << response.DebugString();
        return response.statuscode();
    } else {
        LOG(INFO) << "Received ListPoolset Rpc response success, "
                  << response.DebugString();
    }

    for (int i = 0; i < response.poolsetinfos_size(); i++) {
        poolsetInfos->push_back(response.poolsetinfos(i));
    }
    return 0;
}

int CurvefsTools::ListPhysicalPool(
    std::list<PhysicalPoolInfo> *physicalPoolInfos) {
    TopologyService_Stub stub(&channel_);
    ListPhysicalPoolRequest request;
    ListPhysicalPoolResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
    cntl.set_log_id(1);
    if (!authClient_.GetToken(curve::common::MDS_ROLE,
        request.mutable_authtoken())) {
        LOG(ERROR) << "ListPhysicalPool: GetToken failed";
        return kRetCodeCommonErr;
    }

    LOG(INFO) << "ListPhysicalPool send request: "
              << request.DebugString();

    stub.ListPhysicalPool(&cntl,
        &request,
        &response,
        nullptr);

    if (cntl.Failed()) {
        return kRetCodeRedirectMds;
    }
    if (response.statuscode() != kTopoErrCodeSuccess) {
        LOG(ERROR) << "ListPhysicalPool Rpc response fail. "
                   << "Message is :"
                   << response.DebugString();
        return response.statuscode();
    } else {
        LOG(INFO) << "Received ListPhysicalPool Rpc response success, "
                  << response.DebugString();
    }

    for (int i = 0;
            i < response.physicalpoolinfos_size();
            i++) {
        physicalPoolInfos->push_back(
            response.physicalpoolinfos(i));
    }
    return 0;
}

int CurvefsTools::ListPhysicalPoolsInPoolset(PoolsetIdType poolsetid,
    std::list<PhysicalPoolInfo> *physicalPoolInfos) {
    TopologyService_Stub stub(&channel_);
    ListPhysicalPoolsInPoolsetRequest request;
    ListPhysicalPoolResponse response;
    request.add_poolsetid(poolsetid);

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
    cntl.set_log_id(1);

    if (!authClient_.GetToken(curve::common::MDS_ROLE,
        request.mutable_authtoken())) {
        LOG(ERROR) << "ListPhysicalPool: GetToken failed";
        return kRetCodeCommonErr;
    }

    LOG(INFO) << "ListPhysicalPoolsInPoolset, send request: "
              << request.DebugString();

    stub.ListPhysicalPoolsInPoolset(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        return kRetCodeRedirectMds;
    }
    if (response.statuscode() != kTopoErrCodeSuccess) {
        LOG(ERROR) << "ListPhysicalPoolsInPoolset Rpc response fail. "
                   << "Message is :"
                   << response.DebugString()
                   << " , poolsetid = "
                   << poolsetid;
        return response.statuscode();
    } else {
        LOG(INFO) << "Received ListPhyPoolsInPoolset Rpc resp success,"
                  << response.DebugString();
    }

    for (int i = 0; i < response.physicalpoolinfos_size(); i++) {
        physicalPoolInfos->push_back(response.physicalpoolinfos(i));
    }
    return 0;
}

int CurvefsTools::AddListPoolZone(PoolIdType poolid,
    std::list<ZoneInfo> *zoneInfos) {
    TopologyService_Stub stub(&channel_);
    ListPoolZoneRequest request;
    ListPoolZoneResponse response;
    request.set_physicalpoolid(poolid);
    if (!authClient_.GetToken(curve::common::MDS_ROLE,
        request.mutable_authtoken())) {
        LOG(ERROR) << "AddListPoolZone: GetToken failed";
        return kRetCodeCommonErr;
    }

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
    cntl.set_log_id(1);

    LOG(INFO) << "ListPoolZone, send request: "
              << request.DebugString();

    stub.ListPoolZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        return kRetCodeRedirectMds;
    }
    if (response.statuscode() != kTopoErrCodeSuccess) {
        LOG(ERROR) << "ListPoolZone Rpc response fail. "
                   << "Message is :"
                   << response.DebugString()
                   << " , physicalpoolid = "
                   << poolid;
        return response.statuscode();
    } else {
        LOG(INFO) << "Received ListPoolZone Rpc response success, "
                  << response.DebugString();
    }

    for (int i = 0; i < response.zones_size(); i++) {
        zoneInfos->push_back(response.zones(i));
    }
    return 0;
}

int CurvefsTools::AddListZoneServer(ZoneIdType zoneid,
    std::list<ServerInfo> *serverInfos) {
    TopologyService_Stub stub(&channel_);
    ListZoneServerRequest request;
    ListZoneServerResponse response;
    request.set_zoneid(zoneid);
    if (!authClient_.GetToken(curve::common::MDS_ROLE,
        request.mutable_authtoken())) {
        LOG(ERROR) << "AddListZoneServer: GetToken failed";
        return kRetCodeCommonErr;
    }

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
    cntl.set_log_id(1);

    LOG(INFO) << "ListZoneServer, send request: "
              << request.DebugString();

    stub.ListZoneServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        return kRetCodeRedirectMds;
    }
    if (response.statuscode() != kTopoErrCodeSuccess) {
        LOG(ERROR) << "ListZoneServer Rpc response fail. "
                   << "Message is :"
                   << response.DebugString()
                   << " , zoneid = "
                   << zoneid;
        return response.statuscode();
    } else {
        LOG(INFO) << "ListZoneServer Rpc response success, "
                   << response.DebugString();
    }

    for (int i = 0; i < response.serverinfo_size(); i++) {
        serverInfos->push_back(response.serverinfo(i));
    }
    return 0;
}

int CurvefsTools::ScanCluster() {
    // get all poolsets and compare
    // De-duplication
    for (const auto& poolset : poolsetDatas) {
        if (std::find_if(poolsetToAdd.begin(), poolsetToAdd.end(),
                         [poolset](const CurvePoolsetData& data) {
                             return data.name == poolset.name;
                         }) != poolsetToAdd.end()) {
            continue;
        }
        poolsetToAdd.push_back(poolset);
    }

    std::list<PoolsetInfo> poolsetInfos;
    int ret = ListPoolset(&poolsetInfos);
    if (ret < 0) {
        return ret;
    }

    for (auto it = poolsetInfos.begin(); it != poolsetInfos.end();) {
        if (it->poolsetname() == kDefaultPoolsetName) {
            ++it;
            continue;
        }

        auto ix = std::find_if(poolsetToAdd.begin(), poolsetToAdd.end(),
                               [it](const CurvePoolsetData& data) {
                                   return data.name == it->poolsetname();
                               });
        if (ix != poolsetToAdd.end()) {
            poolsetToAdd.erase(ix);
            it++;
        } else {
            poolsetToDel.push_back(it->poolsetid());
            it = poolsetInfos.erase(it);
        }
    }

    // get all phsicalpool and compare
    // De-duplication
    for (auto server : serverDatas) {
        if (std::find_if(physicalPoolToAdd.begin(),
            physicalPoolToAdd.end(),
            [server](CurvePhysicalPoolData& data) {
            return data.physicalPoolName == server.physicalPoolName;
        }) != physicalPoolToAdd.end()) {
            continue;
        }
        CurvePhysicalPoolData poolData;
        poolData.physicalPoolName = server.physicalPoolName;
        poolData.poolsetName = server.poolsetName.empty() ? kDefaultPoolsetName
                                                          : server.poolsetName;
        physicalPoolToAdd.push_back(poolData);
    }

    std::list<PhysicalPoolInfo> physicalPoolInfos;
    for (auto poolsetid : poolsetToDel) {
        ret = ListPhysicalPoolsInPoolset(poolsetid, &physicalPoolInfos);
        if (ret < 0) {
            return ret;
        }
    }

    for (auto phyPoolinfo : physicalPoolInfos) {
        physicalPoolToDel.push_back(phyPoolinfo.physicalpoolid());
    }

    physicalPoolInfos.clear();

    for (auto it = poolsetInfos.begin(); it != poolsetInfos.end(); it++) {
        PoolsetIdType poolsetid = it->poolsetid();
        ret = ListPhysicalPoolsInPoolset(poolsetid, &physicalPoolInfos);
        if (ret < 0) {
            return ret;
        }
    }

    for (auto it = physicalPoolInfos.begin(); it != physicalPoolInfos.end();) {
        auto ix = std::find_if(
                physicalPoolToAdd.begin(), physicalPoolToAdd.end(),
                [it](const CurvePhysicalPoolData& data) {
                    return (data.poolsetName == it->poolsetname()) &&
                           (data.physicalPoolName == it->physicalpoolname());
                });
        if (ix != physicalPoolToAdd.end()) {
            physicalPoolToAdd.erase(ix);
            it++;
        } else {
            physicalPoolToDel.push_back(it->physicalpoolid());
            it = physicalPoolInfos.erase(it);
        }
    }

    // get zone and compare
    // De-duplication
    for (auto server : serverDatas) {
        if (std::find_if(zoneToAdd.begin(),
            zoneToAdd.end(),
            [server](CurveZoneData& data) {
            return (data.physicalPoolName ==
                server.physicalPoolName) &&
                   (data.zoneName ==
                server.zoneName);
        }) != zoneToAdd.end()) {
            continue;
        }
        CurveZoneData CurveZoneData;
        CurveZoneData.physicalPoolName = server.physicalPoolName;
        CurveZoneData.zoneName = server.zoneName;
        zoneToAdd.push_back(CurveZoneData);
    }

    std::list<ZoneInfo> zoneInfos;
    for (auto poolid : physicalPoolToDel) {
        ret = AddListPoolZone(poolid, &zoneInfos);
        if (ret < 0) {
            return ret;
        }
    }

    for (auto zinfo : zoneInfos) {
        zoneToDel.push_back(zinfo.zoneid());
    }

    zoneInfos.clear();
    for (auto it = physicalPoolInfos.begin();
            it != physicalPoolInfos.end();
            it++) {
        PoolIdType poolid = it->physicalpoolid();
        ret = AddListPoolZone(poolid, &zoneInfos);
        if (ret < 0) {
            return ret;
        }
    }

    for (auto it = zoneInfos.begin();
            it != zoneInfos.end();) {
        auto ix = std::find_if(zoneToAdd.begin(),
            zoneToAdd.end(),
            [it] (CurveZoneData &data) {
                return (data.physicalPoolName ==
                    it->physicalpoolname()) &&
                       (data.zoneName ==
                    it->zonename());
            });
        if (ix != zoneToAdd.end()) {
            zoneToAdd.erase(ix);
            it++;
        } else {
            zoneToDel.push_back(it->zoneid());
            it = zoneInfos.erase(it);
        }
    }

    // get server and compare
    // De-duplication
    for (auto server : serverDatas) {
        if (std::find_if(serverToAdd.begin(),
            serverToAdd.end(),
            [server](CurveServerData& data) {
            return data.serverName ==
                 server.serverName;
            }) != serverToAdd.end()) {
            LOG(WARNING) << "WARING! Duplicated Server Name: "
                        << server.serverName
                        << " , ignored.";
            continue;
        }
        serverToAdd.push_back(server);
    }

    std::list<ServerInfo> serverInfos;
    for (auto zoneid : zoneToDel) {
        ret = AddListZoneServer(zoneid, &serverInfos);
        if (ret < 0) {
            return ret;
        }
    }

    for (auto sinfo : serverInfos) {
        serverToDel.push_back(sinfo.serverid());
    }

    serverInfos.clear();
    for (auto it = zoneInfos.begin();
            it != zoneInfos.end();
            it++) {
        ZoneIdType zoneid = it->zoneid();
        ret = AddListZoneServer(zoneid, &serverInfos);
        if (ret < 0) {
            return ret;
        }
    }

    for (auto it = serverInfos.begin();
            it != serverInfos.end();
            it++) {
        auto ix = std::find_if(serverToAdd.begin(),
            serverToAdd.end(),
            [it] (CurveServerData &data) {
                    return (data.serverName == it->hostname()) &&
                    (data.zoneName == it->zonename()) &&
                    (data.physicalPoolName == it->physicalpoolname());
            });
        if (ix != serverToAdd.end()) {
            serverToAdd.erase(ix);
        } else {
            serverToDel.push_back(it->serverid());
        }
    }

    return 0;
}

int CurvefsTools::CreatePoolset() {
    TopologyService_Stub stub(&channel_);
    for (const auto& it : poolsetToAdd) {
        if (it.name == kDefaultPoolsetName) {
            continue;
        }

        PoolsetRequest request;
        request.set_poolsetname(it.name);
        request.set_type(it.type);
        request.set_desc("");
        if (!authClient_.GetToken(curve::common::MDS_ROLE,
            request.mutable_authtoken())) {
            LOG(ERROR) << "CreatePoolset: GetToken failed";
            return kRetCodeCommonErr;
        }

        PoolsetResponse response;

        brpc::Controller cntl;
        cntl.set_max_retry(0);
        cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
        cntl.set_log_id(1);

        LOG(INFO) << "CreatePoolset, send request: "
                  << request.DebugString();

        stub.CreatePoolset(&cntl, &request, &response, nullptr);

        if (cntl.Failed()) {
            LOG(WARNING) << "send rpc get cntl Failed, error context:"
                       << cntl.ErrorText();
            return kRetCodeRedirectMds;
        }
        if (response.statuscode() != kTopoErrCodeSuccess) {
            LOG(ERROR) << "CreatePoolset Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , poolsetName ="
                       << it.name;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received CreatePoolset response success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsTools::CreatePhysicalPool() {
    TopologyService_Stub stub(&channel_);
    for (auto it : physicalPoolToAdd) {
        PhysicalPoolRequest request;
        request.set_physicalpoolname(it.physicalPoolName);
        request.set_desc("");
        request.set_poolsetname(it.poolsetName);
        if (!authClient_.GetToken(curve::common::MDS_ROLE,
            request.mutable_authtoken())) {
            LOG(ERROR) << "CreatePhysicalPool: GetToken failed";
            return kRetCodeCommonErr;
        }

        PhysicalPoolResponse response;

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
        cntl.set_log_id(1);

        LOG(INFO) << "CreatePhysicalPool, send request: "
                  << request.DebugString();

        stub.CreatePhysicalPool(&cntl, &request, &response, nullptr);

        if (cntl.Failed()) {
            LOG(WARNING) << "send rpc get cntl Failed, error context:"
                       << cntl.ErrorText();
            return kRetCodeRedirectMds;
        }
        if (response.statuscode() != kTopoErrCodeSuccess) {
            LOG(ERROR) << "CreatePhysicalPool Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , physicalPoolName ="
                       << it.physicalPoolName;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received CreatePhysicalPool response success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsTools::CreateZone() {
    TopologyService_Stub stub(&channel_);
    for (auto it : zoneToAdd) {
        ZoneRequest request;
        request.set_zonename(it.zoneName);
        request.set_physicalpoolname(it.physicalPoolName);
        request.set_desc("");
        if (!authClient_.GetToken(curve::common::MDS_ROLE,
            request.mutable_authtoken())) {
            LOG(ERROR) << "CreateZone: GetToken failed";
            return kRetCodeCommonErr;
        }

        ZoneResponse response;

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
        cntl.set_log_id(1);

        LOG(INFO) << "CreateZone, send request: "
                  << request.DebugString();

        stub.CreateZone(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "CreateZone, errcorde = "
                       << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText()
                       << " , zoneName = "
                       << it.zoneName;
            return kRetCodeCommonErr;
        }
        if (response.statuscode() != 0) {
            LOG(ERROR) << "CreateZone Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , zoneName = "
                       << it.zoneName;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received CreateZone Rpc success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsTools::CreateServer() {
    TopologyService_Stub stub(&channel_);
    for (auto it : serverToAdd) {
        ServerRegistRequest request;
        request.set_hostname(it.serverName);
        request.set_internalip(it.internalIp);
        request.set_internalport(it.internalPort);
        request.set_externalip(it.externalIp);
        request.set_externalport(it.externalPort);
        request.set_zonename(it.zoneName);
        request.set_physicalpoolname(it.physicalPoolName);
        request.set_poolsetname(it.poolsetName);
        request.set_desc("");
        if (!authClient_.GetToken(curve::common::MDS_ROLE,
            request.mutable_authtoken())) {
            LOG(ERROR) << "CreateServer: GetToken failed";
            return kRetCodeCommonErr;
        }

        ServerRegistResponse response;

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
        cntl.set_log_id(1);

        LOG(INFO) << "CreateServer, send request: "
                  << request.DebugString();

        stub.RegistServer(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "RegistServer, errcorde = "
                       << response.statuscode()
                       << ", error content : "
                       << cntl.ErrorText()
                       << " , serverName = "
                       << it.serverName;
            return kRetCodeCommonErr;
        }
        if (response.statuscode() == kTopoErrCodeSuccess) {
            LOG(INFO) << "Received RegistServer Rpc response success, "
                      << response.DebugString();
        } else if (response.statuscode() == kTopoErrCodeIpPortDuplicated) {
            LOG(INFO) << "Server already exist";
        } else {
            LOG(ERROR) << "RegistServer Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , serverName = "
                       << it.serverName;
            return response.statuscode();
        }
    }
    return 0;
}

int CurvefsTools::ClearPhysicalPool() {
    TopologyService_Stub stub(&channel_);
    for (auto it : physicalPoolToDel) {
        PhysicalPoolRequest request;
        request.set_physicalpoolid(it);
        if (!authClient_.GetToken(curve::common::MDS_ROLE,
            request.mutable_authtoken())) {
            LOG(ERROR) << "ClearPhysicalPool: GetToken failed";
            return kRetCodeCommonErr;
        }

        PhysicalPoolResponse response;

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
        cntl.set_log_id(1);

        LOG(INFO) << "DeletePhysicalPool, send request: "
                  << request.DebugString();

        stub.DeletePhysicalPool(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "DeletePhysicalPool, errcorde = "
                       << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText()
                       << " , physicalPoolId = "
                       << it;
            return kRetCodeCommonErr;
        }
        if (response.statuscode() != kTopoErrCodeSuccess) {
            LOG(ERROR) << "DeletePhysicalPool Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , physicalPoolId = "
                       << it;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received DeletePhysicalPool Rpc response success, "
                      << response.statuscode();
        }
    }
    return 0;
}

int CurvefsTools::ClearPoolset() {
    TopologyService_Stub stub(&channel_);
    for (const auto& it : poolsetToDel) {
        PoolsetRequest request;
        request.set_poolsetid(it);
        if (!authClient_.GetToken(curve::common::MDS_ROLE,
            request.mutable_authtoken())) {
            LOG(ERROR) << "ClearPoolset: GetToken failed";
            return kRetCodeCommonErr;
        }

        PoolsetResponse response;

        brpc::Controller cntl;
        cntl.set_max_retry(0);
        cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
        cntl.set_log_id(1);

        LOG(INFO) << "DeletePoolset, send request: " << request.DebugString();

        stub.DeletePoolset(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "DeletePoolset, errcode = " << response.statuscode()
                       << ", error content:" << cntl.ErrorText()
                       << " , PoolsetId = " << it;
            return kRetCodeCommonErr;
        } else if (response.statuscode() != kTopoErrCodeSuccess &&
                   response.statuscode() !=
                           kTopoErrCodeCannotDeleteDefaultPoolset) {
            LOG(ERROR) << "DeletePoolset Rpc response fail. "
                       << "Message is :" << response.DebugString()
                       << " , PoolsetId = " << it;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received DeletePoolset Rpc success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsTools::ClearZone() {
    TopologyService_Stub stub(&channel_);
    for (auto it : zoneToDel) {
        ZoneRequest request;
        request.set_zoneid(it);
        if (!authClient_.GetToken(curve::common::MDS_ROLE,
            request.mutable_authtoken())) {
            LOG(ERROR) << "ClearZone: GetToken failed";
            return kRetCodeCommonErr;
        }

        ZoneResponse response;

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
        cntl.set_log_id(1);

        LOG(INFO) << "DeleteZone, send request: "
                  << request.DebugString();

        stub.DeleteZone(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "DeleteZone, errcorde = "
                       << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText()
                       << " , zoneId = "
                       << it;
            return kRetCodeCommonErr;
        } else if (response.statuscode() != kTopoErrCodeSuccess) {
            LOG(ERROR) << "DeleteZone Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , zoneId = "
                       << it;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received DeleteZone Rpc success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsTools::ClearServer() {
    TopologyService_Stub stub(&channel_);
    for (auto it : serverToDel) {
        DeleteServerRequest request;
        request.set_serverid(it);
        if (!authClient_.GetToken(curve::common::MDS_ROLE,
            request.mutable_authtoken())) {
            LOG(ERROR) << "ClearServer: GetToken failed";
            return kRetCodeCommonErr;
        }

        DeleteServerResponse response;

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
        cntl.set_log_id(1);

        LOG(INFO) << "DeleteServer, send request: "
                  << request.DebugString();

        stub.DeleteServer(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "DeleteServer, errcorde = "
                       << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText()
                       << " , serverId = "
                       << it;
            return kRetCodeCommonErr;
        }
        if (response.statuscode() != kTopoErrCodeSuccess) {
            LOG(ERROR) << "DeleteServer Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , serverId = "
                       << it;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received DeleteServer Rpc response success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsTools::SetChunkServer() {
    SetChunkServerStatusRequest request;
    request.set_chunkserverid(FLAGS_chunkserver_id);
    if (FLAGS_chunkserver_status == "pendding") {
        request.set_chunkserverstatus(ChunkServerStatus::PENDDING);
    } else if (FLAGS_chunkserver_status == "readwrite") {
        request.set_chunkserverstatus(ChunkServerStatus::READWRITE);
    } else if (FLAGS_chunkserver_status == "retired") {
        LOG(ERROR) << "SetChunkServer retired not unsupport!";
        return kRetCodeCommonErr;
    } else {
        LOG(ERROR) << "SetChunkServer param error, unknown chunkserver status";
        return kRetCodeCommonErr;
    }
    if (!authClient_.GetToken(curve::common::MDS_ROLE,
        request.mutable_authtoken())) {
        LOG(ERROR) << "SetChunkServer: GetToken failed";
        return kRetCodeCommonErr;
    }

    SetChunkServerStatusResponse response;
    TopologyService_Stub stub(&channel_);
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
    cntl.set_log_id(1);

    LOG(INFO) << "SetChunkServerStatusRequest, send request: "
              << request.DebugString();

    stub.SetChunkServer(&cntl, &request, &response, nullptr);

    if (cntl.ErrorCode() == EHOSTDOWN ||
        cntl.ErrorCode() == brpc::ELOGOFF) {
        return kRetCodeRedirectMds;
    } else if (cntl.Failed()) {
        LOG(ERROR) << "SetChunkServerStatusRequest, errcorde = "
                   << response.statuscode()
                   << ", error content:"
                   << cntl.ErrorText();
        return kRetCodeCommonErr;
    }
    if (response.statuscode() != kTopoErrCodeSuccess) {
        LOG(ERROR) << "SetChunkServerStatusRequest Rpc response fail. "
                   << "Message is :"
                   << response.DebugString();
        return response.statuscode();
    } else {
        LOG(INFO) << "Received SetChunkServerStatusRequest Rpc "
                  << "response success, "
                  << response.DebugString();
    }
    return 0;
}

int CurvefsTools::ScanPoolset() {
    for (const auto& poolset : poolsetDatas) {
        if (std::find_if(poolsetToAdd.begin(), poolsetToAdd.end(),
                         [poolset](CurvePoolsetData& data) {
                             return data.name == poolset.name;
                         }) != poolsetToAdd.end()) {
            continue;
        }
        // CurvePoolsetData poolsetData;
        // poolsetData.name = poolset.;
        poolsetToAdd.push_back(poolset);
    }
    std::list<PoolsetInfo> poolsetInfos;
    int ret = ListPoolset(&poolsetInfos);
    if (ret < 0) {
        return ret;
    }
    for (auto it = poolsetInfos.begin(); it != poolsetInfos.end();) {
        auto ix = std::find_if(poolsetToAdd.begin(), poolsetToAdd.end(),
                               [it](CurvePoolsetData& data) {
                                   return data.name == it->poolsetname();
                               });
        if (ix != poolsetToAdd.end()) {
            poolsetToAdd.erase(ix);
            it++;
        } else {
            poolsetToDel.push_back(static_cast<PoolsetIdType>(it->poolsetid()));
            it = poolsetInfos.erase(it);
        }
    }
    return 0;
}

int CurvefsTools::SetLogicalPool() {
    SetLogicalPoolRequest request;
    request.set_logicalpoolid(FLAGS_logicalpool_id);
    if (FLAGS_logicalpool_status == "allow") {
        request.set_status(AllocateStatus::ALLOW);
    } else if (FLAGS_logicalpool_status == "deny") {
        request.set_status(AllocateStatus::DENY);
    } else {
        LOG(ERROR) << "SetLogicalPool param error, unknown logicalpool status";
        return kRetCodeCommonErr;
    }
    if (!authClient_.GetToken(curve::common::MDS_ROLE,
        request.mutable_authtoken())) {
        LOG(ERROR) << "SetLogicalPool: GetToken failed";
        return kRetCodeCommonErr;
    }

    SetLogicalPoolResponse response;
    TopologyService_Stub stub(&channel_);
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
    cntl.set_log_id(1);

    LOG(INFO) << "SetLogicalPool, send request: "
              << request.DebugString();

    stub.SetLogicalPool(&cntl, &request, &response, nullptr);

    if (cntl.ErrorCode() == EHOSTDOWN ||
        cntl.ErrorCode() == brpc::ELOGOFF) {
        return kRetCodeRedirectMds;
    } else if (cntl.Failed()) {
        LOG(ERROR) << "SetLogicalPool, errcorde = "
                   << response.statuscode()
                   << ", error content:"
                   << cntl.ErrorText();
        return kRetCodeCommonErr;
    }
    if (response.statuscode() != kTopoErrCodeSuccess) {
        LOG(ERROR) << "SetLogicalPool Rpc response fail. "
                   << "Message is :"
                   << response.DebugString();
        return response.statuscode();
    } else {
        LOG(INFO) << "Received SetLogicalPool Rpc "
                  << "response success, "
                  << response.DebugString();
    }
    return 0;
}

}  // namespace topology
}  // namespace mds
}  // namespace curve



int main(int argc, char **argv) {
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);

    int ret = 0;
    curve::mds::topology::CurvefsTools tools;
    if (tools.Init() < 0) {
        LOG(ERROR) << "curvefsTool init error.";
        return kRetCodeCommonErr;
    }

    int maxTry = tools.GetMaxTry();
    int retry = 0;
    for (; retry < maxTry; retry++) {
        ret = tools.TryAnotherMdsAddress();
        if (ret < 0) {
            return kRetCodeCommonErr;
        }

        std::string operation = FLAGS_op;
        if (operation == "create_logicalpool") {
            ret = tools.HandleCreateLogicalPool();
        } else if (operation == "create_physicalpool") {
            ret = tools.HandleBuildCluster();
        } else if (operation == "set_chunkserver") {
            ret = tools.SetChunkServer();
        } else if (operation == "set_logicalpool") {
            ret = tools.SetLogicalPool();
        } else {
            LOG(ERROR) << "undefined op.";
            ret = kRetCodeCommonErr;
            break;
        }
        if (ret != kRetCodeRedirectMds) {
            break;
        }
    }

    if (retry >= maxTry) {
        LOG(ERROR) << "rpc retry times exceed.";
        return kRetCodeCommonErr;
    }
    if (ret < 0) {
        LOG(ERROR) << "exec fail, ret = " << ret;
    } else {
        LOG(INFO) << "exec success, ret = " << ret;
    }

    return ret;
}
