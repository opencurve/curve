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
 * Created Date: Thu Jun 18 2020
 * Author: xuchaojie
 */
#include "src/mds/topology/topology_storge_etcd.h"

#include <string>
#include <vector>
#include <map>
#include <utility>
#include "src/mds/topology/topology_storage_codec.h"

namespace curve {
namespace mds {
namespace topology {

bool TopologyStorageEtcd::LoadLogicalPool(
    std::unordered_map<PoolIdType, LogicalPool> *logicalPoolMap,
    PoolIdType *maxLogicalPoolId) {
    std::string startKey = TopologyStorageCodec::GetLogicalPoolKeyPrefix();
    std::string endKey = TopologyStorageCodec::GetLogicalPoolKeyEnd();
    std::vector<std::string> out;
    logicalPoolMap->clear();
    *maxLogicalPoolId = 0;
    int errCode = client_->List(startKey, endKey, &out);
    if (errCode == EtcdErrCode::EtcdKeyNotExist) {
        return true;
    }
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "etcd list err:" << errCode;
        return false;
    }
    for (int i = 0; i < out.size(); i++) {
        LogicalPool data;
        errCode = codec_->DecodeLogicalPoolData(out[i], &data);
        if (!errCode) {
            LOG(ERROR) << "DecodeLogicalPoolData err";
            return false;
        }
        LogicalPoolIdType id = data.GetId();
        auto ret = logicalPoolMap->emplace(id, std::move(data));
        if (!ret.second) {
            LOG(ERROR) << "LoadLogicalPool: "
                       << "LogicalPoolId duplicated, logicalPoolId = "
                       << id;
            return false;
        }
        if (*maxLogicalPoolId < id) {
            *maxLogicalPoolId = id;
        }
    }
    return true;
}

bool TopologyStorageEtcd::LoadPhysicalPool(
    std::unordered_map<PoolIdType, PhysicalPool> *physicalPoolMap,
    PoolIdType *maxPhysicalPoolId) {
    std::string startKey = TopologyStorageCodec::GetPhysicalPoolKeyPrefix();
    std::string endKey = TopologyStorageCodec::GetPhysicalPoolKeyEnd();
    std::vector<std::string> out;
    physicalPoolMap->clear();
    *maxPhysicalPoolId = 0;
    int errCode = client_->List(startKey, endKey, &out);
    if (errCode == EtcdErrCode::EtcdKeyNotExist) {
        return true;
    }
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "etcd list err:" << errCode;
        return false;
    }
    for (int i = 0; i < out.size(); i++) {
        PhysicalPool data;
        errCode = codec_->DecodePhysicalPoolData(out[i], &data);
        if (!errCode) {
            LOG(ERROR) << "DecodePhysicalPoolData err";
            return false;
        }
        PhysicalPoolIdType id = data.GetId();
        auto ret = physicalPoolMap->emplace(id, std::move(data));
        if (!ret.second) {
            LOG(ERROR) << "LoadPhysicalPool: "
                       << "PhysicalPoolId duplicated, PhysicalPoolId = "
                       << id;
            return false;
        }
        if (*maxPhysicalPoolId < id) {
            *maxPhysicalPoolId = id;
        }
    }
    return true;
}

bool TopologyStorageEtcd::LoadZone(
    std::unordered_map<ZoneIdType, Zone> *zoneMap,
    ZoneIdType *maxZoneId) {
    std::string startKey = TopologyStorageCodec::GetZoneKeyPrefix();
    std::string endKey = TopologyStorageCodec::GetZoneKeyEnd();
    std::vector<std::string> out;
    zoneMap->clear();
    *maxZoneId = 0;
    int errCode = client_->List(startKey, endKey, &out);
    if (errCode == EtcdErrCode::EtcdKeyNotExist) {
        return true;
    }
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "etcd list err:" << errCode;
        return false;
    }
    for (int i = 0; i < out.size(); i++) {
        Zone data;
        errCode = codec_->DecodeZoneData(out[i], &data);
        if (!errCode) {
            LOG(ERROR) << "DecodeZoneData err";
            return false;
        }
        ZoneIdType id = data.GetId();
        auto ret = zoneMap->emplace(id, std::move(data));
        if (!ret.second) {
            LOG(ERROR) << "LoadZone: "
                       << "ZoneId duplicated, ZoneId = "
                       << id;
            return false;
        }
        if (*maxZoneId < id) {
            *maxZoneId = id;
        }
    }
    return true;
}

bool TopologyStorageEtcd::LoadServer(
    std::unordered_map<ServerIdType, Server> *serverMap,
    ServerIdType *maxServerId) {
    std::string startKey = TopologyStorageCodec::GetServerKeyPrefix();
    std::string endKey = TopologyStorageCodec::GetServerKeyEnd();
    std::vector<std::string> out;
    serverMap->clear();
    *maxServerId = 0;
    int errCode = client_->List(startKey, endKey, &out);
    if (errCode == EtcdErrCode::EtcdKeyNotExist) {
        return true;
    }
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "etcd list err:" << errCode;
        return false;
    }
    for (int i = 0; i < out.size(); i++) {
        Server data;
        errCode = codec_->DecodeServerData(out[i], &data);
        if (!errCode) {
            LOG(ERROR) << "DecodeServerData err";
            return false;
        }
        ServerIdType id = data.GetId();
        auto ret = serverMap->emplace(id, std::move(data));
        if (!ret.second) {
            LOG(ERROR) << "LoadSever: "
                       << "SererId duplicated, ServerId = "
                       << id;
            return false;
        }
        if (*maxServerId < id) {
            *maxServerId = id;
        }
    }
    return true;
}

bool TopologyStorageEtcd::LoadChunkServer(
    std::unordered_map<ChunkServerIdType, ChunkServer> *chunkServerMap,
    ChunkServerIdType *maxChunkServerId) {
    std::string startKey = TopologyStorageCodec::GetChunkServerKeyPrefix();
    std::string endKey = TopologyStorageCodec::GetChunkServerKeyEnd();
    std::vector<std::string> out;
    chunkServerMap->clear();
    *maxChunkServerId = 0;
    int errCode = client_->List(startKey, endKey, &out);
    if (errCode == EtcdErrCode::EtcdKeyNotExist) {
        return true;
    }
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "etcd list err:" << errCode;
        return false;
    }
    for (int i = 0; i < out.size(); i++) {
        ChunkServer data;
        errCode = codec_->DecodeChunkServerData(out[i], &data);
        if (!errCode) {
            LOG(ERROR) << "DecodeChunkServerData err";
            return false;
        }
        // set chunkserver unstable when loaded
        data.SetOnlineState(OnlineState::UNSTABLE);
        ChunkServerIdType id = data.GetId();
        auto ret = chunkServerMap->emplace(id, std::move(data));
        if (!ret.second) {
            LOG(ERROR) << "LoadChunkSever: "
                       << "ChunkSererId duplicated, ChunkServerId = "
                       << id;
            return false;
        }
        if (*maxChunkServerId < id) {
            *maxChunkServerId = id;
        }
    }
    return true;
}

bool TopologyStorageEtcd::LoadCopySet(
    std::map<CopySetKey, CopySetInfo> *copySetMap,
    std::map<PoolIdType, CopySetIdType> *copySetIdMaxMap) {
    std::string startKey = TopologyStorageCodec::GetCopysetKeyPrefix();
    std::string endKey = TopologyStorageCodec::GetCopysetKeyEnd();
    std::vector<std::string> out;
    copySetMap->clear();
    copySetIdMaxMap->clear();
    int errCode = client_->List(startKey, endKey, &out);
    if (errCode == EtcdErrCode::EtcdKeyNotExist) {
        return true;
    }
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "etcd list err:" << errCode;
        return false;
    }
    for (int i = 0; i < out.size(); i++) {
        CopySetInfo data;
        errCode = codec_->DecodeCopySetData(out[i], &data);
        if (!errCode) {
            LOG(ERROR) << "DecodeCopySetData err";
            return false;
        }
        LogicalPoolIdType lpid = data.GetLogicalPoolId();
        CopySetIdType id = data.GetId();
        auto ret = copySetMap->emplace(std::make_pair(lpid, id),
            std::move(data));
        if (!ret.second) {
            LOG(ERROR) << "LoadCopySet: "
                       << "Id duplicated, logicalPoolId = "
                       << lpid
                       << ", copySetId = "
                       << id;
            return false;
        }
        if ((*copySetIdMaxMap)[lpid] < id) {
            (*copySetIdMaxMap)[lpid] = id;
        }
    }
    return true;
}

bool TopologyStorageEtcd::StorageLogicalPool(const LogicalPool &data) {
    std::string key = codec_->EncodeLogicalPoolKey(data.GetId());
    std::string value;
    bool ret = codec_->EncodeLogicalPoolData(data, &value);
    if (!ret) {
        LOG(ERROR) << "EncodeLogicalPoolData err"
                   << ", logicalPoolId = " << data.GetId();
        return false;
    }
    int errCode = client_->Put(key, value);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put logicalPool into etcd err"
                   << ", errcode = " << errCode
                   << ", logicalPoolId = " << data.GetId();
        return false;
    }
    return true;
}

bool TopologyStorageEtcd::StoragePhysicalPool(const PhysicalPool &data) {
    std::string key = codec_->EncodePhysicalPoolKey(data.GetId());
    std::string value;
    bool ret = codec_->EncodePhysicalPoolData(data, &value);
    if (!ret) {
        LOG(ERROR) << "EncodePhysicalPoolData err"
                   << ", physicalPoolId = " << data.GetId();
        return false;
    }
    int errCode = client_->Put(key, value);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put PhysicalPool into etcd err"
                   << ", errcode = " << errCode
                   << ", physicalPoolId = " << data.GetId();
        return false;
    }
    return true;
}

bool TopologyStorageEtcd::StorageZone(const Zone &data) {
    std::string key = codec_->EncodeZoneKey(data.GetId());
    std::string value;
    bool ret = codec_->EncodeZoneData(data, &value);
    if (!ret) {
        LOG(ERROR) << "EncodeZoneData err"
                   << ", zoneId = " << data.GetId();
        return false;
    }
    int errCode = client_->Put(key, value);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put Zone into etcd err"
                   << ", errcode = " << errCode
                   << ", zoneId = " << data.GetId();
        return false;
    }
    return true;
}

bool TopologyStorageEtcd::StorageServer(const Server &data) {
    std::string key = codec_->EncodeServerKey(data.GetId());
    std::string value;
    bool ret = codec_->EncodeServerData(data, &value);
    if (!ret) {
        LOG(ERROR) << "EncodeServerData err"
                   << ", serverId = " << data.GetId();
        return false;
    }
    int errCode = client_->Put(key, value);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put Server into etcd err"
                   << ", errcode = " << errCode
                   << ", serverId = " << data.GetId();
        return false;
    }
    return true;
}

bool TopologyStorageEtcd::StorageChunkServer(const ChunkServer &data) {
    std::string key = codec_->EncodeChunkServerKey(data.GetId());
    std::string value;
    bool ret = codec_->EncodeChunkServerData(data, &value);
    if (!ret) {
        LOG(ERROR) << "EncodeChunkServerData err"
                   << ", chunkServerId = " << data.GetId();
        return false;
    }
    int errCode = client_->Put(key, value);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put ChunkServer into etcd err"
                   << ", errcode = " << errCode
                   << ", chunkServerId = " << data.GetId();
        return false;
    }
    return true;
}

bool TopologyStorageEtcd::StorageCopySet(const CopySetInfo &data) {
    CopySetKey id(data.GetLogicalPoolId(), data.GetId());
    std::string key = codec_->EncodeCopySetKey(id);
    std::string value;
    bool ret = codec_->EncodeCopySetData(data, &value);
    if (!ret) {
        LOG(ERROR) << "EncodeCopySetData err"
                   << ", logicalPoolId = " << data.GetLogicalPoolId()
                   << ", copysetId = " << data.GetId();
        return false;
    }
    int errCode = client_->Put(key, value);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put Copyset into etcd err"
                   << ", errcode = " << errCode
                   << ", logicalPoolId = " << data.GetLogicalPoolId()
                   << ", copysetId = " << data.GetId();
        return false;
    }
    return true;
}

bool TopologyStorageEtcd::DeleteLogicalPool(PoolIdType id) {
    std::string key = codec_->EncodeLogicalPoolKey(id);
    int errCode = client_->Delete(key);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "delete LogicalPool from etcd err"
                   << ", errcode = " << errCode
                   << ", logicalPoolId = " << id;
        return false;
    }
    return true;
}

bool TopologyStorageEtcd::DeletePhysicalPool(PoolIdType id) {
    std::string key = codec_->EncodePhysicalPoolKey(id);
    int errCode = client_->Delete(key);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "delete PhysicalPool from etcd err"
                   << ", errcode = " << errCode
                   << ", physicalPoolId = " << id;
        return false;
    }
    return true;
}

bool TopologyStorageEtcd::DeleteZone(ZoneIdType id) {
    std::string key = codec_->EncodeZoneKey(id);
    int errCode = client_->Delete(key);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "delete Zone from etcd err"
                   << ", errcode = " << errCode
                   << ", zoneId = " << id;
        return false;
    }
    return true;
}

bool TopologyStorageEtcd::DeleteServer(ServerIdType id) {
    std::string key = codec_->EncodeServerKey(id);
    int errCode = client_->Delete(key);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "delete Server from etcd err"
                   << ", errcode = " << errCode
                   << ", serverId = " << id;
        return false;
    }
    return true;
}

bool TopologyStorageEtcd::DeleteChunkServer(ChunkServerIdType id) {
    std::string key = codec_->EncodeChunkServerKey(id);
    int errCode = client_->Delete(key);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "delete ChunkServer from etcd err"
                   << ", errcode = " << errCode
                   << ", chunkServerId = " << id;
        return false;
    }
    return true;
}

bool TopologyStorageEtcd::DeleteCopySet(CopySetKey key) {
    std::string etcdkey = codec_->EncodeCopySetKey(key);
    int errCode = client_->Delete(etcdkey);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "delete Copyset from etcd err"
                   << ", errcode = " << errCode
                   << ", logicalPoolId = " << key.first
                   << ", copysetId = " << key.second;
        return false;
    }
    return true;
}

bool TopologyStorageEtcd::UpdateLogicalPool(const LogicalPool &data) {
    return StorageLogicalPool(data);
}

bool TopologyStorageEtcd::UpdatePhysicalPool(const PhysicalPool &data) {
    return StoragePhysicalPool(data);
}

bool TopologyStorageEtcd::UpdateZone(const Zone &data) {
    return StorageZone(data);
}

bool TopologyStorageEtcd::UpdateServer(const Server &data) {
    return StorageServer(data);
}

bool TopologyStorageEtcd::UpdateChunkServer(const ChunkServer &data) {
    return StorageChunkServer(data);
}

bool TopologyStorageEtcd::UpdateCopySet(const CopySetInfo &data) {
    return StorageCopySet(data);
}

bool TopologyStorageEtcd::LoadClusterInfo(
    std::vector<ClusterInformation> *info) {
    std::string key = TopologyStorageCodec::GetClusterInfoKey();
    std::string value;
    int errCode = client_->Get(key, &value);
    if (errCode == EtcdErrCode::EtcdKeyNotExist) {
        return true;
    }
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Get ClusterInfo from etcd err"
                   << ", errCode = " << errCode;
        return false;
    }
    ClusterInformation data;
    errCode = codec_->DecodeCluserInfoData(value, &data);
    if (!errCode) {
        LOG(ERROR) << "DecodeCluserInfoData err";
        return false;
    }
    info->emplace_back(std::move(data));
    return true;
}

bool TopologyStorageEtcd::StorageClusterInfo(const ClusterInformation &info) {
    std::string key = TopologyStorageCodec::GetClusterInfoKey();
    std::string value;
    if (codec_->EncodeClusterInfoData(info, &value)
        != true) {
        LOG(ERROR) << "EncodeClusterInfoData err";
        return false;
    }
    int errCode = client_->Put(key, value);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put ClusterInfo into etcd err"
                   << ", errcode = " << errCode;
        return false;
    }
    return true;
}

}  // namespace topology
}  // namespace mds
}  // namespace curve

