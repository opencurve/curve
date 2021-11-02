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
 * Created Date: 2021-08-27
 * Author: wanghai01
 */
#include "curvefs/src/mds/topology/topology_storge_etcd.h"

#include <string>
#include <vector>
#include <map>
#include <utility>

namespace curvefs {
namespace mds {
namespace topology {

bool TopologyStorageEtcd::LoadPool(
    std::unordered_map<PoolIdType, Pool> *poolMap,
    PoolIdType *maxPoolId) {
    std::vector<std::string> out;
    poolMap->clear();
    *maxPoolId = 0;
    int errCode = client_->List(POOLKEYPREFIX, POOLKEYEND, &out);
    if (errCode == EtcdErrCode::EtcdKeyNotExist) {
        return true;
    }
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "etcd list err:" << errCode;
        return false;
    }
    for (uint32_t i = 0; i < out.size(); i++) {
        Pool data;
        errCode = codec_->DecodePoolData(out[i], &data);
        if (!errCode) {
            LOG(ERROR) << "DecodePoolData err";
            return false;
        }
        PoolIdType id = data.GetId();
        auto ret = poolMap->emplace(id, std::move(data));
        if (!ret.second) {
            LOG(ERROR) << "LoadPool: "
                       << "PoolId duplicated, PoolId = "
                       << id;
            return false;
        }
        if (*maxPoolId < id) {
            *maxPoolId = id;
        }
    }
    return true;
}

bool TopologyStorageEtcd::LoadZone(
    std::unordered_map<ZoneIdType, Zone> *zoneMap,
    ZoneIdType *maxZoneId) {
    std::vector<std::string> out;
    zoneMap->clear();
    *maxZoneId = 0;
    int errCode = client_->List(ZONEKEYPREFIX, ZONEKEYEND, &out);
    if (errCode == EtcdErrCode::EtcdKeyNotExist) {
        return true;
    }
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "etcd list err:" << errCode;
        return false;
    }
    for (uint32_t i = 0; i < out.size(); i++) {
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
    std::vector<std::string> out;
    serverMap->clear();
    *maxServerId = 0;
    int errCode = client_->List(SERVERKEYPREFIX, SERVERKEYEND, &out);
    if (errCode == EtcdErrCode::EtcdKeyNotExist) {
        return true;
    }
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "etcd list err:" << errCode;
        return false;
    }
    for (uint32_t i = 0; i < out.size(); i++) {
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

bool TopologyStorageEtcd::LoadMetaServer(
    std::unordered_map<MetaServerIdType, MetaServer> *metaServerMap,
    MetaServerIdType *maxMetaServerId) {
    std::vector<std::string> out;
    metaServerMap->clear();
    *maxMetaServerId = 0;
    int errCode = client_->List(METASERVERKEYPREFIX, METASERVERKEYEND, &out);
    if (errCode == EtcdErrCode::EtcdKeyNotExist) {
        return true;
    }
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "etcd list err:" << errCode;
        return false;
    }
    for (uint32_t i = 0; i < out.size(); i++) {
        MetaServer data;
        errCode = codec_->DecodeMetaServerData(out[i], &data);
        if (!errCode) {
            LOG(ERROR) << "DecodeMetaServerData err";
            return false;
        }

        MetaServerIdType id = data.GetId();
        auto ret = metaServerMap->emplace(id, std::move(data));
        if (!ret.second) {
            LOG(ERROR) << "LoadChunkSever: "
                       << "ChunkSererId duplicated, MetaServerId = "
                       << id;
            return false;
        }
        if (*maxMetaServerId < id) {
            *maxMetaServerId = id;
        }
    }
    return true;
}

bool TopologyStorageEtcd::LoadCopySet(
    std::map<CopySetKey, CopySetInfo> *copySetMap,
    std::map<PoolIdType, CopySetIdType> *copySetIdMaxMap) {
    std::vector<std::string> out;
    copySetMap->clear();
    copySetIdMaxMap->clear();
    int errCode = client_->List(COPYSETKEYPREFIX, COPYSETKEYEND, &out);
    if (errCode == EtcdErrCode::EtcdKeyNotExist) {
        return true;
    }
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "etcd list err:" << errCode;
        return false;
    }
    for (uint32_t i = 0; i < out.size(); i++) {
        CopySetInfo data;
        errCode = codec_->DecodeCopySetData(out[i], &data);
        if (!errCode) {
            LOG(ERROR) << "DecodeCopySetData err";
            return false;
        }
        PoolIdType pid = data.GetPoolId();
        CopySetIdType id = data.GetId();
        auto ret = copySetMap->emplace(std::make_pair(pid, id),
                                       std::move(data));
        if (!ret.second) {
            LOG(ERROR) << "LoadCopySet: "
                       << "Id duplicated, poolId = " << pid
                       << ", copySetId = " << id;
            return false;
        }
        if ((*copySetIdMaxMap)[pid] < id) {
            (*copySetIdMaxMap)[pid] = id;
        }
    }
    return true;
}

bool TopologyStorageEtcd::LoadPartition(
    std::unordered_map<PartitionIdType, Partition> *partitionMap,
    PartitionIdType *maxPartitionId) {
    std::vector<std::string> out;
    partitionMap->clear();
    *maxPartitionId = 0;
    int errCode = client_->List(PARTITIONKEYPREFIX, PARTITIONKEYEND, &out);
    if (errCode == EtcdErrCode::EtcdKeyNotExist) {
        return true;
    }
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "etcd list err:" << errCode;
        return false;
    }
    for (uint32_t i = 0; i < out.size(); i++) {
        Partition data;
        errCode = codec_->DecodePartitionData(out[i], &data);
        if (!errCode) {
            LOG(ERROR) << "DecodePartitionData err";
            return false;
        }
        PartitionIdType id = data.GetPartitionId();
        auto ret = partitionMap->emplace(id, std::move(data));
        if (!ret.second) {
            LOG(ERROR) << "LoadPartition: "
                       << "PartitionId duplicated, PartitionId = "
                       << id;
            return false;
        }
        if (*maxPartitionId < id) {
            *maxPartitionId = id;
        }
    }
    return true;
}

bool TopologyStorageEtcd::StoragePool(const Pool &data) {
    std::string key = codec_->EncodePoolKey(data.GetId());
    std::string value;
    bool ret = codec_->EncodePoolData(data, &value);
    if (!ret) {
        LOG(ERROR) << "EncodePoolData err"
                   << ", poolId = " << data.GetId();
        return false;
    }
    int errCode = client_->Put(key, value);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put Pool into etcd err"
                   << ", errcode = " << errCode
                   << ", poolId = " << data.GetId();
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

bool TopologyStorageEtcd::StorageMetaServer(const MetaServer &data) {
    std::string key = codec_->EncodeMetaServerKey(data.GetId());
    std::string value;
    bool ret = codec_->EncodeMetaServerData(data, &value);
    if (!ret) {
        LOG(ERROR) << "EncodeMetaServerData err"
                   << ", metaServerId = " << data.GetId();
        return false;
    }
    int errCode = client_->Put(key, value);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put MetaServer into etcd err"
                   << ", errcode = " << errCode
                   << ", metaServerId = " << data.GetId();
        return false;
    }
    return true;
}

bool TopologyStorageEtcd::StorageCopySet(const CopySetInfo &data) {
    CopySetKey id(data.GetPoolId(), data.GetId());
    std::string key = codec_->EncodeCopySetKey(id);
    std::string value;
    bool ret = codec_->EncodeCopySetData(data, &value);
    if (!ret) {
        LOG(ERROR) << "EncodeCopySetData err"
                   << ", logicalPoolId = " << data.GetPoolId()
                   << ", copysetId = " << data.GetId();
        return false;
    }
    int errCode = client_->Put(key, value);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put Copyset into etcd err"
                   << ", errcode = " << errCode
                   << ", logicalPoolId = " << data.GetPoolId()
                   << ", copysetId = " << data.GetId();
        return false;
    }
    return true;
}

bool TopologyStorageEtcd::StoragePartition(const Partition &data) {
    std::string key = codec_->EncodePartitionKey(data.GetPartitionId());
    std::string value;
    bool ret = codec_->EncodePartitionData(data, &value);
    if (!ret) {
        LOG(ERROR) << "EncodePartitionData err"
                   << ", partitionId = " << data.GetPartitionId();
        return false;
    }
    int errCode = client_->Put(key, value);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put Partition into etcd err"
                   << ", errcode = " << errCode
                   << ", partitionId = " << data.GetPartitionId();
        return false;
    }
    return true;
}

bool TopologyStorageEtcd::DeletePool(PoolIdType id) {
    std::string key = codec_->EncodePoolKey(id);
    int errCode = client_->Delete(key);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "delete Pool from etcd err"
                   << ", errcode = " << errCode
                   << ", poolId = " << id;
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

bool TopologyStorageEtcd::DeleteMetaServer(MetaServerIdType id) {
    std::string key = codec_->EncodeMetaServerKey(id);
    int errCode = client_->Delete(key);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "delete MetaServer from etcd err"
                   << ", errcode = " << errCode
                   << ", metaServerId = " << id;
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

bool TopologyStorageEtcd::DeletePartition(PartitionIdType id) {
    std::string key = codec_->EncodePartitionKey(id);
    int errCode = client_->Delete(key);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "delete Partition from etcd err"
                   << ", errcode = " << errCode
                   << ", partitionId = " << id;
        return false;
    }
    return true;
}

bool TopologyStorageEtcd::UpdatePool(const Pool &data) {
    return StoragePool(data);
}

bool TopologyStorageEtcd::UpdateZone(const Zone &data) {
    return StorageZone(data);
}

bool TopologyStorageEtcd::UpdateServer(const Server &data) {
    return StorageServer(data);
}

bool TopologyStorageEtcd::UpdateMetaServer(const MetaServer &data) {
    return StorageMetaServer(data);
}

bool TopologyStorageEtcd::UpdateCopySet(const CopySetInfo &data) {
    return StorageCopySet(data);
}

bool TopologyStorageEtcd::UpdatePartition(const Partition &data) {
    return StoragePartition(data);
}

bool TopologyStorageEtcd::UpdatePartitions(
    const std::vector<Partition> &datas) {
    if (datas.size() == 1) {
        return StoragePartition(datas[0]);
    }

    std::vector<Operation> ops;
    std::vector<std::string> keys(datas.size());
    std::vector<std::string> values(datas.size());

    for (uint32_t i = 0; i < datas.size(); i++) {
        keys[i] = codec_->EncodePartitionKey(datas[i].GetPartitionId());
        bool ret = codec_->EncodePartitionData(datas[i], &values[i]);
        if (!ret) {
            LOG(ERROR) << "EncodePartitionData err"
                    << ", partitionId = " << datas[i].GetPartitionId();
            return false;
        }

        Operation op {
            OpType::OpPut,
            const_cast<char*>(keys[i].data()),
            const_cast<char*>(values[i].data()),
            keys[i].size(),
            values[i].size()
        };
        ops.emplace_back(op);
    }

    int errCode = client_->TxnN(ops);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "UpdatePartitions failed when store txs."
                   << " error code = " << errCode;
        return false;
    }
    return true;
}

bool TopologyStorageEtcd::LoadClusterInfo(
    std::vector<ClusterInformation> *info) {
    std::string value;
    int errCode = client_->Get(CLUSTERINFOKEY, &value);
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
    std::string key = CLUSTERINFOKEY;
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
}  // namespace curvefs
