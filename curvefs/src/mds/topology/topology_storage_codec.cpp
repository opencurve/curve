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
 * Created Date: 2021-08-25
 * Author: wanghai01
 */
#include "curvefs/src/mds/topology/topology_storage_codec.h"

#include <string>
#include "curvefs/src/mds/common/storage_key.h"

namespace curvefs {
namespace mds {
namespace topology {

std::string TopologyStorageCodec::EncodePoolKey(PoolIdType id) {
    std::string key = POOLKEYPREFIX;
    size_t prefixLen = TOPOLOGY_PREFIX_LENGTH;
    key.resize(prefixLen + sizeof(uint64_t));
    EncodeBigEndian(&(key[prefixLen]), id);
    return key;
}

bool TopologyStorageCodec::EncodePoolData(const Pool &data,
                                          std::string *value) {
    return data.SerializeToString(value);
}

bool TopologyStorageCodec::DecodePoolData(const std::string &value,
                                          Pool *data) {
    return data->ParseFromString(value);
}

std::string TopologyStorageCodec::EncodeZoneKey(ZoneIdType id) {
    std::string key = ZONEKEYPREFIX;
    size_t prefixLen = TOPOLOGY_PREFIX_LENGTH;
    key.resize(prefixLen + sizeof(uint64_t));
    EncodeBigEndian(&(key[prefixLen]), id);
    return key;
}

bool TopologyStorageCodec::EncodeZoneData(const Zone &data,
                                          std::string *value) {
    return data.SerializeToString(value);
}

bool TopologyStorageCodec::DecodeZoneData(const std::string &value,
                                          Zone *data) {
    return data->ParseFromString(value);
}

std::string TopologyStorageCodec::EncodeServerKey(ServerIdType id) {
    std::string key = SERVERKEYPREFIX;
    size_t prefixLen = TOPOLOGY_PREFIX_LENGTH;
    key.resize(prefixLen + sizeof(uint64_t));
    EncodeBigEndian(&(key[prefixLen]), id);
    return key;
}

bool TopologyStorageCodec::EncodeServerData(const Server &data,
                                            std::string *value) {
    return data.SerializeToString(value);
}

bool TopologyStorageCodec::DecodeServerData(const std::string &value,
                                            Server *data) {
    return data->ParseFromString(value);
}

std::string TopologyStorageCodec::EncodeMetaServerKey(MetaServerIdType id) {
    std::string key = METASERVERKEYPREFIX;
    size_t prefixLen = TOPOLOGY_PREFIX_LENGTH;
    key.resize(prefixLen + sizeof(uint64_t));
    EncodeBigEndian(&(key[prefixLen]), id);
    return key;
}

bool TopologyStorageCodec::EncodeMetaServerData(const MetaServer &data,
                                                std::string *value) {
    return data.SerializeToString(value);
}

bool TopologyStorageCodec::DecodeMetaServerData(const std::string &value,
                                                MetaServer *data) {
    return data->ParseFromString(value);
}

std::string TopologyStorageCodec::EncodeCopySetKey(const CopySetKey &id) {
    std::string key = COPYSETKEYPREFIX;
    size_t prefixLen = TOPOLOGY_PREFIX_LENGTH;
    key.resize(prefixLen + sizeof(uint64_t) + sizeof(uint64_t));
    EncodeBigEndian(&(key[prefixLen]), id.first);
    EncodeBigEndian(&(key[prefixLen + sizeof(uint64_t)]), id.second);
    return key;
}

bool TopologyStorageCodec::EncodeCopySetData(const CopySetInfo &data,
                                             std::string *value) {
    return data.SerializeToString(value);
}

bool TopologyStorageCodec::DecodeCopySetData(const std::string &value,
                                             CopySetInfo *data) {
    return data->ParseFromString(value);
}

std::string TopologyStorageCodec::EncodePartitionKey(PartitionIdType id) {
    std::string key = PARTITIONKEYPREFIX;
    size_t prefixLen = TOPOLOGY_PREFIX_LENGTH;
    key.resize(prefixLen + sizeof(uint64_t));
    EncodeBigEndian(&(key[prefixLen]), id);
    return key;
}

bool TopologyStorageCodec::EncodePartitionData(const Partition &data,
                                               std::string *value) {
    return data.SerializeToString(value);
}

bool TopologyStorageCodec::DecodePartitionData(const std::string &value,
                                               Partition *data) {
    return data->ParseFromString(value);
}

bool TopologyStorageCodec::EncodeClusterInfoData(const ClusterInformation &data,
                                                 std::string *value) {
    return data.SerializeToString(value);
}

bool TopologyStorageCodec::DecodeClusterInfoData(const std::string &value,
                                                ClusterInformation *data) {
    return data->ParseFromString(value);
}

std::string TopologyStorageCodec::EncodeMemcacheClusterKey(
    MetaServerIdType id) {
    std::string key = MEMCACHECLUSTERKEYPREFIX;
    size_t prefixLen = TOPOLOGY_PREFIX_LENGTH;
    key.resize(prefixLen + sizeof(uint64_t));
    EncodeBigEndian(&(key[prefixLen]), id);
    return key;
}

bool TopologyStorageCodec::EncodeMemcacheClusterData(
    const MemcacheCluster& data, std::string* value) {
    return data.SerializeToString(value);
}

bool TopologyStorageCodec::DecodeMemcacheClusterData(const std::string& value,
                                                     MemcacheCluster* data) {
    return data->ParseFromString(value);
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
