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
#include "src/mds/topology/topology_storage_codec.h"

#include <string>

#include "src/common/namespace_define.h"
#include "src/common/encode.h"


namespace curve {
namespace mds {
namespace topology {

std::string TopologyStorageCodec::EncodeLogicalPoolKey(
    LogicalPoolIdType id) {
    std::string key = TopologyStorageCodec::GetLogicalPoolKeyPrefix();
    size_t prefixLen = key.size();
    key.resize(prefixLen + sizeof(uint64_t));
    ::curve::common::EncodeBigEndian(&(key[prefixLen]), id);
    return key;
}

bool TopologyStorageCodec::EncodeLogicalPoolData(
    const LogicalPool &data, std::string *value) {
    return data.SerializeToString(value);
}

bool TopologyStorageCodec::DecodeLogicalPoolData(
    const std::string &value, LogicalPool *data) {
    return data->ParseFromString(value);
}

std::string TopologyStorageCodec::EncodePhysicalPoolKey(
    PhysicalPoolIdType id) {
    std::string key = TopologyStorageCodec::GetPhysicalPoolKeyPrefix();
    size_t prefixLen = key.size();
    key.resize(prefixLen + sizeof(uint64_t));
    ::curve::common::EncodeBigEndian(&(key[prefixLen]), id);
    return key;
}

bool TopologyStorageCodec::EncodePhysicalPoolData(
    const PhysicalPool &data, std::string *value) {
    return data.SerializeToString(value);
}

bool TopologyStorageCodec::DecodePhysicalPoolData(
    const std::string &value, PhysicalPool *data) {
    return data->ParseFromString(value);
}

std::string TopologyStorageCodec::EncodeZoneKey(
    ZoneIdType id) {
    std::string key = TopologyStorageCodec::GetZoneKeyPrefix();
    size_t prefixLen = key.size();
    key.resize(prefixLen + sizeof(uint64_t));
    ::curve::common::EncodeBigEndian(&(key[prefixLen]), id);
    return key;
}

bool TopologyStorageCodec::EncodeZoneData(
    const Zone &data, std::string *value) {
    return data.SerializeToString(value);
}

bool TopologyStorageCodec::DecodeZoneData(
    const std::string &value, Zone *data) {
    return data->ParseFromString(value);
}

std::string TopologyStorageCodec::EncodeServerKey(
    ServerIdType id) {
    std::string key = TopologyStorageCodec::GetServerKeyPrefix();
    size_t prefixLen = key.size();
    key.resize(prefixLen + sizeof(uint64_t));
    ::curve::common::EncodeBigEndian(&(key[prefixLen]), id);
    return key;
}

bool TopologyStorageCodec::EncodeServerData(
    const Server &data, std::string *value) {
    return data.SerializeToString(value);
}

bool TopologyStorageCodec::DecodeServerData(
    const std::string &value, Server *data) {
    return data->ParseFromString(value);
}

std::string TopologyStorageCodec::EncodeChunkServerKey(
    ChunkServerIdType id) {
    std::string key = TopologyStorageCodec::GetChunkServerKeyPrefix();
    size_t prefixLen = key.size();
    key.resize(prefixLen + sizeof(uint64_t));
    ::curve::common::EncodeBigEndian(&(key[prefixLen]), id);
    return key;
}

bool TopologyStorageCodec::EncodeChunkServerData(
    const ChunkServer &data, std::string *value) {
    return data.SerializeToString(value);
}

bool TopologyStorageCodec::DecodeChunkServerData(
    const std::string &value, ChunkServer *data) {
    return data->ParseFromString(value);
}

std::string TopologyStorageCodec::EncodeCopySetKey(
    const CopySetKey &id) {
    std::string key = TopologyStorageCodec::GetCopysetKeyPrefix();
    size_t prefixLen = key.size();
    key.resize(prefixLen + sizeof(uint64_t) + sizeof(uint64_t));
    ::curve::common::EncodeBigEndian(&(key[prefixLen]), id.first);
    ::curve::common::EncodeBigEndian(&(key[prefixLen + sizeof(id.first)]),
        id.second);
    return key;
}

bool TopologyStorageCodec::EncodeCopySetData(
    const CopySetInfo &data, std::string *value) {
    return data.SerializeToString(value);
}

bool TopologyStorageCodec::DecodeCopySetData(
    const std::string &value, CopySetInfo *data) {
    return data->ParseFromString(value);
}

bool TopologyStorageCodec::EncodeClusterInfoData(
    const ClusterInformation &data, std::string *value) {
    return data.SerializeToString(value);
}

bool TopologyStorageCodec::DecodeCluserInfoData(const std::string &value,
    ClusterInformation *data) {
    return data->ParseFromString(value);
}

}  // namespace topology
}  // namespace mds
}  // namespace curve

