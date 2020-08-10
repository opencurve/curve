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

#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_STORAGE_CODEC_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_STORAGE_CODEC_H_

#include <string>

#include "src/common/namespace_define.h"
#include "src/mds/topology/topology_item.h"

using ::curve::common::TOPOLOGYITEMPRIFIX;

namespace curve {
namespace mds {
namespace topology {

class TopologyStorageCodec {
 public:
    // there are three types of function here:
    // Encode__Key: attach item id to item prefix
    // Encode__Data: convert data structure to a string
    // Decode__Data: convert a string to data structure
    std::string EncodeLogicalPoolKey(
        LogicalPoolIdType id);
    bool EncodeLogicalPoolData(
        const LogicalPool &data, std::string *value);
    bool DecodeLogicalPoolData(const std::string &value, LogicalPool *data);

    std::string EncodePhysicalPoolKey(
        PhysicalPoolIdType id);
    bool EncodePhysicalPoolData(
        const PhysicalPool &data, std::string *value);
    bool DecodePhysicalPoolData(const std::string &value, PhysicalPool *data);

    std::string EncodeZoneKey(
        ZoneIdType id);
    bool EncodeZoneData(
        const Zone &data, std::string *value);
    bool DecodeZoneData(const std::string &value, Zone *data);

    std::string EncodeServerKey(
        ServerIdType id);
    bool EncodeServerData(
        const Server &data, std::string *value);
    bool DecodeServerData(const std::string &value, Server *data);

    std::string EncodeChunkServerKey(
        ChunkServerIdType id);
    bool EncodeChunkServerData(
        const ChunkServer &data, std::string *value);
    bool DecodeChunkServerData(const std::string &value, ChunkServer *data);

    std::string EncodeCopySetKey(
        const CopySetKey &id);
    bool EncodeCopySetData(
        const CopySetInfo &data, std::string *value);
    bool DecodeCopySetData(const std::string &value, CopySetInfo *data);

    bool EncodeClusterInfoData(
        const ClusterInformation &data, std::string *value);
    bool DecodeCluserInfoData(const std::string &value,
        ClusterInformation *data);

 public:
    static std::string GetLogicalPoolKeyPrefix() {
        static std::string key = std::string(TOPOLOGYITEMPRIFIX) + "01";
        return key;
    }

    static std::string GetPhysicalPoolKeyPrefix() {
        static std::string key = std::string(TOPOLOGYITEMPRIFIX) + "02";
        return key;
    }

    static std::string GetZoneKeyPrefix() {
        static std::string key = std::string(TOPOLOGYITEMPRIFIX) + "03";
         return key;
    }

    static std::string GetServerKeyPrefix() {
        static std::string key = std::string(TOPOLOGYITEMPRIFIX) + "04";
        return key;
    }

    static std::string GetChunkServerKeyPrefix() {
        static std::string key = std::string(TOPOLOGYITEMPRIFIX) + "05";
        return key;
    }

    static std::string GetCopysetKeyPrefix() {
        static std::string key = std::string(TOPOLOGYITEMPRIFIX) + "06";
        return key;
    }

    static std::string GetClusterInfoKey() {
        static std::string key = std::string(TOPOLOGYITEMPRIFIX) + "07";
        return key;
    }

    static std::string GetLogicalPoolKeyEnd() {
        return GetPhysicalPoolKeyPrefix();
    }

    static std::string GetPhysicalPoolKeyEnd() {
        return GetZoneKeyPrefix();
    }

    static std::string GetZoneKeyEnd() {
        return GetServerKeyPrefix();
    }

    static std::string GetServerKeyEnd() {
        return GetChunkServerKeyPrefix();
    }

    static std::string GetChunkServerKeyEnd() {
        return GetCopysetKeyPrefix();
    }

    static std::string GetCopysetKeyEnd() {
        return GetClusterInfoKey();
    }
};


}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_STORAGE_CODEC_H_
