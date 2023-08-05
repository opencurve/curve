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

using ::curve::common::LOGICALPOOLKEYPREFIX;
using ::curve::common::LOGICALPOOLKEYEND;
using ::curve::common::PHYSICALPOOLKEYPREFIX;
using ::curve::common::PHYSICALPOOLKEYEND;
using ::curve::common::ZONEKEYPREFIX;
using ::curve::common::ZONEKEYEND;
using ::curve::common::SERVERKEYPREFIX;
using ::curve::common::SERVERKEYEND;
using ::curve::common::CHUNKSERVERKEYPREFIX;
using ::curve::common::CHUNKSERVERKEYEND;
using ::curve::common::CLUSTERINFOKEY;
using ::curve::common::COPYSETKEYPREFIX;
using ::curve::common::COPYSETKEYEND;
using ::curve::common::POOLSETKEYPREFIX;
using ::curve::common::POOLSETKEYEND;

namespace curve {
namespace mds {
namespace topology {

class TopologyStorageCodec {
 public:
    // there are three types of function here:
    // Encode__Key: attach item id to item prefix
    // Encode__Data: convert data structure to a string
    // Decode__Data: convert a string to data structure

    std::string EncodePoolsetKey(PoolsetIdType id);
    bool EncodePoolsetData(const Poolset &data, std::string *value);
    bool DecodePoolsetData(const std::string &value, Poolset *data);

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
};


}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_STORAGE_CODEC_H_
