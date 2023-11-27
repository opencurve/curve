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
 * Created Date: Thursday August 30th 2018
 * Author: tongguangxun
 */

#ifndef INCLUDE_CHUNKSERVER_CHUNKSERVER_COMMON_H_
#define INCLUDE_CHUNKSERVER_CHUNKSERVER_COMMON_H_

#include <braft/configuration.h>
#include <braft/file_system_adaptor.h>
#include <braft/raft.h>
#include <braft/snapshot_throttle.h>

#include <cstdint>
#include <string>

namespace curve {
namespace chunkserver {

/* for IDs */
using LogicPoolID = uint32_t;
using CopysetID = uint32_t;
using ChunkID = uint64_t;
using SnapshotID = uint64_t;
using SequenceNum = uint64_t;

using ChunkSizeType = uint32_t;
using PageSizeType = uint32_t;

using GroupNid = uint64_t;
using ChunkServerID = uint32_t;

// braft
using Configuration = braft::Configuration;
using GroupId = braft::GroupId;
using PeerId = braft::PeerId;
using Node = braft::Node;
using NodeOptions = braft::NodeOptions;
using NodeStatus = braft::NodeStatus;
using FileSystemAdaptor = braft::FileSystemAdaptor;
using DirReader = braft::DirReader;
using PosixFileSystemAdaptor = braft::PosixFileSystemAdaptor;
using SnapshotThrottle = braft::SnapshotThrottle;
using ThroughputSnapshotThrottle = braft::ThroughputSnapshotThrottle;

// TODO(lixiaocui): Consider how to proceed with subsequent unit testing or
// validation
/*
 * IO performance statistics composite metric type
 */
struct IoPerfMetric {
    uint64_t readCount;
    uint64_t writeCount;
    uint64_t readBytes;
    uint64_t writeBytes;
    uint64_t readIops;
    uint64_t writeIops;
    uint64_t readBps;
    uint64_t writeBps;
};

/**
 * Convert the (LogicPoolID, CopysetID) binary into a copy group ID in numerical
 * format, as follows:
 *  |            group id           |
 *  |     32         |      32      |
 *  | logic pool id  |  copyset id  |
 */
inline GroupNid ToGroupNid(const LogicPoolID& logicPoolId,
                           const CopysetID& copysetId) {
    return (static_cast<uint64_t>(logicPoolId) << 32) | copysetId;
}
/**
 * Convert the (LogicPoolID, CopysetID) binary to a copy group ID in string
 * format
 */
inline GroupId ToGroupId(const LogicPoolID& logicPoolId,
                         const CopysetID& copysetId) {
    return std::to_string(ToGroupNid(logicPoolId, copysetId));
}
#define ToBraftGroupId ToGroupId

/**
 * Parsing LogicPoolID from Copy Group ID in Numeric Format
 */
inline LogicPoolID GetPoolID(const GroupNid& groupId) { return groupId >> 32; }
/**
 * Parsing CopysetID from Copy Group ID in Numeric Format
 */
inline CopysetID GetCopysetID(const GroupNid& groupId) {
    return groupId & (((uint64_t)1 << 32) - 1);
}

/* Format output string for group ID (logicPoolId, copysetId) */
inline std::string ToGroupIdString(const LogicPoolID& logicPoolId,
                                   const CopysetID& copysetId) {
    std::string groupIdString;
    groupIdString.append("(");
    groupIdString.append(std::to_string(logicPoolId));
    groupIdString.append(", ");
    groupIdString.append(std::to_string(copysetId));
    groupIdString.append(", ");
    groupIdString.append(ToGroupId(logicPoolId, copysetId));
    groupIdString.append(")");
    return groupIdString;
}
#define ToGroupIdStr ToGroupIdString

// Meta page is header of chunkfile, and is used to store meta data of
// chunkfile.
// Currently, we need to ensure the atomicity of the meta page update, so set
// its size to 4k.
constexpr size_t kChunkfileMetaPageSize = 4096;

}  // namespace chunkserver
}  // namespace curve

#endif  // INCLUDE_CHUNKSERVER_CHUNKSERVER_COMMON_H_
