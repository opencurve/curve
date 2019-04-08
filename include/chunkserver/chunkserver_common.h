/*
 * Project: curve
 * Created Date: Thursday August 30th 2018
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef INCLUDE_CHUNKSERVER_CHUNKSERVER_COMMON_H_
#define INCLUDE_CHUNKSERVER_CHUNKSERVER_COMMON_H_

#include <braft/configuration.h>
#include <braft/file_system_adaptor.h>

#include <cstdint>
#include <string>

namespace curve {
namespace chunkserver {

/* for IDs */
using LogicPoolID   = uint32_t;
using CopysetID     = uint32_t;
using ChunkID       = uint64_t;
using SnapshotID    = uint64_t;
using SequenceNum   = uint64_t;

using ChunkSizeType = uint32_t;
using PageSizeType  = uint32_t;

using GroupNid      = uint64_t;
using ChunkServerID = uint32_t;

// braft
using Configuration = braft::Configuration;
using GroupId = braft::GroupId;
using PeerId = braft::PeerId;
using Node = braft::Node;
using NodeOptions = braft::NodeOptions;
using FileSystemAdaptor = braft::FileSystemAdaptor;
using DirReader = braft::DirReader;
using PosixFileSystemAdaptor = braft::PosixFileSystemAdaptor;

/**
 *  将(LogicPoolID, CopysetID)二元组转换成数字格式的复制组ID,格式如下：
 *  |            group id           |
 *  |     32         |      32      |
 *  | logic pool id  |  copyset id  |
 */
inline GroupNid ToGroupNid(const LogicPoolID &logicPoolId,
                         const CopysetID &copysetId) {
    return (static_cast<uint64_t>(logicPoolId) << 32) | copysetId;
}
/**
 *  将(LogicPoolID, CopysetID)二元组转换成字符串格式的复制组ID
 */
inline GroupId ToGroupId(const LogicPoolID &logicPoolId,
                         const CopysetID &copysetId) {
    return std::to_string(ToGroupNid(logicPoolId, copysetId));
}
#define ToBraftGroupId   ToGroupId

/**
 *  从数字格式的复制组ID中解析LogicPoolID
 */
inline LogicPoolID GetPoolID(const GroupNid &groupId) {
    return groupId >> 32;
}
/**
 *  从数字格式的复制组ID中解析CopysetID
 */
inline CopysetID GetCopysetID(const GroupNid &groupId) {
    return groupId & (((uint64_t)1 << 32) - 1);
}

/* 格式输出 group id 的 字符串 (logicPoolId, copysetId) */
inline std::string ToGroupIdString(const LogicPoolID &logicPoolId,
                                   const CopysetID &copysetId) {
    std::string groupIdString;
    groupIdString.append("(");
    groupIdString.append(std::to_string(logicPoolId));
    groupIdString.append(", ");
    groupIdString.append(std::to_string(copysetId));
    groupIdString.append(")");
    return groupIdString;
}
#define ToGroupIdStr   ToGroupIdString

}  // namespace chunkserver
}  // namespace curve

#endif  // INCLUDE_CHUNKSERVER_CHUNKSERVER_COMMON_H_
