/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2022-03-31
 * Author: Jingli Chen (Wine93)
 */

#include <glog/logging.h>

#include <vector>
#include <limits>
#include <iomanip>
#include <iostream>

#include "src/common/string_util.h"
#include "curvefs/src/metaserver/storage_common.h"

namespace curvefs {
namespace metaserver {

using ::curve::common::StringToUl;
using ::curve::common::StringToUll;
using ::curve::common::SplitString;
using ::curvefs::common::PartitionInfo;

static bool CompareType(const std::string& str, KEY_TYPE keyType) {
    uint32_t n;
    return StringToUl(str, &n) && n == keyType;
}

Key4Inode::Key4Inode()
    : fsId(0), inodeId(0) {}

Key4Inode::Key4Inode(uint32_t fsId, uint64_t inodeId)
        : fsId(fsId), inodeId(inodeId) {}

Key4Inode::Key4Inode(const Inode& inode):
    fsId(inode.fsid()), inodeId(inode.inodeid()) {}

bool Key4Inode::operator==(const Key4Inode& rhs) {
    return fsId == rhs.fsId && inodeId == rhs.inodeId;
}

std::string Key4Inode::SerializeToString() const {
    std::ostringstream oss;
    oss << keyType_ << ":" << fsId << ":" << inodeId;
    return oss.str();
}

bool Key4Inode::ParseFromString(const std::string& value) {
    std::vector<std::string> items;
    SplitString(value, ":", &items);
    return items.size() == 3 && CompareType(items[0], keyType_) &&
        StringToUl(items[1], &fsId) && StringToUll(items[2], &inodeId);
}

std::string Prefix4AllInode::SerializeToString() const {
    std::ostringstream oss;
    oss << keyType_ << ":";
    return oss.str();
}

bool Prefix4AllInode::ParseFromString(const std::string& value) {
    std::vector<std::string> items;
    SplitString(value, ":", &items);
    return items.size() == 1 && CompareType(items[0], keyType_);
}

const size_t Key4S3ChunkInfoList::kMaxUint64Length_ =
    std::to_string(std::numeric_limits<uint64_t>::max()).size();

Key4S3ChunkInfoList::Key4S3ChunkInfoList()
    : fsId(0),
      inodeId(0),
      chunkIndex(0),
      firstChunkId(0),
      lastChunkId(0) {}

Key4S3ChunkInfoList::Key4S3ChunkInfoList(uint32_t fsId,
                                         uint64_t inodeId,
                                         uint64_t chunkIndex,
                                         uint64_t firstChunkId,
                                         uint64_t lastChunkId)
    : fsId(fsId),
      inodeId(inodeId),
      chunkIndex(chunkIndex),
      firstChunkId(firstChunkId),
      lastChunkId(lastChunkId) {}

std::string Key4S3ChunkInfoList::SerializeToString() const {
    std::ostringstream oss;
    oss << keyType_ << ":" << fsId << ":"
        << inodeId << ":" << chunkIndex << ":"
        << std::setw(kMaxUint64Length_) << std::setfill('0') << firstChunkId
        << ":"
        << std::setw(kMaxUint64Length_) << std::setfill('0') << lastChunkId;
    return oss.str();
}

bool Key4S3ChunkInfoList::ParseFromString(const std::string& value) {
    std::vector<std::string> items;
    SplitString(value, ":", &items);
    return items.size() == 6 && CompareType(items[0], keyType_) &&
        StringToUl(items[1], &fsId) && StringToUll(items[2], &inodeId) &&
        StringToUll(items[3], &chunkIndex) &&
        StringToUll(items[4], &firstChunkId) &&
        StringToUll(items[5], &lastChunkId);
}

Prefix4ChunkIndexS3ChunkInfoList::Prefix4ChunkIndexS3ChunkInfoList()
    : fsId(0), inodeId(0), chunkIndex(0) {}

Prefix4ChunkIndexS3ChunkInfoList::Prefix4ChunkIndexS3ChunkInfoList(
    uint32_t fsId,
    uint64_t inodeId,
    uint64_t chunkIndex)
    : fsId(fsId), inodeId(inodeId), chunkIndex(chunkIndex) {}

std::string Prefix4ChunkIndexS3ChunkInfoList::SerializeToString() const {
    std::ostringstream oss;
    oss << keyType_ << ":" << fsId << ":" << inodeId << ":"
        << chunkIndex << ":";
    return oss.str();
}

bool Prefix4ChunkIndexS3ChunkInfoList::ParseFromString(
    const std::string& value) {
    std::vector<std::string> items;
    SplitString(value, ":", &items);
    return items.size() == 4 && CompareType(items[0], keyType_) &&
        StringToUl(items[1], &fsId) && StringToUll(items[2], &inodeId) &&
        StringToUll(items[3], &chunkIndex);
}

Prefix4InodeS3ChunkInfoList::Prefix4InodeS3ChunkInfoList()
    : fsId(0), inodeId(0) {}

Prefix4InodeS3ChunkInfoList::Prefix4InodeS3ChunkInfoList(uint32_t fsId,
                                                         uint64_t inodeId)
    : fsId(fsId), inodeId(inodeId) {}

std::string Prefix4InodeS3ChunkInfoList::SerializeToString() const {
    std::ostringstream oss;
    oss << keyType_ << ":" << fsId << ":" << inodeId << ":";
    return oss.str();
}

bool Prefix4InodeS3ChunkInfoList::ParseFromString(const std::string& value) {
    std::vector<std::string> items;
    SplitString(value, ":", &items);
    return items.size() == 3 && CompareType(items[0], keyType_) &&
        StringToUl(items[1], &fsId) && StringToUll(items[2], &inodeId);
}

std::string Prefix4AllS3ChunkInfoList::SerializeToString() const {
    std::ostringstream oss;
    oss << kTypeS3ChunkInfo << ":";
    return oss.str();
}

bool Prefix4AllS3ChunkInfoList::ParseFromString(const std::string& value) {
    std::vector<std::string> items;
    SplitString(value, ":", &items);
    return items.size() == 1 && CompareType(items[0], keyType_);
}

std::string Converter::SerializeToString(const StorageKey& key) {
    return key.SerializeToString();
}

bool Converter::SerializeToString(const google::protobuf::Message& entry,
                                  std::string* value) {
    if (!entry.IsInitialized()) {
        LOG(ERROR) << "Message not initialized";
        return false;
    }
    return entry.SerializeToString(value);
}

}  // namespace metaserver
}  // namespace curvefs
