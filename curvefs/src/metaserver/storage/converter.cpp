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

#include <inttypes.h>
#include <glog/logging.h>

#include <cstring>
#include <string>
#include <vector>
#include <limits>
#include <iomanip>
#include <iostream>
#include <iterator>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "src/common/string_util.h"
#include "curvefs/src/metaserver/storage/converter.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curve::common::StringToUl;
using ::curve::common::StringToUll;
using ::curve::common::SplitString;
using ::curvefs::common::PartitionInfo;

static const char* const kDelimiter = ":";

static bool CompareType(const std::string& str, KEY_TYPE keyType) {
    uint32_t n;
    return StringToUl(str, &n) && n == keyType;
}

NameGenerator::NameGenerator(uint32_t partitionId)
    : tableName4Inode_(Format(kTypeInode, partitionId)),
      tableName4DeallocatableIndoe_(
          Format(kTypeDeallocatableInode, partitionId)),
      tableName4DeallocatableBlockGroup_(
          Format(kTypeDeallocatableBlockGroup, partitionId)),
      tableName4S3ChunkInfo_(Format(kTypeS3ChunkInfo, partitionId)),
      tableName4Dentry_(Format(kTypeDentry, partitionId)),
      tableName4VolumeExtent_(Format(kTypeVolumeExtent, partitionId)),
      tableName4InodeAuxInfo_(Format(kTypeInodeAuxInfo, partitionId)) {}

std::string NameGenerator::GetInodeTableName() const {
    return tableName4Inode_;
}

std::string NameGenerator::GetDeallocatableInodeTableName() const {
    return tableName4DeallocatableIndoe_;
}

std::string NameGenerator::GetDeallocatableBlockGroupTableName() const {
    return tableName4DeallocatableBlockGroup_;
}

std::string NameGenerator::GetS3ChunkInfoTableName() const {
    return tableName4S3ChunkInfo_;
}

std::string NameGenerator::GetDentryTableName() const {
    return tableName4Dentry_;
}

std::string NameGenerator::GetVolumeExtentTableName() const {
    return tableName4VolumeExtent_;
}

std::string NameGenerator::GetInodeAuxInfoTableName() const {
    return tableName4InodeAuxInfo_;
}

size_t NameGenerator::GetFixedLength() {
    size_t length = sizeof(kTypeInode) + sizeof(uint32_t) + strlen(kDelimiter);
    LOG(INFO) << "Tablename fixed length is " << length;
    return length;
}

std::string NameGenerator::Format(KEY_TYPE type, uint32_t partitionId) {
    char buf[sizeof(partitionId)];
    std::memcpy(buf, reinterpret_cast<char*>(&partitionId),
        sizeof(buf));
    return absl::StrCat(type, kDelimiter,
        absl::string_view(buf, sizeof(buf)));
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
    return absl::StrCat(keyType_, ":", fsId, ":", inodeId);
}

bool Key4Inode::ParseFromString(const std::string& value) {
    std::vector<std::string> items;
    SplitString(value, ":", &items);
    return items.size() == 3 && CompareType(items[0], keyType_) &&
        StringToUl(items[1], &fsId) && StringToUll(items[2], &inodeId);
}

std::string Prefix4AllInode::SerializeToString() const {
    return absl::StrCat(keyType_, ":");
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
      lastChunkId(0),
      size(0) {}

Key4S3ChunkInfoList::Key4S3ChunkInfoList(uint32_t fsId,
                                         uint64_t inodeId,
                                         uint64_t chunkIndex,
                                         uint64_t firstChunkId,
                                         uint64_t lastChunkId,
                                         uint64_t size)
    : fsId(fsId),
      inodeId(inodeId),
      chunkIndex(chunkIndex),
      firstChunkId(firstChunkId),
      lastChunkId(lastChunkId),
      size(size) {}

std::string Key4S3ChunkInfoList::SerializeToString() const {
    return absl::StrCat(keyType_, ":", fsId, ":", inodeId, ":",
        chunkIndex, ":", absl::StrFormat("%020" PRIu64"", firstChunkId), ":",
        absl::StrFormat("%020" PRIu64"", lastChunkId), ":", size);
}

bool Key4S3ChunkInfoList::ParseFromString(const std::string& value) {
    std::vector<std::string> items;
    SplitString(value, ":", &items);
    return items.size() == 7 && CompareType(items[0], keyType_) &&
        StringToUl(items[1], &fsId) && StringToUll(items[2], &inodeId) &&
        StringToUll(items[3], &chunkIndex) &&
        StringToUll(items[4], &firstChunkId) &&
        StringToUll(items[5], &lastChunkId) &&
        StringToUll(items[6], &size);
}

Prefix4ChunkIndexS3ChunkInfoList::Prefix4ChunkIndexS3ChunkInfoList()
    : fsId(0), inodeId(0), chunkIndex(0) {}

Prefix4ChunkIndexS3ChunkInfoList::Prefix4ChunkIndexS3ChunkInfoList(
    uint32_t fsId,
    uint64_t inodeId,
    uint64_t chunkIndex)
    : fsId(fsId), inodeId(inodeId), chunkIndex(chunkIndex) {}

std::string Prefix4ChunkIndexS3ChunkInfoList::SerializeToString() const {
    return absl::StrCat(keyType_, ":", fsId, ":", inodeId, ":",
        chunkIndex, ":");
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
    return absl::StrCat(keyType_, ":", fsId, ":", inodeId, ":");
}

bool Prefix4InodeS3ChunkInfoList::ParseFromString(const std::string& value) {
    std::vector<std::string> items;
    SplitString(value, ":", &items);
    return items.size() == 3 && CompareType(items[0], keyType_) &&
        StringToUl(items[1], &fsId) && StringToUll(items[2], &inodeId);
}

std::string Prefix4AllS3ChunkInfoList::SerializeToString() const {
    return absl::StrCat(kTypeS3ChunkInfo, ":");
}

bool Prefix4AllS3ChunkInfoList::ParseFromString(const std::string& value) {
    std::vector<std::string> items;
    SplitString(value, ":", &items);
    return items.size() == 1 && CompareType(items[0], keyType_);
}

Key4Dentry::Key4Dentry(uint32_t fsId,
                       uint64_t parentInodeId,
                       const std::string& name)
    : fsId(fsId), parentInodeId(parentInodeId), name(name) {}

std::string Key4Dentry::SerializeToString() const {
    return absl::StrCat(keyType_, kDelimiter, fsId,
                        kDelimiter, parentInodeId,
                        kDelimiter, name);
}

bool Key4Dentry::ParseFromString(const std::string& value) {
    std::vector<std::string> items;
    SplitString(value, ":", &items);
    if (items.size() < 3 ||
        !CompareType(items[0], keyType_) ||
        !StringToUl(items[1], &fsId) ||
        !StringToUll(items[2], &parentInodeId)) {
        return false;
    }

    size_t prefixLength = items[0].size() +
                          items[1].size() +
                          items[2].size() +
                          3 * strlen(kDelimiter);
    if (value.size() < prefixLength) {
        return false;
    }
    name = value.substr(prefixLength);
    return true;
}

Prefix4SameParentDentry::Prefix4SameParentDentry(uint32_t fsId,
                                                 uint64_t parentInodeId)
    : fsId(fsId), parentInodeId(parentInodeId) {}

std::string Prefix4SameParentDentry::SerializeToString() const {
    return absl::StrCat(keyType_, kDelimiter, fsId,
                        kDelimiter, parentInodeId,
                        kDelimiter);
}

bool Prefix4SameParentDentry::ParseFromString(const std::string& value) {
    std::vector<std::string> items;
    SplitString(value, ":", &items);
    return items.size() == 3 && CompareType(items[0], keyType_) &&
           StringToUl(items[1], &fsId) && StringToUll(items[2], &parentInodeId);
}

std::string Prefix4AllDentry::SerializeToString() const {
    return absl::StrCat(keyType_, ":");
}

bool Prefix4AllDentry::ParseFromString(const std::string& value) {
    std::vector<std::string> items;
    SplitString(value, ":", &items);
    return items.size() == 1 && CompareType(items[0], keyType_);
}

Key4VolumeExtentSlice::Key4VolumeExtentSlice(uint32_t fsId,
                                             uint64_t inodeId,
                                             uint64_t offset)
    : fsId_(fsId), inodeId_(inodeId), offset_(offset) {}

std::string Key4VolumeExtentSlice::SerializeToString() const {
    return absl::StrCat(keyType_, kDelimiter, fsId_, kDelimiter, inodeId_,
                        kDelimiter, offset_);
}

bool Key4VolumeExtentSlice::ParseFromString(const std::string& value) {
    // TODO(wuhanqing): reduce unnecessary creation of temporary strings,
    //                  but, currently, `absl::from_chars` only support floating
    //                  point
    std::vector<std::string> items;
    SplitString(value, kDelimiter, &items);
    return items.size() == 4 && CompareType(items[0], keyType_) &&
           StringToUl(items[1], &fsId_) && StringToUll(items[2], &inodeId_) &&
           StringToUll(items[3], &offset_);
}

Prefix4InodeVolumeExtent::Prefix4InodeVolumeExtent(uint32_t fsId,
                                                   uint64_t inodeId)
    : fsId_(fsId), inodeId_(inodeId) {}

std::string Prefix4InodeVolumeExtent::SerializeToString() const {
    return absl::StrCat(keyType_, kDelimiter, fsId_, kDelimiter, inodeId_,
                        kDelimiter);
}

bool Prefix4InodeVolumeExtent::ParseFromString(const std::string &value) {
    std::vector<std::string> items;
    SplitString(value, kDelimiter, &items);
    return items.size() == 3 && CompareType(items[0], keyType_) &&
           StringToUl(items[1], &fsId_) && StringToUll(items[2], &inodeId_);
}

std::string Prefix4AllVolumeExtent::SerializeToString() const {
    return absl::StrCat(keyType_, kDelimiter);
}

bool Prefix4AllVolumeExtent::ParseFromString(const std::string &value) {
    std::vector<std::string> items;
    SplitString(value, kDelimiter, &items);
    return items.size() == 1 && CompareType(items[0], keyType_);
}

Key4InodeAuxInfo::Key4InodeAuxInfo(uint32_t fsId,
                                   uint64_t inodeId)
    : fsId(fsId), inodeId(inodeId) {}

std::string Key4InodeAuxInfo::SerializeToString() const {
    return absl::StrCat(keyType_, kDelimiter, fsId, kDelimiter, inodeId);
}

bool Key4InodeAuxInfo::ParseFromString(const std::string& value) {
    std::vector<std::string> items;
    SplitString(value, kDelimiter, &items);
    return items.size() == 3 && CompareType(items[0], keyType_) &&
           StringToUl(items[1], &fsId) && StringToUll(items[2], &inodeId);
}

std::string Key4DeallocatableBlockGroup::SerializeToString() const {
    return absl::StrCat(keyType_, kDelimiter, fsId, kDelimiter, volumeOffset);
}


bool Key4DeallocatableBlockGroup::ParseFromString(const std::string &value) {
    std::vector<std::string> items;
    SplitString(value, kDelimiter, &items);
    return items.size() == 3 && CompareType(items[0], keyType_) &&
           StringToUl(items[1], &fsId) && StringToUll(items[2], &volumeOffset);
}

std::string Key4DeallocatableInode::SerializeToString() const {
    return absl::StrCat(keyType_, kDelimiter, fsId, kDelimiter, inodeId);
}

bool Key4DeallocatableInode::ParseFromString(const std::string &value) {
    std::vector<std::string> items;
    SplitString(value, kDelimiter, &items);
    return items.size() == 3 && CompareType(items[0], keyType_) &&
           StringToUl(items[1], &fsId) && StringToUll(items[2], &inodeId);
}

std::string Prefix4AllDeallocatableBlockGroup::SerializeToString() const {
    return absl::StrCat(keyType_, ":");
}

bool Prefix4AllDeallocatableBlockGroup::ParseFromString(
    const std::string &value) {
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
}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
