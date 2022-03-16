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
 * Created Date: 2022-03-16
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_METASERVER_STORAGE_CONVERTER_H_
#define CURVEFS_SRC_METASERVER_STORAGE_CONVERTER_H_

#include <glog/logging.h>

#include <string>
#include <limits>
#include <iomanip>
#include <iostream>

#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "src/common/string_util.h"

namespace curvefs {
namespace metaserver {

using ::curve::common::StringToUl;
using ::curve::common::StringToUll;
using ::curve::common::SplitString;
using ::curvefs::common::PartitionInfo;

enum KEY_TYPE : unsigned char {
    kTypeInode = 0,
    kTypeS3ChunkInfo = 1,
    kTypeDentry = 2,
};

struct Key4Inode {
    uint32_t fsId;
    uint64_t inodeId;
};

struct Prefix4AllInode {};

 struct Key4S3ChunkInfoList {
     uint32_t fsId;
     uint64_t inodeId;
     uint64_t chunkIndex;
     uint64_t firstChunkId;
     uint64_t lastChunkId;
 };

 struct Prefix4ChunkIndexS3ChunkInfoList {
     uint32_t fsId;
     uint64_t inodeId;
     uint64_t chunkIndex;
 };

 struct Prefix4InodeS3ChunkInfoList {
     uint32_t fsId;
     uint64_t inodeId;
 };

 struct Prefix4AllS3ChunkInfoList {};

class Converter {
 public:
    Converter();

    // key
    std::string SerializeToString(Key4Inode key);

    std::string SerializeToString(Key4AllInode key);

    std::string SerializeToString(Key4S3ChunkInfoList key);

    std::string SerializeToString(Prefix4ChunkIndexS3ChunkInfoList key);

    std::string SerializeToString(Prefix4InodeS3ChunkInfoList key);

    std::string SerializeToString(Prefix4AllS3ChunkInfoList key);

    bool PraseFromString(const std::string& s, Key4Inode* key);

    bool PraseFromString(const std::string& s, Key4S3ChunkInfoList* key);

    // value
    bool SerializeToString(PartitionInfo& info, std::string* value);

    bool SerializeToString(Inode& inode, std::string* value);

    bool SerializeToString(Dentry& dentry, std::string* value);

    bool SerializeToString(S3ChunkInfoList& list, std::string* value);

    bool SerializeToString(PrepareRenameTxRequest& request, std::string* value);

    bool PraseFromString(std::string& value, PartitionInfo* info);

    bool PraseFromString(std::string& value, Inode* inode);

    bool PraseFromString(std::string& value, Dentry* dentry);

    bool PraseFromString(std::string& value, S3ChunkInfoList* list);

    bool PraseFromString(std::string& value, PrepareRenameTxRequest* request);

 private:
    static const size_t kMaxUint64Length_;
};

const size_t Converter::kMaxUint64Length_ =
    std::to_string(std::numeric_limits<uint64_t>::max()).size();

// key
// kTypeInode:fsId:InodeId
inline std::string Converter::SerializeToString(Key4Inode key) {
    std::ostringstream oss;
    oss << kTypeInode << ":" << key.fsId << ":" << key.inodeId;
    return oss.str();
}

// kTypeInode:
inline std::string Converter::SerializeToString(Key4AllInode key) {
    std::ostringstream oss;
    oss << kTypeInode << ":";
    return oss.str();
}

// kTypeS3ChunkInfo:fsId:inodeId:chunkIndex:firstChunkId:lastChunkId
inline std::string Converter::SerializeToString(Key4S3ChunkInfoList key) {
    std::ostringstream oss;
    oss << kTypeS3ChunkInfo << ":" << key.fsId << ":"
        << key.inodeId << ":" << key.chunkIndex << ":"
        << std::setw(kMaxUint64Length_) << std::setfill('0') << key.firstChunkId
        << std::setw(kMaxUint64Length_) << std::setfill('0') << key.lastChunkId;
    return oss.str();
}

// kTypeS3ChunkInfo:fsId:inodeId:chunkIndex:
inline std::string Converter::SerializeToString(
    Prefix4ChunkIndexS3ChunkInfoList key) {
    std::ostringstream oss;
    oss << kTypeS3ChunkInfo << ":" << key.fsId << ":" << key.inodeId << ":"
        << key.chunkIndex << ":";
    return oss.str();
}

// kTypeS3ChunkInfo:fsId:inodeId:
inline std::string Converter::SerializeToString(
    Prefix4InodeS3ChunkInfoList key) {
    std::ostringstream oss;
    oss << kTypeS3ChunkInfo << ":" << key.fsId << ":" << key.inodeId << ":";
    return oss.str();
}

// kTypeS3ChunkInfo:
inline std::string Converter::SerializeToString(Prefix4AllS3ChunkInfoList key) {
    std::ostringstream oss;
    oss << kTypeS3ChunkInfo << ":";
    return oss.str();
}

inline bool Converter::PraseFromString(const std::string& s,
                                       Key4Inode* key) {
    std::vector<std::string> items;
    SplitString(s, ":", &items);
    if (items.size() != 3 ||
        !StringToUl(items[1], &key->fsId) ||
        !StringToUll(items[2], &key->inodeId)) {
        return false;
    }
    return true;
}

inline bool Converter::PraseFromString(const std::string& s,
                                       Key4S3ChunkInfoList* key) {
    std::vector<std::string> items;
    SplitString(s, ":", &items);
    if (items.size() != 6 ||
        !StringToUl(items[1], &key->fsId) ||
        !StringToUll(items[2], &key->inodeId) ||
        !StringToUll(items[3], &key->chunkIndex) ||
        !StringToUll(items[4], &key->firstChunkId) ||
        !StringToUll(items[5], &key->lastChunkId)) {
        return false;
    }
    return true;
}

// value
#define SERIALIZE_TO_STRING(TYPE) \
inline bool Converter::SerializeToString(TYPE& entry, \
                                         std::string* value) { \
    if (!entry.IsInitialized()) { \
        return false; \
    } \
    return entry.SerializeToString(value); \
}

SERIALIZE_TO_STRING(PartitionInfo)
SERIALIZE_TO_STRING(Inode)
SERIALIZE_TO_STRING(Dentry)
SERIALIZE_TO_STRING(S3ChunkInfoList)
SERIALIZE_TO_STRING(PrepareRenameTxRequest)

#undef SERIALIZE_TO_STRING

#define PARSE_FROM_STRING(TYPE) \
inline bool Converter::PraseFromString(std::string& value, \
                                       TYPE* entry) { \
    return entry->ParseFromString(value); \
}

PARSE_FROM_STRING(PartitionInfo)
PARSE_FROM_STRING(Inode)
PARSE_FROM_STRING(Dentry)
PARSE_FROM_STRING(S3ChunkInfoList)
PARSE_FROM_STRING(PrepareRenameTxRequest)

#undef PARSE_FROM_STRING

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_STORAGE_CONVERTER_H_
