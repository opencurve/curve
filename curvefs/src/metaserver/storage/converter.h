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


#include <string>

#include "curvefs/src/metaserver/storage/common.h"

namespace curvefs {
namespace metaserver {
namespace storage {

enum KEY_TYPE : unsigned char {
    kTypeInode = 1,
    kTypeS3ChunkInfo = 2,
    kTypeDentry = 3,
};

class StorageKey {
 public:
    virtual std::string SerializeToString() const = 0;
    virtual bool ParseFromString(const std::string& value) = 0;
};

/* rules for key serialization:
 *   Key4Inode                        : kTypeInode:fsId:InodeId
 *   Prefix4AllInode                  : kTypeInode:
 *   Key4S3ChunkInfoList              : kTypeS3ChunkInfo:fsId:inodeId:chunkIndex:firstChunkId:lastChunkId  // NOLINT
 *   Prefix4ChunkIndexS3ChunkInfoList : kTypeS3ChunkInfo:fsId:inodeId:chunkIndex:  // NOLINT
 *   Prefix4InodeS3ChunkInfoList      : kTypeS3ChunkInfo:fsId:inodeId:
 *   Prefix4AllS3ChunkInfoList        : kTypeS3ChunkInfo:
 */

class Key4Inode : public StorageKey {
 public:
    Key4Inode();

    Key4Inode(uint32_t fsId, uint64_t inodeId);

    explicit Key4Inode(const Inode& inode);

    bool operator==(const Key4Inode& rhs);

    std::string SerializeToString() const override;

    bool ParseFromString(const std::string& value) override;

 public:
    static const KEY_TYPE keyType_ = kTypeInode;

    uint32_t fsId;
    uint64_t inodeId;
};

class Prefix4AllInode : public StorageKey {
 public:
    Prefix4AllInode() = default;

    std::string SerializeToString() const override;

     bool ParseFromString(const std::string& value) override;

 public:
    static const KEY_TYPE keyType_ = kTypeInode;
};

class Key4S3ChunkInfoList : public StorageKey {
 public:
    Key4S3ChunkInfoList();

    Key4S3ChunkInfoList(uint32_t fsId,
                        uint64_t inodeId,
                        uint64_t chunkIndex,
                        uint64_t firstChunkId,
                        uint64_t lastChunkId,
                        uint64_t size);

    std::string SerializeToString() const override;

    bool ParseFromString(const std::string& value) override;

 public:
    static const size_t kMaxUint64Length_;
    static const KEY_TYPE keyType_ = kTypeS3ChunkInfo;

     uint32_t fsId;
     uint64_t inodeId;
     uint64_t chunkIndex;
     uint64_t firstChunkId;
     uint64_t lastChunkId;
     uint64_t size;
};

class Prefix4ChunkIndexS3ChunkInfoList : public StorageKey {
 public:
    Prefix4ChunkIndexS3ChunkInfoList();

    Prefix4ChunkIndexS3ChunkInfoList(uint32_t fsId,
                                     uint64_t inodeId,
                                     uint64_t chunkIndex);

    std::string SerializeToString() const override;

    bool ParseFromString(const std::string& value) override;

 public:
    static const KEY_TYPE keyType_ = kTypeS3ChunkInfo;

    uint32_t fsId;
    uint64_t inodeId;
    uint64_t chunkIndex;
};

class Prefix4InodeS3ChunkInfoList : public StorageKey {
 public:
    Prefix4InodeS3ChunkInfoList();

    Prefix4InodeS3ChunkInfoList(uint32_t fsId,
                                uint64_t inodeId);

    std::string SerializeToString() const override;

    bool ParseFromString(const std::string& value) override;

 public:
    static const KEY_TYPE keyType_ = kTypeS3ChunkInfo;

    uint32_t fsId;
    uint64_t inodeId;
};

class Prefix4AllS3ChunkInfoList : public StorageKey {
 public:
    Prefix4AllS3ChunkInfoList() = default;

    std::string SerializeToString() const override;

    bool ParseFromString(const std::string& value) override;

 public:
    static const KEY_TYPE keyType_ = kTypeS3ChunkInfo;
};

// converter
class Converter {
 public:
    Converter() = default;

    // for key
    std::string SerializeToString(const StorageKey& key);

    // for value
    bool SerializeToString(const google::protobuf::Message& entry,
                           std::string* value);

    // for key&value
    template <typename Message>
    bool ParseFromString(const std::string& value, Message* entry) {
        return entry->ParseFromString(value);
    }
};

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_STORAGE_CONVERTER_H_
