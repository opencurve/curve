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

/**
 * Project: Curve
 * Created Date: 2022-02-25
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_METASERVER_STORAGE_STORAGE_FSTREAM_H_
#define CURVEFS_SRC_METASERVER_STORAGE_STORAGE_FSTREAM_H_

#include <string>
#include <vector>
#include <utility>
#include <memory>

#include "absl/cleanup/cleanup.h"
#include "src/common/string_util.h"
#include "curvefs/src/metaserver/storage/dumpfile.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curve::common::StringToUl;
using ::curve::common::SplitString;
using ::curvefs::common::PartitionInfo;

enum class ENTRY_TYPE {
    INODE,
    DENTRY,
    PARTITION,
    PENDING_TX,
    S3_CHUNK_INFO_LIST,
    VOLUME_EXTENT,
    UNKNOWN,
};

using Pair = std::pair<ENTRY_TYPE, std::string>;
static const std::vector<Pair> pairs{
    Pair(ENTRY_TYPE::INODE, "i"),
    Pair(ENTRY_TYPE::DENTRY, "d"),
    Pair(ENTRY_TYPE::PARTITION, "p"),
    Pair(ENTRY_TYPE::PENDING_TX, "t"),
    Pair(ENTRY_TYPE::S3_CHUNK_INFO_LIST, "s"),
    Pair(ENTRY_TYPE::VOLUME_EXTENT, "v"),
    Pair(ENTRY_TYPE::UNKNOWN, "u"),
};

static std::string Type2Str(ENTRY_TYPE t) {
    for (const auto& pair : pairs) {
        if (pair.first == t) {
            return pair.second;
        }
    }
    return "";
}

static ENTRY_TYPE Str2Type(const std::string& s) {
    for (const auto& pair : pairs) {
        if (pair.second == s) {
            return pair.first;
        }
    }
    return ENTRY_TYPE::UNKNOWN;
}

static std::string InternalKey(ENTRY_TYPE t,
                               uint32_t partitionId,
                               const std::string& ukey) {
    std::ostringstream oss;
    oss << Type2Str(t) << partitionId << ":" << ukey;
    return oss.str();
}

static std::pair<std::string, std::string> UserKey(const std::string& ikey) {
    std::string prefix, ukey;
    std::vector<std::string> items;
    SplitString(ikey, ":", &items);
    if (items.size() >= 2) {
        prefix = items[0];
        ukey = ikey.substr(prefix.size() + 1);
    } else if (items.size() == 1) {  // e.g: t3:
        prefix = items[0];
    }
    return std::make_pair(prefix, ukey);
}

static std::pair<ENTRY_TYPE, uint32_t> Extract(const std::string& prefix) {
    if (prefix.size() == 0) {
        return std::make_pair(ENTRY_TYPE::UNKNOWN, 0);
    }

    std::vector<std::string> items{
        prefix.substr(0, 1),  // eg: i
        prefix.substr(1),  // eg: 100
    };

    ENTRY_TYPE entryType = Str2Type(items[0]);
    uint32_t partitionId = 0;
    if (!StringToUl(items[1], &partitionId)) {
        partitionId = 0;
    }
    return std::make_pair(entryType, partitionId);
}

static bool SaveToFile(const std::string& pathname,
                       std::shared_ptr<MergeIterator> iterator,
                       bool background,
                       DumpFileClosure* done = nullptr) {
    auto dumpfile = DumpFile(pathname);
    if (dumpfile.Open() != DUMPFILE_ERROR::OK) {
        LOG(ERROR) << "Open dumpfile failed";
        if (done != nullptr) {
            done->Runned();
        }
        return false;
    }

    DUMPFILE_ERROR rc;
    auto defer = absl::MakeCleanup([&dumpfile]() { dumpfile.Close(); });
    if (background) {
        rc = dumpfile.SaveBackground(iterator, done);
    } else {
        if (done != nullptr) {
            done->Runned();
        }
        rc = dumpfile.Save(iterator);
    }
    LOG(INFO) << "SaveToFile retcode = " << rc;
    return (rc == DUMPFILE_ERROR::OK) && (iterator->Status() == 0);
}

template<typename Callback>
inline bool InvokeCallback(ENTRY_TYPE entryType,
                           uint32_t partitionId,
                           const std::string& key,
                           const std::string& value,
                           Callback&& callback) {
    if (!std::forward<Callback>(callback)(entryType, partitionId, key, value)) {
        LOG(ERROR) << "Invoke callback for entry failed.";
        return false;
    }
    return true;
}

#define CASE_TYPE_CALLBACK(TYPE)                                             \
    case ENTRY_TYPE::TYPE:                                                   \
        if (!InvokeCallback(entryType, partitionId, key, value, callback)) { \
            return false;                                                    \
        }                                                                    \
        break

template<typename Callback>
inline bool LoadFromFile(const std::string& pathname,
                         Callback callback) {
    auto dumpfile = DumpFile(pathname);
    if (dumpfile.Open() != DUMPFILE_ERROR::OK) {
        return false;
    }
    auto defer = absl::MakeCleanup([&dumpfile]() { dumpfile.Close(); });

    auto iter = dumpfile.Load();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        auto ikey = iter->Key();  // internal key
        auto ukey = UserKey(ikey);  // <prefix, ukey>
        auto pair = Extract(ukey.first);  // prefix

        ENTRY_TYPE entryType = pair.first;
        uint32_t partitionId = pair.second;
        std::string key = ukey.second;
        std::string value = iter->Value();
        switch (entryType) {
            CASE_TYPE_CALLBACK(INODE);
            CASE_TYPE_CALLBACK(DENTRY);
            CASE_TYPE_CALLBACK(PARTITION);
            CASE_TYPE_CALLBACK(PENDING_TX);
            CASE_TYPE_CALLBACK(S3_CHUNK_INFO_LIST);
            CASE_TYPE_CALLBACK(VOLUME_EXTENT);
            default:
                LOG(ERROR) << "Unknown entry type, key = " << key;
                return false;
        }
    }

    return dumpfile.GetLoadStatus() == DUMPFILE_LOAD_STATUS::COMPLETE;
}

// The IteratorWrapper will overwrite the key with prefix which
// contain entry type and partition id.
class IteratorWrapper : public Iterator {
 public:
    IteratorWrapper(ENTRY_TYPE entryType,
                    uint32_t partitionId,
                    std::shared_ptr<Iterator> iterator)
        : entryType_(entryType),
          partitionId_(partitionId),
          iterator_(std::move(iterator)) {}

    uint64_t Size() override {
        return iterator_->Size();
    }

    bool Valid() override {
        return iterator_->Valid();
    }

    void SeekToFirst() override {
        iterator_->SeekToFirst();
    }

    void Next() override {
        iterator_->Next();
    }

    std::string Key() override {
        auto key = iterator_->Key();
        return InternalKey(entryType_, partitionId_, key);
    }

    std::string Value() override {
        return iterator_->Value();
    }

    bool ParseFromValue(ValueType* value) override {
        return true;
    }

    int Status() override {
        return iterator_->Status();
    }

 protected:
    ENTRY_TYPE entryType_;
    uint32_t partitionId_;
    std::shared_ptr<Iterator> iterator_;
};

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_STORAGE_STORAGE_FSTREAM_H_
