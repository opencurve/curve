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
 * Created Date: 2021-07-23
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_METASERVER_STORAGE_H_
#define CURVEFS_SRC_METASERVER_STORAGE_H_

#include <glog/logging.h>

#include <string>
#include <memory>
#include <vector>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "src/common/string_util.h"
#include "curvefs/proto/common.pb.h"
#include "curvefs/src/metaserver/dumpfile.h"
#include "curvefs/src/metaserver/iterator.h"
#include "curvefs/src/metaserver/inode_storage.h"
#include "curvefs/src/metaserver/dentry_storage.h"

namespace curvefs {
namespace metaserver {

using ::curvefs::common::PartitionInfo;

enum class ENTRY_TYPE {
    INODE,
    DENTRY,
    PARTITION,
    PENDING_TX,
    UNKNOWN,
};

using Pair = std::pair<ENTRY_TYPE, std::string>;
static std::vector<Pair> pairs{
    Pair(ENTRY_TYPE::INODE, "i"),
    Pair(ENTRY_TYPE::DENTRY, "d"),
    Pair(ENTRY_TYPE::PARTITION, "p"),
    Pair(ENTRY_TYPE::PENDING_TX, "t"),
    Pair(ENTRY_TYPE::UNKNOWN, "u"),
};

static std::string type2str(ENTRY_TYPE t) {
    for (const auto& pair : pairs) {
        if (pair.first == t) {
            return pair.second;
        }
    }
    return "";
}

static ENTRY_TYPE str2type(const std::string& s) {
    for (const auto& pair : pairs) {
        if (pair.second == s) {
            return pair.first;
        }
    }
    return ENTRY_TYPE::UNKNOWN;
}

template<typename ContainerType>
class ContainerIterator : public Iterator {
 public:
    ContainerIterator(ENTRY_TYPE entryType,
                      uint32_t partitionId,
                      const ContainerType* container)
        : entryType_(entryType),
          partitionId_(partitionId),
          container_(container),
          status_(0) {
        auto str4type = type2str(entryType);
        if (str4type == "") {
            status_ = 1;
        }
        key_ = str4type + ":" + std::to_string(partitionId);
    }

    uint64_t Size() override {
        return container_->size();
    }

    bool Valid() override {
        return (status_ == 0) && (iter_ != container_->end());
    }

    void SeekToFirst() override {
        iter_ = container_->begin();
    }

    void Next() override {
        iter_++;
    }

    std::string Key() override {
        return key_;
    }

    std::string Value() override {
        std::string value;
        if (!iter_->second.SerializeToString(&value)) {
            status_ = 1;
        }
        return value;
    }

    int Status() override {
        return status_;
    }

 private:
    ENTRY_TYPE entryType_;
    uint32_t partitionId_;
    const ContainerType* container_;
    typename ContainerType::const_iterator iter_;
    int status_;  // 0: success, 1: fail
    std::string key_;
};

class MergeIterator : public Iterator {
 public:
    explicit MergeIterator(
        const std::vector<std::shared_ptr<Iterator>>& children)
        : size_(0), current_(nullptr), children_(children) {
        for (const auto& child : children) {
            size_ += child->Size();
        }
    }

    uint64_t Size() override {
        return size_;
    }

    bool Valid() override {
        return (current_ != nullptr) && (Status() == 0);
    }

    void SeekToFirst() override {
        for (const auto& child : children_) {
            child->SeekToFirst();
        }
        FindCurrent();
    }

    void Next() override {
        current_->Next();
        FindCurrent();
    }

    std::string Key() override {
        return current_->Key();
    }

    std::string Value() override {
        return current_->Value();
    }

    int Status() override {
        for (const auto& child : children_) {
            if (child->Status() == 1) {
                return 1;
            }
        }
        return 0;
    }

 private:
    void FindCurrent() {
        current_ = nullptr;
        for (const auto& child : children_) {
            if (child->Valid()) {
                current_ = child;
                break;
            }
        }
    }

 private:
    uint64_t size_;
    std::shared_ptr<Iterator> current_;
    std::vector<std::shared_ptr<Iterator>> children_;
};

inline std::pair<ENTRY_TYPE, uint32_t> Extract(const std::string& key) {
    uint32_t partitionId = 0;
    ENTRY_TYPE entryType = ENTRY_TYPE::UNKNOWN;

    int32_t num;
    std::vector<std::string> ss;
    ::curve::common::SplitString(key, ":", &ss);
    if (ss.size() == 2 &&
        str2type(ss[0]) != ENTRY_TYPE::UNKNOWN &&
        ::curve::common::StringToInt(ss[1], &num) &&
        num >= 0) {
        entryType = str2type(ss[0]);
        partitionId = num;
    }
    return std::make_pair(entryType, partitionId);
}

template<typename EntryType, typename Callback>
inline bool InvokeCallback(ENTRY_TYPE entryType,
                           uint32_t partitionId,
                           const std::string& value,
                           Callback callback) {
    EntryType entry;
    if (!entry.ParseFromString(value)) {
        LOG(ERROR) << "Decode string to entry failed.";
        return false;
    } else if (!callback(entryType, partitionId, (void*)(&entry))) {  // NOLINT
        LOG(ERROR) << "Invoke callback for entry failed.";
        return false;
    }
    return true;
}

inline bool SaveToFile(const std::string& pathname,
                       std::shared_ptr<MergeIterator> iterator) {
    auto dumpfile = DumpFile(pathname);
    if (dumpfile.Open() != DUMPFILE_ERROR::OK) {
        LOG(ERROR) << "Open dumpfile failed";
        return false;
    }

    auto defer = absl::MakeCleanup([&dumpfile]() { dumpfile.Close(); });
    auto retCode = dumpfile.SaveBackground(iterator);
    LOG(ERROR) << "retcode = " << retCode;
    return (retCode == DUMPFILE_ERROR::OK) && (iterator->Status() == 0);
}

#define CASE_TYPE_CALLBACK(TYPE, type) \
case ENTRY_TYPE::TYPE: \
    if (!InvokeCallback<type, Callback>( \
        entryType, partitionId, value, callback)) { \
        return false; \
    } \
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
        auto key = iter->Key();
        auto value = iter->Value();

        auto ret = Extract(key);
        auto entryType = ret.first;
        auto partitionId = ret.second;
        switch (entryType) {
            CASE_TYPE_CALLBACK(INODE, Inode);
            CASE_TYPE_CALLBACK(DENTRY, Dentry);
            CASE_TYPE_CALLBACK(PARTITION, PartitionInfo);
            // TODO(Wine93): add pending tx
            default:
                LOG(ERROR) << "Unknown entry type, key = " << key;
                return false;
        }
    }

    return dumpfile.GetLoadStatus() == DUMPFILE_LOAD_STATUS::COMPLETE;
}

};  // namespace metaserver
};  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_STORAGE_H_
