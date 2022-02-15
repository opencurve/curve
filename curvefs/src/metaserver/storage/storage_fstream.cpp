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
namespace storage {

using ::curvefs::common::PartitionInfo;

std::vector<Pair> StorageFstream::pairs_ = std::vector{
    StorageFstream::Pair(ENTRY_TYPE::INODE, "i"),
    StorageFstream::Pair(ENTRY_TYPE::DENTRY, "d"),
    StorageFstream::Pair(ENTRY_TYPE::PARTITION, "p"),
    StorageFstream::Pair(ENTRY_TYPE::PENDING_TX, "t"),
    StorageFstream::Pair(ENTRY_TYPE::UNKNOWN, "u"),
}

// e.g: ENTRY_TYPE::INODE -> "i"
std::string StorageFstream::Type2Str(ENTRY_TYPE t) {
    for (const auto& pair : pairs_) {
        if (pair.first == t) {
            return pair.second;
        }
    }
    return "";
}

// e.g: "i" -> ENTRY_TYPE::INODE
ENTRY_TYPE StorageFstream::Str2Type(const std::string& s) {
    for (const auto& pair : pairs_) {
        if (pair.second == s) {
            return pair.first;
        }
    }
    return ENTRY_TYPE::UNKNOWN;
}

// e.g: i100:key -> i100:key
std::pair<std::string, std::string> StorageFstream::UserKey(std::string& ikey) {
}

std::pair<ENTRY_TYPE, uint32_t> StorageFstream::Extract(
    const std::string& key) {
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

bool StorageFstream::SaveToFile(const std::string& pathname,
                                std::shared_ptr<MergeIterator> iterator,
                                bool background) {
    auto dumpfile = DumpFile(pathname);
    if (dumpfile.Open() != DUMPFILE_ERROR::OK) {
        LOG(ERROR) << "Open dumpfile failed";
        return false;
    }

    auto defer = absl::MakeCleanup([&dumpfile]() { dumpfile.Close(); });
    auto retCode = background ? dumpfile.SaveBackground(iterator)
                              : dumpfile.Save(iterator);
    LOG(INFO) << "SaveToFile retcode = " << retCode;
    return (retCode == DUMPFILE_ERROR::OK) && (iterator->Status() == 0);
}

template<typename EntryType, typename Callback>
bool StorageFstream::InvokeCallback(ENTRY_TYPE entryType,
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

#define CASE_TYPE_CALLBACK(TYPE, type) \
case ENTRY_TYPE::TYPE: \
    if (!InvokeCallback<type, Callback>( \
        entryType, partitionId, value, callback)) { \
        return false; \
    } \
    break

template<typename Callback>
bool StorageFstream::LoadFromFile(const std::string& pathname,
                                  Callback callback) {
    auto dumpfile = DumpFile(pathname);
    if (dumpfile.Open() != DUMPFILE_ERROR::OK) {
        return false;
    }

    auto defer = absl::MakeCleanup([&dumpfile]() { dumpfile.Close(); });

    auto iter = dumpfile.Load();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        auto ikey = iter->Key();
        auto value = iter->Value();
        auto ukey = iter->Key();

        auto ret = Extract(key);
        auto entryType = ret.first;
        auto partitionId = ret.second;
        switch (entryType) {
            CASE_TYPE_CALLBACK(INODE, Inode);
            CASE_TYPE_CALLBACK(DENTRY, Dentry);
            CASE_TYPE_CALLBACK(PARTITION, PartitionInfo);
            CASE_TYPE_CALLBACK(PENDING_TX, PrepareRenameTxRequest);
            default:
                LOG(ERROR) << "Unknown entry type, key = " << key;
                return false;
        }
    }

    return dumpfile.GetLoadStatus() == DUMPFILE_LOAD_STATUS::COMPLETE;
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_STORAGE_H_
