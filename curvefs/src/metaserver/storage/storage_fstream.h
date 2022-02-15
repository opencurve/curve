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

#ifndef CURVEFS_SRC_METASERVER_ITERATOR_H_
#define CURVEFS_SRC_METASERVER_ITERATOR_H_

#include <string>
#include <vector>

namespace curvefs {
namespace metaserver {
namespace storage {

class StorageFstream {
 public:
    enum class ENTRY_TYPE {
        INODE,
        DENTRY,
        PARTITION,
        PENDING_TX,
        UNKNOWN,
    };

    using Pair = std::pair<ENTRY_TYPE, std::string>;

 public:
    static bool SaveToFile(const std::string& pathname,
                           std::shared_ptr<MergeIterator> iterator,
                           bool background);

    template<typename Callback>
    static bool LoadFromFile(const std::string& pathname,
                             Callback callback);

 private:
    static std::string Type2Str(ENTRY_TYPE t);

    static ENTRY_TYPE Str2Type(const std::string& s);

    static std::pair<ENTRY_TYPE, uint32_t> Extract(const std::string& key);

    template<typename EntryType, typename Callback>
    bool InvokeCallback(ENTRY_TYPE entryType,
                        uint32_t partitionId,
                        const std::string& value,
                        Callback callback);

 private:
    static std::vector<Pair> pairs_;
};

// The IteratorWrapper will overwrite the key with
class IteratorWrapper : public Iterator {
 public:
    using ENTRY_TYPE = StorageFstream::ENTRY_TYPE;

 public:
    InteratorWrapper(ENTRY_TYPE entryType,
                     uint32_t partitionId,
                     std::shared_ptr<Iterator> iterator)
        : entryType_(entryType),
          partitionId_(partitionId),
          iterator_(iterator) {}

    uint64_t Size() override {
        return iterator_->size();
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
        return StorageFstream::InternalKey(entryType_, partitionId_, key);
    }

    std::string Value() override {
        return iterator_->Value();
    }

    int Status() override {
        iterator_->Status();
    }

 protected:
    ENTRY_TYPE entryType_;
    uint32_t partitionId_;
    std::shared_ptr<Iterator> iterator_;
};

};  // namespace storage
};  // namespace metaserver
};  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_ITERATOR_H_
