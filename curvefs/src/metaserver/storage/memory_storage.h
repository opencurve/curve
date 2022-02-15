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
 * Date: 2022-02-18
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_METASERVER_STORAGE_MEMORY_STORAGE_H_
#define CURVEFS_SRC_METASERVER_STORAGE_MEMORY_STORAGE_H_

#include <unordered_map>

#include "absl/container/btree_set.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/iterator.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curve::common::StringStartWith;
using ::curvefs::metaserver::storage::Status;
using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::MemoryStorage;
using ::curvefs::metaserver::storage::RocksDBStorage;

template<typename ContainerType>
class MemoryStorageIterator : public Iterator {
 public:
    MemoryStorageIterator(std::shared_ptr<ContainerType> container,
                          const std::string& prefix)
        : container_(container), prefix_(prefix) {}

    uint64_t Size() override {
        return prefix_.size() > 0 ? 0 : container_->size();
    }

    bool Valid() override {
        if (current_ == container_->end()) {
            return false;
        } else if (prefix_.size() > 0 &&
            !StringStartWith(current_.first, prefix)) {
            return false;
        }

        return true;
    }

    void SeekToFirst() override {
        if (prefix_.size() > 0) {
            current_ = container_->lower_bound(prefix);
        } else {
            current_ = container_->begin();
        }
    }

    void Next() override {
        current_++;
    }

    std::string Key() override {
        return current_->first;
    }

    std::string Value() override {
        return current_->second;
    }

 private:
    std::shared_ptr<ContainerType> container_;
    std::string prefix_;
    typename ContainerType::const_iterator current_;
};

class MemoryStorage : public KVStorage {
 public:
    using UnorderedType = std::unordered_map<std::string, std::string>;
    using OrderedType = absl::btree_map<std::string, std::string>;

 public:
    explicit MemoryStorage(StorageOptions options);

    STORAGE_TYPE Type() override;

    bool GetStatistic(StorageStatistics* Statistics) override;

    // unordered
    Status HGet(const std::string& name,
                const std::string& key,
                std::string* value) override;

    Status HSet(const std::string& name,
                const std::string& key,
                const std::string& value) override;

    Status HDel(const std::string& name, const std::string& key) override;

    Iterator HGetAll(const std::string& name) override;

    Status HClear(const std::string& name) override;

    // ordered
    Status SGet(const std::string& name,
                const std::string& key,
                std::string* value) override;

    Status SSet(const std::string& name,
                const std::string& key,
                const std::string& value) override;

    Status SDel(const std::string& name, const std::string& key) override;

    Iterator SRange(const std::string& name,
                    const std::string& prefix) override;

    Iterator SGetAll(const std::string& name) override;

    Status SClear(const std::string& name) override;

 private:
    std::shared_ptr<UnorderedType> getUnordered();
    std::shared_ptr<OrderedType> getOrdered();

 private:
    StorageOptions options_;
    std::unordered_map<std::string,
                       std::shared_ptr<UnorderedType>> unorderedDict_;
    std::unordered_map<std::string,
                       std::shared_ptr<OrderedType>> orderedDict_;
};

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_STORAGE_MEMORY_STORAGE_H_