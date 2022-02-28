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

#include <string>
#include <memory>
#include <unordered_map>

#include "absl/container/btree_map.h"
#include "src/common/string_util.h"
#include "src/common/concurrent/rw_lock.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/iterator.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curve::common::RWLock;
using ::curve::common::StringStartWith;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;
using STORAGE_TYPE = KVStorage::STORAGE_TYPE;

template<typename ContainerType>
class MemoryStorageIterator : public Iterator {
 public:
    MemoryStorageIterator(std::shared_ptr<ContainerType> container,
                          const std::string& prefix)
        : container_(container),
          prefix_(prefix) {}

    // NOTE: now we can't caclute the size for range operate
    uint64_t Size() override {
        return (prefix_.size() > 0) ? 0 : container_->size();
    }

    bool Valid() override {
        return current_ != container_->end();
    }

    void SeekToFirst() override {}

    void Next() override {
        current_++;
    }

    std::string Key() override {
        return current_->first;
    }

    std::string Value() override {
        return current_->second;
    }

    int Status() override {
        return 0;
    }

 protected:
    std::string prefix_;
    std::shared_ptr<ContainerType> container_;
    typename ContainerType::const_iterator current_;
};

template<typename ContainerType>
class OrderedContainerIterator : public MemoryStorageIterator<ContainerType> {
 public:
    using MemoryStorageIterator<ContainerType>::MemoryStorageIterator;

    void SeekToFirst() override {
        this->current_ = this->container_->lower_bound(this->prefix_);
    }
};

template<typename ContainerType>
class UnorderedContainerIterator : public MemoryStorageIterator<ContainerType> {
 public:
    using MemoryStorageIterator<ContainerType>::MemoryStorageIterator;

    void SeekToFirst() override {
        this->current_ = this->container_->begin();
    }
};

class MemoryStorage : public KVStorage, public StorageTransaction {
 public:
    using UnorderedType = std::unordered_map<std::string, std::string>;
    using OrderedType = absl::btree_map<std::string, std::string>;

 public:
    explicit MemoryStorage(StorageOptions options);

    STORAGE_TYPE Type() override;

    bool Open() override;

    bool Close() override;

    bool GetStatistics(StorageStatistics* Statistics) override;

    Status HGet(const std::string& name,
                const std::string& key,
                std::string* value) override;

    Status HSet(const std::string& name,
                const std::string& key,
                const std::string& value) override;

    Status HDel(const std::string& name, const std::string& key) override;

    std::shared_ptr<Iterator> HGetAll(const std::string& name) override;

    size_t HSize(const std::string& name) override;

    Status HClear(const std::string& name) override;

    Status SGet(const std::string& name,
                const std::string& key,
                std::string* value) override;

    Status SSet(const std::string& name,
                const std::string& key,
                const std::string& value) override;

    Status SDel(const std::string& name, const std::string& key) override;

    std::shared_ptr<Iterator> SSeek(const std::string& name,
                                     const std::string& prefix) override;

    std::shared_ptr<Iterator> SGetAll(const std::string& name) override;

    size_t SSize(const std::string& name) override;

    Status SClear(const std::string& name) override;

    // NOTE: now we can't support transaction for memory storage,
    // so these interface is dummy, it will pretend everything works well.
    std::shared_ptr<StorageTransaction> BeginTransaction() override;

    Status Commit() override;

    Status Rollback() override;

 private:
    std::shared_ptr<UnorderedType> GetUnordered(const std::string& name);
    std::shared_ptr<OrderedType> GetOrdered(const std::string& name);

 private:
    RWLock rwLock_;
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
