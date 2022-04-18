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
#include <utility>
#include <unordered_map>

#include "absl/container/btree_map.h"
#include "src/common/string_util.h"
#include "src/common/concurrent/rw_lock.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/storage/common.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/iterator.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curve::common::RWLock;
using ::curve::common::StringStartWith;
using ::curvefs::metaserver::Inode;
using ::curvefs::metaserver::Dentry;
using ::curvefs::metaserver::S3ChunkInfoList;
using STORAGE_TYPE = KVStorage::STORAGE_TYPE;

#define RETURN_IF_CONVERT_VALUE_SUCCESS(TYPE)            \
do {                                                     \
    const TYPE* ptr = dynamic_cast<const TYPE*>(&value); \
    if (ptr != nullptr) {                                \
        value_ = std::make_shared<TYPE>(*ptr);           \
        return;                                          \
    }                                                    \
} while (0)

class ValueWrapper {
 public:
    explicit ValueWrapper(const ValueType& value) {
        RETURN_IF_CONVERT_VALUE_SUCCESS(Dentry);
        RETURN_IF_CONVERT_VALUE_SUCCESS(Inode);
        RETURN_IF_CONVERT_VALUE_SUCCESS(S3ChunkInfoList);
    }

    std::shared_ptr<ValueType> Message() const {
        return value_;
    }

 private:
    std::shared_ptr<ValueType> value_;
};

class MemoryStorage : public KVStorage, public StorageTransaction {
 public:
    using UnorderedContainerType =
        std::unordered_map<std::string, ValueWrapper>;

    using UnorderedSeralizedContainerType =
        std::unordered_map<std::string, std::string>;

    using OrderedContainerType =
        absl::btree_map<std::string, ValueWrapper>;

    using OrderedSeralizedContainerType =
        absl::btree_map<std::string, std::string>;

 public:
    explicit MemoryStorage(StorageOptions options);

    STORAGE_TYPE Type() override;

    bool Open() override;

    bool Close() override;

    bool GetStatistics(StorageStatistics* Statistics) override;

    StorageOptions GetStorageOptions() override;

    Status HGet(const std::string& name,
                const std::string& key,
                ValueType* value) override;

    Status HSet(const std::string& name,
                const std::string& key,
                const ValueType& value) override;

    Status HDel(const std::string& name, const std::string& key) override;

    std::shared_ptr<Iterator> HGetAll(const std::string& name) override;

    size_t HSize(const std::string& name) override;

    Status HClear(const std::string& name) override;

    Status SGet(const std::string& name,
                const std::string& key,
                ValueType* value) override;

    Status SSet(const std::string& name,
                const std::string& key,
                const ValueType& value) override;

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
    RWLock rwLock_;
    StorageOptions options_;

    std::unordered_map<std::string,
                       std::shared_ptr<UnorderedContainerType>>
        UnorderedContainerDict_;

    std::unordered_map<std::string,
                       std::shared_ptr<UnorderedSeralizedContainerType>>
        UnorderedSeralizedContainerDict_;

    std::unordered_map<std::string,
                       std::shared_ptr<OrderedContainerType>>
        OrderedContainerDict_;

    std::unordered_map<std::string,
                       std::shared_ptr<OrderedSeralizedContainerType>>
        OrderedSeralizedContainerDict_;
};

template<typename ContainerType>
class MemoryStorageIterator : public Iterator {
 public:
    MemoryStorageIterator(std::shared_ptr<ContainerType> container,
                          const std::string& prefix)
        : container_(container),
          prefix_(prefix),
          prefixChecking_(true),
          status_(0) {}

    // NOTE: now we can't caclute the size for range operate
    uint64_t Size() override {
        return (prefix_.size() > 0) ? 0 : container_->size();
    }

    bool Valid() override {
        if (status_ != 0) {
            return false;
        } else if (current_ == container_->end()) {
            return false;
        } else if (prefixChecking_ && prefix_.size() > 0 &&
            !StringStartWith(current_->first, prefix_)) {
            return false;
        }
        return true;
    }

    void SeekToFirst() override {}

    void Next() override {
        current_++;
    }

    std::string Key() override {
        return current_->first;
    }

    std::string Value() override {
        return "";
    }

    int Status() override {
        return status_;
    }

    void DisablePrefixChecking() override {
        prefixChecking_ = false;
    }

 protected:
    std::string prefix_;
    int status_;
    bool prefixChecking_;
    std::shared_ptr<ContainerType> container_;
    typename ContainerType::const_iterator current_;
};

template<typename ContainerType>
class UnorderedContainerIterator : public MemoryStorageIterator<ContainerType> {
 public:
    using MemoryStorageIterator<ContainerType>::MemoryStorageIterator;

    void SeekToFirst() override {
        this->current_ = this->container_->begin();
    }

    std::string Value() override {
        std::string svalue;
        auto message = this->current_->second.Message();
        if (!message->SerializeToString(&svalue)) {
            this->status_ = -1;
        }
        return svalue;
    }

    bool ParseFromValue(ValueType* value) override {
        auto message = this->current_->second.Message();
        value->CopyFrom(*message);
        return true;
    }
};

template<typename ContainerType>
class UnorderedSeralizedContainerIterator :
public MemoryStorageIterator<ContainerType> {
 public:
    using MemoryStorageIterator<ContainerType>::MemoryStorageIterator;

    void SeekToFirst() override {
        this->current_ = this->container_->begin();
    }

    std::string Value() override {
        return this->current_->second;
    }

    bool ParseFromValue(ValueType* value) override {
        if (!value->ParseFromString(this->current_->second)) {
            return false;
        }
        return true;
    }
};

template<typename ContainerType>
class OrderedContainerIterator : public MemoryStorageIterator<ContainerType> {
 public:
    using MemoryStorageIterator<ContainerType>::MemoryStorageIterator;

    void SeekToFirst() override {
        this->current_ = this->container_->lower_bound(this->prefix_);
    }

    std::string Value() override {
        std::string svalue;
        auto message = this->current_->second.Message();
        if (!message->SerializeToString(&svalue)) {
            this->status_ = -1;
        }
        return svalue;
    }

    bool ParseFromValue(ValueType* value) override {
        auto message = this->current_->second.Message();
        value->CopyFrom(*message);
        return true;
    }
};

template<typename ContainerType>
class OrderedSeralizedContainerIterator :
public MemoryStorageIterator<ContainerType> {
 public:
    using MemoryStorageIterator<ContainerType>::MemoryStorageIterator;

    void SeekToFirst() override {
        this->current_ = this->container_->lower_bound(this->prefix_);
    }

    std::string Value() override {
        return this->current_->second;
    }

    bool ParseFromValue(ValueType* value) override {
        if (!value->ParseFromString(this->current_->second)) {
            return false;
        }
        return true;
    }
};

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_STORAGE_MEMORY_STORAGE_H_
