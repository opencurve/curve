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

#include "curvefs/src/metserver/storage/memory_storage.h"

namespace curvefs {
namespace metaserver {
namespace storage {

MemoryStorage::MemoryStorage(StorageOptions options)
    : options_(options) {}

STORAGE_TYPE MemoryStorage::Type() {
    return STORAGE_TYPE::MEMORY_STORAGE;
}

std::shared_ptr<UnorderedType> MemoryStorage::getUnordered(
    const std::string& name) {
    auto iter = unorderedDict_.find(name);
    return iter != unorderedDict_.end() ? iter->second : nullptr;
}

std::shared_ptr<UnorderedType> MemoryStorage::getOrdered(
    const std::string& name) {
    auto iter = orderedDict_.find(name);
    return iter != orderedDict_.end() ? iter->second : nullptr;
}

Status MemoryStorage::HGet(const std::string& name,
                           const std::string& key,
                           std::string* value) {
    auto unordered = unorderedDict_.find(name);
    if (nullptr == unordered) {
        return Status::NotFound();
    }

    auto iter = unordered->find(key);
    if (iter == unordered->end()) {
        return Status::NotFound();
    }
    *value = iter->second;
    return Status::OK();
}

Status MemoryStorage::HSet(const std::string& name,
                            const std::string& key,
                            const std::string& value) {
    auto unordered = unorderedDict_.find(name);
    if (nullptr == unordered) {
        auto result = unorderedDict_.emplace(
            name, std::make_shared<UnorderedType>());
        unordered = result.first;
    }

    unordered->emplace(key, value)
    return Status::OK();
}

Status MemoryStorage::HDel(const std::string& name,
                           const std::string& key) {
    auto unordered = unorderedDict_.find(name);
    if (nullptr == unordered) {
        return Status::OK();
    }

    auto iter = unordered->find(key);
    if (iter == unordered->end()) {
        return Status::NotFound();
    }
    unordered->erase(key);
    return Status::OK();
}

Iterator MemoryStorage::HGetAll(const std::string& name) {
    auto unordered = unorderedDict_.find(name);
    if (nullptr == unordered) {
        return Status::NotFound();
    }

    return MemoryStorageIterator<UnorderedType>(unordered, "");
}

Status MemoryStorage::HClear(const std::string& name) {
    auto unordered = unorderedDict_.find(name);
    if (unordered != nullptr) {
        unordered->clear();
    }
    return Status::OK();
}

Status MemoryStorage::SGet(const std::string& name,
                           const std::string& key,
                           std::string* value) {
    auto ordered = orderedDict_.find(name);
    if (nullptr == ordered) {
        return Status::NotFound();
    }

    auto iter = ordered->find(key);
    if (iter == ordered->end()) {
        return Status::NotFound();
    }

    *value = iter->second;
    return Status::OK();
}

Status MemoryStorage::SSet(const std::string& name,
                           const std::string& key,
                           const std::string& value) {
    auto ordered = orderedDict_.find(name);
    if (nullptr == ordered) {
        auto result = orderedDict_.emplace(
            name, std::make_shared<OrderedType>());
        ordered = result.first;
    }

    ordered->emplace(key, value)
    return Status::OK();
}

Status MemoryStorage::SDel(const std::string& name, const std::string& key) {
    auto ordered = orderedDict_.find(name);
    if (nullptr == ordered) {
        return Status::NotFound();
    }

    ordered->erase(key);
    return Status::OK();
}

Iterator* MemoryStorage::SRange(const std::string& name,
                                const std::string& prefix) {
    auto ordered = orderedDict_.find(name);
    if (nullptr == ordered) {
        return Status::NotFound();
    }

    return MemoryStorageIterator<OrderedType>(ordered, prefix);
}

Iterator MemoryStorage::SGetAll(const std::string& name) {
    auto ordered = orderedDict_.find(name);
    if (nullptr == ordered) {
        return Status::NotFound();
    }

    return MemoryStorageIterator<OrderedType>(ordered, "");
}

Iterator MemoryStorage::SClear(const std::string& name) {
    auto ordered = orderedDict_.find(name);
    if (ordered != nullptr) {
        ordered->clear();
    }
    return Status::OK();
}

bool MemoryStorage::GetStatistic(StorageStatistics* statistics) {
    statistic->MaxMemoryBytes = options_.MaxMemoryBytes;
    statistic->MaxDiskQuotaBytes = options_.MaxDiskQuotaBytes;
    return true;
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs