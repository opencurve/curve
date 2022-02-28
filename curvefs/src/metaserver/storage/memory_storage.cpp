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

#include <glog/logging.h>

#include <string>
#include <memory>

#include "curvefs/src/metaserver/storage/utils.h"
#include "curvefs/src/metaserver/storage/memory_storage.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using UnorderedType = MemoryStorage::UnorderedType;
using OrderedType = MemoryStorage::OrderedType;

MemoryStorage::MemoryStorage(StorageOptions options)
    : options_(options) {}

STORAGE_TYPE MemoryStorage::Type() {
    return STORAGE_TYPE::MEMORY_STORAGE;
}

bool MemoryStorage::Open() {
    return true;
}

bool MemoryStorage::Close() {
    return true;
}

std::shared_ptr<UnorderedType> MemoryStorage::GetUnordered(
    const std::string& name) {
    {
        ReadLockGuard readLockGuard(rwLock_);
        auto iter = unorderedDict_.find(name);
        if (iter != unorderedDict_.end()) {
            return iter->second;
        }
    }
    {
        WriteLockGuard writeLockGuard(rwLock_);
        auto ret = unorderedDict_.emplace(
            name, std::make_shared<UnorderedType>());
        return ret.first->second;
    }
}

std::shared_ptr<OrderedType> MemoryStorage::GetOrdered(
    const std::string& name) {
    {
        ReadLockGuard readLockGuard(rwLock_);
        auto iter = orderedDict_.find(name);
        if (iter != orderedDict_.end()) {
            return iter->second;
        }
    }
    {
        WriteLockGuard writeLockGuard(rwLock_);
        auto ret = orderedDict_.emplace(
            name, std::make_shared<OrderedType>());
        return ret.first->second;
    }
}

Status MemoryStorage::HGet(const std::string& name,
                           const std::string& key,
                           std::string* value) {
    auto container = GetUnordered(name);
    auto iter = container->find(key);
    if (iter == container->end()) {
        return Status::NotFound();
    }
    *value = iter->second;
    return Status::OK();
}

Status MemoryStorage::HSet(const std::string& name,
                           const std::string& key,
                           const std::string& value) {
    auto container = GetUnordered(name);
    auto ret = container->emplace(key, value);
    if (!ret.second) {  // already exist
        ret.first->second = value;
    }
    return Status::OK();
}

Status MemoryStorage::HDel(const std::string& name,
                           const std::string& key) {
    auto container = GetUnordered(name);
    auto iter = container->find(key);
    if (iter == container->end()) {
        return Status::NotFound();
    }
    container->erase(iter);
    return Status::OK();
}

std::shared_ptr<Iterator> MemoryStorage::HGetAll(const std::string& name) {
    auto container = GetUnordered(name);
    return std::make_shared<UnorderedContainerIterator<UnorderedType>>(
        container, "");
}

size_t MemoryStorage::HSize(const std::string& name) {
    auto container = GetUnordered(name);
    return container->size();
}

Status MemoryStorage::HClear(const std::string& name) {
    auto container = GetUnordered(name);
    container->clear();
    return Status::OK();
}

Status MemoryStorage::SGet(const std::string& name,
                           const std::string& key,
                           std::string* value) {
    auto container = GetOrdered(name);
    auto iter = container->find(key);
    if (iter == container->end()) {
        return Status::NotFound();
    }
    *value = iter->second;
    return Status::OK();
}

Status MemoryStorage::SSet(const std::string& name,
                           const std::string& key,
                           const std::string& value) {
    auto container = GetOrdered(name);
    auto ret = container->emplace(key, value);
    if (!ret.second) {  // already exist
        ret.first->second = value;
    }
    return Status::OK();
}

Status MemoryStorage::SDel(const std::string& name, const std::string& key) {
    auto container = GetOrdered(name);
    auto iter = container->find(key);
    if (iter == container->end()) {
        return Status::NotFound();
    }
    container->erase(iter);
    return Status::OK();
}

std::shared_ptr<Iterator> MemoryStorage::SSeek(const std::string& name,
                                                const std::string& prefix) {
    auto container = GetOrdered(name);
    return std::make_shared<OrderedContainerIterator<OrderedType>>(
        container, prefix);
}

std::shared_ptr<Iterator> MemoryStorage::SGetAll(const std::string& name) {
    auto container = GetOrdered(name);
    return std::make_shared<OrderedContainerIterator<OrderedType>>(
        container, "");
}

size_t MemoryStorage::SSize(const std::string& name) {
    auto container = GetOrdered(name);
    return container->size();
}

Status MemoryStorage::SClear(const std::string& name) {
    auto container = GetOrdered(name);
    container->clear();
    return Status::OK();
}

std::shared_ptr<StorageTransaction> MemoryStorage::BeginTransaction() {
    return std::make_shared<MemoryStorage>(*this);
}

Status MemoryStorage::Commit() {
    return Status::OK();
}

void RocksDBStorage::Rollback()  {
    // do nothing
}

bool MemoryStorage::GetStatistics(StorageStatistics* statistics) {
    statistics->maxMemoryQuotaBytes = options_.maxMemoryQuotaBytes;
    statistics->maxDiskQuotaBytes = options_.maxDiskQuotaBytes;

    // memory usage bytes
    uint64_t vmRSS;
    if (!GetProcMemory(&vmRSS)) {
        return false;
    }
    statistics->memoryUsageBytes = vmRSS;

    // disk usage bytes
    uint64_t total, available;
    if (!GetFileSystemSpaces(options_.dataDir, &total, &available)) {
        LOG(ERROR) << "Get filesystem space failed.";
        return false;
    }
    statistics->diskUsageBytes = total - available;

    return true;
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
