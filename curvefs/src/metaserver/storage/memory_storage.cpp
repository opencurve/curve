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

using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;
using UnorderedContainerType =
    MemoryStorage::UnorderedContainerType;
using UnorderedSeralizedContainerType =
    MemoryStorage::UnorderedSeralizedContainerType;
using OrderedContainerType =
    MemoryStorage::OrderedContainerType;
using OrderedSeralizedContainerType =
    MemoryStorage::OrderedSeralizedContainerType;

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


#define GET_CONTAINER(TYPE, NAME)                                      \
[&]() {                                                                \
    auto dict = TYPE##Dict_;                                           \
    {                                                                  \
        ReadLockGuard readLockGuard(rwLock_);                          \
        auto iter = dict.find(NAME);                                   \
        if (iter != dict.end()) {                                      \
            return iter->second;                                       \
        }                                                              \
    }                                                                  \
    {                                                                  \
        WriteLockGuard writeLockGuard(rwLock_);                        \
        auto iter = dict.find(NAME);                                   \
        if (iter != dict.end()) {                                      \
            return iter->second;                                       \
        }                                                              \
        auto ret = dict.emplace(NAME, std::make_shared<TYPE##Type>()); \
        return ret.first->second;                                      \
    }                                                                  \
}()


#define GET(TYPE, NAME, KEY, VALUE)             \
do {                                            \
    auto container = GET_CONTAINER(TYPE, NAME); \
    auto iter = container->find(KEY);           \
    if (iter == container->end()) {             \
        return Status::NotFound();              \
    }                                           \
    VALUE->CopyFrom(*iter->second->Message());  \
    return Status::OK();                        \
} while (0)


#define GET_SERALIZED(TYPE, NAME, KEY, VALUE)    \
do {                                             \
    auto container = GET_CONTAINER(TYPE, NAME);  \
    auto iter = container->find(KEY);            \
    if (iter == container->end()) {              \
        return Status::NotFound();               \
    }                                            \
                                                 \
    if (!VALUE->ParseFromString(iter->second)) { \
        return Status::ParsedFailed();           \
    }                                            \
    return Status::OK();                         \
} while (0)


#define SET(TYPE, NAME, KEY, VALUE)                            \
do {                                                           \
    auto container = GET_CONTAINER(TYPE, NAME);                \
    auto valueWrapper = std::make_shared<ValueWrapper>(VALUE); \
    auto ret = container->emplace(KEY, valueWrapper);          \
    if (!ret.second) {                                         \
        ret.first->second = valueWrapper;                      \
    }                                                          \
    return Status::OK();                                       \
} while (0)


#define SET_SERALIZED(TYPE, NAME, KEY, VALUE)   \
do {                                            \
    auto container = GET_CONTAINER(TYPE, NAME); \
    std::string svalue;                         \
    if (!VALUE.SerializeToString(&svalue)) {    \
        return Status::SerializedFailed();      \
    }                                           \
    auto ret = container->emplace(KEY, svalue); \
    if (!ret.second) {                          \
        ret.first->second = svalue;             \
    }                                           \
    return Status::OK();                        \
} while (0)


#define DEL(TYPE, NAME, KEY)                    \
do {                                            \
    auto container = GET_CONTAINER(TYPE, NAME); \
    auto iter = container->find(KEY);           \
    if (iter == container->end()) {             \
        return Status::NotFound();              \
    }                                           \
    container->erase(iter);                     \
    return Status::OK();                        \
} while (0)


#define SEEK(TYPE, NAME, PREFIX)                                   \
do {                                                               \
    auto container = GET_CONTAINER(TYPE, NAME);                    \
    return std::make_shared<OrderedContainerIterator<TYPE##Type>>( \
        container, PREFIX, options_.compression);                  \
} while (0)


#define GET_ALL(TYPE, NAME)                              \
do {                                                     \
    auto container = GET_CONTAINER(TYPE, NAME);          \
    return std::make_shared<TYPE##Iterator<TYPE##Type>>( \
        container, "", options_.compression);            \
} while (0)


#define SIZE(TYPE, NAME)                        \
do {                                            \
    auto container = GET_CONTAINER(TYPE, NAME); \
    return container->size();                   \
} while (0)


#define CLEAR(TYPE, NAME)                       \
do {                                            \
    auto container = GET_CONTAINER(TYPE, NAME); \
    container->clear();                         \
    return Status::OK();                        \
} while (0)

Status MemoryStorage::HGet(const std::string& name,
                           const std::string& key,
                           ValueType* value) {
    if (options_.compression) {
        GET_SERALIZED(UnorderedSeralizedContainer, name, key, value);
    }
    GET(UnorderedContainer, name, key, value);
}

Status MemoryStorage::HSet(const std::string& name,
                           const std::string& key,
                           const ValueType& value) {
    if (options_.compression) {
        SET_SERALIZED(UnorderedSeralizedContainer, name, key, value);
    }
    SET(UnorderedContainer, name, key, value);
}

Status MemoryStorage::HDel(const std::string& name,
                           const std::string& key) {
    if (options_.compression) {
        DEL(UnorderedSeralizedContainer, name, key);
    }
    DEL(UnorderedContainer, name, key);
}

std::shared_ptr<Iterator> MemoryStorage::HGetAll(const std::string& name) {
    if (options_.compression) {
        GET_ALL(UnorderedSeralizedContainer, name);
    }
    GET_ALL(UnorderedContainer, name);
}

size_t MemoryStorage::HSize(const std::string& name) {
    if (options_.compression) {
        SIZE(UnorderedSeralizedContainer, name);
    }
    SIZE(UnorderedContainer, name);
}

Status MemoryStorage::HClear(const std::string& name) {
    if (options_.compression) {
        CLEAR(UnorderedSeralizedContainer, name);
    }
    CLEAR(UnorderedContainer, name);
}

Status MemoryStorage::SGet(const std::string& name,
                           const std::string& key,
                           ValueType* value) {
    if (options_.compression) {
        GET_SERALIZED(OrderedSeralizedContainer, name, key, value);
    }
    GET(OrderedContainer, name, key, value);
}

Status MemoryStorage::SSet(const std::string& name,
                           const std::string& key,
                           const ValueType& value) {
    if (options_.compression) {
        SET_SERALIZED(OrderedSeralizedContainer, name, key, value);
    }
    SET(OrderedContainer, name, key, value);
}

Status MemoryStorage::SDel(const std::string& name, const std::string& key) {
    if (options_.compression) {
        DEL(OrderedSeralizedContainer, name, key);
    }
    DEL(OrderedContainer, name, key);
}

std::shared_ptr<Iterator> MemoryStorage::SSeek(const std::string& name,
                                                const std::string& prefix) {
    if (options_.compression) {
        SEEK(OrderedSeralizedContainer, name, prefix);
    }
    SEEK(OrderedContainer, name, prefix);
}

std::shared_ptr<Iterator> MemoryStorage::SGetAll(const std::string& name) {
    if (options_.compression) {
        GET_ALL(OrderedSeralizedContainer, name);
    }
    GET_ALL(OrderedContainer, name);
}

size_t MemoryStorage::SSize(const std::string& name) {
    if (options_.compression) {
        SIZE(OrderedSeralizedContainer, name);
    }
    SIZE(OrderedContainer, name);
}

Status MemoryStorage::SClear(const std::string& name) {
    if (options_.compression) {
        CLEAR(OrderedSeralizedContainer, name);
    }
    CLEAR(OrderedContainer, name);
}

std::shared_ptr<StorageTransaction> MemoryStorage::BeginTransaction() {
    return std::shared_ptr<MemoryStorage>(this, [](MemoryStorage*){});
}

Status MemoryStorage::Commit() {
    return Status::OK();
}

Status MemoryStorage::Rollback()  {
    return Status::OK();
}

bool MemoryStorage::GetStatistics(StorageStatistics* statistics) {
    statistics->maxMemoryQuotaBytes = options_.maxMemoryQuotaBytes;
    statistics->maxDiskQuotaBytes = options_.maxDiskQuotaBytes;

    // memory usage bytes
    if (!GetProcMemory(&statistics->memoryUsageBytes)) {
        return false;
    }

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
