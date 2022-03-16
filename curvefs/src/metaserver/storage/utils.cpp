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
 * Date: 2022-02-27
 * Author: Jingli Chen (Wine93)
 */

#include <glog/logging.h>

#include <cstring>
#include <fstream>
#include <algorithm>

#include "src/fs/fs_common.h"
#include "src/fs/local_filesystem.h"
#include "src/fs/ext4_filesystem_impl.h"
#include "src/common/string_util.h"
#include "curvefs/src/metaserver/storage/utils.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curve::common::StringToUll;
using ::curve::common::RWLock;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;
using ::curve::fs::LocalFileSystem;
using ::curve::fs::Ext4FileSystemImpl;
using ContainerType = Counter::ContainerType;

std::shared_ptr<ContainerType> Counter::GetContainer(const std::string& name) {
    {
        ReadLockGuard readLockGuard(rwLock_);
        auto iter = containerDict_.find(name);
        if (iter != containerDict_.end()) {
            return iter->second;
        }
    }
    {
        WriteLockGuard writeLockGuard(rwLock_);
        auto ret = containerDict_.emplace(
            name, std::make_shared<ContainerType>());
        return ret.first->second;
    }
}

inline size_t Counter::ToInternalKey(const std::string& key) {
    return Hash(key);
}

inline void Counter::Insert(const std::string& name, const std::string& key) {
    auto container = GetContainer(name);
    auto ikey = ToInternalKey(key);
    container->emplace(ikey);
}

inline void Counter::Erase(const std::string& name, const std::string& key) {
    auto container = GetContainer(name);
    auto ikey = ToInternalKey(key);
    container->erase(ikey);
}

inline bool Counter::Find(const std::string& name, const std::string& key) {
    auto container = GetContainer(name);
    auto ikey = ToInternalKey(key);
    return container->find(ikey) != container->end();
}

inline size_t Counter::Size(const std::string& name) {
    auto container = GetContainer(name);
    return container->size();
}

std::string EncodeNumber(size_t num) {
    char buffer[sizeof(size_t)];
    std::memcpy(buffer, reinterpret_cast<char*>(&num), sizeof(size_t));
    return std::string(buffer, sizeof(size_t));
}

bool GetFileSystemSpaces(const std::string& path,
                         uint64_t* total,
                         uint64_t* available) {
    struct curve::fs::FileSystemInfo info;

    auto localFS = Ext4FileSystemImpl::getInstance();
    int ret = localFS->Statfs(path, &info);
    if (ret != 0) {
        LOG(ERROR) << "Failed to get file system space information, "
                   << " error message: " << strerror(errno);
        return false;
    }

    *total = info.total;
    *available = info.available;
    return true;
}

bool GetProcMemory(uint64_t* vmRSS) {
    std::string fileName = "/proc/self/status";
    std::ifstream file(fileName);
    if (!file.is_open()) {
        LOG(ERROR) << "Open file " << fileName << " failed";
        return false;
    }

    std::string line;
    while (getline(file, line)) {
        auto position = line.find("VmRSS:");
        if (position == line.npos) {
            continue;
        }

        std::string value = line.substr(position + 6);
        position = value.find("kB");
        value = value.substr(0, position);

        value.erase(std::remove_if(value.begin(), value.end(), isspace),
                    value.end());
        return StringToUll(value, vmRSS);
    }

    return false;
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
