/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: nebd
 * Created Date: Thursday January 16th 2020
 * Author: yangyaokai
 */

#include "nebd/src/part2/file_manager.h"

#include <google/protobuf/util/message_differencer.h>

#include <algorithm>
#include <chrono>  // NOLINT
#include <vector>

#include "nebd/src/part2/util.h"
#include "src/common/telemetry/telemetry.h"


namespace nebd {
namespace server {
NebdFileManager::NebdFileManager(MetaFileManagerPtr metaFileManager)
    : isRunning_(false)
    , metaFileManager_(metaFileManager) {}

NebdFileManager::~NebdFileManager() {}

int NebdFileManager::Run() {
    if (isRunning_.exchange(true)) {
        LOG(WARNING) << "file manager is on running.";
        return -1;
    }

    int ret = Load();
    if (ret < 0) {
        LOG(ERROR) << "Run file manager failed.";
        isRunning_.store(false);
        return -1;
    }
    return 0;
}

int NebdFileManager::Fini() {
    isRunning_.store(false);
    fileMap_.clear();
    LOG(INFO) << "Stop file manager success.";
    return 0;
}

int NebdFileManager::Load() {
    // 从元数据文件中读取持久化的文件信息
    std::vector<NebdFileMeta> fileMetas;
    int ret = metaFileManager_->ListFileMeta(&fileMetas);
    if (ret < 0) {
        LOG(ERROR) << "Load file metas failed.";
        return ret;
    }
    // 根据持久化的信息重新open文件
    int maxFd = 0;
    for (auto& fileMeta : fileMetas) {
        NebdFileEntityPtr entity =
            GenerateFileEntity(fileMeta.fd, fileMeta.fileName);
        CHECK(entity != nullptr) << "file entity is null.";
        int ret = entity->Reopen(fileMeta.xattr);
        if (ret < 0) {
            LOG(WARNING) << "Reopen file failed. "
                         << "filename: " << fileMeta.fileName
                         << ", fd: " << fileMeta.fd;
        }
        maxFd = std::max(maxFd, fileMeta.fd);
    }
    fdAlloc_.InitFd(maxFd);
    LOG(INFO) << "Load file record finished.";
    return 0;
}

int NebdFileManager::Open(const std::string& filename, const OpenFlags* flags) {
    NebdFileEntityPtr entity = GetOrCreateFileEntity(filename);
    if (entity == nullptr) {
        LOG(ERROR) << "Open file failed. filename: " << filename;
        return -1;
    }
    return entity->Open(flags);
}

int NebdFileManager::Close(int fd, bool removeRecord) {
    NebdFileEntityPtr entity = GetFileEntity(fd);
    if (entity == nullptr) {
        LOG(WARNING) << "Close file failed. fd: " << fd;
        return 0;
    }
    int ret = entity->Close(removeRecord);
    if (ret == 0 && removeRecord) {
        RemoveEntity(fd);
        LOG(INFO) << "file entity is removed. fd: " << fd;
    }
    return ret;
}

int NebdFileManager::Discard(int fd, NebdServerAioContext* aioctx) {
    NebdFileEntityPtr entity = GetFileEntity(fd);
    if (entity == nullptr) {
        LOG(ERROR) << "Discard file failed. fd: " << fd;
        return -1;
    }
    return entity->Discard(aioctx);
}

int NebdFileManager::AioRead(int fd, NebdServerAioContext* aioctx) {
    auto span = curve::telemetry::GetTracer("AioRead")->StartSpan(
        "NebdClient::AioRead");
    NebdFileEntityPtr entity = GetFileEntity(fd);
    if (entity == nullptr) {
        LOG(ERROR) << "AioRead file failed. fd: " << fd;
        return -1;
    }
    return entity->AioRead(aioctx);
}

int NebdFileManager::AioWrite(int fd, NebdServerAioContext* aioctx) {
    NebdFileEntityPtr entity = GetFileEntity(fd);
    if (entity == nullptr) {
        LOG(ERROR) << "AioWrite file failed. fd: " << fd;
        return -1;
    }
    return entity->AioWrite(aioctx);
}

int NebdFileManager::Flush(int fd, NebdServerAioContext* aioctx) {
    NebdFileEntityPtr entity = GetFileEntity(fd);
    if (entity == nullptr) {
        LOG(ERROR) << "Flush file failed. fd: " << fd;
        return -1;
    }
    return entity->Flush(aioctx);
}

int NebdFileManager::Extend(int fd, int64_t newsize) {
    NebdFileEntityPtr entity = GetFileEntity(fd);
    if (entity == nullptr) {
        LOG(ERROR) << "Extend file failed. fd: " << fd;
        return -1;
    }
    return entity->Extend(newsize);
}

int NebdFileManager::GetInfo(int fd, NebdFileInfo* fileInfo) {
    NebdFileEntityPtr entity = GetFileEntity(fd);
    if (entity == nullptr) {
        LOG(ERROR) << "Get file info failed. fd: " << fd;
        return -1;
    }
    return entity->GetInfo(fileInfo);
}

int NebdFileManager::InvalidCache(int fd) {
    NebdFileEntityPtr entity = GetFileEntity(fd);
    if (entity == nullptr) {
        LOG(ERROR) << "Invalid file cache failed. fd: " << fd;
        return -1;
    }
    return entity->InvalidCache();
}

NebdFileEntityPtr
NebdFileManager::GetFileEntity(int fd) {
    auto span = curve::telemetry::GetTracer("AioRead")->StartSpan(
        "NebdFileManager::GetFileEntity");
    ReadLockGuard readLock(rwLock_);
    auto iter = fileMap_.find(fd);
    if (iter == fileMap_.end()) {
        return nullptr;
    }
    return iter->second;
}

int NebdFileManager::GenerateValidFd() {
    int fd = 0;
    while (true) {
        fd = fdAlloc_.GetNext();
        if (fileMap_.find(fd) == fileMap_.end()) {
            break;
        }
    }
    return fd;
}

NebdFileEntityPtr NebdFileManager::GetOrCreateFileEntity(
    const std::string& fileName) {
    {
        ReadLockGuard readLock(rwLock_);
        for (const auto& pair : fileMap_) {
            std::string curFile = pair.second->GetFileName();
            if (fileName == curFile) {
                return pair.second;
            }
        }
    }

    int fd = GenerateValidFd();
    return GenerateFileEntity(fd, fileName);
}

NebdFileEntityPtr NebdFileManager::GenerateFileEntity(
    int fd, const std::string& fileName) {
    WriteLockGuard writeLock(rwLock_);
    for (const auto& pair : fileMap_) {
        std::string curFile = pair.second->GetFileName();
        if (fileName == curFile) {
            return pair.second;
        }
    }

    // 检测是否存在冲突的文件记录
    auto iter = fileMap_.find(fd);
    if (iter != fileMap_.end()) {
        LOG(ERROR) << "File entity conflict. "
                   << "Exist filename: " << iter->second->GetFileName()
                   << ", Exist fd: " << iter->first
                   << ", Create filename: " << fileName
                   << ", Create fd: " << fd;
        return nullptr;
    }

    NebdFileEntityOption option;
    option.fd = fd;
    option.fileName = fileName;
    option.metaFileManager_ = metaFileManager_;
    NebdFileEntityPtr entity = std::make_shared<NebdFileEntity>();
    int ret = entity->Init(option);
    if (ret != 0) {
        LOG(ERROR) << "Generate file entity failed.";
        return nullptr;
    }
    fileMap_.emplace(fd, entity);
    return entity;
}

void NebdFileManager::RemoveEntity(int fd) {
    WriteLockGuard writeLock(rwLock_);
    auto iter = fileMap_.find(fd);
    if (iter != fileMap_.end()) {
        fileMap_.erase(iter);
    }
}

FileEntityMap NebdFileManager::GetFileEntityMap() {
    ReadLockGuard readLock(rwLock_);
    return fileMap_;
}

std::string NebdFileManager::DumpAllFileStatus() {
    ReadLockGuard readLock(rwLock_);
    std::ostringstream os;
    os << "{";
    for (const auto& pair : fileMap_) {
        os << *(pair.second);
    }
    os << "}";
    return os.str();
}

}  // namespace server
}  // namespace nebd
