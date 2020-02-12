/*
 * Project: nebd
 * Created Date: Thursday February 13th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#include <utility>

#include "src/part2/filerecord_manager.h"

namespace nebd {
namespace server {

int FileRecordManager::Load() {
    int ret = metaFileManager_->ListFileRecord(&map_);
    if (ret < 0) {
        LOG(ERROR) << "List file records from meta file failed. ";
        return -1;
    }
    return 0;
}

bool FileRecordManager::GetRecord(int fd, NebdFileRecord* fileRecord) {
    ReadLockGuard readGuard(rwLock_);
    if (map_.find(fd) == map_.end()) {
        return false;
    }
    *fileRecord = map_[fd];
    return true;
}

bool FileRecordManager::GetRecord(const std::string& fileName,
                                  NebdFileRecord* fileRecord) {
    ReadLockGuard readGuard(rwLock_);
    int fd = FindRecordUnlocked(fileName);
    if (fd > 0) {
        *fileRecord = map_[fd];
        return true;
    }
    return false;
}

bool FileRecordManager::UpdateRecord(const NebdFileRecord& fileRecord) {
    WriteLockGuard writeGuard(rwLock_);
    int fd = fileRecord.fd;

    // 如果fd已存在，判断文件名是否相同，不同则返回错误
    bool isConflict = map_.find(fd) != map_.end() &&
                      map_[fd].fileName != fileRecord.fileName;
    if (isConflict) {
        LOG(ERROR) << "Conflict record. "
                   << "Old fileName: " << map_[fd].fileName
                   << ", new fileName: " << fileRecord.fileName
                   << ", fd: " << fd;
        return false;
    }

    FileRecordMap tempMap = map_;
    // 如果该文件记录已存在，且fd不同，则先删除旧的记录
    int oldFd = FindRecordUnlocked(fileRecord.fileName);
    if (oldFd != fileRecord.fd) {
        LOG(WARNING) << "Remove old record, filename: "
                     << fileRecord.fileName
                     << ", old fd: " << oldFd
                     << ", newFd: " << fileRecord.fd;
        tempMap.erase(oldFd);
    }
    tempMap[fd] = fileRecord;

    // 先更新元数据文件，如果更新成功再修改内存记录，否则返回失败
    int ret = metaFileManager_->UpdateMetaFile(tempMap);
    if (ret < 0) {
        LOG(ERROR) << "Update meta file failed. "
                   << "file name: " << fileRecord.fileName
                   << ", fd: " << fd;
        return false;
    }
    map_ = std::move(tempMap);
    return true;
}

bool FileRecordManager::RemoveRecord(int fd) {
    WriteLockGuard writeGuard(rwLock_);
    if (map_.find(fd) == map_.end()) {
        return true;
    }

    FileRecordMap tempMap = map_;
    tempMap.erase(fd);

    // 先更新元数据文件，如果更新成功再修改内存记录，否则返回失败
    int ret = metaFileManager_->UpdateMetaFile(tempMap);
    if (ret < 0) {
        LOG(ERROR) << "Update meta file failed. "
                   << "file name: " << map_[fd].fileName
                   << ", fd: " << fd;
        return false;
    }
    map_ = std::move(tempMap);
    return true;
}

bool FileRecordManager::Exist(int fd) {
    ReadLockGuard readGuard(rwLock_);
    return map_.find(fd) != map_.end();
}

void FileRecordManager::Clear() {
    WriteLockGuard writeGuard(rwLock_);
    map_.clear();
}

FileRecordMap FileRecordManager::ListRecords() {
    ReadLockGuard readGuard(rwLock_);
    return map_;
}

int FileRecordManager::FindRecordUnlocked(const std::string& fileName) {
    int fd = -1;
    for (const auto& pair : map_) {
        if (pair.second.fileName == fileName) {
            fd = pair.first;
            break;
        }
    }
    return fd;
}

bool FileRecordManager::UpdateFileTimestamp(int fd, uint64_t timestamp) {
    WriteLockGuard writeGuard(rwLock_);
    if (map_.find(fd) == map_.end()) {
        return false;
    }
    map_[fd].timeStamp = timestamp;
    return true;
}

bool FileRecordManager::GetFileTimestamp(int fd, uint64_t* timestamp) {
    ReadLockGuard readGuard(rwLock_);
    if (map_.find(fd) == map_.end()) {
        return false;
    }
    *timestamp = map_[fd].timeStamp;
    return true;
}

bool FileRecordManager::UpdateFileStatus(int fd, NebdFileStatus status) {
    WriteLockGuard writeGuard(rwLock_);
    if (map_.find(fd) == map_.end()) {
        return false;
    }
    map_[fd].status = status;
    return true;
}

bool FileRecordManager::GetFileStatus(int fd, NebdFileStatus* status) {
    ReadLockGuard readGuard(rwLock_);
    if (map_.find(fd) == map_.end()) {
        return false;
    }
    *status = map_[fd].status;
    return true;
}

}  // namespace server
}  // namespace nebd

