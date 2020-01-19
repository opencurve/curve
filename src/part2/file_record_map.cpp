/*
 * Project: nebd
 * Created Date: Thursday January 16th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#include "src/part2/file_record_map.h"

namespace nebd {
namespace server {

NebdFileRecordPtr FileRecordMap::GetRecord(int fd) {
    ReadLockGuard readGuard(rwLock_);
    if (map_.find(fd) == map_.end()) {
        return nullptr;
    }
    return map_[fd];
}

NebdFileRecordPtr FileRecordMap::GetRecord(const std::string& fileName) {
    ReadLockGuard readGuard(rwLock_);
    int fd = FindRecordUnlocked(fileName);
    if (fd > 0) {
        return map_[fd];
    }
    return nullptr;
}

bool FileRecordMap::UpdateRecord(NebdFileRecordPtr fileRecord) {
    WriteLockGuard writeGuard(rwLock_);
    int fd = fileRecord->fd;

    // 如果fd已存在，判断文件名是否相同，不同则返回错误，相同则直接返回成功
    if (map_.find(fd) != map_.end()) {
        if (map_[fd]->fileName == fileRecord->fileName) {
            return true;
        } else {
            LOG(ERROR) << "Conflict record. "
                       << "Old fileName: " << map_[fd]->fileName
                       << ", new fileName: " << fileRecord->fileName
                       << ", fd: " << fd;
            return false;
        }
    }

    // 如果该文件记录已存在，则先删除旧的记录
    int oldFd = FindRecordUnlocked(fileRecord->fileName);
    if (oldFd > 0) {
        LOG(WARNING) << "Remove old record, filename: "
                        << fileRecord->fileName
                        << ", old fd: " << oldFd
                        << ", newFd: " << fd;
        map_.erase(oldFd);
    }

    map_[fd] = fileRecord;
    return true;
}

void FileRecordMap::RemoveRecord(int fd) {
    WriteLockGuard writeGuard(rwLock_);
    if (map_.find(fd) != map_.end()) {
        map_.erase(fd);
    }
}

bool FileRecordMap::Exist(int fd) {
    ReadLockGuard readGuard(rwLock_);
    return map_.find(fd) != map_.end();
}

void FileRecordMap::Clear() {
    WriteLockGuard writeGuard(rwLock_);
    map_.clear();
}

std::unordered_map<int, NebdFileRecordPtr> FileRecordMap::ListRecords() {
    ReadLockGuard readGuard(rwLock_);
    return map_;
}

int FileRecordMap::FindRecordUnlocked(const std::string& fileName) {
    int fd = 0;
    for (const auto& pair : map_) {
        if (pair.second->fileName == fileName) {
            fd = pair.first;
            break;
        }
    }
    return fd;
}

}  // namespace server
}  // namespace nebd

