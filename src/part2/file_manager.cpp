/*
 * Project: nebd
 * Created Date: Thursday January 16th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#include "src/part2/file_manager.h"

namespace nebd {
namespace server {

NebdFileManager::NebdFileManager() 
    :  metaFileManager_(nullptr)
    ,  heartbeatTimeoutS_(10) {

}

NebdFileManager::~NebdFileManager() {
    // TODO
}

int NebdFileManager::Init(NebdFileManagerOption option) {
    // TODO
    return 0;
}

int NebdFileManager::Load() {
    // TODO
    return 0;
}

int NebdFileManager::UpdateFileTimestamp(int fd) {
    // TODO
    return 0;
}

int NebdFileManager::Open(const std::string& filename) {
    // TODO
    return 0;
}

int NebdFileManager::Close(int fd) {
    // TODO
    return 0;
}

int NebdFileManager::Extend(int fd, int64_t newsize) {
    // TODO
    return 0;
}

int NebdFileManager::StatFile(int fd, NebdFileInfo* fileInfo) {
    // TODO
    return 0;
}

int NebdFileManager::Discard(int fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int NebdFileManager::AioRead(int fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int NebdFileManager::AioWrite(int fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int NebdFileManager::Flush(int fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int NebdFileManager::GetInfo(int fd, NebdFileInfo* fileInfo) {
    // TODO
    return 0;
}

int NebdFileManager::InvalidCache(int fd) {
    // TODO
    return 0;
}

void NebdFileManager::CheckTimeoutFunc() {
    // TODO
}

}  // namespace server
}  // namespace nebd

