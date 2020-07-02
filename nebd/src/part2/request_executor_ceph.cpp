/*
 * Project: nebd
 * Created Date: 2020-02-14
 * Author: charisu
 * Copyright (c) 2020 netease
 */

#include "src/part2/request_executor_ceph.h"

namespace nebd {
namespace server {

std::shared_ptr<NebdFileInstance>
CephRequestExecutor::Open(const std::string& filename) {
    return nullptr;
}

std::shared_ptr<NebdFileInstance>
CephRequestExecutor::Reopen(const std::string& filename,
                            const ExtendAttribute& xattr) {
    return nullptr;
}

int CephRequestExecutor::Close(NebdFileInstance* fd) {
    return 0;
}

int CephRequestExecutor::Extend(NebdFileInstance* fd,
                                int64_t newsize) {
    return 0;
}

int CephRequestExecutor::Discard(NebdFileInstance* fd,
                                 NebdServerAioContext* aioctx) {
    return 0;
}

int CephRequestExecutor::AioRead(NebdFileInstance* fd,
                                 NebdServerAioContext* aioctx) {
    return 0;
}

int CephRequestExecutor::AioWrite(NebdFileInstance* fd,
                                  NebdServerAioContext* aioctx) {
    return 0;
}

int CephRequestExecutor::Flush(NebdFileInstance* fd,
                               NebdServerAioContext* aioctx) {
    return 0;
}

int CephRequestExecutor::GetInfo(NebdFileInstance* fd,
                                 NebdFileInfo* fileInfo) {
    return 0;
}

int CephRequestExecutor::InvalidCache(NebdFileInstance* fd) {
    return 0;
}


}  // namespace server
}  // namespace nebd

