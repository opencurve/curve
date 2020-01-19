#include "src/part2/request_executor_ceph.h"

namespace nebd {
namespace server {

std::shared_ptr<NebdFileInstance>
CephRequestExecutor::Open(const std::string& filename) {
    // TODO
    return nullptr;
}

std::shared_ptr<NebdFileInstance>
CephRequestExecutor::Reopen(const std::string& filename,
                            AdditionType addtion) {
    // TODO
    return nullptr;
}

int CephRequestExecutor::Close(NebdFileInstance* fd) {
    // TODO
    return 0;
}

int CephRequestExecutor::Extend(NebdFileInstance* fd, int64_t newsize) {
    // TODO
    return 0;
}

int CephRequestExecutor::StatFile(NebdFileInstance* fd, NebdFileInfo* fileInfo) {
    // TODO
    return 0;
}

int CephRequestExecutor::Discard(NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int CephRequestExecutor::AioRead(NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int CephRequestExecutor::AioWrite(NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int CephRequestExecutor::Flush(NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int CephRequestExecutor::GetInfo(NebdFileInstance* fd, NebdFileInfo* fileInfo) {
    // TODO
    return 0;
}

int CephRequestExecutor::InvalidCache(NebdFileInstance* fd) {
    // TODO
    return 0;
}


}  // namespace server
}  // namespace nebd

