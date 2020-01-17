#include "src/part2/request_executor_ceph.h"

namespace nebd {
namespace server {

std::shared_ptr<NebdFileInstance>
CephRequestExcutor::Open(const std::string& filename) {
    // TODO
    return nullptr;
}

int CephRequestExcutor::Close(NebdFileInstance* fd) {
    // TODO
    return 0;
}

int CephRequestExcutor::Extend(NebdFileInstance* fd, int64_t newsize) {
    // TODO
    return 0;
}

int CephRequestExcutor::StatFile(NebdFileInstance* fd, NebdFileInfo* fileInfo) {
    // TODO
    return 0;
}

int CephRequestExcutor::Discard(NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int CephRequestExcutor::AioRead(NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int CephRequestExcutor::AioWrite(NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int CephRequestExcutor::Flush(NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int CephRequestExcutor::GetInfo(NebdFileInstance* fd, NebdFileInfo* fileInfo) {
    // TODO
    return 0;
}

int CephRequestExcutor::InvalidCache(NebdFileInstance* fd) {
    // TODO
    return 0;
}


}  // namespace server
}  // namespace nebd

