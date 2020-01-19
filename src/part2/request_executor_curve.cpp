#include "src/part2/request_executor_curve.h"

namespace nebd {
namespace server {

std::shared_ptr<NebdFileInstance>
CurveRequestExecutor::Open(const std::string& filename) {
    // TODO
    return nullptr;
}

std::shared_ptr<NebdFileInstance>
CurveRequestExecutor::Reopen(const std::string& filename,
                             AdditionType addtion) {
    // TODO
    return nullptr;
}

int CurveRequestExecutor::Close(NebdFileInstance* fd) {
    // TODO
    return 0;
}

int CurveRequestExecutor::Extend(NebdFileInstance* fd, int64_t newsize) {
    // TODO
    return 0;
}

int CurveRequestExecutor::StatFile(NebdFileInstance* fd, NebdFileInfo* fileInfo) {
    // TODO
    return 0;
}

int CurveRequestExecutor::Discard(NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int CurveRequestExecutor::AioRead(NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int CurveRequestExecutor::AioWrite(NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int CurveRequestExecutor::Flush(NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int CurveRequestExecutor::GetInfo(NebdFileInstance* fd, NebdFileInfo* fileInfo) {
    // TODO
    return 0;
}

int CurveRequestExecutor::InvalidCache(NebdFileInstance* fd) {
    // TODO
    return 0;
}


}  // namespace server
}  // namespace nebd

