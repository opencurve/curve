#include "src/part2/request_executor_curve.h"

namespace nebd {
namespace server {

std::shared_ptr<NebdFileInstance>
CurveRequestExcutor::Open(const std::string& filename) {
    // TODO
    return nullptr;
}

int CurveRequestExcutor::Close(NebdFileInstance* fd) {
    // TODO
    return 0;
}

int CurveRequestExcutor::Extend(NebdFileInstance* fd, int64_t newsize) {
    // TODO
    return 0;
}

int CurveRequestExcutor::StatFile(NebdFileInstance* fd) {
    // TODO
    return 0;
}

int CurveRequestExcutor::Discard(NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int CurveRequestExcutor::AioRead(NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int CurveRequestExcutor::AioWrite(NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int CurveRequestExcutor::Flush(NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    // TODO
    return 0;
}

int CurveRequestExcutor::GetInfo(NebdFileInstance* fd) {
    // TODO
    return 0;
}

int CurveRequestExcutor::InvalidCache(NebdFileInstance* fd) {
    // TODO
    return 0;
}


}  // namespace server
}  // namespace nebd

