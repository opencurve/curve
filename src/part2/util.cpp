/*
 * Project: nebd
 * Created Date: Sunday January 19th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#include "src/part2/util.h"

namespace nebd {
namespace server {

NebdFileType GetFileType(const std::string& fileName) {
    size_t pos = fileName.find_first_of(':');
    if (pos == std::string::npos) {
        return NebdFileType::UNKWOWN;
    }
    std::string type = fileName.substr(0, pos);
    if (type == CURVE_PREFIX) {
        return NebdFileType::CURVE;
    } else if (type == CEPH_PREFIX) {
        return NebdFileType::CEPH;
    } else if (type == TEST_PREFIX) {
        return NebdFileType::TEST;
    } else {
        return NebdFileType::UNKWOWN;
    }
}

std::string Op2Str(LIBAIO_OP op) {
    switch (op) {
        case LIBAIO_OP::LIBAIO_OP_READ:
            return "READ";
        case LIBAIO_OP::LIBAIO_OP_WRITE:
            return "WRITE";
        case LIBAIO_OP::LIBAIO_OP_DISCARD:
            return "DISCARD";
        case LIBAIO_OP::LIBAIO_OP_FLUSH:
            return "FLUSH";
        default:
            return "UNKWOWN";
    }
}

std::ostream& operator<<(std::ostream& os, const NebdServerAioContext& c) {
    os << "[type: " << Op2Str(c.op)
       << ", offset: " << c.offset
       << ", length: " << c.length
       << ", ret: " << c.ret
       << "]";
    return os;
}

int FdAllocator::GetNext() {
    std::unique_lock<std::mutex> lock(mtx_);
    if (fd_ == INT_MAX || fd_ < 0) {
        fd_ = 0;
    }
    return ++fd_;
}

void FdAllocator::InitFd(int fd) {
    fd_ = fd;
}

}  // namespace server
}  // namespace nebd

