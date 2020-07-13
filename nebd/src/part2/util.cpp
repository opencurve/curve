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
 * Created Date: Sunday January 19th 2020
 * Author: yangyaokai
 */

#include "nebd/src/part2/util.h"

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
    } else if (type == TEST_PREFIX) {
        return NebdFileType::TEST;
    } else {
        return NebdFileType::UNKWOWN;
    }
}

std::string NebdFileStatus2Str(NebdFileStatus status) {
    switch (status) {
        case NebdFileStatus::CLOSED:
            return "CLOSED";
        case NebdFileStatus::OPENED:
            return "OPENED";
        case NebdFileStatus::DESTROYED:
            return "DESTROYED";
        default:
            return "UNKWOWN";
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
       << ", size: " << c.size
       << ", ret: " << c.ret
       << "]";
    return os;
}

std::ostream& operator<<(std::ostream& os, const NebdFileMeta& meta) {
    os << "[filename: " << meta.fileName << ", fd: " << meta.fd;
    for (const auto& pair : meta.xattr) {
        os << ", " << pair.first << ": " << pair.second;
    }
    os << "]";
    return os;
}

bool operator==(const NebdFileMeta& lMeta, const NebdFileMeta& rMeta) {
    return lMeta.fd == rMeta.fd &&
           lMeta.fileName == rMeta.fileName &&
           lMeta.xattr == rMeta.xattr;
}

bool operator!=(const NebdFileMeta& lMeta, const NebdFileMeta& rMeta) {
    return !(lMeta == rMeta);
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
