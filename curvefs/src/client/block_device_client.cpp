/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Project: curve
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#include <string>

#include "src/client/service_helper.h"
#include "curvefs/src/client/block_device_client.h"

namespace curvefs {
namespace client {

using ::curve::client::UserInfo;

CURVEFS_ERROR BlockDeviceClientImpl::Init(
    const BlockDeviceClientOptions& options) {
    if (fileClient_->Init(options.configPath) != LIBCURVE_ERROR::OK) {
        return CURVEFS_ERROR::FAILED;
    }

    std::string filename;
    UserInfo userInfo;
    bool succ = ::curve::client::ServiceHelper::GetUserInfoFromFilename(
        options.volumeName, &filename, &userInfo.owner);

    if (!succ) {
        LOG(ERROR) << "Get user info from filename failed";
        return CURVEFS_ERROR::FAILED;
    }

    auto retCode = fileClient_->Open(filename, userInfo, nullptr);
    if (retCode <= 0) {
        LOG(ERROR) << "Open file failed, filename = " << filename
                   << ", retCode = " << retCode;
        return CURVEFS_ERROR::FAILED;
    }

    fd_ = retCode;
    isOpen_ = true;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR BlockDeviceClientImpl::UnInit() {
    if (!isOpen_ || fd_ <= 0) {
        LOG(WARNING) << "File already closed";
        return CURVEFS_ERROR::BAD_FD;
    }

    int retCode;
    if ((retCode = fileClient_->Close(fd_)) != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "Close file failed, retCode = " << retCode;
        return CURVEFS_ERROR::FAILED;
    }

    isOpen_ = false;
    fileClient_->UnInit();
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR BlockDeviceClientImpl::Read(char* buf,
                                          off_t offset,
                                          size_t length) {
    if (!isOpen_ || fd_ <= 0) {
        return CURVEFS_ERROR::BAD_FD;
    } else if (0 == length) {
        return CURVEFS_ERROR::OK;
    } else if (IsAligned(offset, length)) {
        return AlignRead(buf, offset, length);
    }

    auto range = CalcAlignRange(offset, offset + length);  // [start, end)
    off_t readStart = range.first;
    off_t readEnd = range.second;
    size_t readLength = readEnd - readStart;
    std::unique_ptr<char[]> readBuffer(new (std::nothrow) char[readLength]);

    auto retCode = AlignRead(readBuffer.get(), readStart, readLength);
    if (retCode == CURVEFS_ERROR::OK) {
        memcpy(buf, readBuffer.get() + (offset - readStart), length);
    }

    return retCode;
}

CURVEFS_ERROR BlockDeviceClientImpl::Write(const char* buf,
                                           off_t offset,
                                           size_t length) {
    if (!isOpen_ || fd_ <= 0) {
        return CURVEFS_ERROR::BAD_FD;
    } else if (0 == length) {
        return CURVEFS_ERROR::OK;
    } else if (IsAligned(offset, length)) {
        return AlignWrite(buf, offset, length);
    }

    auto range = CalcAlignRange(offset, offset + length);  // [start, end)
    off_t writeStart = range.first;
    off_t writeEnd = range.second;
    size_t writeLength = writeEnd - writeStart;
    std::unique_ptr<char[]> writeBuffer(new (std::nothrow) char[writeLength]);

    auto retCode = WritePadding(
        writeBuffer.get(), writeStart, writeEnd, offset, length);
    if (retCode == CURVEFS_ERROR::OK) {
        memcpy(writeBuffer.get() + (offset - writeStart), buf, length);
        retCode = AlignWrite(writeBuffer.get(), writeStart, writeLength);
    }

    return retCode;
}

CURVEFS_ERROR BlockDeviceClientImpl::WritePadding(char* writeBuffer,
                                                  off_t writeStart,
                                                  off_t writeEnd,
                                                  off_t offset,
                                                  size_t length) {
    off_t readEnd = 0;
    size_t writeLength = writeEnd - writeStart;

    if (offset != writeStart) {  // Padding leading
        auto retCode = AlignRead(writeBuffer, writeStart,
                                 IO_ALIGNED_BLOCK_SIZE);
        if (retCode != CURVEFS_ERROR::OK) {
            return retCode;
        }
        readEnd = writeStart + IO_ALIGNED_BLOCK_SIZE;
    }

    // Readed or the write trailing is aligned
    if (offset + length <= readEnd || offset + length == writeEnd) {
        return CURVEFS_ERROR::OK;
    }

    // Padding trailing
    return AlignRead(writeBuffer + writeLength - IO_ALIGNED_BLOCK_SIZE,
                     writeEnd - IO_ALIGNED_BLOCK_SIZE,
                     IO_ALIGNED_BLOCK_SIZE);
}

CURVEFS_ERROR BlockDeviceClientImpl::AlignRead(char* buf,
                                               off_t offset,
                                               size_t length) {
    auto retCode = fileClient_->Read(fd_, buf, offset, length);
    if (retCode == LIBCURVE_ERROR::OK) {
        return CURVEFS_ERROR::OK;
    }

    LOG(ERROR) << "Read file failed, retCode = " << retCode;
    return CURVEFS_ERROR::FAILED;
}

CURVEFS_ERROR BlockDeviceClientImpl::AlignWrite(const char* buf,
                                                off_t offset,
                                                size_t length) {
    auto retCode = fileClient_->Write(fd_, buf, offset, length);
    if (retCode == LIBCURVE_ERROR::OK) {
        return CURVEFS_ERROR::OK;
    }

    LOG(ERROR) << "Write file failed, retCode = " << retCode;
    return CURVEFS_ERROR::FAILED;
}

inline bool BlockDeviceClientImpl::IsAligned(off_t offset, size_t length) {
    return (offset % IO_ALIGNED_BLOCK_SIZE == 0) &&
           (length % IO_ALIGNED_BLOCK_SIZE == 0);
}

inline Range BlockDeviceClientImpl::CalcAlignRange(off_t start, off_t end) {
    return Range(start - start % IO_ALIGNED_BLOCK_SIZE,
                 CURVE_ALIGN(end, IO_ALIGNED_BLOCK_SIZE));
}

}  // namespace client
}  // namespace curvefs
