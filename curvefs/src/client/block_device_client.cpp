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
#include <vector>
#include <map>

#include "src/client/service_helper.h"
#include "curvefs/src/client/block_device_client.h"

namespace curvefs {
namespace client {

using ::curve::client::UserInfo;

BlockDeviceClientImpl::BlockDeviceClientImpl()
    : fileClient_(std::make_shared<FileClient>()),
      fd_(-1) {}

BlockDeviceClientImpl::BlockDeviceClientImpl(
    const std::shared_ptr<FileClient>& fileClient)
    : fileClient_(fileClient),
      fd_(-1) {}

CURVEFS_ERROR BlockDeviceClientImpl::Init(
    const BlockDeviceClientOptions& options) {
    if (fileClient_->Init(options.configPath) != LIBCURVE_ERROR::OK) {
        return CURVEFS_ERROR::INTERNAL;
    }

    return CURVEFS_ERROR::OK;
}

void BlockDeviceClientImpl::UnInit() {
    fileClient_->UnInit();
}

CURVEFS_ERROR BlockDeviceClientImpl::Open(const std::string& filename,
                                          const std::string& owner) {
    UserInfo userInfo(owner);
    curve::client::OpenFlags flags;
    auto retCode = fileClient_->Open(filename, userInfo, flags);
    if (retCode < 0) {
        LOG(ERROR) << "Open file failed, filename = " << filename
                   << ", retCode = " << retCode;
        return CURVEFS_ERROR::INTERNAL;
    }

    fd_ = retCode;
    filename_ = filename;
    owner_ = owner;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR BlockDeviceClientImpl::Close() {
    if (fd_ < 0) {
        return CURVEFS_ERROR::OK;
    }

    int retCode;
    if ((retCode = fileClient_->Close(fd_)) != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "Close file failed, retCode = " << retCode;
        return CURVEFS_ERROR::INTERNAL;
    }

    fd_ = -1;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR BlockDeviceClientImpl::Stat(const std::string& filename,
                                          const std::string& owner,
                                          BlockDeviceStat* statInfo) {
    FileStatInfo fileStatInfo;
    UserInfo userInfo(owner);
    auto retCode = fileClient_->StatFile(filename, userInfo, &fileStatInfo);
    if (retCode != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "Stat file failed, retCode = " << retCode;
        return CURVEFS_ERROR::INTERNAL;
    }

    statInfo->length = fileStatInfo.length;
    if (!ConvertFileStatus(fileStatInfo.fileStatus, &statInfo->status)) {
        LOG(ERROR) << "Stat file failed, unknown file status: "
                   << fileStatInfo.fileStatus;
        return CURVEFS_ERROR::INTERNAL;
    }

    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR BlockDeviceClientImpl::Read(char* buf,
                                          off_t offset,
                                          size_t length) {
    if (fd_ < 0) {
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
    if (fd_ < 0) {
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
    std::vector<std::pair<off_t, size_t>> readvec;  // Align reads
    off_t readEnd = 0;

    // Padding leading
    if (offset != writeStart) {
        readvec.push_back(std::make_pair(writeStart, IO_ALIGNED_BLOCK_SIZE));
        readEnd = writeStart + IO_ALIGNED_BLOCK_SIZE;
    }

    // Padding trailing
    if (offset + length > readEnd && offset + length != writeEnd) {
        off_t readStart = writeEnd - IO_ALIGNED_BLOCK_SIZE;
        if (readvec.size() == 1 && readStart == readEnd) {
            readvec[0].second = IO_ALIGNED_BLOCK_SIZE * 2;
        } else {
            readvec.push_back(std::make_pair(readStart, IO_ALIGNED_BLOCK_SIZE));
        }
    }

    for (const auto& item : readvec) {
        auto retCode = AlignRead(writeBuffer + item.first - writeStart,
                                 item.first, item.second);
        if (retCode != CURVEFS_ERROR::OK) {
            return retCode;
        }
    }

    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR BlockDeviceClientImpl::AlignRead(char* buf,
                                               off_t offset,
                                               size_t length) {
    auto ret = fileClient_->Read(fd_, buf, offset, length);
    if (ret < 0) {
        LOG(ERROR) << "Read file failed, retCode = " << ret;
        return CURVEFS_ERROR::INTERNAL;
    } else if (ret != length) {
        LOG(ERROR) << "Read file failed, expect read " << length
                   << " bytes, actual read " << ret << " bytes";
        return CURVEFS_ERROR::INTERNAL;
    }

    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR BlockDeviceClientImpl::AlignWrite(const char* buf,
                                                off_t offset,
                                                size_t length) {
    auto ret = fileClient_->Write(fd_, buf, offset, length);
    if (ret < 0) {
        LOG(ERROR) << "Write file failed, retCode = " << ret;
        return CURVEFS_ERROR::INTERNAL;
    } else if (ret != length) {
        LOG(ERROR) << "Write file failed, expect write " << length
                   << " bytes, actual write " << ret << " bytes";
        return CURVEFS_ERROR::INTERNAL;
    }

    return CURVEFS_ERROR::OK;
}

bool BlockDeviceClientImpl::ConvertFileStatus(int fileStatus,
                                              BlockDeviceStatus* bdStatus) {
    static const std::map<int, BlockDeviceStatus> fileStatusMap {
        { 0, BlockDeviceStatus::CREATED },
        { 1, BlockDeviceStatus::DELETING },
        { 2, BlockDeviceStatus::CLONING },
        { 3, BlockDeviceStatus::CLONE_META_INSTALLED },
        { 4, BlockDeviceStatus::CLONED },
        { 5, BlockDeviceStatus::BEING_CLONED }
    };

    auto iter = fileStatusMap.find(fileStatus);
    if (iter == fileStatusMap.end()) {
        return false;
    }

    *bdStatus = iter->second;
    return true;
}

inline bool BlockDeviceClientImpl::IsAligned(off_t offset, size_t length) {
    return (offset % IO_ALIGNED_BLOCK_SIZE == 0) &&
           (length % IO_ALIGNED_BLOCK_SIZE == 0);
}

inline off_t BlockDeviceClientImpl::Align(off_t offset, size_t alignment) {
    return (offset + (alignment - 1)) & ~(alignment - 1);
}

inline Range BlockDeviceClientImpl::CalcAlignRange(off_t start, off_t end) {
    return Range(start - start % IO_ALIGNED_BLOCK_SIZE,
                 Align(end, IO_ALIGNED_BLOCK_SIZE));
}

}  // namespace client
}  // namespace curvefs
