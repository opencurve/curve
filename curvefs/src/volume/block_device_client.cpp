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

#include "curvefs/src/volume/block_device_client.h"

#include <glog/logging.h>

#include <map>
#include <string>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "curvefs/src/common/metric_utils.h"
#include "src/client/service_helper.h"
#include "src/common/concurrent/count_down_event.h"

namespace curvefs {
namespace volume {

using ::curve::client::UserInfo;
using ::curve::common::CountDownEvent;
using ::curvefs::common::LatencyUpdater;

namespace {

bvar::LatencyRecorder g_write_latency("block_device_write");
bvar::LatencyRecorder g_read_latency("block_device_read");

}  // namespace

BlockDeviceClientImpl::BlockDeviceClientImpl()
    : fileClient_(std::make_shared<FileClient>()),
      fd_(-1) {}

BlockDeviceClientImpl::BlockDeviceClientImpl(
    const std::shared_ptr<FileClient>& fileClient)
    : fileClient_(fileClient),
      fd_(-1) {}

bool BlockDeviceClientImpl::Init(const BlockDeviceClientOptions& options) {
    auto ret = fileClient_->Init(options.configPath);
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "Init file client error: " << ret;
        return false;
    }

    ret = taskpool_.Start(options.threadnum);
    if (ret != 0) {
        LOG(ERROR) << "Start task pool failed";
        return false;
    }

    return true;
}

void BlockDeviceClientImpl::UnInit() {
    taskpool_.Stop();
    fileClient_->UnInit();
}

bool BlockDeviceClientImpl::Open(const std::string& filename,
                                 const std::string& owner) {
    UserInfo userInfo(owner);
    curve::client::OpenFlags flags;
    auto retCode = fileClient_->Open(filename, userInfo, flags);
    if (retCode < 0) {
        LOG(ERROR) << "Open file failed, filename = " << filename
                   << ", retCode = " << retCode;
        return false;
    }

    fd_ = retCode;
    filename_ = filename;
    owner_ = owner;
    return true;
}

bool BlockDeviceClientImpl::Close() {
    if (fd_ < 0) {
        return true;
    }

    int retCode;
    if ((retCode = fileClient_->Close(fd_)) != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "Close file failed, retCode = " << retCode;
        return false;
    }

    fd_ = -1;
    return true;
}

bool BlockDeviceClientImpl::Stat(const std::string& filename,
                                          const std::string& owner,
                                          BlockDeviceStat* statInfo) {
    FileStatInfo fileStatInfo;
    UserInfo userInfo(owner);
    auto retCode = fileClient_->StatFile(filename, userInfo, &fileStatInfo);
    if (retCode != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "Stat file failed, retCode = " << retCode;
        return false;
    }

    statInfo->length = fileStatInfo.length;
    if (!ConvertFileStatus(fileStatInfo.fileStatus, &statInfo->status)) {
        LOG(ERROR) << "Stat file failed, unknown file status: "
                   << fileStatInfo.fileStatus;
        return false;
    }

    return true;
}

ssize_t BlockDeviceClientImpl::Read(char* buf, off_t offset, size_t length) {
    VLOG(9) << "read request, offset: " << offset << ", length: " << length;

    LatencyUpdater updater(&g_read_latency);

    if (fd_ < 0) {
        return -1;
    } else if (0 == length) {
        return length;
    } else if (IsAligned(offset, length)) {
        return AlignRead(buf, offset, length);
    }

    auto range = CalcAlignRange(offset, offset + length);  // [start, end)
    off_t readStart = range.first;
    off_t readEnd = range.second;
    size_t readLength = readEnd - readStart;
    std::unique_ptr<char[]> readBuffer(new (std::nothrow) char[readLength]);

    auto retCode = AlignRead(readBuffer.get(), readStart, readLength);
    if (retCode >= 0) {
        memcpy(buf, readBuffer.get() + (offset - readStart), length);
    }

    return retCode;
}

ssize_t BlockDeviceClientImpl::Readv(const std::vector<ReadPart>& iov) {
    if (iov.size() == 1) {
        VLOG(9) << "read block offset: " << iov[0].offset
                << ", length: " << iov[0].length;
        return Read(iov[0].data, iov[0].offset, iov[0].length);
    }

    CountDownEvent counter(iov.size());
    std::atomic<ssize_t> res(0);

    for (auto& io : iov) {
        auto task = [this, &counter, &io, &res]() {
            VLOG(9) << "read block offset: " << io.offset
                    << ", length: " << io.length;
            auto nr = this->Read(io.data, io.offset, io.length);
            if (nr < 0) {
                LOG(ERROR) << "IO Error, offseet: " << io.offset
                           << ", len: " << io.length;
                res.store(nr, std::memory_order_relaxed);
            } else {
                auto old = res.load(std::memory_order_release);
                if (old < 0) {
                    // already error
                } else {
                    while (!res.compare_exchange_strong(
                        old, old + nr, std::memory_order_relaxed)) {
                        if (old < 0) {
                            // another request is error
                            break;
                        }
                    }
                }
            }

            counter.Signal();
        };

        taskpool_.Enqueue(std::move(task));
    }

    counter.Wait();
    return res.load(std::memory_order_relaxed);
}

ssize_t BlockDeviceClientImpl::Write(const char* buf,
                                     off_t offset,
                                     size_t length) {
    VLOG(9) << "write request, offset: " << offset << ", length: " << length;

    LatencyUpdater updater(&g_write_latency);

    if (fd_ < 0) {
        return -1;
    } else if (0 == length) {
        return length;
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
    if (!retCode) {
        return -1;
    }

    memcpy(writeBuffer.get() + (offset - writeStart), buf, length);
    return AlignWrite(writeBuffer.get(), writeStart, writeLength);
}

ssize_t BlockDeviceClientImpl::Writev(const std::vector<WritePart>& writes) {
    if (writes.size() == 1) {
        return Write(writes[0].data, writes[0].offset, writes[0].length);
    }

    CountDownEvent counter(writes.size());
    std::atomic<ssize_t> res(0);

    for (const auto& io : writes) {
        auto task = [this, &counter, &io, &res]() {
            auto nr = this->Write(io.data, io.offset, io.length);
            if (nr < 0) {
                LOG(ERROR) << "IO Error, offseet: " << io.offset
                           << ", len: " << io.length;
                res.store(nr, std::memory_order_relaxed);
            } else {
                auto old = res.load(std::memory_order_release);
                if (old < 0) {
                    // already error
                } else {
                    while (!res.compare_exchange_strong(
                        old, old + io.length, std::memory_order_relaxed)) {
                        if (old < 0) {
                            // another request is error
                            break;
                        }
                    }
                }
            }

            counter.Signal();
        };

        taskpool_.Enqueue(std::move(task));
    }

    counter.Wait();
    return res.load(std::memory_order_relaxed);
}

bool BlockDeviceClientImpl::WritePadding(char* writeBuffer,
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
        if (retCode != item.second) {
            return false;
        }
    }

    return true;
}

ssize_t BlockDeviceClientImpl::AlignRead(char* buf,
                                         off_t offset,
                                         size_t length) {
    auto ret = fileClient_->Read(fd_, buf, offset, length);
    if (ret < 0) {
        LOG(ERROR) << "Read file failed, retCode = " << ret;
        return -1;
    } else if (ret != length) {
        LOG(ERROR) << "Read file failed, expect read " << length
                   << " bytes, actual read " << ret << " bytes";
        return -1;
    }

    return length;
}

ssize_t BlockDeviceClientImpl::AlignWrite(const char* buf,
                                          off_t offset,
                                          size_t length) {
    auto ret = fileClient_->Write(fd_, buf, offset, length);
    if (ret < 0) {
        LOG(ERROR) << "Write file failed, retCode = " << ret;
        return -1;
    } else if (ret != length) {
        LOG(ERROR) << "Write file failed, expect write " << length
                   << " bytes, actual write " << ret << " bytes";
        return -1;
    }

    return length;
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

}  // namespace volume
}  // namespace curvefs
