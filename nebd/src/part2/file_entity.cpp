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
 * Created Date: Tuesday March 3rd 2020
 * Author: yangyaokai
 */

#include "nebd/src/part2/file_entity.h"

#include <google/protobuf/util/message_differencer.h>

#include <algorithm>
#include <chrono>  // NOLINT
#include <vector>

#include "nebd/src/part2/util.h"

namespace nebd {
namespace server {

extern const char* kOpenFlagsAttrKey;

bool IsOpenFlagsExactlySame(const OpenFlags* lhs, const OpenFlags* rhs) {
    if (lhs == nullptr && rhs == nullptr) {
        return true;
    } else if ((lhs != nullptr && rhs == nullptr) ||
               (lhs == nullptr && rhs != nullptr)) {
        return false;
    }

    return google::protobuf::util::MessageDifferencer::Equals(*lhs, *rhs);
}

std::ostream& operator<<(std::ostream& os, const OpenFlags* flags) {
    if (!flags) {
        os << "[empty]";
    } else {
        os << "[exclusive: " << flags->exclusive() << "]";
    }

    return os;
}

NebdFileEntity::NebdFileEntity()
    : fd_(0)
    , fileName_("")
    , status_(NebdFileStatus::CLOSED)
    , timeStamp_(0)
    , fileInstance_(nullptr)
    , executor_(nullptr)
    , metaFileManager_(nullptr) {}

NebdFileEntity::~NebdFileEntity() {}

int NebdFileEntity::Init(const NebdFileEntityOption& option) {
    fd_ = option.fd;
    fileName_ = option.fileName;
    metaFileManager_ = option.metaFileManager_;

    NebdFileType type = GetFileType(fileName_);
    executor_ = NebdRequestExecutorFactory::GetExecutor(type);
    if (executor_ == nullptr) {
        LOG(ERROR) << "Init file failed, invalid filename. "
                   << "filename: " << fileName_;
        return -1;
    }

    return 0;
}

int NebdFileEntity::Open(const OpenFlags* openflags) {
    CHECK(executor_ != nullptr) << "file entity is not inited. "
                                << "filename: " << fileName_;
    std::unique_lock<bthread::Mutex> lock(fileStatusMtx_);
    if (status_ == NebdFileStatus::OPENED) {
        if (IsOpenFlagsExactlySame(openFlags_.get(), openflags)) {
            LOG(WARNING) << "File is already opened. "
                         << "filename: " << fileName_ << "fd: " << fd_;
            return fd_;
        } else {
            LOG(ERROR) << "File " << fileName_
                       << " is already opened, but open flags is not same, "
                          "previous open flags: "
                       << openFlags_.get()
                       << ", current open flags: " << openflags;
            return -1;
        }
    }

    NebdFileInstancePtr fileInstance = executor_->Open(fileName_, openflags);
    if (fileInstance == nullptr) {
        LOG(ERROR) << "open file failed. "
                   << "filename: " << fileName_;
        return -1;
    }

    int ret = UpdateFileStatus(fileInstance);
    if (ret != 0) {
        executor_->Close(fileInstance.get());
        LOG(ERROR) << "Open file failed. "
                   << "filename: " << fileName_;
        return -1;
    }
    LOG(INFO) << "Open file success. "
              << "fd: " << fd_
              << ", filename: " << fileName_;

    if (openflags) {
        openFlags_.reset(new OpenFlags{*openflags});
    } else {
        openFlags_.reset();
    }

    return fd_;
}

int NebdFileEntity::Reopen(const ExtendAttribute& xattr) {
    CHECK(executor_ != nullptr) << "file entity is not inited. "
                                << "filename: " << fileName_;
    std::unique_lock<bthread::Mutex> lock(fileStatusMtx_);
    NebdFileInstancePtr fileInstance = executor_->Reopen(fileName_, xattr);
    if (fileInstance == nullptr) {
        LOG(ERROR) << "Reopen file failed. "
                   << "filename: " << fileName_;
        return -1;
    }

    int ret = UpdateFileStatus(fileInstance);
    if (ret != 0) {
        executor_->Close(fileInstance.get());
        LOG(ERROR) << "Reopen file failed. "
                   << "filename: " << fileName_;
        return -1;
    }

    OpenFlags flags;
    bool hasOpenFlags =
        fileInstance->xattr.count(kOpenFlagsAttrKey) &&
        flags.ParseFromString(fileInstance->xattr.at(kOpenFlagsAttrKey));
    if (hasOpenFlags) {
        openFlags_.reset(new OpenFlags{flags});
    }

    LOG(INFO) << "Reopen file success. "
              << "fd: " << fd_
              << ", filename: " << fileName_;
    return fd_;
}

int NebdFileEntity::Close(bool removeMeta) {
    CHECK(executor_ != nullptr) << "file entity is not inited. "
                                << "filename: " << fileName_;
    //Used to mutually exclusive requests from other users, to avoid sending requests to the backend that may result in a return failure after the file is closed
    WriteLockGuard writeLock(rwLock_);
    //The mutex lock here is designed to be mutually exclusive with the open request, and the following situations may cause concurrency between close and open
    //Part2 restarted, causing the file to be reopened, and then due to timeout, the file was prepared to be closed
    //At this point, the user sent a mount volume request to open the file
    std::unique_lock<bthread::Mutex> lock(fileStatusMtx_);
    if (status_ == NebdFileStatus::OPENED) {
        int ret = executor_->Close(fileInstance_.get());
        if (ret < 0) {
            LOG(ERROR) << "Close file failed. "
                       << "fd: " << fd_
                       << ", filename: " << fileName_;
            return -1;
        }
        status_ = NebdFileStatus::CLOSED;
    }

    if (removeMeta && status_ != NebdFileStatus::DESTROYED) {
        int ret = metaFileManager_->RemoveFileMeta(fileName_);
        if (ret != 0) {
            LOG(ERROR) << "Remove file record failed. "
                    << "fd: " << fd_
                    << ", filename: " << fileName_;
            return -1;
        }
        status_ = NebdFileStatus::DESTROYED;
    }
    LOG(INFO) << "Close file success. "
              << "fd: " << fd_
              << ", filename: " << fileName_
              << ", meta removed? " << (removeMeta ? "yes" : "no");
    return 0;
}

int NebdFileEntity::Discard(NebdServerAioContext* aioctx) {
    auto task = [&]() {
        int ret = executor_->Discard(fileInstance_.get(), aioctx);
        if (ret < 0) {
            LOG(ERROR) << "Discard file failed. "
                       << "fd: " << fd_
                       << ", fileName: " << fileName_
                       << ", context: " << *aioctx;
            return -1;
        }
        return 0;
    };
    return ProcessAsyncRequest(task, aioctx);
}

int NebdFileEntity::AioRead(NebdServerAioContext* aioctx) {
    auto task = [&]() {
        int ret = executor_->AioRead(fileInstance_.get(), aioctx);
        if (ret < 0) {
            LOG(ERROR) << "AioRead file failed. "
                       << "fd: " << fd_
                       << ", fileName: " << fileName_
                       << ", context: " << *aioctx;
            return -1;
        }
        return 0;
    };
    return ProcessAsyncRequest(task, aioctx);
}

int NebdFileEntity::AioWrite(NebdServerAioContext* aioctx) {
    auto task = [&]() {
        int ret = executor_->AioWrite(fileInstance_.get(), aioctx);
        if (ret < 0) {
            LOG(ERROR) << "AioWrite file failed. "
                       << "fd: " << fd_
                       << ", fileName: " << fileName_
                       << ", context: " << *aioctx;
            return -1;
        }
        return 0;
    };
    return ProcessAsyncRequest(task, aioctx);
}

int NebdFileEntity::Flush(NebdServerAioContext* aioctx) {
    auto task = [&]() {
        int ret = executor_->Flush(fileInstance_.get(), aioctx);
        if (ret < 0) {
            LOG(ERROR) << "Flush file failed. "
                       << "fd: " << fd_
                       << ", fileName: " << fileName_
                       << ", context: " << *aioctx;
            return -1;
        }
        return 0;
    };
    return ProcessAsyncRequest(task, aioctx);
}

int NebdFileEntity::Extend(int64_t newsize) {
    auto task = [&]() {
        int ret = executor_->Extend(fileInstance_.get(), newsize);
        if (ret < 0) {
            LOG(ERROR) << "Extend file failed. "
                       << "fd: " << fd_
                       << ", newsize: " << newsize
                       << ", fileName" << fileName_;
            return -1;
        }
        return 0;
    };
    return ProcessSyncRequest(task);
}

int NebdFileEntity::GetInfo(NebdFileInfo* fileInfo) {
    auto task = [&]() {
        int ret = executor_->GetInfo(fileInstance_.get(), fileInfo);
        if (ret < 0) {
            LOG(ERROR) << "Get file info failed. "
                       << "fd: " << fd_
                       << ", fileName" << fileName_;
            return -1;
        }
        return 0;
    };
    return ProcessSyncRequest(task);
}

int NebdFileEntity::InvalidCache() {
    auto task = [&]() {
        int ret = executor_->InvalidCache(fileInstance_.get());
        if (ret < 0) {
            LOG(ERROR) << "Invalid cache failed. "
                       << "fd: " << fd_
                       << ", fileName" << fileName_;
            return -1;
        }
        return 0;
    };
    return ProcessSyncRequest(task);
}

int NebdFileEntity::ProcessSyncRequest(ProcessTask task) {
    CHECK(executor_ != nullptr) << "file entity is not inited. "
                                << "filename: " << fileName_;

    NebdRequestReadLockClosure* done =
        new (std::nothrow) NebdRequestReadLockClosure(rwLock_);
    brpc::ClosureGuard doneGuard(done);

    bool isFileOpened = GuaranteeFileOpened();
    if (!isFileOpened) {
        return -1;
    }

    int ret = task();
    if (ret < 0) {
        LOG(ERROR) << "Process sync request failed. "
                   << "fd: " << fd_
                   << ", fileName" << fileName_;
        return -1;
    }
    return 0;
}

int NebdFileEntity::ProcessAsyncRequest(ProcessTask task,
                                        NebdServerAioContext* aioctx) {
    CHECK(executor_ != nullptr) << "file entity is not inited. "
                                << "filename: " << fileName_;
    CHECK(aioctx != nullptr) << "AioContext should not be null.";

    NebdRequestReadLockClosure* done =
        new (std::nothrow) NebdRequestReadLockClosure(rwLock_);
    brpc::ClosureGuard doneGuard(done);

    bool isFileOpened = GuaranteeFileOpened();
    if (!isFileOpened) {
        return -1;
    }

    //For asynchronous requests, pass this closure to aiocontext to release the read lock when the request returns
    done->SetClosure(aioctx->done);
    aioctx->done = doneGuard.release();
    int ret = task();
    if (ret < 0) {
        //If the request fails, the lock should be actively released here and the aiocontext should be restored back
        brpc::ClosureGuard doneGuard(done);
        aioctx->done = done->GetClosure();
        done->SetClosure(nullptr);
        LOG(ERROR) << "Process async request failed. "
                   << "fd: " << fd_
                   << ", fileName" << fileName_;
        return -1;
    }
    return 0;
}

int NebdFileEntity::UpdateFileStatus(NebdFileInstancePtr fileInstance) {
    NebdFileMeta fileMeta;
    fileMeta.fd = fd_;
    fileMeta.fileName = fileName_;
    fileMeta.xattr = fileInstance->xattr;
    int ret = metaFileManager_->UpdateFileMeta(fileName_, fileMeta);
    if (ret != 0) {
        LOG(ERROR) << "Update file meta failed. "
                   << "filename: " << fileName_;
        return -1;
    }

    fileInstance_ = fileInstance;
    status_ = NebdFileStatus::OPENED;
    timeStamp_ = TimeUtility::GetTimeofDayMs();
    OpenFlags flags;
    if (fileInstance->xattr.count(kOpenFlagsAttrKey) &&
        flags.ParseFromString(fileInstance->xattr.at(kOpenFlagsAttrKey))) {
        openFlags_.reset(new OpenFlags{flags});
    }
    return 0;
}

bool NebdFileEntity::GuaranteeFileOpened() {
    //If the file has already been closed by the user, subsequent requests for automatic opening for operation are not allowed
    if (status_ == NebdFileStatus::DESTROYED) {
        LOG(ERROR) << "File has been destroyed. "
                   << "filename: " << fileName_
                   << ", fd: " << fd_;
        return false;
    }

    if (status_ == NebdFileStatus::CLOSED) {
        int ret = Open(openFlags_.get());
        if (ret != fd_) {
            LOG(ERROR) << "Get opened file failed. "
                       << "filename: " << fileName_
                       << ", fd: " << fd_
                       << ", ret: " << ret;
            return false;
        }
    }
    return true;
}

std::ostream& operator<<(std::ostream& os, const NebdFileEntity& entity) {
    std::string standardTime;
    TimeUtility::TimeStampToStandard(
        entity.GetFileTimeStamp() / 1000, &standardTime);
    os << "[filename: " << entity.GetFileName() << ", fd: " << entity.GetFd()
       << ", status: " << NebdFileStatus2Str(entity.GetFileStatus())
       << ", timestamp: " << standardTime << "]";
    return os;
}

}  // namespace server
}  // namespace nebd
