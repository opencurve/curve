/*
 * Project: nebd
 * Created Date: Thursday January 16th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#include <vector>
#include <chrono>  // NOLINT
#include <algorithm>

#include "src/part2/file_manager.h"
#include "src/part2/util.h"

namespace nebd {
namespace server {

NebdFileManager::NebdFileManager()
    : heartbeatTimeoutS_(0)
    , checkTimeoutIntervalMs_(0)
    , isRunning_(false)
    , metaFileManager_(nullptr) {}

NebdFileManager::~NebdFileManager() {}

int NebdFileManager::Init(NebdFileManagerOption option) {
    if (isRunning_.load()) {
        LOG(WARNING) << "Init failed, file manager is on running. ";
        return -1;
    }
    metaFileManager_ = option.metaFileManager;
    heartbeatTimeoutS_ = option.heartbeatTimeoutS;
    checkTimeoutIntervalMs_ = option.checkTimeoutIntervalMs;
    return 0;
}

int NebdFileManager::Run() {
    if (isRunning_.exchange(true)) {
        LOG(WARNING) << "file manager is on running.";
        return -1;
    }

    CHECK(metaFileManager_ != nullptr) << "meta file manager is null.";
    int ret = Load();
    if (ret < 0) {
        LOG(ERROR) << "Run file manager failed.";
        isRunning_.store(false);
        return -1;
    }

    checkTimeoutThread_ =
        std::thread(&NebdFileManager::CheckTimeoutFunc, this);
    LOG(INFO) << "Run file manager success.";
    return 0;
}

int NebdFileManager::Fini() {
    if (isRunning_.exchange(false)) {
        LOG(INFO) << "Stop file manager...";
        sleeper_.interrupt();
        checkTimeoutThread_.join();
    }
    LOG(INFO) << "Stop file manager ok.";
    return 0;
}

int NebdFileManager::Load() {
    std::vector<NebdFileRecordPtr> fileRecords;
    // 从元数据文件中读取持久化的文件信息
    int ret = metaFileManager_->ListFileRecord(&fileRecords);
    if (ret < 0) {
        LOG(ERROR) << "List file record failed.";
        return ret;
    }
    // 根据持久化的信息重新open文件
    int maxFd = 0;
    for (auto& fileRecord : fileRecords) {
        maxFd = std::max(maxFd, fileRecord->fd);
        Reopen(fileRecord);
        bool updateSuccess = fileRecordMap_.UpdateRecord(fileRecord);
        if (!updateSuccess) {
            LOG(ERROR) << "Update file record failed. "
                       << "filename: " << fileRecord->fileName;
            return -1;
        }
    }
    fdAlloc_.InitFd(maxFd);
    LOG(INFO) << "Load file record finished.";
    return 0;
}

int NebdFileManager::UpdateFileTimestamp(int fd) {
    NebdFileRecordPtr fileRecord = fileRecordMap_.GetRecord(fd);
    if (fileRecord == nullptr) {
        LOG(WARNING) << "Update file timestamp failed, no record. "
                     << "fd: " << fd;
        return -1;
    }
    fileRecord->timeStamp = TimeUtility::GetTimeofDayMs();
    return 0;
}

int NebdFileManager::Open(const std::string& filename) {
    int ret = OpenInternal(filename, true);
    if (ret < 0) {
        LOG(ERROR) << "open file failed. "
                   << "filename: " << filename
                   << ", ret: " << ret;
        return -1;
    }
    return ret;
}

int NebdFileManager::Close(int fd) {
    NebdFileRecordPtr fileRecord = fileRecordMap_.GetRecord(fd);
    if (fileRecord == nullptr) {
        LOG(WARNING) << "File record not exist, fd: " << fd;
        return 0;
    }
    int ret = CloseInternal(fileRecord);
    if (ret < 0) {
        LOG(ERROR) << "Close file failed. "
                   << "fd: " << fd
                   << ", filename: " << fileRecord->fileName;
        return -1;
    }
    ret = metaFileManager_->RemoveFileRecord(fileRecord->fileName);
    if (ret < 0) {
        LOG(ERROR) << "Close file failed. "
                   << "fd: " << fd
                   << ", filename: " << fileRecord->fileName;
        return -1;
    }
    fileRecordMap_.RemoveRecord(fd);
    LOG(INFO) << "Close file success. "
              << "fd: " << fd
              << ", filename: " << fileRecord->fileName;
    return 0;
}

int NebdFileManager::Discard(int fd, NebdServerAioContext* aioctx) {
    auto task = [&](NebdProcessClosure* done) {
        NebdFileRecordPtr fileRecord = done->GetFileRecord();
        done->SetClosure(aioctx->done);
        aioctx->done = done;
        int ret = fileRecord->executor->Discard(
            fileRecord->fileInstance.get(), aioctx);
        if (ret < 0) {
            brpc::ClosureGuard doneGuard(done);
            aioctx->done = done->GetClosure();
            done->SetClosure(nullptr);
            LOG(ERROR) << "Discard file failed. "
                       << "fd: " << fd
                       << ", fileName: " << fileRecord->fileName
                       << ", context: " << *aioctx;
            return -1;
        }
        return 0;
    };
    return ProcessRequest(fd, task);
}

int NebdFileManager::AioRead(int fd, NebdServerAioContext* aioctx) {
    auto task = [&](NebdProcessClosure* done) {
        NebdFileRecordPtr fileRecord = done->GetFileRecord();
        done->SetClosure(aioctx->done);
        aioctx->done = done;
        int ret = fileRecord->executor->AioRead(
            fileRecord->fileInstance.get(), aioctx);
        if (ret < 0) {
            brpc::ClosureGuard doneGuard(done);
            aioctx->done = done->GetClosure();
            done->SetClosure(nullptr);
            LOG(ERROR) << "AioRead file failed. "
                       << "fd: " << fd
                       << ", fileName: " << fileRecord->fileName
                       << ", context: " << *aioctx;
            return -1;
        }
        return 0;
    };
    return ProcessRequest(fd, task);
}

int NebdFileManager::AioWrite(int fd, NebdServerAioContext* aioctx) {
    auto task = [&](NebdProcessClosure* done) {
        NebdFileRecordPtr fileRecord = done->GetFileRecord();
        done->SetClosure(aioctx->done);
        aioctx->done = done;
        int ret = fileRecord->executor->AioWrite(
            fileRecord->fileInstance.get(), aioctx);
        if (ret < 0) {
            brpc::ClosureGuard doneGuard(done);
            aioctx->done = done->GetClosure();
            done->SetClosure(nullptr);
            LOG(ERROR) << "AioWrite file failed. "
                       << "fd: " << fd
                       << ", fileName: " << fileRecord->fileName
                       << ", context: " << *aioctx;
            return -1;
        }
        return 0;
    };
    return ProcessRequest(fd, task);
}

int NebdFileManager::Flush(int fd, NebdServerAioContext* aioctx) {
    auto task = [&](NebdProcessClosure* done) {
        NebdFileRecordPtr fileRecord = done->GetFileRecord();
        done->SetClosure(aioctx->done);
        aioctx->done = done;
        int ret = fileRecord->executor->Flush(
            fileRecord->fileInstance.get(), aioctx);
        if (ret < 0) {
            brpc::ClosureGuard doneGuard(done);
            aioctx->done = done->GetClosure();
            done->SetClosure(nullptr);
            LOG(ERROR) << "Flush file failed. "
                       << "fd: " << fd
                       << ", fileName: " << fileRecord->fileName
                       << ", context: " << *aioctx;
            return -1;
        }
        return 0;
    };
    return ProcessRequest(fd, task);
}

int NebdFileManager::Extend(int fd, int64_t newsize) {
    auto task = [&](NebdProcessClosure* done) {
        brpc::ClosureGuard doneGuard(done);
        NebdFileRecordPtr fileRecord = done->GetFileRecord();
        int ret = fileRecord->executor->Extend(
            fileRecord->fileInstance.get(), newsize);
        if (ret < 0) {
            LOG(ERROR) << "Extend file failed. "
                       << "fd: " << fd
                       << ", newsize: " << newsize
                       << ", fileName" << fileRecord->fileName;
            return -1;
        }
        return 0;
    };
    return ProcessRequest(fd, task);
}

int NebdFileManager::GetInfo(int fd, NebdFileInfo* fileInfo) {
    auto task = [&](NebdProcessClosure* done) {
        brpc::ClosureGuard doneGuard(done);
        NebdFileRecordPtr fileRecord = done->GetFileRecord();
        int ret = fileRecord->executor->GetInfo(
            fileRecord->fileInstance.get(), fileInfo);
        if (ret < 0) {
            LOG(ERROR) << "Get file info failed. "
                       << "fd: " << fd
                       << ", fileName" << fileRecord->fileName;
            return -1;
        }
        return 0;
    };
    return ProcessRequest(fd, task);
}

int NebdFileManager::InvalidCache(int fd) {
    auto task = [&](NebdProcessClosure* done) {
        brpc::ClosureGuard doneGuard(done);
        NebdFileRecordPtr fileRecord = done->GetFileRecord();
        int ret = fileRecord->executor->InvalidCache(
            fileRecord->fileInstance.get());
        if (ret < 0) {
            LOG(ERROR) << "Invalid cache failed. "
                       << "fd: " << fd
                       << ", fileName" << fileRecord->fileName;
            return -1;
        }
        return 0;
    };
    return ProcessRequest(fd, task);
}

void NebdFileManager::CheckTimeoutFunc() {
    while (sleeper_.wait_for(
        std::chrono::milliseconds(checkTimeoutIntervalMs_))) {
        std::unordered_map<int, NebdFileRecordPtr> fileRecords
            = fileRecordMap_.ListRecords();
        for (auto& fileRecord : fileRecords) {
            uint64_t interval =
                TimeUtility::GetTimeofDayMs() - fileRecord.second->timeStamp;
            if (interval < (uint64_t)1000 * heartbeatTimeoutS_) {
                continue;
            }
            std::string standardTime;
            TimeUtility::TimeStampToStandard(
                fileRecord.second->timeStamp / 1000, &standardTime);
            LOG(INFO) << "Close file which has timed out. "
                      << "Last time received heartbeat: " << standardTime;
            CloseInternal(fileRecord.second);
        }
    }
}

int NebdFileManager::OpenInternal(const std::string& fileName,
                                  bool create) {
    // 同名文件open需要互斥
    NameLockGuard(nameLock_, fileName);

    NebdFileRecordPtr fileRecord = fileRecordMap_.GetRecord(fileName);
    if (fileRecord != nullptr && fileRecord->status == NebdFileStatus::OPENED) {
        return fileRecord->fd;
    }

    if (create) {
        fileRecord = std::make_shared<NebdFileRecord>();
        fileRecord->fileName = fileName;
        int newFd = GetValidFd();
        fileRecord->fd = newFd;
        fileRecord->type = GetFileType(fileName);
    } else {
        if (fileRecord == nullptr) {
            LOG(ERROR) << "open file failed: no record. "
                       << "filename: " << fileName;
            return -1;
        }
    }

    NebdRequestExecutor* executor =
        NebdRequestExecutorFactory::GetExecutor(fileRecord->type);
    if (executor == nullptr) {
        LOG(ERROR) << "open file failed, invalid filename. "
                   << "filename: " << fileRecord->fileName;
        return -1;
    }

    NebdFileInstancePtr fileInstance = executor->Open(fileRecord->fileName);
    if (fileInstance == nullptr) {
        LOG(ERROR) << "open file failed. "
                   << "filename: " << fileRecord->fileName;
        return -1;
    }

    // 先持久化元数据信息到文件，再去更新内存
    int ret = metaFileManager_->UpdateFileRecord(fileRecord);
    if (ret < 0) {
        LOG(ERROR) << "persist file record failed. filename: "
                   << fileRecord->fileName;
        return -1;
    }

    fileRecord->executor = executor;
    fileRecord->fileInstance = fileInstance;
    fileRecord->status = NebdFileStatus::OPENED;
    fileRecord->timeStamp = TimeUtility::GetTimeofDayMs();

    bool updateSuccess = fileRecordMap_.UpdateRecord(fileRecord);
    if (!updateSuccess) {
        metaFileManager_->RemoveFileRecord(fileRecord->fileName);
        LOG(ERROR) << "Update file record failed. "
                   << "filename: " << fileRecord->fileName;
        return -1;
    }
    LOG(INFO) << "Open file success. "
              << "fd: " << fileRecord->fd
              << ", filename: " << fileRecord->fileName;
    return fileRecord->fd;
}

int NebdFileManager::Reopen(NebdFileRecordPtr fileRecord) {
    fileRecord->type = GetFileType(fileRecord->fileName);
    NebdRequestExecutor* executor =
        NebdRequestExecutorFactory::GetExecutor(fileRecord->type);
    if (executor == nullptr) {
        LOG(ERROR) << "open file failed, invalid filename. "
                   << "filename: " << fileRecord->fileName;
        return -1;
    }
    NebdFileInstancePtr fileInstance = executor->Reopen(
        fileRecord->fileName, fileRecord->fileInstance->addition);
    if (fileInstance == nullptr) {
        LOG(ERROR) << "Reopen file failed. "
                   << "filename: " << fileRecord->fileName;
        return -1;
    }

    fileRecord->executor = executor;
    fileRecord->fileInstance = fileInstance;
    fileRecord->status = NebdFileStatus::OPENED;
    fileRecord->timeStamp = TimeUtility::GetTimeofDayMs();

    LOG(INFO) << "Reopen file success. "
              << "fd: " << fileRecord->fd
              << ", filename: " << fileRecord->fileName;
    return 0;
}

int NebdFileManager::CloseInternal(NebdFileRecordPtr fileRecord) {
    if (fileRecord->status != NebdFileStatus::OPENED) {
        LOG(INFO) << "File has been closed. "
                  << "fd: " << fileRecord->fd
                  << ", filename: " << fileRecord->fileName;
        return 0;
    }

    // 用于和其他用户请求互斥，避免文件被close后，请求发到后端导致返回失败
    WriteLockGuard writeLock(fileRecord->rwLock);
    int ret = fileRecord->executor->Close(fileRecord->fileInstance.get());
    if (ret < 0) {
        LOG(ERROR) << "Close file failed. "
                   << "fd: " << fileRecord->fd
                   << ", filename: " << fileRecord->fileName;
        return -1;
    }
    fileRecord->status = NebdFileStatus::CLOSED;
    LOG(INFO) << "Close file success. "
              << "fd: " << fileRecord->fd
              << ", filename: " << fileRecord->fileName;
    return 0;
}

int NebdFileManager::ProcessRequest(int fd, ProcessTask task) {
    NebdFileRecordPtr fileRecord = fileRecordMap_.GetRecord(fd);
    if (fileRecord == nullptr) {
        LOG(WARNING) << "File record not exist, fd: " << fd;
        return -1;
    }

    NebdProcessClosure* done =
        new (std::nothrow) NebdProcessClosure(fileRecord);
    brpc::ClosureGuard doneGuard(done);

    if (fileRecord->status != NebdFileStatus::OPENED) {
        int ret = OpenInternal(fileRecord->fileName);
        if (ret != fd) {
            LOG(WARNING) << "Get opened file failed. "
                        << "filename: " << fileRecord->fileName
                        << ", fd: " << fd
                        << ", ret: " << ret;
            return -1;
        }
    }

    int ret = task(dynamic_cast<NebdProcessClosure*>(doneGuard.release()));
    if (ret < 0) {
        LOG(ERROR) << "Process request failed. "
                   << "fd: " << fd
                   << ", fileName" << fileRecord->fileName;
        return -1;
    }
    return 0;
}

std::unordered_map<int, NebdFileRecordPtr> NebdFileManager::GetRecordMap() {
    return fileRecordMap_.ListRecords();
}

int NebdFileManager::GetValidFd() {
    int fd = 0;
    while (true) {
        fd = fdAlloc_.GetNext();
        if (!fileRecordMap_.Exist(fd)) {
            break;
        }
    }
    return fd;
}

}  // namespace server
}  // namespace nebd

