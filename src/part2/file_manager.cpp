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

NebdFileManager::NebdFileManager(FileRecordManagerPtr recordManager)
    : isRunning_(false)
    , fileRecordManager_(recordManager) {}

NebdFileManager::~NebdFileManager() {}

int NebdFileManager::Run() {
    if (isRunning_.exchange(true)) {
        LOG(WARNING) << "file manager is on running.";
        return -1;
    }

    int ret = Load();
    if (ret < 0) {
        LOG(ERROR) << "Run file manager failed.";
        isRunning_.store(false);
        return -1;
    }
    return 0;
}

int NebdFileManager::Fini() {
    isRunning_.store(false);
    LOG(INFO) << "Stop file manager success.";
    return 0;
}

int NebdFileManager::Load() {
    // 从元数据文件中读取持久化的文件信息
    int ret = fileRecordManager_->Load();
    if (ret < 0) {
        LOG(ERROR) << "Load file record failed.";
        return ret;
    }
    FileRecordMap fileRecords = fileRecordManager_->ListRecords();
    // 根据持久化的信息重新open文件
    int maxFd = 0;
    for (auto& fileRecord : fileRecords) {
        maxFd = std::max(maxFd, fileRecord.first);
        // reopen失败忽略，此时文件状态为closed，下次访问仍然会去open
        // 这么考虑是为了防止个别文件有问题导致整个part2不可用
        int ret = Reopen(fileRecord.second);
        if (ret < 0) {
            LOG(WARNING) << "Reopen file failed. "
                         << "filenam: " << fileRecord.second.fileName;
        }
    }
    fdAlloc_.InitFd(maxFd);
    LOG(INFO) << "Load file record finished.";
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

int NebdFileManager::Close(int fd, bool removeRecord) {
    NebdFileRecord fileRecord;
    bool getSuccess = fileRecordManager_->GetRecord(fd, &fileRecord);
    if (!getSuccess) {
        LOG(WARNING) << "File record not exist, fd: " << fd;
        return 0;
    }

    // 用于和其他用户请求互斥，避免文件被close后，请求发到后端导致返回失败
    WriteLockGuard writeLock(*fileRecord.rwLock);
    int ret = CloseInternal(fd);
    if (ret < 0) {
        LOG(ERROR) << "Close file failed. "
                   << "fd: " << fd
                   << ", filename: " << fileRecord.fileName;
        return -1;
    }
    if (removeRecord) {
        bool removeSuccess = fileRecordManager_->RemoveRecord(fd);
        if (!removeSuccess) {
            LOG(ERROR) << "Remove file record failed. "
                    << "fd: " << fd
                    << ", filename: " << fileRecord.fileName;
            return -1;
        }
    }
    LOG(INFO) << "Close file success. "
              << "fd: " << fd
              << ", filename: " << fileRecord.fileName;
    return 0;
}

int NebdFileManager::Discard(int fd, NebdServerAioContext* aioctx) {
    auto task = [&](NebdProcessClosure* done) {
        const NebdFileRecord& fileRecord = done->GetFileRecord();
        done->SetClosure(aioctx->done);
        aioctx->done = done;
        int ret = fileRecord.executor->Discard(
            fileRecord.fileInstance.get(), aioctx);
        if (ret < 0) {
            brpc::ClosureGuard doneGuard(done);
            aioctx->done = done->GetClosure();
            done->SetClosure(nullptr);
            LOG(ERROR) << "Discard file failed. "
                       << "fd: " << fd
                       << ", fileName: " << fileRecord.fileName
                       << ", context: " << *aioctx;
            return -1;
        }
        return 0;
    };
    return ProcessRequest(fd, task);
}

int NebdFileManager::AioRead(int fd, NebdServerAioContext* aioctx) {
    auto task = [&](NebdProcessClosure* done) {
        const NebdFileRecord& fileRecord = done->GetFileRecord();
        done->SetClosure(aioctx->done);
        aioctx->done = done;
        int ret = fileRecord.executor->AioRead(
            fileRecord.fileInstance.get(), aioctx);
        if (ret < 0) {
            brpc::ClosureGuard doneGuard(done);
            aioctx->done = done->GetClosure();
            done->SetClosure(nullptr);
            LOG(ERROR) << "AioRead file failed. "
                       << "fd: " << fd
                       << ", fileName: " << fileRecord.fileName
                       << ", context: " << *aioctx;
            return -1;
        }
        return 0;
    };
    return ProcessRequest(fd, task);
}

int NebdFileManager::AioWrite(int fd, NebdServerAioContext* aioctx) {
    auto task = [&](NebdProcessClosure* done) {
        const NebdFileRecord& fileRecord = done->GetFileRecord();
        done->SetClosure(aioctx->done);
        aioctx->done = done;
        int ret = fileRecord.executor->AioWrite(
            fileRecord.fileInstance.get(), aioctx);
        if (ret < 0) {
            brpc::ClosureGuard doneGuard(done);
            aioctx->done = done->GetClosure();
            done->SetClosure(nullptr);
            LOG(ERROR) << "AioWrite file failed. "
                       << "fd: " << fd
                       << ", fileName: " << fileRecord.fileName
                       << ", context: " << *aioctx;
            return -1;
        }
        return 0;
    };
    return ProcessRequest(fd, task);
}

int NebdFileManager::Flush(int fd, NebdServerAioContext* aioctx) {
    auto task = [&](NebdProcessClosure* done) {
        const NebdFileRecord& fileRecord = done->GetFileRecord();
        done->SetClosure(aioctx->done);
        aioctx->done = done;
        int ret = fileRecord.executor->Flush(
            fileRecord.fileInstance.get(), aioctx);
        if (ret < 0) {
            brpc::ClosureGuard doneGuard(done);
            aioctx->done = done->GetClosure();
            done->SetClosure(nullptr);
            LOG(ERROR) << "Flush file failed. "
                       << "fd: " << fd
                       << ", fileName: " << fileRecord.fileName
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
        const NebdFileRecord& fileRecord = done->GetFileRecord();
        int ret = fileRecord.executor->Extend(
            fileRecord.fileInstance.get(), newsize);
        if (ret < 0) {
            LOG(ERROR) << "Extend file failed. "
                       << "fd: " << fd
                       << ", newsize: " << newsize
                       << ", fileName" << fileRecord.fileName;
            return -1;
        }
        return 0;
    };
    return ProcessRequest(fd, task);
}

int NebdFileManager::GetInfo(int fd, NebdFileInfo* fileInfo) {
    auto task = [&](NebdProcessClosure* done) {
        brpc::ClosureGuard doneGuard(done);
        const NebdFileRecord& fileRecord = done->GetFileRecord();
        int ret = fileRecord.executor->GetInfo(
            fileRecord.fileInstance.get(), fileInfo);
        if (ret < 0) {
            LOG(ERROR) << "Get file info failed. "
                       << "fd: " << fd
                       << ", fileName" << fileRecord.fileName;
            return -1;
        }
        return 0;
    };
    return ProcessRequest(fd, task);
}

int NebdFileManager::InvalidCache(int fd) {
    auto task = [&](NebdProcessClosure* done) {
        brpc::ClosureGuard doneGuard(done);
        const NebdFileRecord& fileRecord = done->GetFileRecord();
        int ret = fileRecord.executor->InvalidCache(
            fileRecord.fileInstance.get());
        if (ret < 0) {
            LOG(ERROR) << "Invalid cache failed. "
                       << "fd: " << fd
                       << ", fileName" << fileRecord.fileName;
            return -1;
        }
        return 0;
    };
    return ProcessRequest(fd, task);
}

int NebdFileManager::OpenInternal(const std::string& fileName,
                                  bool create) {
    // 同名文件open需要互斥
    NameLockGuard openGuard(nameLock_, fileName);

    NebdFileRecord fileRecord;
    bool getSuccess = fileRecordManager_->GetRecord(fileName, &fileRecord);
    if (getSuccess && fileRecord.status == NebdFileStatus::OPENED) {
        LOG(WARNING) << "File is already opened. "
                     << "filename: " << fileName
                     << "fd: " << fileRecord.fd;
        return fileRecord.fd;
    }

    if (create) {
        fileRecord.fileName = fileName;
        fileRecord.fd = GetValidFd();
    } else {
        // create为false的情况下，如果record不存在，需要返回错误
        if (!getSuccess) {
            LOG(ERROR) << "open file failed: no record. "
                       << "filename: " << fileName;
            return -1;
        }
    }

    NebdFileType type = GetFileType(fileName);
    NebdRequestExecutor* executor =
        NebdRequestExecutorFactory::GetExecutor(type);
    if (executor == nullptr) {
        LOG(ERROR) << "open file failed, invalid filename. "
                   << "filename: " << fileName;
        return -1;
    }

    NebdFileInstancePtr fileInstance = executor->Open(fileName);
    if (fileInstance == nullptr) {
        LOG(ERROR) << "open file failed. "
                   << "filename: " << fileName;
        return -1;
    }

    fileRecord.executor = executor;
    fileRecord.fileInstance = fileInstance;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.timeStamp = TimeUtility::GetTimeofDayMs();

    bool updateSuccess = fileRecordManager_->UpdateRecord(fileRecord);
    if (!updateSuccess) {
        executor->Close(fileInstance.get());
        LOG(ERROR) << "Update file record failed. "
                   << "filename: " << fileName;
        return -1;
    }
    LOG(INFO) << "Open file success. "
              << "fd: " << fileRecord.fd
              << ", filename: " << fileRecord.fileName;
    return fileRecord.fd;
}

int NebdFileManager::Reopen(NebdFileRecord fileRecord) {
    NebdFileType type = GetFileType(fileRecord.fileName);
    NebdRequestExecutor* executor =
        NebdRequestExecutorFactory::GetExecutor(type);
    if (executor == nullptr) {
        LOG(ERROR) << "Reopen file failed, invalid filename. "
                   << "filename: " << fileRecord.fileName;
        return -1;
    }
    NebdFileInstancePtr fileInstance = executor->Reopen(
        fileRecord.fileName, fileRecord.fileInstance->addition);
    if (fileInstance == nullptr) {
        LOG(ERROR) << "Reopen file failed. "
                   << "filename: " << fileRecord.fileName;
        return -1;
    }

    fileRecord.executor = executor;
    fileRecord.fileInstance = fileInstance;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.timeStamp = TimeUtility::GetTimeofDayMs();

    bool updateSuccess = fileRecordManager_->UpdateRecord(fileRecord);
    if (!updateSuccess) {
        executor->Close(fileInstance.get());
        LOG(ERROR) << "Update file record failed. "
                   << "filename: " << fileRecord.fileName;
        return -1;
    }

    LOG(INFO) << "Reopen file success. "
              << "fd: " << fileRecord.fd
              << ", filename: " << fileRecord.fileName;
    return 0;
}

int NebdFileManager::CloseInternal(int fd) {
    NebdFileRecord fileRecord;
    bool getSuccess = fileRecordManager_->GetRecord(fd, &fileRecord);
    if (!getSuccess) {
        LOG(WARNING) << "File record not exist, fd: " << fd;
        return 0;
    }

    if (fileRecord.status != NebdFileStatus::OPENED) {
        LOG(INFO) << "File has been closed. "
                  << "fd: " << fileRecord.fd
                  << ", filename: " << fileRecord.fileName;
        return 0;
    }

    int ret = fileRecord.executor->Close(fileRecord.fileInstance.get());
    if (ret < 0) {
        LOG(ERROR) << "Close file failed. "
                   << "fd: " << fileRecord.fd
                   << ", filename: " << fileRecord.fileName;
        return -1;
    }

    fileRecordManager_->UpdateFileStatus(fileRecord.fd, NebdFileStatus::CLOSED);
    return 0;
}

int NebdFileManager::ProcessRequest(int fd, ProcessTask task) {
    NebdFileRecord fileRecord;
    bool getSuccess = fileRecordManager_->GetRecord(fd, &fileRecord);
    if (!getSuccess) {
        LOG(ERROR) << "File record not exist, fd: " << fd;
        return -1;
    }

    NebdProcessClosure* done =
        new (std::nothrow) NebdProcessClosure(fileRecord);
    brpc::ClosureGuard doneGuard(done);

    if (fileRecord.status != NebdFileStatus::OPENED) {
        int ret = OpenInternal(fileRecord.fileName);
        if (ret != fd) {
            LOG(ERROR) << "Get opened file failed. "
                        << "filename: " << fileRecord.fileName
                        << ", fd: " << fd
                        << ", ret: " << ret;
            return -1;
        }
        // 重新open后要获取新的record
        getSuccess = fileRecordManager_->GetRecord(fd, &fileRecord);
        if (!getSuccess) {
            LOG(ERROR) << "File record not exist, fd: " << fd;
            return -1;
        }
        done->SetFileRecord(fileRecord);
    }

    int ret = task(dynamic_cast<NebdProcessClosure*>(doneGuard.release()));
    if (ret < 0) {
        LOG(ERROR) << "Process request failed. "
                   << "fd: " << fd
                   << ", fileName" << fileRecord.fileName;
        return -1;
    }
    return 0;
}

FileRecordManagerPtr NebdFileManager::GetRecordManager() {
    return fileRecordManager_;
}

int NebdFileManager::GetValidFd() {
    int fd = 0;
    while (true) {
        fd = fdAlloc_.GetNext();
        if (!fileRecordManager_->Exist(fd)) {
            break;
        }
    }
    return fd;
}

}  // namespace server
}  // namespace nebd

