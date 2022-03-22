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
 * Project: curve
 * Created Date: 21-08-13
 * Author: hzwuhongsong
 */

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <dirent.h>

#include <vector>

#include "curvefs/src/client/s3/disk_cache_write.h"
#include "curvefs/src/common/s3util.h"

namespace curvefs {

namespace client {

void DiskCacheWrite::Init(S3Client *client,
                          std::shared_ptr<PosixWrapper> posixWrapper,
                          const std::string cacheDir,
                          uint64_t asyncLoadPeriodMs,
                          std::shared_ptr<LRUCache<
                            std::string, bool>> cachedObjName) {
    client_ = client;
    posixWrapper_ = posixWrapper;
    asyncLoadPeriodMs_ = asyncLoadPeriodMs;
    cachedObjName_ = cachedObjName;
    DiskCacheBase::Init(posixWrapper, cacheDir);
}

void DiskCacheWrite::AsyncUploadEnqueue(const std::string objName) {
    std::lock_guard<bthread::Mutex> lk(mtx_);
    waitUpload_.push_back(objName);
}

int DiskCacheWrite::ReadFile(const std::string name, char **buf,
                             uint64_t *size) {
    std::string fileFullPath;
    bool fileExist;
    fileFullPath = GetCacheIoFullDir() + "/" + name;
    fileExist = IsFileExist(fileFullPath);
    if (!fileExist) {
        LOG(ERROR) << "file is not exist, file = " << name;
        return -1;
    }
    struct stat statFile;
    int fd, ret;
    ret = posixWrapper_->stat(fileFullPath.c_str(), &statFile);
    if (ret < 0) {
        LOG(ERROR) << "get file size error, file = " << name;
        return -1;
    }
    off_t fileSize = statFile.st_size;
    *size = fileSize;
    fd = posixWrapper_->open(fileFullPath.c_str(), O_RDONLY, MODE);
    if (fd < 0) {
        LOG(ERROR) << "open disk file error. errno = " << errno
                   << ", file = " << name;
        return fd;
    }

    uint64_t allocSize;
    allocSize = fileSize * sizeof(char) + 1;
    char *buffer = reinterpret_cast<char *>(posixWrapper_->malloc(allocSize));
    if (buffer == NULL) {
        LOG(ERROR) << "malloc failed in UploadFile.";
        posixWrapper_->close(fd);
        return -1;
    }
    void *memRet = posixWrapper_->memset(buffer, '0', fileSize * sizeof(char));
    if (memRet == NULL) {
        LOG(ERROR) << "memset failed in UploadFile.";
        posixWrapper_->free(buffer);
        posixWrapper_->close(fd);
        return -1;
    }
    ssize_t readLen = posixWrapper_->read(fd, buffer, fileSize);
    if (readLen < 0) {
        LOG(ERROR) << "read file error, ret = " << readLen
                   << ", errno = " << errno << ", file = " << name;
        posixWrapper_->free(buffer);
        posixWrapper_->close(fd);
        return -1;
    }
    if (readLen < fileSize) {
        LOG(ERROR) << "read disk file is not entirely. read len = " << readLen
                   << ", but file size = " << fileSize << ", file = " << name;
        posixWrapper_->free(buffer);
        posixWrapper_->close(fd);
        return -1;
    }
    posixWrapper_->close(fd);
    *buf = buffer;
    return 0;
}

int DiskCacheWrite::UploadFile(const std::string &name,
                               std::shared_ptr<SynchronizationTask> syncTask) {
    uint64_t fileSize;
    char *buffer = nullptr;
    int ret = ReadFile(name, &buffer, &fileSize);
    if (ret < 0) {
        if (buffer != nullptr) {
            posixWrapper_->free(buffer);
        }

        if (syncTask) {
            // need signal fail
            syncTask->Signal();
            VLOG(3) << "UploadFile, read file " << name << " error";
            return -1;
        }

        LOG(ERROR) << "read file failed";
        return -1;
    }
    VLOG(9) << "async upload start, file = " << name;
    PutObjectAsyncCallBack cb =
        [&, buffer, syncTask, name]
            (const std::shared_ptr<PutObjectAsyncContext> &context) {
            if (context->retCode == 0) {
                if (metric_.get() != nullptr) {
                    metric_->writeS3.bps.count << context->bufferSize;
                    metric_->writeS3.qps.count << 1;
                    metric_->writeS3.latency
                        << (butil::cpuwide_time_us() - context->startTime);
                }
                RemoveFile(context->key);
                VLOG(9) << " PutObjectAsyncCallBack success, "
                        << "remove file: " << context->key;
                posixWrapper_->free(buffer);
                if (syncTask) {
                    VLOG(9) << "UploadFile, name = "
                            << name << " signal start";
                    syncTask->Signal();
                    VLOG(9) << "UploadFile, name = "
                            << name << " signal finish";
                }
                return;
            }
            LOG(WARNING) << "upload object failed: " << context->key;
            client_->UploadAsync(context);
        };
    auto context = std::make_shared<PutObjectAsyncContext>();
    context->key = name;
    context->buffer = buffer;
    context->bufferSize = fileSize;
    context->cb = cb;
    context->startTime = butil::cpuwide_time_us();
    client_->UploadAsync(context);
    VLOG(9) << "async upload end, file = " << name;
    return 0;
}

void DiskCacheWrite::UploadFile(const std::list<std::string> &toUpload,
                                std::shared_ptr<SynchronizationTask> syncTask) {
    std::list<std::string>::const_iterator iter;
    for (iter = toUpload.begin(); iter != toUpload.end(); iter++) {
        UploadFile(*iter, syncTask);
    }
}

bool DiskCacheWrite::WriteCacheValid() {
    return IsFileExist(GetCacheIoFullDir());
}

int DiskCacheWrite::GetUploadFile(const std::string &inode,
                                  std::list<std::string> *toUpload) {
    std::unique_lock<bthread::Mutex> lk(mtx_);
    if (waitUpload_.empty()) {
        return 0;
    }
    if (inode.empty()) {
        toUpload->swap(waitUpload_);
        return toUpload->size();
    }
    waitUpload_.remove_if([&](const std::string &filename) {
        bool inodeFile =
            curvefs::common::s3util::ValidNameOfInode(inode, filename);
        if (inodeFile) {
            toUpload->emplace_back(filename);
        }

        return inodeFile;
    });
    return toUpload->size();
}

int DiskCacheWrite::FileExist(const std::string &inode) {
    // load all write cacahe
    std::set<std::string> cachedObj;
    int ret = LoadAllCacheFile(&cachedObj);
    if (ret < 0) {
        LOG(ERROR) << "DiskCacheWrite, load all cacched file fail ret = "
                   << ret;
        return ret;
    }

    for (auto iter = cachedObj.begin(); iter != cachedObj.end(); iter++) {
        bool exist = curvefs::common::s3util::ValidNameOfInode(inode, *iter);
        if (exist) {
            return 1;
        }
    }

    return 0;
}

int DiskCacheWrite::UploadFileByInode(const std::string &inode) {
    if (!WriteCacheValid()) {
        LOG(ERROR) << "UploadFileByInode, cache write dir is not exist.";
        return -1;
    }

    // upload file of inode
    std::list<std::string> toUpload;
    do {
        // get upload files
        toUpload.clear();
        int num = GetUploadFile(inode, &toUpload);
        if (num <= 0) {
            break;
        }

        // upload file and wait finish
        auto syncTask = std::make_shared<SynchronizationTask>(num);
        VLOG(3) << "UploadFileByInode, inode: " << inode
                << ", task num: " << num;
        UploadFile(toUpload, syncTask);
        syncTask->Wait();
        if (!syncTask->Success()) {
            return -1;
        }
    } while (!toUpload.empty());

    // wait all file upload ok
    int ret = 1;
    while (ret) {
        ret = FileExist(inode);
        if (ret <= 0) {
            return ret;
        }
        LOG(INFO) << "UploadFileByInode, need wait file on disk uopload ok";
        sleeper_.wait_for(std::chrono::milliseconds(asyncLoadPeriodMs_));
    }

    return 0;
}

int DiskCacheWrite::AsyncUploadFunc() {
    if (!WriteCacheValid()) {
        LOG(ERROR) << "cache write dir is not exist.";
        return -1;
    }

    std::list<std::string> toUpload;

    VLOG(3) << "async upload function start.";
    while (sleeper_.wait_for(std::chrono::milliseconds(asyncLoadPeriodMs_))) {
        if (!isRunning_) {
            LOG(INFO) << "async upload thread stop.";
            return 0;
        }
        toUpload.clear();
        if (GetUploadFile("", &toUpload) <= 0) {
            VLOG(9) << "no need to upload";
            continue;
        }
        VLOG(6) << "async upload file size = " << toUpload.size();
        UploadFile(toUpload, nullptr);
        VLOG(6) << "async upload all files";
    }
    return 0;
}

int DiskCacheWrite::AsyncUploadRun() {
    if (isRunning_.exchange(true)) {
        LOG(INFO) << "AsyncUpload thread is on running.";
        return -1;
    }
    LOG(INFO) << "AsyncUpload thread is on running.";
    backEndThread_ = std::thread(&DiskCacheWrite::AsyncUploadFunc, this);
    return 0;
}

int DiskCacheWrite::AsyncUploadStop() {
    if (isRunning_.exchange(false)) {
        LOG(INFO) << "stop AsyncUpload thread...";
        sleeper_.interrupt();
        backEndThread_.join();
        LOG(INFO) << "stop AsyncUpload thread ok.";
        return -1;
    } else {
        LOG(INFO) << "AsyncUpload thread not running.";
    }
    return 0;
}

int DiskCacheWrite::UploadAllCacheWriteFile() {
    VLOG(3) << "upload all cached write file start.";
    std::string fileFullPath;
    bool ret;
    DIR *cacheWriteDir = NULL;
    struct dirent *cacheWriteDirent = NULL;
    fileFullPath = GetCacheIoFullDir();
    ret = IsFileExist(fileFullPath);
    if (!ret) {
        LOG(ERROR) << "cache write dir is not exist.";
        return -1;
    }
    cacheWriteDir = posixWrapper_->opendir(fileFullPath.c_str());
    if (!cacheWriteDir) {
        LOG(ERROR) << "opendir error, errno = " << errno;
        return -1;
    }
    int doRet;
    std::vector<std::string> uploadObjs;
    while ((cacheWriteDirent = posixWrapper_->readdir(cacheWriteDir)) != NULL) {
        if ((!strncmp(cacheWriteDirent->d_name, ".", 1)) ||
            (!strncmp(cacheWriteDirent->d_name, "..", 2)))
            continue;

        std::string fileName = cacheWriteDirent->d_name;
        uploadObjs.push_back(fileName);
    }
    doRet = posixWrapper_->closedir(cacheWriteDir);
    if (doRet < 0) {
        LOG(ERROR) << "close error， errno = " << errno;
        return doRet;
    }
    if (uploadObjs.empty()) {
        return 0;
    }
    curve::common::CountDownEvent cond(1);
    std::atomic<uint64_t> pendingReq(0);
    pendingReq.fetch_add(uploadObjs.size(), std::memory_order_seq_cst);
    for (auto iter = uploadObjs.begin(); iter != uploadObjs.end(); iter++) {
        uint64_t fileSize;
        char *buffer = nullptr;
        doRet = ReadFile(*iter, &buffer, &fileSize);
        if (doRet < 0 || buffer == nullptr) {
            if (buffer != nullptr)
                posixWrapper_->free(buffer);
            LOG(WARNING) << "read failed, file name is: " << *iter;
            pendingReq.fetch_sub(1, std::memory_order_seq_cst);
            continue;
        }
        PutObjectAsyncCallBack cb =
        [&, buffer](const std::shared_ptr<PutObjectAsyncContext> &context) {
            if (context->retCode == 0) {
                if (pendingReq.fetch_sub(1, std::memory_order_seq_cst) == 1) {
                    VLOG(3) << "pendingReq is over";
                    cond.Signal();
                }
                VLOG(3) << "PutObjectAsyncCallBack success"
                        << ", file: " << context->key;
                posixWrapper_->free(buffer);
                return;
            }
            LOG(WARNING) << "upload object failed: " << context->key;
            client_->UploadAsync(context);
        };
        auto context = std::make_shared<PutObjectAsyncContext>();
        context->key = *iter;
        context->buffer = buffer;
        context->bufferSize = fileSize;
        context->cb = cb;
        client_->UploadAsync(context);
    }
    if (pendingReq.load(std::memory_order_seq_cst)) {
        VLOG(9) << "wait for pendingReq";
        cond.Wait();
    }
    for (auto iter = uploadObjs.begin(); iter != uploadObjs.end(); iter++) {
        RemoveFile(*iter);
        cachedObjName_->Put(*iter, false);
    }
    VLOG(3) << "upload all cached write file end.";
    return 0;
}

int DiskCacheWrite::RemoveFile(const std::string fileName) {
    // del disk file
    std::string fileFullPath;
    fileFullPath = GetCacheIoFullDir();
    std::string fullFileName = fileFullPath + "/" + fileName;
    int ret = posixWrapper_->remove(fullFileName.c_str());
    if (ret < 0) {
        LOG(ERROR) << "remove disk file error, file = " << fileName
                   << ", errno = " << errno;
        return -1;
    }
    cachedObjName_->Put(fileName, false);
    VLOG(9) << "remove file success, file = " << fileName;
    return 0;
}

int DiskCacheWrite::WriteDiskFile(const std::string fileName, const char *buf,
                                  uint64_t length, bool force) {
    VLOG(6) << "WriteDiskFile start. name = " << fileName
            << ", force = " << force << ", length = " << length;
    std::string fileFullPath;
    int fd, ret;
    fileFullPath = GetCacheIoFullDir() + "/" + fileName;
    fd = posixWrapper_->open(fileFullPath.c_str(), O_RDWR | O_CREAT, MODE);
    if (fd < 0) {
        LOG(ERROR) << "open disk file error. errno = " << errno
                   << ", file = " << fileName;
        return fd;
    }
    ssize_t writeLen = posixWrapper_->write(fd, buf, length);
    if (writeLen < 0 || writeLen < length) {
        LOG(ERROR) << "write disk file error. ret: " << writeLen
                   << ", file: " << fileName
                   << ", error: " << errno;
        posixWrapper_->close(fd);
        return -1;
    }

    // force to flush
    if (force) {
        ret = posixWrapper_->fdatasync(fd);
        if (ret < 0) {
            LOG(ERROR) << "fdatasync error. errno = " << errno
                       << ", file = " << fileName;
            posixWrapper_->close(fd);
            return -1;
        }
    }

    ret = posixWrapper_->close(fd);
    if (ret < 0) {
        LOG(ERROR) << "close disk file error. errno = " << errno
                   << ", file = " << fileName;
        return -1;
    }

    VLOG(6) << "WriteDiskFile success. name = " << fileName
            << ", force = " << force << ", length = " << length;
    return writeLen;
}

}  // namespace client
}  // namespace curvefs
