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

#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <dirent.h>
#include <memory>

#include "curvefs/src/client/s3/disk_cache_read.h"

namespace curvefs {

namespace client {

void DiskCacheRead::Init(std::shared_ptr<PosixWrapper> posixWrapper,
                         const std::string cacheDir) {
    posixWrapper_ = posixWrapper;
    DiskCacheBase::Init(posixWrapper, cacheDir);
}

int DiskCacheRead::ReadDiskFile(const std::string name, char *buf,
                                uint64_t offset, uint64_t length) {
    VLOG(6) << "ReadDiskFile start. name = " << name << ", offset = " << offset
            << ", length = " << length;
    std::string fileFullPath;
    int fd, ret;
    fileFullPath = GetCacheIoFullDir() + "/" + name;
    fd = posixWrapper_->open(fileFullPath.c_str(), O_RDONLY, MODE);
    if (fd < 0) {
        LOG(ERROR) << "open disk file error. file = " << name
                   << ", errno = " << errno;
        return fd;
    }
    off_t seekPos = posixWrapper_->lseek(fd, offset, SEEK_SET);
    if (seekPos < 0) {
        LOG(ERROR) << "lseek disk file error. file = " << name
                   << ", errno = " << errno;
        posixWrapper_->close(fd);
        return seekPos;
    }
    ssize_t readLen = posixWrapper_->read(fd, buf, length);
    if (readLen < 0) {
        LOG(ERROR) << "read disk error, ret = " << readLen
                   << ", errno = " << errno << ", file = " << name;
        posixWrapper_->close(fd);
        return readLen;
    }
    if (readLen < length) {
        LOG(ERROR) << "read disk file is not entirely. read len = " << readLen
                   << ", but want len = " << length << ", file = " << name;
        posixWrapper_->close(fd);
        return readLen;
    }
    posixWrapper_->close(fd);
    VLOG(6) << "ReadDiskFile success. name = " << name
            << ", offset = " << offset << ", length = " << length;
    return readLen;
}

int DiskCacheRead::LinkWriteToRead(const std::string fileName,
                                   const std::string fullWriteDir,
                                   const std::string fullReadDir) {
    VLOG(6) << "LinkWriteToRead start. name = " << fileName;
    std::string fullReadPath, fullWritePath;
    fullWritePath = fullWriteDir + "/" + fileName;
    fullReadPath = fullReadDir + "/" + fileName;
    int ret;
    if (!IsFileExist(fullWritePath)) {
        LOG(ERROR) << "link error because of file is not exist."
                   << ", file = " << fullWritePath;
        return -1;
    }
    ret = posixWrapper_->link(fullWritePath.c_str(), fullReadPath.c_str());
    if (ret < 0) {
        LOG(ERROR) << "link error. ret = " << ret << ", errno = " << errno
                   << ", write path = " << fullWritePath
                   << ", read path = " << fullReadPath;
        return -1;
    }
    VLOG(6) << "LinkWriteToRead success. name = " << fileName;
    return 0;
}

int DiskCacheRead::LoadAllCacheReadFile(
  std::shared_ptr<SglLRUCache<std::string>> cachedObj) {
    LOG(INFO) << "LoadAllCacheReadFile start. ";
    std::string cacheReadPath;
    bool ret;
    DIR *cacheReadDir = NULL;
    struct dirent *cacheReadDirent = NULL;
    cacheReadPath = GetCacheIoFullDir();
    ret = IsFileExist(cacheReadPath);
    if (!ret) {
        LOG(ERROR) << "cache read dir is not exist.";
        return -1;
    }
    cacheReadDir = posixWrapper_->opendir(cacheReadPath.c_str());
    if (!cacheReadDir) {
        LOG(ERROR) << "opendir error， errno = " << errno;
        return -1;
    }
    while ((cacheReadDirent = posixWrapper_->readdir(cacheReadDir)) != NULL) {
        if ((!strncmp(cacheReadDirent->d_name, ".", 1)) ||
            (!strncmp(cacheReadDirent->d_name, "..", 2)))
            continue;
        std::string fileName = cacheReadDirent->d_name;
        cachedObj->Put(fileName);
        VLOG(3) << "LoadAllCacheReadFile obj, name = " << fileName;
    }
    VLOG(6) << "close start.";
    int rc = posixWrapper_->closedir(cacheReadDir);
    if (rc < 0) {
        LOG(ERROR) << "opendir error， errno = " << errno;
        return rc;
    }

    LOG(INFO) << "LoadAllCacheReadFile success.";
    return 0;
}

int DiskCacheRead::WriteDiskFile(const std::string fileName,
                      const char* buf, uint64_t length) {
    VLOG(9) << "WriteDiskFile start. name = " << fileName
                 << ", length = " << length;
    std::string fileFullPath;
    int fd, ret;
    fileFullPath = GetCacheIoFullDir() + "/" + fileName;
    fd = posixWrapper_->open(fileFullPath.c_str(), O_RDWR|O_CREAT, MODE);
    if (fd < 0) {
        LOG(ERROR) << "open disk file error. errno = " << errno
                   << ", file = " << fileName;
        return fd;
    }
    ssize_t writeLen = posixWrapper_->write(fd, buf, length);
    if (writeLen < 0 || writeLen < length) {
        LOG(ERROR) << "write disk file error. ret = " << writeLen
                   << ", file = " << fileName;
        posixWrapper_->close(fd);
        return -1;
    }
    ret = posixWrapper_->close(fd);
    if (ret < 0) {
        LOG(ERROR) << "close disk file error. errno = " << errno
                   << ", file = " << fileName;
        return -1;
    }

    VLOG(9) << "WriteDiskFile success. name = " << fileName
              << ", length = " << length;
    return writeLen;
}

}  // namespace client
}  // namespace curvefs
