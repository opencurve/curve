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

#include "curvefs/src/client/s3/disk_cache_base.h"

namespace curvefs {

namespace client {

#define CACHE_WRITE_DIR "cachewrite"
#define CACHE_READ_DIR  "cacheread"

void DiskCacheBase::Init(std::shared_ptr<PosixWrapper> wrapper,
                         const std::string cacheDir) {
    cacheDir_ = cacheDir;
    posixWrapper_ = wrapper;
}

int DiskCacheBase::CreateIoDir(bool writreDir) {
    struct stat statFile;
    bool ret;
    std::string FullDirPath;

    if (writreDir) {
        cacheIoDir_ = CACHE_WRITE_DIR;
    } else {
        cacheIoDir_ = CACHE_READ_DIR;
    }
    FullDirPath = cacheDir_ + "/" + cacheIoDir_;
    ret = IsFileExist(FullDirPath);
    if (!ret) {
        int rc = posixWrapper_->mkdir(FullDirPath.c_str(), 0755);
        if ((rc < 0) &&
            (errno != EEXIST)) {
            LOG(ERROR) << "create cache dir error. errno = " << errno
                       << ", dir = " << FullDirPath;
            return -1;
        }
        VLOG(6) << "read cache dir is not exist, create it success."
                << ", dir = " << FullDirPath;
    }
    return 0;
}

bool DiskCacheBase::IsFileExist(const std::string file) {
    struct stat statFile;
    int ret;
    ret = posixWrapper_->stat(file.c_str(), &statFile);
    if (ret < 0) {
        VLOG(6) << "file is not exist, dir = " << file << ", errno = " << errno;
        return false;
    }
    return true;
}

std::string DiskCacheBase::GetCacheIoFullDir() {
    std::string fullPath;
    fullPath = cacheDir_ + "/" + cacheIoDir_;
    return fullPath;
}

int DiskCacheBase::LoadAllCacheFile(std::set<std::string> *cachedObj) {
    std::string cachePath = GetCacheIoFullDir();
    bool ret = IsFileExist(cachePath);
    if (!ret) {
        LOG(ERROR) << "LoadAllCacheFile, cache read dir is not exist.";
        return -1;
    }

    LOG(INFO) << "LoadAllCacheFile start, dir: " << cachePath;
    DIR *cacheDir = NULL;
    struct dirent *cacheDirent = NULL;
    cacheDir = posixWrapper_->opendir(cachePath.c_str());
    if (!cacheDir) {
        LOG(ERROR) << "LoadAllCacheFile, opendir error, errno = " << errno;
        return -1;
    }
    while ((cacheDirent = posixWrapper_->readdir(cacheDir)) != NULL) {
        if ((!strncmp(cacheDirent->d_name, ".", 1)) ||
            (!strncmp(cacheDirent->d_name, "..", 2)))
            continue;
        std::string fileName = cacheDirent->d_name;
        cachedObj->emplace(fileName);
        VLOG(9) << "LoadAllCacheFile obj, name = " << fileName;
    }

    int rc = posixWrapper_->closedir(cacheDir);
    if (rc < 0) {
        LOG(ERROR) << "LoadAllCacheFile, opendir error, errno = " << errno;
        return rc;
    }
    LOG(INFO) << "LoadAllCacheReadFile end, dir: " << cachePath;
    return 0;
}

}  // namespace client
}  // namespace curvefs
