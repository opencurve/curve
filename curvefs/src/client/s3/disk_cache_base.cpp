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
#include <functional>

#include "curvefs/src/client/s3/disk_cache_base.h"

namespace curvefs {

namespace client {

#define CACHE_WRITE_DIR "cachewrite"
#define CACHE_READ_DIR  "cacheread"

void DiskCacheBase::Init(std::shared_ptr<PosixWrapper> wrapper,
                         const std::string cacheDir, uint32_t objectPrefix) {
    cacheDir_ = cacheDir;
    posixWrapper_ = wrapper;
    objectPrefix_ = objectPrefix;
}

int DiskCacheBase::CreateIoDir(bool writreDir) {
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

int DiskCacheBase::CreateDir(const std::string dir) {
    size_t p = dir.find_last_of('/');
    std::string dirPath = dir;
    if (p != -1ULL) {
        dirPath.erase(dirPath.begin()+p, dirPath.end());
    }
    std::vector<std::string> names;
    ::curve::common::SplitString(dirPath, "/", &names);
    // root dir must exists
    if (0 == names.size())
        return 0;

    std::string path;
    for (size_t i = 0; i < names.size(); ++i) {
        if (0 == i && dirPath[0] != '/')
            path = path + names[i];
        else
            path = path + "/" + names[i];

        if (IsFileExist(path)) {
            continue;
        }
        // dir needs 755 permission，or “Permission denied”
        if (posixWrapper_->mkdir(path.c_str(), 0755) < 0) {
            LOG(WARNING) << "mkdir " << path << " failed. "<< strerror(errno);
            return -errno;
        }
    }
    return 0;
}

int DiskCacheBase::LoadAllCacheFile(std::set<std::string> *cachedObj) {
    std::string cachePath = GetCacheIoFullDir();
    bool ret = IsFileExist(cachePath);
    if (!ret) {
        LOG(ERROR) << "LoadAllCacheFile, cache read dir is not exist.";
        return -1;
    }

    VLOG(3) << "LoadAllCacheFile start, dir: " << cachePath;
    std::function<bool(const std::string &path,
                       std::set<std::string> *cacheObj)> listDir;

    listDir = [&listDir, this](const std::string &path,
                               std::set<std::string> *cacheObj) -> bool {
        DIR *dir;
        struct dirent *ent;
        std::string fileName, nextdir;
        if ((dir = posixWrapper_->opendir(path.c_str())) != NULL) {
            while ((ent = posixWrapper_->readdir(dir)) != NULL) {
                VLOG(9) << "LoadAllCacheFile obj, name = " << ent->d_name;
                if (strncmp(ent->d_name, ".", 1) == 0 ||
                        strncmp(ent->d_name, "..", 2) == 0) {
                    continue;
                } else if (ent->d_type == 8) {
                    fileName = std::string(ent->d_name);
                    VLOG(9) << "LoadAllCacheFile obj, name = " << fileName;
                    cacheObj->emplace(fileName);
                } else {
                    nextdir = std::string(ent->d_name);
                    nextdir = path + '/' + nextdir;
                    if (!listDir(nextdir, cacheObj)) {
                        return false;
                    }
                }
            }
            int ret = posixWrapper_->closedir(dir);
            if (ret < 0) {
                LOG(ERROR) << "close dir "  << dir << ", error = " << errno;
            }
            return ret >= 0;
        }
        LOG(ERROR) << "LoadAllCacheFile Opendir error, path =" << path;
        return false;
    };
    ret = listDir(cachePath, cachedObj);
    if (!ret) {
        return -1;
    }
    VLOG(3) << "LoadAllCacheReadFile end, dir: " << cachePath;
    return 0;
}

}  // namespace client
}  // namespace curvefs
