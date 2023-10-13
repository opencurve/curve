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
 * File Created: 18-10-31
 * Author: yangyaokai
 */

#include "src/fs/ext4_filesystem_impl.h"

#include <dirent.h>
#include <glog/logging.h>
#include <linux/version.h>
#include <sys/utsname.h>
#include <sys/vfs.h>

#include "src/common/string_util.h"
#include "src/fs/wrap_posix.h"

#define MIN_KERNEL_VERSION KERNEL_VERSION(3, 15, 0)

namespace curve {
namespace fs {

std::shared_ptr<Ext4FileSystemImpl> Ext4FileSystemImpl::self_ = nullptr;
std::mutex Ext4FileSystemImpl::mutex_;

Ext4FileSystemImpl::Ext4FileSystemImpl(
    std::shared_ptr<PosixWrapper> posixWrapper)
    : posixWrapper_(posixWrapper), enableRenameat2_(false) {
    CHECK(posixWrapper_ != nullptr) << "PosixWrapper is null";
}

Ext4FileSystemImpl::~Ext4FileSystemImpl() {}

std::shared_ptr<Ext4FileSystemImpl> Ext4FileSystemImpl::getInstance() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (self_ == nullptr) {
        std::shared_ptr<PosixWrapper> wrapper =
            std::make_shared<PosixWrapper>();
        self_ = std::shared_ptr<Ext4FileSystemImpl>(
            new (std::nothrow) Ext4FileSystemImpl(wrapper));
        CHECK(self_ != nullptr) << "Failed to new ext4 local fs.";
    }
    return self_;
}

void Ext4FileSystemImpl::SetPosixWrapper(
    std::shared_ptr<PosixWrapper> wrapper) {  // NOLINT
    CHECK(wrapper != nullptr) << "PosixWrapper is null";
    posixWrapper_ = wrapper;
}

bool Ext4FileSystemImpl::CheckKernelVersion() {
    struct utsname kernel_info;
    int ret = 0;

    ret = posixWrapper_->uname(&kernel_info);
    if (ret != 0) {
        LOG(ERROR) << "Get kernel info failed.";
        return false;
    }

    LOG(INFO) << "Kernel version: " << kernel_info.release;
    LOG(INFO) << "System version: " << kernel_info.version;
    LOG(INFO) << "Machine: " << kernel_info.machine;

    // The version string format obtained through uname may be a.b.c-xxx
    // A is the main version number, b is the version number, and c is the
    // revision number
    vector<string> elements;
    ::curve::common::SplitString(kernel_info.release, "-", &elements);
    if (elements.size() == 0) {
        LOG(ERROR) << "parse kenel version failed.";
        return false;
    }

    vector<string> numbers;
    ::curve::common::SplitString(elements[0], ".", &numbers);
    // Some systems may have a version format with the front part being a.b.c.d,
    // but a.b.c remains unchanged
    if (numbers.size() < 3) {
        LOG(ERROR) << "parse kenel version failed.";
        return false;
    }

    int major = std::stoi(numbers[0]);
    int minor = std::stoi(numbers[1]);
    int revision = std::stoi(numbers[2]);
    LOG(INFO) << "major: " << major << ", minor: " << minor
              << ", revision: " << revision;

    // The kernel version must be greater than 3.15 to support renameat2
    if (KERNEL_VERSION(major, minor, revision) < MIN_KERNEL_VERSION) {
        LOG(ERROR) << "Kernel older than 3.15 is not supported.";
        return false;
    }
    return true;
}

int Ext4FileSystemImpl::Init(const LocalFileSystemOption& option) {
    enableRenameat2_ = option.enableRenameat2;
    if (enableRenameat2_) {
        if (!CheckKernelVersion()) return -1;
    }
    return 0;
}

int Ext4FileSystemImpl::Statfs(const string& path,
                               struct FileSystemInfo* info) {
    struct statfs diskInfo;
    int rc = posixWrapper_->statfs(path.c_str(), &diskInfo);
    if (rc < 0) {
        LOG(WARNING) << "fstat failed: " << strerror(errno)
                     << ", file path = " << path.c_str();
        return -errno;
    }
    info->total = diskInfo.f_blocks * diskInfo.f_bsize;
    info->available = diskInfo.f_bavail * diskInfo.f_bsize;
    info->stored = info->total - diskInfo.f_bfree * diskInfo.f_bsize;
    info->allocated = info->stored;
    return 0;
}

int Ext4FileSystemImpl::Open(const string& path, int flags) {
    int fd = posixWrapper_->open(path.c_str(), flags, 0644);
    if (fd < 0) {
        LOG(WARNING) << "open failed: " << strerror(errno)
                     << ", file path = " << path.c_str();
        return -errno;
    }
    return fd;
}

int Ext4FileSystemImpl::Close(int fd) {
    int rc = posixWrapper_->close(fd);
    if (rc < 0) {
        LOG(ERROR) << "close failed: " << strerror(errno);
        return -errno;
    }
    return 0;
}

int Ext4FileSystemImpl::Delete(const string& path) {
    int rc = 0;
    // If the deleted object is a directory, you need to first delete the sub
    // objects under the directory
    if (DirExists(path)) {
        vector<string> names;
        rc = List(path, &names);
        if (rc < 0) {
            LOG(WARNING) << "List " << path << " failed.";
            return rc;
        }
        for (auto& name : names) {
            string subPath = path + "/" + name;
            // Recursively delete sub objects
            rc = Delete(subPath);
            if (rc < 0) {
                LOG(WARNING) << "Delete " << subPath << " failed.";
                return rc;
            }
        }
    }
    rc = posixWrapper_->remove(path.c_str());
    if (rc < 0) {
        LOG(WARNING) << "remove failed: " << strerror(errno)
                     << ", file path = " << path.c_str();
        return -errno;
    }
    return rc;
}

int Ext4FileSystemImpl::Mkdir(const string& dirName) {
    vector<string> names;
    ::curve::common::SplitString(dirName, "/", &names);

    // root dir must exists
    if (0 == names.size()) return 0;

    string path;
    for (size_t i = 0; i < names.size(); ++i) {
        if (0 == i && dirName[0] != '/')  // Relative path
            path = path + names[i];
        else
            path = path + "/" + names[i];
        if (DirExists(path)) continue;
        // Directory requires 755 permissions, otherwise 'Permission denied'
        // will appear
        if (posixWrapper_->mkdir(path.c_str(), 0755) < 0) {
            LOG(WARNING) << "mkdir " << path << " failed. " << strerror(errno);
            return -errno;
        }
    }

    return 0;
}

bool Ext4FileSystemImpl::DirExists(const string& dirName) {
    struct stat path_stat;
    if (0 == posixWrapper_->stat(dirName.c_str(), &path_stat))
        return S_ISDIR(path_stat.st_mode);
    else
        return false;
}

bool Ext4FileSystemImpl::FileExists(const string& filePath) {
    struct stat path_stat;
    if (0 == posixWrapper_->stat(filePath.c_str(), &path_stat))
        return S_ISREG(path_stat.st_mode);
    else
        return false;
}

int Ext4FileSystemImpl::DoRename(const string& oldPath, const string& newPath,
                                 unsigned int flags) {
    int rc = 0;
    if (enableRenameat2_) {
        rc = posixWrapper_->renameat2(oldPath.c_str(), newPath.c_str(), flags);
    } else {
        rc = posixWrapper_->rename(oldPath.c_str(), newPath.c_str());
    }
    if (rc < 0) {
        LOG(WARNING) << "rename failed: " << strerror(errno)
                     << ". old path: " << oldPath << ", new path: " << newPath
                     << ", flag: " << flags;
        return -errno;
    }
    return 0;
}

int Ext4FileSystemImpl::List(const string& dirName,
                             vector<std::string>* names) {
    DIR* dir = posixWrapper_->opendir(dirName.c_str());
    if (nullptr == dir) {
        LOG(WARNING) << "opendir:" << dirName << " failed:" << strerror(errno);
        return -errno;
    }
    struct dirent* dirIter;
    errno = 0;
    while ((dirIter = posixWrapper_->readdir(dir)) != nullptr) {
        if (strcmp(dirIter->d_name, ".") == 0 ||
            strcmp(dirIter->d_name, "..") == 0)
            continue;
        names->push_back(dirIter->d_name);
    }
    // There may be other Ctrip changes to errno, but this is the only way to
    // determine whether readdir is successful
    if (errno != 0) {
        LOG(WARNING) << "readdir failed: " << strerror(errno);
    }
    posixWrapper_->closedir(dir);
    return -errno;
}

int Ext4FileSystemImpl::Read(int fd, char* buf, uint64_t offset, int length) {
    int remainLength = length;
    int relativeOffset = 0;
    int retryTimes = 0;
    while (remainLength > 0) {
        int ret = posixWrapper_->pread(fd, buf + relativeOffset, remainLength,
                                       offset);
        // If the offset is greater than the file length, pread will return 0
        if (ret == 0) {
            LOG(WARNING) << "pread returns zero."
                         << "offset: " << offset
                         << ", length: " << remainLength;
            break;
        }
        if (ret < 0) {
            if (errno == EINTR && retryTimes < MAX_RETYR_TIME) {
                ++retryTimes;
                continue;
            }
            LOG(ERROR) << "pread failed, fd: " << fd
                       << ", size: " << remainLength << ", offset: " << offset
                       << ", error: " << strerror(errno);
            return -errno;
        }
        remainLength -= ret;
        offset += ret;
        relativeOffset += ret;
    }
    return length - remainLength;
}

int Ext4FileSystemImpl::Write(int fd, const char* buf, uint64_t offset,
                              int length) {
    int remainLength = length;
    int relativeOffset = 0;
    int retryTimes = 0;
    while (remainLength > 0) {
        int ret = posixWrapper_->pwrite(fd, buf + relativeOffset, remainLength,
                                        offset);
        if (ret < 0) {
            if (errno == EINTR && retryTimes < MAX_RETYR_TIME) {
                ++retryTimes;
                continue;
            }
            LOG(ERROR) << "pwrite failed, fd: " << fd
                       << ", size: " << remainLength << ", offset: " << offset
                       << ", error: " << strerror(errno);
            return -errno;
        }
        remainLength -= ret;
        offset += ret;
        relativeOffset += ret;
    }
    return length;
}

int Ext4FileSystemImpl::Write(int fd, butil::IOBuf buf, uint64_t offset,
                              int length) {
    if (length != static_cast<int>(buf.size())) {
        LOG(ERROR) << "IOBuf::pcut_into_file_descriptor failed, fd: " << fd
                   << ", data size doesn't equal to length, data size: "
                   << buf.size() << ", length: " << length;
        return -EINVAL;
    }

    int remainLength = length;
    int retryTimes = 0;

    while (remainLength > 0) {
        ssize_t ret = buf.pcut_into_file_descriptor(fd, offset, remainLength);
        if (ret < 0) {
            if (errno == EINTR || retryTimes < MAX_RETYR_TIME) {
                ++retryTimes;
                continue;
            }
            LOG(ERROR) << "IOBuf::pcut_into_file_descriptor failed, fd: " << fd
                       << ", size: " << remainLength << ", offset: " << offset
                       << ", error: " << strerror(errno);
            return -errno;
        }

        remainLength -= ret;
        offset += ret;
    }

    return length;
}

int Ext4FileSystemImpl::Sync(int fd) {
    int rc = posixWrapper_->fdatasync(fd);
    if (rc < 0) {
        LOG(ERROR) << "fdatasync failed: " << strerror(errno);
        return -errno;
    }
    return 0;
}

int Ext4FileSystemImpl::Append(int fd, const char* buf, int length) {
    (void)fd;
    (void)buf;
    (void)length;
    // TODO(yyk)
    return 0;
}

int Ext4FileSystemImpl::Fallocate(int fd, int op, uint64_t offset, int length) {
    int rc = posixWrapper_->fallocate(fd, op, offset, length);
    if (rc < 0) {
        LOG(ERROR) << "fallocate failed: " << strerror(errno);
        return -errno;
    }
    return 0;
}

int Ext4FileSystemImpl::Fstat(int fd, struct stat* info) {
    int rc = posixWrapper_->fstat(fd, info);
    if (rc < 0) {
        LOG(ERROR) << "fstat failed: " << strerror(errno);
        return -errno;
    }
    return 0;
}

int Ext4FileSystemImpl::Fsync(int fd) {
    int rc = posixWrapper_->fsync(fd);
    if (rc < 0) {
        LOG(ERROR) << "fsync failed: " << strerror(errno);
        return -errno;
    }
    return 0;
}

}  // namespace fs
}  // namespace curve
