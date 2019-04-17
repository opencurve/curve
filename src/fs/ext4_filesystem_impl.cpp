/*
 * Project: curve
 * File Created: 18-10-31
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <sys/vfs.h>
#include <dirent.h>

#include "src/common/string_util.h"
#include "src/fs/ext4_filesystem_impl.h"
#include "src/fs/wrap_posix.h"

namespace curve {
namespace fs {

std::shared_ptr<Ext4FileSystemImpl> Ext4FileSystemImpl::self_ = nullptr;
std::mutex Ext4FileSystemImpl::mutex_;

Ext4FileSystemImpl::Ext4FileSystemImpl(
    std::shared_ptr<PosixWrapper> posixWrapper)
    : posixWrapper_(posixWrapper) {
    CHECK(posixWrapper_ != nullptr) << "PosixWrapper is null";
}

Ext4FileSystemImpl::~Ext4FileSystemImpl() {
}

std::shared_ptr<Ext4FileSystemImpl> Ext4FileSystemImpl::getInstance() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (self_ == nullptr) {
        std::shared_ptr<PosixWrapper> wrapper =
            std::make_shared<PosixWrapper>();
        self_ = std::shared_ptr<Ext4FileSystemImpl>(
                new(std::nothrow) Ext4FileSystemImpl(wrapper));
        CHECK(self_ != nullptr) << "Failed to new ext4 local fs.";
    }
    return self_;
}

void Ext4FileSystemImpl::SetPosixWrapper(std::shared_ptr<PosixWrapper> wrapper) {  //NOLINT
    CHECK(wrapper != nullptr) << "PosixWrapper is null";
    posixWrapper_ = wrapper;
}

int Ext4FileSystemImpl::Init() {
    // ext4实现无需初始化
    return 0;
}

int Ext4FileSystemImpl::Statfs(const string& path,
                               struct FileSystemInfo *info) {
    struct statfs diskInfo;
    int rc = posixWrapper_->statfs(path.c_str(), &diskInfo);
    if (rc < 0) {
        LOG(ERROR) << "fstat failed: " << strerror(errno);
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
        LOG(ERROR) << "open failed: " << strerror(errno);
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
    // 如果删除对象是目录的话，需要先删除目录下的子对象
    if (DirExists(path)) {
        vector<string> names;
        rc = List(path, &names);
        if (rc < 0) {
            LOG(ERROR) << "List " << path << " failed.";
            return rc;
        }
        for (auto &name : names) {
            string subPath = path + "/" + name;
            // 递归删除子对象
            rc = Delete(subPath);
            if (rc < 0) {
                LOG(ERROR) << "Delete " << subPath << " failed.";
                return rc;
            }
        }
    }
    rc = posixWrapper_->remove(path.c_str());
    if (rc < 0) {
        LOG(ERROR) << "remove failed: " << strerror(errno);
        return -errno;
    }
    return rc;
}

int Ext4FileSystemImpl::Mkdir(const string& dirName) {
    vector<string> names;
    ::curve::common::SplitString(dirName, "/", &names);

    // root dir must exists
    if (0 == names.size())
        return 0;

    string path;
    for (size_t i = 0; i < names.size(); ++i) {
        if (0 == i && dirName[0] != '/')  // 相对路径
            path = path + names[i];
        else
            path = path + "/" + names[i];
        if (DirExists(path))
            continue;
        // 目录需要755权限，不然会出现“Permission denied”
        if (posixWrapper_->mkdir(path.c_str(), 0755) < 0) {
            LOG(ERROR) << "mkdir " << path << " failed. "<< strerror(errno);
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

int Ext4FileSystemImpl::DoRename(const string& oldPath,
                                 const string& newPath,
                                 unsigned int flags) {
    int rc = posixWrapper_->rename(oldPath.c_str(), newPath.c_str(), flags);
    if (rc < 0) {
        LOG(ERROR) << "rename failed: " << strerror(errno)
                   << ". old path: " << oldPath
                   << ", new path: " << newPath
                   << ", flag: " << flags;
        return -errno;
    }
    return 0;
}

int Ext4FileSystemImpl::List(const string& dirName,
                             vector<std::string> *names) {
    DIR *dir = posixWrapper_->opendir(dirName.c_str());
    if (nullptr == dir) {
        LOG(ERROR) << "opendir failed: " << strerror(errno);
        return -errno;
    }
    struct dirent *dirIter;
    errno = 0;
    while ((dirIter=posixWrapper_->readdir(dir)) != nullptr) {
        if (strcmp(dirIter->d_name, ".") == 0
                || strcmp(dirIter->d_name, "..") == 0)
            continue;
        names->push_back(dirIter->d_name);
    }
    // 可能存在其他携程改变了errno，但是只能通过此方式判断readdir是否成功
    if (errno != 0) {
        LOG(ERROR) << "readdir failed: " << strerror(errno);
    }
    posixWrapper_->closedir(dir);
    return -errno;
}

int Ext4FileSystemImpl::Read(int fd,
                             char *buf,
                             uint64_t offset,
                             int length) {
    int remainLength = length;
    int relativeOffset = 0;
    int retryTimes = 0;
    while (remainLength > 0) {
        int ret = posixWrapper_->pread(fd,
                                       buf + relativeOffset,
                                       remainLength,
                                       offset);
        // 如果offset大于文件长度，pread会返回0
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
            LOG(ERROR) << "pread failed: " << strerror(errno);
            return -errno;
        }
        remainLength -= ret;
        offset += ret;
        relativeOffset += ret;
    }
    return length - remainLength;
}

int Ext4FileSystemImpl::Write(int fd,
                              const char *buf,
                              uint64_t offset,
                              int length) {
    int remainLength = length;
    int relativeOffset = 0;
    int retryTimes = 0;
    while (remainLength > 0) {
        int ret = posixWrapper_->pwrite(fd,
                                        buf + relativeOffset,
                                        remainLength,
                                        offset);
        if (ret < 0) {
            if (errno == EINTR && retryTimes < MAX_RETYR_TIME) {
                ++retryTimes;
                continue;
            }
            LOG(ERROR) << "pwrite failed: " << strerror(errno);
            return -errno;
        }
        remainLength -= ret;
        offset += ret;
        relativeOffset += ret;
    }
    return length;
}

int Ext4FileSystemImpl::Append(int fd,
                               const char *buf,
                               int length) {
    // TODO(yyk)
    return 0;
}

int Ext4FileSystemImpl::Fallocate(int fd,
                                  int op,
                                  uint64_t offset,
                                  int length) {
    int rc = posixWrapper_->fallocate(fd, op, offset, length);
    if (rc < 0) {
        LOG(ERROR) << "fallocate failed: " << strerror(errno);
        return -errno;
    }
    return 0;
}

int Ext4FileSystemImpl::Fstat(int fd, struct stat *info) {
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
