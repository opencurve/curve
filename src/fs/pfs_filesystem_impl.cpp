/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * File Created: 2022-06-01
 * Author: xuchaojie
 */

#include "src/fs/pfs_filesystem_impl.h"

#include <glog/logging.h>
#include <sys/vfs.h>
#include <sys/utsname.h>
#include <linux/version.h>
#include <dirent.h>
#include <brpc/server.h>
#include <butil/iobuf.h>

#include <pfs_api.h>
#include <sys/uio.h>

#include <string>
#include <vector>

#include "src/common/string_util.h"


#define NVME_ALIGN 8
#define SECTOR_SIZE 512

namespace curve {
namespace fs {

static bvar::Adder<uint64_t> g_preadv_user("preadv_user_count");
static bvar::Adder<uint64_t> g_preadv_not_aligned("preadv_not_aligned_count");
static bvar::Adder<uint64_t> g_preadv_aligned("preadv_aligned_count");

static bvar::Adder<uint64_t> g_pwritev_user("pwritev_user_count");
static bvar::Adder<uint64_t> g_pwritev_not_aligned("pwritev_not_aligned_count");
static bvar::Adder<uint64_t> g_pwritev_aligned("pwritev_aligned_count");

static bvar::Adder<uint64_t> g_readv_user("readv_user_count");
static bvar::Adder<uint64_t> g_readv_not_aligned("readv_not_aligned_count");
static bvar::Adder<uint64_t> g_readv_aligned("readv_aligned_count");

static bvar::Adder<uint64_t> g_writev_user("writev_user_count");
static bvar::Adder<uint64_t> g_writev_not_aligned("writev_not_aligned_count");
static bvar::Adder<uint64_t> g_writev_aligned("writev_aligned_count");

std::string DumpIOVec(const struct iovec *vec, size_t nvec) {
    std::stringstream ss;
    ss << "{";
    for (size_t i = 0; i < nvec; ++i) {
        ss << "[" << uintptr_t(vec[i].iov_base) << ","
           << vec[i].iov_len << "]";
    }
    ss << "}";
    return ss.str();
}

bool is_aligned(const struct iovec *vec, size_t nvec, unsigned align) {
    for (size_t i = 0; i < nvec; ++i) {
        uintptr_t address = uintptr_t(vec[i].iov_base);
        size_t len = vec[i].iov_len;
        if ((address & (align - 1)) || (len & (align - 1)))
            return false;
    }
    return true;
}

ssize_t preadv_dispatch(int fd, const struct iovec *vector,
                        int nvec, off_t offset) {
    if (PFS_FD_ISVALID(fd)) {
        int ret = 0;
        bool isUser = butil::iobuf::iobuf_is_user();
        bool isAligned = is_aligned(vector, nvec, NVME_ALIGN);
        if (isUser) {
            g_preadv_user << 1;
            ret = pfs_preadv(fd, vector, nvec, offset);
        } else if (!isAligned) {
            g_preadv_not_aligned << 1;
            ret = pfs_preadv(fd, vector, nvec, offset);
        } else {
            g_preadv_aligned << 1;
            // TODO(xuchaojie): use pfs_preadv_dma in future
            ret = pfs_preadv(fd, vector, nvec, offset);
        }
        if (ret < 0) {
            LOG(ERROR) << "preadv_dispatch failed, ret: " << ret
                       << ", fd: " << fd
                       << ", nvec: " << nvec
                       << ", offset: " << offset
                       << ", isUser: " << isUser
                       << ", isAligned: " << isAligned
                       << ", iovec: " << DumpIOVec(vector, nvec);
        }
        return ret;
    }
    return preadv(fd, vector, nvec, offset);
}

ssize_t pwritev_dispatch(int fd, const struct iovec *vector,
                         int nvec, off_t offset) {
    if (PFS_FD_ISVALID(fd)) {
        int ret = 0;
        bool isUser = butil::iobuf::iobuf_is_user();
        bool isAligned = is_aligned(vector, nvec, NVME_ALIGN);
        if (isUser) {
            g_pwritev_user << 1;
            ret = pfs_pwritev(fd, vector, nvec, offset);
        } else if (!isAligned) {
            g_pwritev_not_aligned << 1;
            ret = pfs_pwritev(fd, vector, nvec, offset);
        } else {
            g_pwritev_aligned << 1;
            // TODO(xuchaojie): use pfs_pwritev_dma in future
            ret = pfs_pwritev(fd, vector, nvec, offset);
        }
        if (ret < 0) {
            LOG(ERROR) << "pwritev_dispatch failed, ret: " << ret
                       << ", fd: " << fd
                       << ", nvec: " << nvec
                       << ", offset: " << offset
                       << ", isUser: " << isUser
                       << ", isAligned: " << isAligned
                       << ", iovec: " << DumpIOVec(vector, nvec);
        }
        return ret;
    }
    return pwritev(fd, vector, nvec, offset);
}

ssize_t readv_dispatch(int fd, const struct iovec *vector, int nvec) {
    if (PFS_FD_ISVALID(fd)) {
        int ret = 0;
        bool isUser = butil::iobuf::iobuf_is_user();
        bool isAligned = is_aligned(vector, nvec, NVME_ALIGN);
        if (isUser) {
            g_readv_user << 1;
            ret = pfs_readv(fd, vector, nvec);
        } else if (!isAligned) {
            g_readv_not_aligned << 1;
            ret = pfs_readv(fd, vector, nvec);
        } else {
            g_readv_aligned << 1;
            // TODO(xuchaojie): use pfs_readv_dma in future
            ret = pfs_readv(fd, vector, nvec);
        }
        if (ret < 0) {
            LOG(ERROR) << "readv_dispatch failed, ret: " << ret
                       << ", fd: " << fd
                       << ", nvec: " << nvec
                       << ", isUser: " << isUser
                       << ", isAligned: " << isAligned
                       << ", iovec: " << DumpIOVec(vector, nvec);
        }
        return ret;
    }
    return readv(fd, vector, nvec);
}

ssize_t writev_dispatch(int fd, const struct iovec *vector, int nvec) {
    if (PFS_FD_ISVALID(fd)) {
        int ret = 0;
        bool isUser = butil::iobuf::iobuf_is_user();
        bool isAligned = is_aligned(vector, nvec, NVME_ALIGN);
        if (isUser) {
            g_writev_user << 1;
            ret = pfs_writev(fd, vector, nvec);
        } else if (!isAligned) {
            g_writev_not_aligned << 1;
            ret = pfs_writev(fd, vector, nvec);
        } else {
            g_writev_aligned << 1;
            // TODO(xuchaojie): use pfs_writev_dma in future
            ret = pfs_writev(fd, vector, nvec);
        }
        if (ret < 0) {
            LOG(ERROR) << "pwritev_dispatch failed, ret: " << ret
                       << ", fd: " << fd
                       << ", nvec: " << nvec
                       << ", isUser: " << isUser
                       << ", isAligned: " << isAligned
                       << ", iovec: " << DumpIOVec(vector, nvec);
        }
        return ret;
    }
    return writev(fd, vector, nvec);
}

void HookIOBufIOFuncs() {
    butil::iobuf::iobuf_io_funcs iofuncs;
    iofuncs.iof_preadv = preadv_dispatch;
    iofuncs.iof_pwritev = pwritev_dispatch;
    iofuncs.iof_readv = readv_dispatch;
    iofuncs.iof_writev = writev_dispatch;
    butil::iobuf::set_external_io_funcs(iofuncs);
}

std::shared_ptr<PfsFileSystemImpl> PfsFileSystemImpl::self_ = nullptr;
std::mutex PfsFileSystemImpl::mutex_;

std::shared_ptr<PfsFileSystemImpl> PfsFileSystemImpl::getInstance() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (self_ == nullptr) {
        self_ = std::shared_ptr<PfsFileSystemImpl>(
                new(std::nothrow) PfsFileSystemImpl());
        CHECK(self_ != nullptr) << "Failed to new pfs.";
    }
    return self_;
}

int PfsFileSystemImpl::Init(const LocalFileSystemOption& option) {
    int rc = pfs_mount_acquire(option.pfs_cluster.c_str(),
        option.pfs_pbd_name.c_str(),
        option.pfs_host_id, PFS_RDWR|MNTFLG_PFSD_INTERNAL);
    if (rc) {
        LOG(ERROR) << "can not mount pfs"
                   << ", cluster: " << option.pfs_cluster
                   << ", host_id: " << option.pfs_host_id
                   << ", pbd name: " << option.pfs_pbd_name;
    }
    return 0;
}

int PfsFileSystemImpl::Statfs(const string& path, struct FileSystemInfo* info) {
    struct statfs diskInfo;
    int rc = pfs_statfs(path.c_str(), &diskInfo);
    if (rc < 0) {
        LOG(WARNING) << "pfs_statfs failed: " << strerror(errno)
                   << ", past: " << path;
        return -errno;
    }
    info->total = diskInfo.f_blocks * diskInfo.f_bsize;
    info->available = diskInfo.f_bavail * diskInfo.f_bsize;
    info->stored = info->total - diskInfo.f_bfree * diskInfo.f_bsize;
    info->allocated = info->stored;
    return 0;
}

int PfsFileSystemImpl::Open(const string& path, int flags) {
    int fd = pfs_open(path.c_str(), flags, 0644);
    if (fd < 0) {
        LOG(WARNING) << "pfs_open failed: " << strerror(errno)
                     << ", path: " << path;
        return -errno;
    }
    return fd;
}

int PfsFileSystemImpl::Close(int fd) {
    int rc = pfs_close(fd);
    if (rc < 0) {
        LOG(ERROR) << "close failed: " << strerror(errno)
                   << ", fd: " << fd;
        return -errno;
    }
    return rc;
}

int PfsFileSystemImpl::Delete(const string& path) {
    int rc = 0;
    if (DirExists(path)) {
        std::vector<std::string> names;
        rc = List(path, &names);
        if (rc < 0) {
            LOG(WARNING) << "List " << path << " failed.";
            return rc;
        }
        for (auto &name : names) {
            string subPath = path + "/" + name;
            rc = Delete(subPath);
            if (rc < 0) {
                LOG(WARNING) << "Delete " << subPath << " failed.";
                return rc;
            }
        }
    }
    rc = pfs_unlink(path.c_str());
    if (rc < 0) {
        LOG(WARNING) << "pfs_unlink failed: " << strerror(errno)
                     << ", path: " << path;
        return -errno;
    }
    return rc;
}

int PfsFileSystemImpl::Mkdir(const string& dirPath) {
    std::vector<std::string> names;
    ::curve::common::SplitString(dirPath, "/", &names);

    // root dir must exists
    if (0 == names.size())
        return 0;

    std::string path;
    for (size_t i = 0; i < names.size(); ++i) {
        if (0 == i && dirPath[0] != '/') {
            path = path + names[i];
        } else {
            path = path + "/" + names[i];
        }
        if (DirExists(path))
            continue;
        if (pfs_mkdir(path.c_str(), 0755) < 0) {
            LOG(WARNING) << "pfs_mkdir " << path
                         << " failed: "<< strerror(errno);
            return -errno;
        }
    }
    return 0;
}

bool PfsFileSystemImpl::DirExists(const string& dirPath) {
    struct stat path_stat;
    if (0 == pfs_stat(dirPath.c_str(), &path_stat))
        return S_ISDIR(path_stat.st_mode);
    else
        return false;
}

bool PfsFileSystemImpl::FileExists(const string& filePath) {
    struct stat path_stat;
    if (0 == pfs_stat(filePath.c_str(), &path_stat))
        return S_ISREG(path_stat.st_mode);
    else
        return false;
}

bool PfsFileSystemImpl::PathExists(const string& path) {
    return pfs_access(path.c_str(), F_OK) == 0;
}

int PfsFileSystemImpl::DoRename(const string& oldPath,
                   const string& newPath,
                   unsigned int flags) {
    int rc = pfs_rename2(oldPath.c_str(), newPath.c_str(), flags);
    if (rc < 0) {
        LOG(WARNING) << "rename failed: " << strerror(errno)
                     << ". old path: " << oldPath
                     << ", new path: " << newPath
                     << ", flag: " << flags;
        return -errno;
    }
    return 0;
}

int PfsFileSystemImpl::List(const string& dirPath, vector<std::string>* names) {
    DIR *dir = pfs_opendir(dirPath.c_str());
    if (nullptr == dir) {
        LOG(WARNING) << "pfs_opendir:" << dirPath
                     << " failed:" << strerror(errno);
        return -errno;
    }
    struct dirent *dirIter;
    errno = 0;
    while ((dirIter = pfs_readdir(dir)) != nullptr) {
        if (strcmp(dirIter->d_name, ".") == 0
                || strcmp(dirIter->d_name, "..") == 0)
            continue;
        names->push_back(dirIter->d_name);
    }
    if (errno != 0) {
        LOG(WARNING) << "readdir failed: " << strerror(errno);
    }
    pfs_closedir(dir);
    return -errno;
}

DIR* PfsFileSystemImpl::OpenDir(const string& dirPath) {
    return pfs_opendir(dirPath.c_str());
}

struct dirent* PfsFileSystemImpl::ReadDir(DIR *dir) {
    return pfs_readdir(dir);
}

int PfsFileSystemImpl::CloseDir(DIR *dir) {
    pfs_closedir(dir);
    return -errno;
}

int PfsFileSystemImpl::Read(int fd, char* buf, uint64_t offset, int length) {
    int remainLength = length;
    int relativeOffset = 0;
    int retryTimes = 0;
    while (remainLength > 0) {
        int ret = pfs_pread(fd,
                            buf + relativeOffset,
                            remainLength,
                            offset);
        if (ret == 0) {
            LOG(WARNING) << "pread returns zero."
                         << "offset: " << offset
                         << ", length: " << remainLength;
            break;
        }
        if (ret < 0) {
            if (errno == EINTR && retryTimes < 3) {
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

int PfsFileSystemImpl::Read(int fd, butil::IOPortal* portal,
             uint64_t offset, int length) {
    off_t orig_offset = offset;
    ssize_t left = length;
    int retryTimes = 0;
    // portal->set_append_align(NVME_ALIGN, SECTOR_SIZE, true);
    while (left > 0) {
        ssize_t read_len = portal->pappend_from_file_descriptor(
                fd, offset, static_cast<size_t>(left));
        if (read_len > 0) {
            left -= read_len;
            offset += read_len;
        } else if (read_len == 0) {
            break;
        } else if (errno == EINTR && retryTimes < 3) {
            ++retryTimes;
            continue;
        } else {
            LOG(ERROR) << "pread failed: " << strerror(errno);
            return -errno;
        }
    }
    return length - left;
}

int PfsFileSystemImpl::Write(int fd, const char* buf,
                             uint64_t offset, int length) {
    int remainLength = length;
    int relativeOffset = 0;
    int retryTimes = 0;
    while (remainLength > 0) {
        int ret = pfs_pwrite(fd,
                             buf + relativeOffset,
                             remainLength,
                             offset);
        if (ret < 0) {
            if (errno == EINTR && retryTimes < 3) {
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

int PfsFileSystemImpl::Write(int fd, butil::IOBuf buf, uint64_t offset,
    int length) {
    int remainLength = length;
    int relativeOffset = 0;
    int retryTimes = 0;

    while (remainLength > 0) {
        ssize_t ret = buf.pcut_into_file_descriptor(fd, offset, remainLength);
        if (ret < 0) {
            if (errno == EINTR || retryTimes < 3) {
                ++retryTimes;
                continue;
            }
            LOG(ERROR) << "IOBuf::pcut_into_file_descriptor failed: "
                       << strerror(errno)
                       << ", fd: " << fd
                       << ", offset: " << offset
                       << ", length: " << length;
            return -errno;
        }

        remainLength -= ret;
        offset += ret;
    }

    return length;
}

int PfsFileSystemImpl::WriteZero(int fd, uint64_t offset, int length) {
    int remainLength = length;
    int relativeOffset = 0;
    int retryTimes = 0;
    while (remainLength > 0) {
        int ret = pfs_pwrite_zero(fd, remainLength, offset);
        if (ret < 0) {
            if (errno == EINTR && retryTimes < 3) {
                ++retryTimes;
                continue;
            }
            LOG(ERROR) << "pwrite zero failed: " << strerror(errno);
            return -errno;
        }
        remainLength -= ret;
        offset += ret;
        relativeOffset += ret;
    }
    return length;
}

int PfsFileSystemImpl::Fdatasync(int fd) {
    int rc = pfs_fsync(fd);
    if (rc < 0) {
        LOG(ERROR) << "fsync failed: " << strerror(errno);
        return -errno;
    }
    return 0;
}

int PfsFileSystemImpl::Append(int fd, const char* buf, int length) {
    // not implemented
    return 0;
}

int PfsFileSystemImpl::Fallocate(int fd, int op, uint64_t offset, int length) {
    int rc = pfs_fallocate(fd, op, offset, length);
    if (rc < 0) {
        LOG(ERROR) << "fallocate failed: " << strerror(errno);
        return -errno;
    }
    return 0;
}

int PfsFileSystemImpl::Fstat(int fd, struct stat* info) {
    int rc = pfs_fstat(fd, info);
    if (rc < 0) {
        LOG(ERROR) << "fstat failed: " << strerror(errno);
        return -errno;
    }
    return 0;
}

int PfsFileSystemImpl::Fsync(int fd) {
    int rc = pfs_fsync(fd);
    if (rc < 0) {
        LOG(ERROR) << "fsync failed: " << strerror(errno);
        return -errno;
    }
    return 0;
}

off_t PfsFileSystemImpl::Lseek(int fd, off_t offset, int whence) {
    off_t rc = pfs_lseek(fd, offset, whence);
    if (rc < 0) {
        LOG(ERROR) << "lseek failed: " << strerror(errno);
    }
    return rc;
}

int PfsFileSystemImpl::Link(const std::string &oldPath,
    const std::string &newPath) {
    LOG(ERROR) << "PfsFileSystem do not support link!";
    return -1;
}

}  // namespace fs
}  // namespace curve
