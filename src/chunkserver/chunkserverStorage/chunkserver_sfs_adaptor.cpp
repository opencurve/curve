/*
 * Project: curve
 * File Created: Wednesday, 5th September 2018 8:03:04 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <string.h>
#include <list>

#include "src/chunkserver/chunkserverStorage/adaptor_util.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_storage.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_sfs_adaptor.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_sfs_localfs_impl.h"

using curve::chunkserver::CSSfsLocalFsImpl;

namespace curve {
namespace chunkserver {

CSSfsAdaptor::CSSfsAdaptor() {
    lfs_ = nullptr;
    storagePath_.clear();
}

CSSfsAdaptor::~CSSfsAdaptor() {
    if (lfs_ != nullptr) {
        delete lfs_;
        lfs_ = nullptr;
    }
}

bool CSSfsAdaptor::Initialize(const std::string& deviceID, const std::string & uri) {
    bool ret = false;
    if (lfs_ == nullptr) {
        std::string protocol = FsAdaptorUtil::ParserUri(uri, &storagePath_);
        do {
            if (strcmp(protocol.c_str(), "local") == 0) {
                lfs_ = new CSSfsLocalFsImpl();
            } else if (strcmp(protocol.c_str(), "bluestore") == 0) {
                lfs_ = curve::sfs::LocalFsFactory::CreateFs(curve::sfs::FsType::BLUESTORE);
            } else if (strcmp(protocol.c_str(), "sfs") == 0) {
                lfs_ = curve::sfs::LocalFsFactory::CreateFs(curve::sfs::FsType::SFS);
            }
            lfs_ == nullptr ? ret = false : ret = true;
        } while (0);
    } else {
        ret = true;
    }
    if (!ret) {
        return ret;
    }
    std::list<std::string> dirpaths = FsAdaptorUtil::ParserDirPath(storagePath_);
    for (auto iter : dirpaths) {
        if (!lfs_->DirExists(iter.c_str())) {
            int err = lfs_->Mkdir(iter.c_str(), 0755);
            if (err == -1) {
                ret = false;
                break;
            }
        }
    }
    return ret;
}

void CSSfsAdaptor::UnInitialize() {
    lfs_ = nullptr;
}

CSSfsAdaptor::fd_t CSSfsAdaptor::Open(const char* path, int flags, mode_t mode) {
    fd_t ret;
    ret.fd_ = lfs_->Open(path, flags, mode);
    return ret;
}

int CSSfsAdaptor::Close(fd_t fd) {
    return lfs_->Close(fd.fd_);
}

int CSSfsAdaptor::Delete(const char* path) {
    return lfs_->Delete(path);
}

int CSSfsAdaptor::Mkdir(const char* dirName, int flags) {
    return lfs_->Mkdir(dirName, flags);
}

bool CSSfsAdaptor::DirExists(const char* dirName) {
    return lfs_->DirExists(dirName);
}

bool CSSfsAdaptor::FileExists(const char* filePath) {
    return lfs_->FileExists(filePath);
}

int CSSfsAdaptor::Rename(const char* oldPath, const char* newPath) {
    return lfs_->Rename(oldPath, newPath);
}

int CSSfsAdaptor::List(const char* dirName, std::vector<char*>* names, int start, int max) {
    return lfs_->List(dirName, names, start, max);
}

int CSSfsAdaptor::Read(fd_t fd, void* buf, uint64_t offset, int length) {
    int leftcount = length;
    char* ptr = reinterpret_cast<char*>(buf);

    while (leftcount > 0) {
        int readcount = 0;
        if ((readcount = lfs_->Read(fd.fd_, ptr, offset, leftcount)) < 0) {
            if (errno == EINTR) {
                readcount = 0;
            } else if (false /*errno == ERROR_NO_ALLOCATE*/) {
                /* 
                 * if read before write, sfs will return error code
                 * we return user zero
                 * */
                memset(buf, 0, length);
                return 0;
            } else {
                return -1;
            }
        } else if (readcount == 0)  /*EOF*/  {
            break;
        }
        leftcount -= readcount;
        ptr += readcount;
        offset += readcount;
    }
    return length - leftcount;
}

bool CSSfsAdaptor::Write(fd_t fd, const void* buf, uint64_t offset, int length) {
    int leftcount = length;
    const char* ptr = reinterpret_cast<const char*>(buf);

    while (leftcount > 0) {
        int writecount = 0;
        if ((writecount = lfs_->Write(fd.fd_, ptr, offset, leftcount)) <= 0) {
            if (errno == EINTR && writecount < 0) {
                writecount = 0;
            } else {
                return -1;
            }
        }
        leftcount -= writecount;
        ptr += writecount;
        offset += writecount;
    }
    return leftcount == 0;
}

int CSSfsAdaptor::Append(fd_t fd, void* buf, int length) {
    return lfs_->Append(fd.fd_, buf, length);
}

int CSSfsAdaptor::Fallocate(fd_t fd, int op, uint64_t offset, int length) {
    return lfs_->Fallocate(fd.fd_, op, offset, length);
}

int CSSfsAdaptor::Fstat(fd_t fd, struct stat* info) {
    return lfs_->Fstat(fd.fd_, info);
}

int CSSfsAdaptor::Fsync(fd_t fd) {
    return lfs_->Fsync(fd.fd_);
}

int CSSfsAdaptor::Snapshot(const char* path, const char* snapPath) {
    return lfs_->Snapshot(path, snapPath);
}

bool CSSfsAdaptor::Rmdir(const char* path) {
    return lfs_->Rmdir(path);
}
}  // namespace chunkserver
}  // namespace curve
