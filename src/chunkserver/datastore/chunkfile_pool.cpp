/*
 * Project: curve
 * File Created: Monday, 10th December 2018 9:54:45 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <glog/logging.h>
#include <fcntl.h>
#include <errno.h>
#include <cctype>

#include <algorithm>
#include <climits>
#include <vector>

#include "src/common/configuration.h"
#include "src/chunkserver/datastore/chunkfile_pool.h"

// TODO(tongguangxun) : 加chunkfile pool开关
namespace curve {
namespace chunkserver {
ChunkfilePool::ChunkfilePool(std::shared_ptr<LocalFileSystem> fsptr):
                             currentmaxfilenum_(0) {
    CHECK(fsptr != nullptr) << "fs ptr allocate failed!";
    fsptr_ = fsptr;
    tmpChunkvec_.clear();
}

bool ChunkfilePool::Initialize(const ChunkfilePoolOptions& cfopt) {
    chunkPoolOpt_ = cfopt;
    if (!CheckValid()) {
        LOG(ERROR) << "check valid failed!";
        return false;
    }
    if (fsptr_->DirExists(currentdir_.c_str())) {
        return ScanInternal();
    }
    LOG(ERROR) << "chunkfile pool not exists, inited failed!";
    return false;
}

// meta file 格式如下：
// |<-- uint32_t -->|<-- uint32_t -->|<-- uint32_t -->|<-- path size ..->|
// |  chunksize     |   metapagesize | pre allocatepercent | chunfilepool path |
// |<----------------------------- 4096 Bytes -------------------------------->|
bool ChunkfilePool::CheckValid() {
    int fd = fsptr_->Open(chunkPoolOpt_.metapath, O_RDWR);
    if (fd < 0) {
        LOG(ERROR) << "meta file open failed, " << chunkPoolOpt_.metapath;
        return false;
    }
    char readvalid[chunkPoolOpt_.cpmetafilesize] = {0};
    int ret = fsptr_->Read(fd, readvalid, 0, chunkPoolOpt_.cpmetafilesize);
    if (ret != chunkPoolOpt_.cpmetafilesize) {
        fsptr_->Close(fd);
        LOG(ERROR) << "meta file read failed, " << chunkPoolOpt_.metapath;
        return false;
    }

    uint32_t chunksize = 0;
    uint32_t metapagesize = 0;
    uint32_t allocateper = 0;
    char path[256];

    memcpy(&chunksize, readvalid + 0, sizeof(uint32_t));
    memcpy(&metapagesize, readvalid + sizeof(uint32_t) , sizeof(uint32_t));
    memcpy(&allocateper, readvalid + 2*sizeof(uint32_t), sizeof(uint32_t));
    memcpy(path, readvalid + 3*sizeof(uint32_t), 256);

    bool valid = false;
    do {
        if (chunksize != chunkPoolOpt_.chunksize) {
            LOG(ERROR) << "chunksize meta info wrong!";
            break;
        }
        if (metapagesize != chunkPoolOpt_.metapagesize) {
            LOG(ERROR) << "metapagesize meta info wrong!";
            break;
        }
        currentdir_ = path;

        valid = true;
    } while (0);

    fsptr_->Close(fd);
    return valid;
}

int ChunkfilePool::GetChunk(const std::string& targetpath, char* metapage) {
    int ret = -1;
    int retry = 0;
    while (retry < chunkPoolOpt_.retrytimes) {
        std::string srcpath;
        {
            std::unique_lock<std::mutex> lk(mtx_);
            if (tmpChunkvec_.empty()) {
                LOG(ERROR) << "no avaliable chunk!";
                break;
            }
            srcpath = currentdir_ + "/" + std::to_string(tmpChunkvec_.back());
            tmpChunkvec_.pop_back();
        }
        ret = WriteMetaPage(srcpath, metapage);
        LOG(INFO) << "src path = " << srcpath.c_str()
                  << ", dist path = " << targetpath.c_str();
        if (ret == 0) {
            ret = fsptr_->Rename(srcpath.c_str(), targetpath.c_str());
        }
        if (ret < 0) {
            LOG(ERROR) << "file rename failed, " << srcpath.c_str();
            RecycleChunk(srcpath);
        } else {
            LOG(INFO) << "get chunk success!";
            break;
        }
        retry++;
    }
    return ret;
}

int ChunkfilePool::WriteMetaPage(const std::string& sourcepath, char* page) {
    int fd = -1;
    int ret = -1;
    do {
        ret = fsptr_->Open(sourcepath.c_str(), O_RDWR);
        if (ret < 0) {
            LOG(ERROR) << "file open failed, " << sourcepath.c_str();
            break;
        }
        fd = ret;

        ret = fsptr_->Write(fd, page, 0, chunkPoolOpt_.metapagesize);
        if (ret != chunkPoolOpt_.metapagesize) {
            LOG(ERROR) << "write failed, " << sourcepath.c_str();
            break;
        }

        ret = fsptr_->Fsync(fd);
        if (ret < 0) {
            LOG(ERROR) << "fsync failed, " << sourcepath.c_str();
            break;
        }
    } while (0);

    ret = fsptr_->Close(fd);
    return ret;
}

int ChunkfilePool::RecycleChunk(const std::string& chunkpath) {
    uint64_t newfilenum = 0;
    std::string newfilename;
    {
        std::unique_lock<std::mutex> lk(mtx_);
        currentmaxfilenum_.fetch_add(1);
        newfilenum = currentmaxfilenum_.load();
        newfilename = std::to_string(newfilenum);
    }
    std::string targetpath = currentdir_ + "/" + newfilename;

    int ret = fsptr_->Rename(chunkpath.c_str(), targetpath.c_str());
    if (ret < 0) {
        LOG(ERROR) << "file rename failed, " << chunkpath.c_str();
        return -1;
    }
    std::unique_lock<std::mutex> lk(mtx_);
    tmpChunkvec_.push_back(newfilenum);
    return 0;
}

void ChunkfilePool::UnInitialize() {
    fsptr_              = nullptr;
    currentdir_         = "";

    std::unique_lock<std::mutex> lk(mtx_);
    tmpChunkvec_.clear();
}

bool ChunkfilePool::ScanInternal() {
    uint64_t maxnum = 0;
    std::vector<std::string> tmpvec;
    int ret = fsptr_->List(currentdir_.c_str(), &tmpvec);
    if (ret < 0) {
        LOG(ERROR) << "list chunkfile pool dir failed!";
        return false;
    }

    uint64_t chunklen = chunkPoolOpt_.chunksize + chunkPoolOpt_.metapagesize;
    for (auto& iter : tmpvec) {
        auto it =
            std::find_if(iter.begin(), iter.end(), [](unsigned char c) {
            return !std::isdigit(c);
        });
        if (it != iter.end()) {
            LOG(ERROR) << "file name illegal! [" << iter << "]";
            return false;
        }

        std::string filepath = currentdir_ + "/" + iter;
        if (!fsptr_->FileExists(filepath)) {
            LOG(ERROR) << "chunkfile pool dir has subdir! " << filepath.c_str();
            return false;
        }
        int fd = fsptr_->Open(filepath.c_str(), O_RDWR);
        if (fd < 0) {
            LOG(ERROR) << "file open failed!";
            return false;
        }
        struct stat info;
        int ret = fsptr_->Fstat(fd, &info);

        if (ret < 0 || info.st_size != chunklen) {
            LOG(ERROR) << "file size illegal, " << filepath.c_str();
            return false;
        }

        uint64_t filenum = atoll(iter.c_str());
        if (filenum != 0) {
            tmpChunkvec_.push_back(filenum);
            if (filenum > maxnum) {
                maxnum = filenum;
            }
        }
    }

    std::unique_lock<std::mutex> lk(mtx_);
    currentmaxfilenum_.store(maxnum + 1);
    return true;
}

size_t ChunkfilePool::Size() {
    std::unique_lock<std::mutex> lk(mtx_);
    return tmpChunkvec_.size();
}

}   // namespace chunkserver
}   // namespace curve
