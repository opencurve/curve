/*
 * Project: curve
 * File Created: Monday, 10th December 2018 9:54:45 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <glog/logging.h>
#include <json/json.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <errno.h>
#include <cctype>

#include <algorithm>
#include <climits>
#include <vector>

#include "src/common/crc32.h"
#include "src/common/configuration.h"
#include "src/common/curve_define.h"
#include "src/chunkserver/datastore/chunkfile_pool.h"

using curve::common::kChunkFilePoolMaigic;

namespace curve {
namespace chunkserver {
const char* ChunkfilePoolHelper::kChunkSize = "chunkSize";
const char* ChunkfilePoolHelper::kMetaPageSize = "metaPageSize";
const char* ChunkfilePoolHelper::kChunkFilePoolPath = "chunkfilepool_path";
const char* ChunkfilePoolHelper::kCRC = "crc";
const uint32_t ChunkfilePoolHelper::kPersistSize = 4096;

int ChunkfilePoolHelper::PersistEnCodeMetaInfo(
                                    std::shared_ptr<LocalFileSystem> fsptr,
                                    uint32_t chunkSize,
                                    uint32_t metaPageSize,
                                    const std::string& chunkfilepool_path,
                                    const std::string& persistPath) {
    Json::Value root;
    root[kChunkSize] = chunkSize;
    root[kMetaPageSize] = metaPageSize;
    root[kChunkFilePoolPath] = chunkfilepool_path;

    uint32_t crcsize = sizeof(kChunkFilePoolMaigic) +
                       sizeof(chunkSize) +
                       sizeof(metaPageSize) +
                       chunkfilepool_path.size();
    char* crcbuf = new char[crcsize];

    ::memcpy(crcbuf, kChunkFilePoolMaigic,
             sizeof(kChunkFilePoolMaigic));
    ::memcpy(crcbuf + sizeof(kChunkFilePoolMaigic),
             &chunkSize, sizeof(uint32_t));
    ::memcpy(crcbuf + sizeof(uint32_t) + sizeof(kChunkFilePoolMaigic),
             &metaPageSize, sizeof(uint32_t));
    ::memcpy(crcbuf + 2*sizeof(uint32_t) + sizeof(kChunkFilePoolMaigic),
             chunkfilepool_path.c_str(),
             chunkfilepool_path.size());
    uint32_t crc = ::curve::common::CRC32(crcbuf, crcsize);
    delete[] crcbuf;

    root[kCRC] = crc;

    int fd = fsptr->Open(persistPath.c_str(), O_RDWR|O_CREAT|O_SYNC);
    if (fd < 0) {
        LOG(ERROR) << "meta file open failed, "
                   << persistPath.c_str();
        return -1;
    }

    LOG(INFO) << root.toStyledString().c_str();

    char* writeBuffer = new char[kPersistSize];
    memset(writeBuffer, 0, kPersistSize);
    memcpy(writeBuffer, root.toStyledString().c_str(),
           root.toStyledString().size());

    int ret = fsptr->Write(fd, writeBuffer, 0, kPersistSize);
    if (ret != kPersistSize) {
        LOG(ERROR) << "meta file write failed, "
                   << persistPath.c_str()
                   << ", ret = " << ret;
        delete[] writeBuffer;
        return -1;
    }

    fsptr->Close(fd);
    delete[] writeBuffer;
    return 0;
}

int ChunkfilePoolHelper::DecodeMetaInfoFromMetaFile(
                                    std::shared_ptr<LocalFileSystem> fsptr,
                                    const std::string& metaFilePath,
                                    uint32_t metaFileSize,
                                    uint32_t* chunksize,
                                    uint32_t* metapagesize,
                                    std::string* chunkfilePath) {
    int fd = fsptr->Open(metaFilePath, O_RDWR);
    if (fd < 0) {
        LOG(ERROR) << "meta file open failed, " << metaFilePath;
        return -1;
    }
    char* readvalid = new char[metaFileSize];
    memset(readvalid, 0, metaFileSize);
    int ret = fsptr->Read(fd, readvalid, 0, metaFileSize);
    if (ret != metaFileSize) {
        fsptr->Close(fd);
        LOG(ERROR) << "meta file read failed, " << metaFilePath;
        return -1;
    }

    fsptr->Close(fd);

    uint32_t crcvalue = 0;

    Json::Reader reader;
    Json::Value value;
    if (!reader.parse(readvalid, value)) {
        LOG(ERROR) << "chunkfile meta file got error!";
        return -1;
    }

    if (!value[kChunkSize].isNull()) {
        *chunksize = value[kChunkSize].asUInt();
    } else {
        LOG(ERROR) << "chunkfile meta file got error!"
                   << " no chunksize!";
        return -1;
    }

    if (!value[kMetaPageSize].isNull()) {
        *metapagesize = value[kMetaPageSize].asUInt();
    } else {
        LOG(ERROR) << "chunkfile meta file got error!"
                   << " no metaPageSize!";
        return -1;
    }

    if (!value[kChunkFilePoolPath].isNull()) {
        *chunkfilePath = value[kChunkFilePoolPath].asString();
    } else {
        LOG(ERROR) << "chunkfile meta file got error!"
                   << " no chunkfilepool path!";
        return -1;
    }

    if (!value[kCRC].isNull()) {
        crcvalue = value[kCRC].asUInt();
    } else {
        LOG(ERROR) << "chunkfile meta file got error!"
                   << " no crc!";
        return -1;
    }

    uint32_t crcCheckSize = 2*sizeof(uint32_t) +
                            sizeof(kChunkFilePoolMaigic) +
                            chunkfilePath->size();
    char* crcCheckBuf = new char[crcCheckSize];

    ::memcpy(crcCheckBuf, kChunkFilePoolMaigic, sizeof(kChunkFilePoolMaigic));
    ::memcpy(crcCheckBuf + sizeof(kChunkFilePoolMaigic),
             chunksize, sizeof(uint32_t));
    ::memcpy(crcCheckBuf + sizeof(uint32_t) + sizeof(kChunkFilePoolMaigic),
             metapagesize, sizeof(uint32_t));
    ::memcpy(crcCheckBuf + 2*sizeof(uint32_t) + sizeof(kChunkFilePoolMaigic),
             chunkfilePath->c_str(),
             chunkfilePath->size());
    uint32_t crcCalc = ::curve::common::CRC32(crcCheckBuf, crcCheckSize);

    if (crcvalue != crcCalc) {
        LOG(ERROR) << "crc check failed!";
        return -1;
    }

    return 0;
}

ChunkfilePool::ChunkfilePool(std::shared_ptr<LocalFileSystem> fsptr):
                             currentmaxfilenum_(0) {
    CHECK(fsptr != nullptr) << "fs ptr allocate failed!";
    fsptr_ = fsptr;
    tmpChunkvec_.clear();
}

bool ChunkfilePool::Initialize(const ChunkfilePoolOptions& cfopt) {
    chunkPoolOpt_ = cfopt;
    if (chunkPoolOpt_.getChunkFromPool) {
        if (!CheckValid()) {
            LOG(ERROR) << "check valid failed!";
            return false;
        }
        if (fsptr_->DirExists(currentdir_.c_str())) {
            return ScanInternal();
        }
        LOG(ERROR) << "chunkfile pool not exists, inited failed!"
                   << " chunkfile pool path = " << currentdir_.c_str();
    } else {
        currentdir_ = chunkPoolOpt_.chunkFilePoolDir;
        if (!fsptr_->DirExists(currentdir_.c_str())) {
            return fsptr_->Mkdir(currentdir_.c_str()) == 0;
        }
    }
    return true;
}

bool ChunkfilePool::CheckValid() {
    uint32_t chunksize = 0;
    uint32_t metapagesize = 0;
    std::string chunkfilePath;

    int ret = ChunkfilePoolHelper::DecodeMetaInfoFromMetaFile(fsptr_,
                                                chunkPoolOpt_.metaPath,
                                                chunkPoolOpt_.cpMetaFileSize,
                                                &chunksize,
                                                &metapagesize,
                                                &chunkfilePath);
    if (ret == -1) {
        LOG(ERROR) << "Decode meta info from meta file failed!";
        return false;
    }

    currentdir_ = chunkfilePath;
    currentState_.chunkSize = chunksize;
    currentState_.metaPageSize = metapagesize;
    return true;
}

int ChunkfilePool::GetChunk(const std::string& targetpath, char* metapage) {
    int ret = -1;
    int retry = 0;

    while (retry < chunkPoolOpt_.retryTimes) {
        std::string srcpath;
        if (chunkPoolOpt_.getChunkFromPool) {
            std::unique_lock<std::mutex> lk(mtx_);
            if (tmpChunkvec_.empty()) {
                LOG(ERROR) << "no avaliable chunk!";
                break;
            }
            srcpath = currentdir_ + "/" + std::to_string(tmpChunkvec_.back());
            tmpChunkvec_.pop_back();
            --currentState_.preallocatedChunksLeft;
        } else {
            currentmaxfilenum_.fetch_add(1);
            srcpath = currentdir_ + "/" + std::to_string(currentmaxfilenum_);
            int r = AllocateChunk(srcpath);
            if (r < 0) {
                LOG(ERROR) << "file allocate failed, " << srcpath.c_str();
                retry++;
                continue;
            }
        }

        ret = WriteMetaPage(srcpath, metapage);
        LOG(INFO) << "src path = " << srcpath.c_str()
                  << ", dist path = " << targetpath.c_str();
        if (ret == 0) {
            // 这里使用RENAME_NOREPLACE模式来rename文件
            // 当目标文件存在时，不允许被覆盖
            // 也就是说通过chunkfilepool创建文件需要保证目标文件不存在
            // datastore可能存在并发创建文件的场景
            // 通过rename一来保证文件创建的原子性，二来保证不会覆盖已有文件
            ret = fsptr_->Rename(srcpath.c_str(),
                                 targetpath.c_str(),
                                 RENAME_NOREPLACE);
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

int ChunkfilePool::AllocateChunk(const std::string& chunkpath) {
    uint64_t chunklen = chunkPoolOpt_.chunkSize + chunkPoolOpt_.metaPageSize;

    int ret = fsptr_->Open(chunkpath.c_str(), O_RDWR | O_CREAT);
    if (ret < 0) {
        LOG(ERROR) << "file open failed, " << chunkpath.c_str();
        return -1;
    }
    int fd = ret;

    ret = fsptr_->Fallocate(fd, 0, 0, chunklen);
    if (ret < 0) {
        fsptr_->Close(fd);
        LOG(ERROR) << "Fallocate failed, " << chunkpath.c_str();
        return -1;
    }

    char* data = new (std::nothrow) char[chunklen];
    memset(data, 0, chunklen);

    ret = fsptr_->Write(fd, data, 0, chunklen);
    if (ret < 0) {
        fsptr_->Close(fd);
        delete[] data;
        LOG(ERROR) << "write failed, " << chunkpath.c_str();
        return -1;
    }
    delete[] data;

    ret = fsptr_->Fsync(fd);
    if (ret < 0) {
        fsptr_->Close(fd);
        LOG(ERROR) << "fsync failed, " << chunkpath.c_str();
        return -1;
    }

    ret = fsptr_->Close(fd);
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

        ret = fsptr_->Write(fd, page, 0, chunkPoolOpt_.metaPageSize);
        if (ret != chunkPoolOpt_.metaPageSize) {
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
    if (!chunkPoolOpt_.getChunkFromPool) {
        int ret = fsptr_->Delete(chunkpath.c_str());
        if (ret < 0) {
            LOG(ERROR) << "Recycle chunk failed!";
            return -1;
        }
    } else {
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
        ++currentState_.preallocatedChunksLeft;
    }
    return 0;
}

void ChunkfilePool::UnInitialize() {
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

    uint64_t chunklen = chunkPoolOpt_.chunkSize + chunkPoolOpt_.metaPageSize;
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
            LOG(ERROR) << "file size illegal, " << filepath.c_str()
                       << ", standard size = " << chunklen
                       << ", current size = " << info.st_size;
            fsptr_->Close(fd);
            return false;
        }

        fsptr_->Close(fd);

        uint64_t filenum = atoll(iter.c_str());
        if (filenum != 0) {
            tmpChunkvec_.push_back(filenum);
            if (filenum > maxnum) {
                maxnum = filenum;
            }
        }
    }

    currentState_.preallocatedChunksLeft = tmpvec.size();

    std::unique_lock<std::mutex> lk(mtx_);
    currentmaxfilenum_.store(maxnum + 1);
    return true;
}

size_t ChunkfilePool::Size() {
    std::unique_lock<std::mutex> lk(mtx_);
    return tmpChunkvec_.size();
}

ChunkFilePoolState_t ChunkfilePool::GetState() {
    return currentState_;
}

}   // namespace chunkserver
}   // namespace curve
