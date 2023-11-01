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
 * File Created: Monday, 10th December 2018 9:54:45 am
 * Author: tongguangxun
 */

#include "src/chunkserver/datastore/file_pool.h"

#include <errno.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <json/json.h>
#include <linux/fs.h>

#include <algorithm>
#include <cctype>
#include <climits>
#include <memory>
#include <vector>

#include "src/common/configuration.h"
#include "src/common/crc32.h"
#include "src/common/curve_define.h"

using curve::common::kFilePoolMaigic;

namespace curve {
namespace chunkserver {
const char* FilePoolHelper::kFileSize = "chunkSize";
const char* FilePoolHelper::kMetaPageSize = "metaPageSize";
const char* FilePoolHelper::kFilePoolPath = "chunkfilepool_path";
const char* FilePoolHelper::kCRC = "crc";
const uint32_t FilePoolHelper::kPersistSize = 4096;

int FilePoolHelper::PersistEnCodeMetaInfo(
    std::shared_ptr<LocalFileSystem> fsptr, uint32_t chunkSize,
    uint32_t metaPageSize, const std::string& filePoolPath,
    const std::string& persistPath) {
    Json::Value root;
    root[kFileSize] = chunkSize;
    root[kMetaPageSize] = metaPageSize;
    root[kFilePoolPath] = filePoolPath;

    uint32_t crcsize = sizeof(kFilePoolMaigic) + sizeof(chunkSize) +
                       sizeof(metaPageSize) + filePoolPath.size();
    char* crcbuf = new char[crcsize];

    ::memcpy(crcbuf, kFilePoolMaigic, sizeof(kFilePoolMaigic));
    ::memcpy(crcbuf + sizeof(kFilePoolMaigic), &chunkSize, sizeof(uint32_t));
    ::memcpy(crcbuf + sizeof(uint32_t) + sizeof(kFilePoolMaigic), &metaPageSize,
             sizeof(uint32_t));
    ::memcpy(crcbuf + 2 * sizeof(uint32_t) + sizeof(kFilePoolMaigic),
             filePoolPath.c_str(), filePoolPath.size());
    uint32_t crc = ::curve::common::CRC32(crcbuf, crcsize);
    delete[] crcbuf;

    root[kCRC] = crc;

    int fd = fsptr->Open(persistPath.c_str(), O_RDWR | O_CREAT | O_SYNC);
    if (fd < 0) {
        LOG(ERROR) << "meta file open failed, " << persistPath.c_str();
        return -1;
    }

    LOG(INFO) << root.toStyledString().c_str();

    char* writeBuffer = new char[kPersistSize];
    memset(writeBuffer, 0, kPersistSize);
    memcpy(writeBuffer, root.toStyledString().c_str(),
           root.toStyledString().size());

    int ret = fsptr->Write(fd, writeBuffer, 0, kPersistSize);
    if (ret != kPersistSize) {
        LOG(ERROR) << "meta file write failed, " << persistPath.c_str()
                   << ", ret = " << ret;
        fsptr->Close(fd);
        delete[] writeBuffer;
        return -1;
    }

    fsptr->Close(fd);
    delete[] writeBuffer;
    return 0;
}

int FilePoolHelper::DecodeMetaInfoFromMetaFile(
    std::shared_ptr<LocalFileSystem> fsptr, const std::string& metaFilePath,
    uint32_t metaFileSize, uint32_t* chunksize, uint32_t* metapagesize,
    std::string* chunkfilePath) {
    int fd = fsptr->Open(metaFilePath, O_RDWR);
    if (fd < 0) {
        LOG(ERROR) << "meta file open failed, " << metaFilePath;
        return -1;
    }

    std::unique_ptr<char[]> readvalid(new char[metaFileSize]);
    memset(readvalid.get(), 0, metaFileSize);
    int ret = fsptr->Read(fd, readvalid.get(), 0, metaFileSize);
    if (ret != metaFileSize) {
        fsptr->Close(fd);
        LOG(ERROR) << "meta file read failed, " << metaFilePath;
        return -1;
    }

    fsptr->Close(fd);

    uint32_t crcvalue = 0;
    bool parse = false;
    do {
        Json::Reader reader;
        Json::Value value;
        if (!reader.parse(readvalid.get(), value)) {
            LOG(ERROR) << "chunkfile meta file got error!";
            break;
        }

        if (!value[kFileSize].isNull()) {
            *chunksize = value[kFileSize].asUInt();
        } else {
            LOG(ERROR) << "chunkfile meta file got error!"
                       << " no chunksize!";
            break;
        }

        if (!value[kMetaPageSize].isNull()) {
            *metapagesize = value[kMetaPageSize].asUInt();
        } else {
            LOG(ERROR) << "chunkfile meta file got error!"
                       << " no metaPageSize!";
            break;
        }

        if (!value[kFilePoolPath].isNull()) {
            *chunkfilePath = value[kFilePoolPath].asString();
        } else {
            LOG(ERROR) << "chunkfile meta file got error!"
                       << " no FilePool path!";
            break;
        }

        if (!value[kCRC].isNull()) {
            crcvalue = value[kCRC].asUInt();
        } else {
            LOG(ERROR) << "chunkfile meta file got error!"
                       << " no crc!";
            break;
        }

        parse = true;
    } while (false);

    if (!parse) {
        LOG(ERROR) << "parse meta file failed! " << metaFilePath;
        return -1;
    }

    uint32_t crcCheckSize =
        2 * sizeof(uint32_t) + sizeof(kFilePoolMaigic) + chunkfilePath->size();

    std::unique_ptr<char[]> crcCheckBuf(new char[crcCheckSize]);

    ::memcpy(crcCheckBuf.get(), kFilePoolMaigic,
             sizeof(kFilePoolMaigic));  //  NOLINT
    ::memcpy(crcCheckBuf.get() + sizeof(kFilePoolMaigic), chunksize,
             sizeof(uint32_t));
    ::memcpy(crcCheckBuf.get() + sizeof(uint32_t) +
                 sizeof(kFilePoolMaigic),  //  NOLINT
             metapagesize, sizeof(uint32_t));
    ::memcpy(crcCheckBuf.get() + 2 * sizeof(uint32_t) +
                 sizeof(kFilePoolMaigic),  //  NOLINT
             chunkfilePath->c_str(), chunkfilePath->size());
    uint32_t crcCalc = ::curve::common::CRC32(crcCheckBuf.get(), crcCheckSize);

    if (crcvalue != crcCalc) {
        LOG(ERROR) << "crc check failed!";
        return -1;
    }

    return 0;
}

FilePool::FilePool(std::shared_ptr<LocalFileSystem> fsptr)
    : currentmaxfilenum_(0) {
    CHECK(fsptr != nullptr) << "fs ptr allocate failed!";
    fsptr_ = fsptr;
    tmpChunkvec_.clear();
}

bool FilePool::Initialize(const FilePoolOptions& cfopt) {
    poolOpt_ = cfopt;
    if (poolOpt_.getFileFromPool) {
        if (!CheckValid()) {
            LOG(ERROR) << "check valid failed!";
            return false;
        }
        if (fsptr_->DirExists(currentdir_.c_str())) {
            return ScanInternal();
        } else {
            LOG(ERROR) << "chunkfile pool not exists, inited failed!"
                       << " chunkfile pool path = " << currentdir_.c_str();
            return false;
        }
    } else {
        currentdir_ = poolOpt_.filePoolDir;
        if (!fsptr_->DirExists(currentdir_.c_str())) {
            return fsptr_->Mkdir(currentdir_.c_str()) == 0;
        }
    }
    return true;
}

bool FilePool::CheckValid() {
    uint32_t chunksize = 0;
    uint32_t metapagesize = 0;
    std::string filePath;

    int ret = FilePoolHelper::DecodeMetaInfoFromMetaFile(
        fsptr_, poolOpt_.metaPath, poolOpt_.metaFileSize, &chunksize,
        &metapagesize, &filePath);
    if (ret == -1) {
        LOG(ERROR) << "Decode meta info from meta file failed!";
        return false;
    }

    currentdir_ = filePath;
    currentState_.chunkSize = chunksize;
    currentState_.metaPageSize = metapagesize;
    return true;
}

int FilePool::GetFile(const std::string& targetpath, char* metapage) {
    int ret = -1;
    int retry = 0;

    while (retry < poolOpt_.retryTimes) {
        uint64_t chunkID;
        std::string srcpath;
        if (poolOpt_.getFileFromPool) {
            std::unique_lock<std::mutex> lk(mtx_);
            if (tmpChunkvec_.empty()) {
                LOG(ERROR) << "no avaliable chunk!";
                break;
            }
            chunkID = tmpChunkvec_.back();
            srcpath = currentdir_ + "/" + std::to_string(chunkID);
            tmpChunkvec_.pop_back();
            --currentState_.preallocatedChunksLeft;
        } else {
            srcpath = currentdir_ + "/" +
                      std::to_string(currentmaxfilenum_.fetch_add(1));
            int r = AllocateChunk(srcpath);
            if (r < 0) {
                LOG(ERROR) << "file allocate failed, " << srcpath.c_str();
                retry++;
                continue;
            }
        }

        bool rc = WriteMetaPage(srcpath, metapage);
        if (rc) {
            // Here, the RENAME_NOREPLACE mode is used to rename the file.
            // When the target file exists, it is not allowed to be overwritten.
            // That is to say, creating a file through FilePool needs to ensure
            // that the target file does not exist. Datastore may have scenarios
            // where files are created concurrently. Rename is used to ensure
            // the atomicity of file creation, and to ensure that existing files
            // will not be overwritten.
            ret = fsptr_->Rename(srcpath.c_str(), targetpath.c_str(),
                                 RENAME_NOREPLACE);
            // The target file already exists, exit the current logic directly,
            // and delete the written file
            if (ret == -EEXIST) {
                LOG(ERROR) << targetpath
                           << ", already exists! src path = " << srcpath;
                break;
            } else if (ret < 0) {
                LOG(ERROR) << "file rename failed, " << srcpath.c_str();
            } else {
                LOG(INFO) << "rename file " << srcpath 
                          << " to " << targetpath
                          << " success! now pool size = "
                          << tmpChunkvec_.size();
                break;
            }
        } else {
            LOG(ERROR) << "write metapage failed, " << srcpath.c_str();
        }
        retry++;
    }
    return ret;
}

int FilePool::AllocateChunk(const std::string& chunkpath) {
    uint64_t chunklen = poolOpt_.fileSize + poolOpt_.metaPageSize;

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
    if (ret != 0) {
        LOG(ERROR) << "close failed, " << chunkpath.c_str();
    }
    return ret;
}

bool FilePool::WriteMetaPage(const std::string& sourcepath, char* page) {
    int fd = -1;
    int ret = -1;

    ret = fsptr_->Open(sourcepath.c_str(), O_RDWR);
    if (ret < 0) {
        LOG(ERROR) << "file open failed, " << sourcepath.c_str();
        return false;
    }

    fd = ret;

    ret = fsptr_->Write(fd, page, 0, poolOpt_.metaPageSize);
    if (ret != poolOpt_.metaPageSize) {
        fsptr_->Close(fd);
        LOG(ERROR) << "write metapage failed, " << sourcepath.c_str();
        return false;
    }

    ret = fsptr_->Fsync(fd);
    if (ret != 0) {
        fsptr_->Close(fd);
        LOG(ERROR) << "fsync metapage failed, " << sourcepath.c_str();
        return false;
    }

    ret = fsptr_->Close(fd);
    if (ret != 0) {
        LOG(ERROR) << "close failed, " << sourcepath.c_str();
        return false;
    }
    return true;
}

int FilePool::RecycleFile(const std::string& chunkpath) {
    if (!poolOpt_.getFileFromPool) {
        int ret = fsptr_->Delete(chunkpath.c_str());
        if (ret < 0) {
            LOG(ERROR) << "Recycle chunk failed!";
            return -1;
        }
    } else {
        // Check whether the size of the file to be recovered meets the
        // requirements, and delete it if it does not
        uint64_t chunklen = poolOpt_.fileSize + poolOpt_.metaPageSize;
        int fd = fsptr_->Open(chunkpath.c_str(), O_RDWR);
        if (fd < 0) {
            LOG(ERROR) << "file open failed! delete file dirctly"
                       << ", filename = " << chunkpath.c_str();
            return fsptr_->Delete(chunkpath.c_str());
        }

        struct stat info;
        int ret = fsptr_->Fstat(fd, &info);
        if (ret != 0) {
            LOG(ERROR) << "Fstat file " << chunkpath.c_str()
                       << "failed, ret = " << ret << ", delete file dirctly";
            fsptr_->Close(fd);
            return fsptr_->Delete(chunkpath.c_str());
        }

        if (info.st_size != chunklen) {
            LOG(ERROR) << "file size illegal, " << chunkpath.c_str()
                       << ", delete file dirctly"
                       << ", standard size = " << chunklen
                       << ", current file size = " << info.st_size;
            fsptr_->Close(fd);
            return fsptr_->Delete(chunkpath.c_str());
        }

        fsptr_->Close(fd);

        uint64_t newfilenum = 0;
        std::string newfilename;
        {
            std::unique_lock<std::mutex> lk(mtx_);
            currentmaxfilenum_.fetch_add(1);
            newfilenum = currentmaxfilenum_.load();
            newfilename = std::to_string(newfilenum);
        }
        std::string targetpath = currentdir_ + "/" + newfilename;

        ret = fsptr_->Rename(chunkpath.c_str(), targetpath.c_str());
        if (ret < 0) {
            LOG(ERROR) << "file rename failed, " << chunkpath.c_str();
            return -1;
        } else {
            LOG(INFO) << "Recycle " << chunkpath.c_str() << ", success!"
                      << ", now chunkpool size = " << tmpChunkvec_.size() + 1;
        }
        std::unique_lock<std::mutex> lk(mtx_);
        tmpChunkvec_.push_back(newfilenum);
        ++currentState_.preallocatedChunksLeft;
    }
    return 0;
}

void FilePool::UnInitialize() {
    currentdir_ = "";

    std::unique_lock<std::mutex> lk(mtx_);
    tmpChunkvec_.clear();
}

bool FilePool::ScanInternal() {
    uint64_t maxnum = 0;
    std::vector<std::string> tmpvec;
    LOG(INFO) << "scan dir" << currentdir_;
    int ret = fsptr_->List(currentdir_.c_str(), &tmpvec);
    if (ret < 0) {
        LOG(ERROR) << "list file pool dir failed!";
        return false;
    } else {
        LOG(INFO) << "list file pool dir done, size = " << tmpvec.size();
    }

    uint64_t chunklen = poolOpt_.fileSize + poolOpt_.metaPageSize;
    for (auto& iter : tmpvec) {
        auto it = std::find_if(iter.begin(), iter.end(), [](unsigned char c) {
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

        if (ret != 0 || info.st_size != chunklen) {
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

    LOG(INFO) << "scan done, pool size = " << tmpChunkvec_.size();
    return true;
}

size_t FilePool::Size() {
    std::unique_lock<std::mutex> lk(mtx_);
    return tmpChunkvec_.size();
}

FilePoolState_t FilePool::GetState() {
    return currentState_;
}

}  // namespace chunkserver
}  // namespace curve
