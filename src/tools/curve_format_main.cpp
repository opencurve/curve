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
 * File Created: Monday, 28th January 2019 2:59:53 pm
 * Author: tongguangxun
 */

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <json/json.h>

#include <fcntl.h>

#include <set>
#include <mutex>    // NOLINT
#include <thread>   // NOLINT
#include <atomic>
#include <vector>

#include "src/fs/fs_common.h"
#include "src/fs/local_filesystem.h"
#include "src/common/crc32.h"
#include "src/common/bitmap.h"
#include "src/common/curve_define.h"
#include "src/chunkserver/datastore/file_pool.h"
#include "src/common/fast_align.h"

using ::curve::common::is_aligned;

/**
 * chunkfile pool预分配工具，提供两种分配方式
 * 1. 以磁盘空间百分比方式，指定需要分配的百分比
 * 2. 指定以chunk数量分配
 * 默认的分配方式是以磁盘空间百分比作为分配方式，可以通过-allocateByPercent=false/true
 * 调整分配方式。
 */
DEFINE_bool(allocateByPercent,
            true,
            "allocate filePool by percent of disk size or by chunk num!");

DEFINE_uint32(fileSize,
              16 * 1024 * 1024,
              "chunk size");

DEFINE_uint32(metaPageSize,
              4 * 1024,
              "metapage size for every chunk");

static bool ValidateMetaPageSize(const char* /*name*/, uint32_t metaPageSize) {
    return is_aligned(metaPageSize, 4096);
}

DEFINE_validator(metaPageSize, &ValidateMetaPageSize);

DEFINE_uint32(blockSize, 4096, "minimum io alignment supported");

DEFINE_string(fileSystemPath,
              "./",
              "chunkserver disk path");

DEFINE_string(filePoolDir,
              "./filePool/",
              "chunkfile pool dir");

DEFINE_string(filePoolMetaPath,
              "./filePool.meta",
              "chunkfile pool meta info file path.");

// preallocateNum仅在测试的时候使用，测试提前预分配固定数量的chunk
// 当设置这个值的时候可以不用设置allocatepercent
DEFINE_uint32(preAllocateNum,
              0,
              "preallocate chunk nums, this is JUST for curve test");

// 在系统初始化的时候，管理员需要预先格式化磁盘，并进行预分配
// 这时候只需要指定allocatepercent，allocatepercent是占整个盘的空间的百分比
DEFINE_uint32(allocatePercent,
              80,
              "preallocate storage percent of total disk");

// 测试情况下置为false，加快测试速度
DEFINE_bool(needWriteZero,
        true,
        "not write zero for test.");

using curve::fs::FileSystemType;
using curve::fs::LocalFsFactory;
using curve::fs::FileSystemInfo;
using curve::fs::LocalFileSystem;
using curve::common::kFilePoolMagic;
using curve::chunkserver::FilePoolMeta;

class CompareInternal {
 public:
    bool operator()(std::string s1, std::string s2) {
        auto index1 = std::atoi(s1.c_str());
        auto index2 = std::atoi(s2.c_str());
        return index1 < index2;
    }
};

struct AllocateStruct {
    std::shared_ptr<LocalFileSystem> fsptr;
    std::atomic<uint64_t>* allocateChunknum;
    bool* checkwrong;
    std::mutex* mtx;
    uint64_t chunknum;
    std::string cleanChunkSuffix;
};

int AllocateFiles(AllocateStruct* allocatestruct) {
    char* data = new(std::nothrow)char[FLAGS_fileSize + FLAGS_metaPageSize];
    memset(data, 0, FLAGS_fileSize + FLAGS_metaPageSize);

    uint64_t count = 0;
    while (count < allocatestruct->chunknum) {
        std::string filename;
        {
            std::unique_lock<std::mutex> lk(*allocatestruct->mtx);
            allocatestruct->allocateChunknum->fetch_add(1);
            filename = std::to_string(
                            allocatestruct->allocateChunknum->load());
        }
        std::string tmpchunkfilepath = FLAGS_filePoolDir + "/"
            + filename + allocatestruct->cleanChunkSuffix;

        int ret = allocatestruct->fsptr->Open(tmpchunkfilepath,
                                             O_RDWR | O_CREAT);
        if (ret < 0) {
            *allocatestruct->checkwrong = true;
            LOG(ERROR) << "file open failed, " << tmpchunkfilepath;
            break;
        }
        int fd = ret;

        ret = allocatestruct->fsptr->Fallocate(fd, 0, 0,
                                        FLAGS_fileSize + FLAGS_metaPageSize);
        if (ret < 0) {
            allocatestruct->fsptr->Close(fd);
            *allocatestruct->checkwrong = true;
            LOG(ERROR) << "Fallocate failed, " << tmpchunkfilepath;
            break;
        }

        if (FLAGS_needWriteZero) {
            ret = allocatestruct->fsptr->Write(fd, data, 0,
                FLAGS_fileSize + FLAGS_metaPageSize);
            if (ret < 0) {
                allocatestruct->fsptr->Close(fd);
                *allocatestruct->checkwrong = true;
                LOG(ERROR) << "write failed, " << tmpchunkfilepath;
                break;
            }
        }

        ret = allocatestruct->fsptr->Fsync(fd);
        if (ret < 0) {
            allocatestruct->fsptr->Close(fd);
            *allocatestruct->checkwrong = true;
            LOG(ERROR) << "fsync failed, " << tmpchunkfilepath;
            break;
        }

        allocatestruct->fsptr->Close(fd);
        if (ret < 0) {
            *allocatestruct->checkwrong = true;
            LOG(ERROR) << "close failed, " << tmpchunkfilepath;
            break;
        }
        count++;
    }
    delete[] data;
    return *allocatestruct->checkwrong == true ? 0 : -1;
}

// Minimum extra meta size except bitmap.
// See src/chunkserver/datastore/chunkserver_chunkfile.h, for normal/clone chunk
// metapage usage.
// See src/chunkserver/datastore/chunkserver_snapshot.h, for snapshot chunk
// metapage usage. We hard code it for compatibility.
// TODO(all): remove this hard code value, it's should calculated by actual
// usage.
static constexpr uint32_t kMinimumExtraMetaSize = 4096 - 512;

// for clone chunk and snapshot chunk, we have bitmap to identify whether
// corresponding block is written or not,
// so, check whether meta page size is enough for store bitmap
// and other metadata
static bool CheckChunkAndMetaPageSize() {
    if (!is_aligned(FLAGS_fileSize, FLAGS_blockSize)) {
        LOG(ERROR) << "io alignment or file size is wrong";
        return false;
    }

    auto bitmapBytes =
        FLAGS_fileSize / FLAGS_blockSize / curve::common::BITMAP_UNIT_SIZE;
    if (bitmapBytes + kMinimumExtraMetaSize > FLAGS_metaPageSize) {
        LOG(ERROR) << "meta page size is too small";
        return false;
    }

    return true;
}

// TODO(tongguangxun) :添加单元测试
int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, false);
    google::InitGoogleLogging(argv[0]);

    // load current chunkfile pool
    std::mutex mtx;
    std::shared_ptr<LocalFileSystem> fsptr = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");   // NOLINT
    std::set<std::string, CompareInternal> tmpChunkSet_;
    std::atomic<uint64_t> allocateChunknum_(0);
    std::vector<std::string> tmpvec;

    if (!CheckChunkAndMetaPageSize()) {
        return -1;
    }

    if (fsptr->Mkdir(FLAGS_filePoolDir) < 0) {
        LOG(ERROR) << "mkdir failed!, " << FLAGS_filePoolDir;
        return -1;
    }
    if (fsptr->List(FLAGS_filePoolDir, &tmpvec) < 0) {
        LOG(ERROR) << "list dir failed!, " << FLAGS_filePoolDir;
        return -1;
    }

    tmpChunkSet_.insert(tmpvec.begin(), tmpvec.end());
    uint64_t size = tmpChunkSet_.size() ? atoi((*(--tmpChunkSet_.end())).c_str()) : 0;          // NOLINT
    allocateChunknum_.store(size + 1);

    FileSystemInfo finfo;
    int r = fsptr->Statfs(FLAGS_fileSystemPath, &finfo);
    if (r != 0) {
        LOG(ERROR) << "get disk usage info failed!";
        return -1;
    }

    uint64_t freepercent = finfo.available * 100 / finfo.total;
    LOG(INFO) << "free space = " << finfo.available
              << ", total space = " << finfo.total
              << ", freepercent = " << freepercent;

    if (freepercent < FLAGS_allocatePercent && FLAGS_allocateByPercent) {
        LOG(ERROR) << "disk free space not enough.";
        return 0;
    }

    uint64_t preAllocateChunkNum = 0;
    uint64_t preAllocateSize = FLAGS_allocatePercent * finfo.total / 100;

    if (FLAGS_allocateByPercent) {
        preAllocateChunkNum = preAllocateSize
                              / (FLAGS_fileSize + FLAGS_metaPageSize);
    } else {
        preAllocateChunkNum = FLAGS_preAllocateNum;
    }

    bool checkwrong = false;
    // two threads concurrent, can reach the bandwidth of disk.
    uint64_t threadAllocateNum = preAllocateChunkNum/2;
    std::vector<std::thread> thvec;
    AllocateStruct allocateStruct;
    allocateStruct.fsptr = fsptr;
    allocateStruct.allocateChunknum = &allocateChunknum_;
    allocateStruct.checkwrong = &checkwrong;
    allocateStruct.mtx = &mtx;
    allocateStruct.chunknum = threadAllocateNum;
    allocateStruct.cleanChunkSuffix =
        curve::chunkserver::FilePool::GetCleanChunkSuffix();

    thvec.push_back(std::thread(AllocateFiles, &allocateStruct));
    thvec.push_back(std::thread(AllocateFiles, &allocateStruct));

    for (auto& iter : thvec) {
        iter.join();
    }

    if (checkwrong) {
        LOG(ERROR) << "allocate got something wrong, please check.";
        return -1;
    }

    FilePoolMeta meta;
    meta.chunkSize = FLAGS_fileSize;
    meta.metaPageSize = FLAGS_metaPageSize;
    meta.hasBlockSize = true;
    meta.blockSize = FLAGS_blockSize;
    meta.filePoolPath = FLAGS_filePoolDir;
    int ret = curve::chunkserver::FilePoolHelper::PersistEnCodeMetaInfo(
        fsptr, meta, FLAGS_filePoolMetaPath);

    if (ret == -1) {
        LOG(ERROR) << "persist chunkfile pool meta info failed!";
        return -1;
    }

    // 读取meta文件，检查是否写入正确
    FilePoolMeta recordMeta;
    ret = curve::chunkserver::FilePoolHelper::DecodeMetaInfoFromMetaFile(
        fsptr, FLAGS_filePoolMetaPath, 4096, &recordMeta);
    if (ret == -1) {
        LOG(ERROR) << "chunkfile pool meta info file got something wrong!";
        fsptr->Delete(FLAGS_filePoolMetaPath);
        return -1;
    }

    bool valid = false;
    do {
        if (recordMeta.chunkSize != FLAGS_fileSize) {
            LOG(ERROR) << "chunksize meta info persistency wrong!";
            break;
        }

        if (recordMeta.metaPageSize != FLAGS_metaPageSize) {
            LOG(ERROR) << "metapagesize meta info persistency wrong!";
            break;
        }

        if (recordMeta.blockSize != FLAGS_blockSize) {
            LOG(ERROR) << "block size meta info persistency wrong!";
            break;
        }

        if (recordMeta.filePoolPath != FLAGS_filePoolDir) {
            LOG(ERROR) << "meta info persistency failed!"
                    << ", read chunkpath = " << recordMeta.filePoolPath
                    << ", real chunkpath = " << FLAGS_filePoolDir;
            break;
        }

        valid = true;
    } while (0);

    if (!valid) {
        LOG(ERROR) << "meta file has something wrong, already deleted!";
        return -1;
    }

    return 0;
}
