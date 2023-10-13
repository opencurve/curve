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

#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <json/json.h>

#include <atomic>
#include <mutex>  // NOLINT
#include <set>
#include <thread>  // NOLINT
#include <vector>

#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/datastore/file_pool.h"
#include "src/common/bitmap.h"
#include "src/common/crc32.h"
#include "src/common/curve_define.h"
#include "src/common/fast_align.h"
#include "src/fs/fs_common.h"
#include "src/fs/local_filesystem.h"

using ::curve::common::align_up;
using ::curve::common::is_aligned;

/**
 * chunkfile pool pre allocation tool, providing two allocation methods
 * 1. Specify the percentage to be allocated as a percentage of disk space
 * 2. Specify allocation by chunk quantity
 * The default allocation method is based on the percentage of disk space, which
 * can be achieved by -allocateByPercent=false/true Adjust the allocation
 * method.
 */
DEFINE_bool(allocateByPercent, true,
            "allocate filePool by percent of disk size or by chunk num!");

DEFINE_uint32(fileSize, 16 * 1024 * 1024, "chunk size");

DEFINE_uint32(blockSize, 4096, "minimum io alignment supported");

static bool ValidateBlockSize(const char* /*name*/, uint32_t blockSize) {
    // we only support 512/4096 now
    return blockSize == 512 || blockSize == 4096;
}

DEFINE_validator(blockSize, &ValidateBlockSize);

DEFINE_string(fileSystemPath, "./", "chunkserver disk path");

DEFINE_string(filePoolDir, "./filePool/", "chunkfile pool dir");

DEFINE_string(filePoolMetaPath, "./filePool.meta",
              "chunkfile pool meta info file path.");

// preallocateNum is only used during testing, and a fixed number of chunks are
// pre allocated in advance during testing When setting this value, there is no
// need to set allocatepercent
DEFINE_uint32(preAllocateNum, 0,
              "preallocate chunk nums, this is JUST for curve test");

// During system initialization, the administrator needs to pre format the disk
// and pre allocate it At this point, only allocate percentage needs to be
// specified, which is the percentage of the entire disk space occupied by
// allocate percentage
DEFINE_uint32(allocatePercent, 80, "preallocate storage percent of total disk");

// Set to false during testing to accelerate testing speed
DEFINE_bool(needWriteZero, true, "not write zero for test.");

using curve::chunkserver::FilePoolMeta;
using curve::common::kFilePoolMagic;
using curve::fs::FileSystemInfo;
using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;

class CompareInternal {
 public:
    bool operator()(const std::string& s1, const std::string& s2) {
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

    // file size + meta page size
    size_t actualFileSize = 0;
};

static int AllocateFiles(AllocateStruct* allocatestruct) {
    const size_t actualFileSize = allocatestruct->actualFileSize;
    char* data = new (std::nothrow) char[actualFileSize];
    memset(data, 0, actualFileSize);

    uint64_t count = 0;
    while (count < allocatestruct->chunknum) {
        std::string filename;
        {
            std::unique_lock<std::mutex> lk(*allocatestruct->mtx);
            allocatestruct->allocateChunknum->fetch_add(1);
            filename = std::to_string(allocatestruct->allocateChunknum->load());
        }
        std::string tmpchunkfilepath = FLAGS_filePoolDir + "/" + filename +
                                       allocatestruct->cleanChunkSuffix;

        int ret =
            allocatestruct->fsptr->Open(tmpchunkfilepath, O_RDWR | O_CREAT);
        if (ret < 0) {
            *allocatestruct->checkwrong = true;
            LOG(ERROR) << "file open failed, " << tmpchunkfilepath;
            break;
        }
        int fd = ret;

        ret = allocatestruct->fsptr->Fallocate(fd, 0, 0, actualFileSize);
        if (ret < 0) {
            allocatestruct->fsptr->Close(fd);
            *allocatestruct->checkwrong = true;
            LOG(ERROR) << "Fallocate failed, " << tmpchunkfilepath;
            break;
        }

        if (FLAGS_needWriteZero) {
            ret = allocatestruct->fsptr->Write(fd, data, 0, actualFileSize);
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

        ret = allocatestruct->fsptr->Close(fd);
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

static bool CanBitmapFitInMetaPage() {
    // The maximum bytes in meta page except bitmap are
    // (25 bytes + #clonesource bytes), and historically, #clonesource can
    // reaching 3000 bytes,
    // also extra 4 bytes is need to store the bits of bitmap.
    // So, maximum bytes for bitmap are
    // 4096(kChunkfileMetaPageSize) - 3029 = 1067 bytes.
    //
    // See src/chunkserver/datastore/chunkserver_chunkfile.h, for normal/clone
    // chunk metapage usage. See
    // src/chunkserver/datastore/chunkserver_snapshot.h, for snapshot chunk
    // metapage usage.
    constexpr size_t kMaximumBitmapBytes = 1024;

    auto bitmapBytes =
        FLAGS_fileSize / FLAGS_blockSize / curve::common::BITMAP_UNIT_SIZE;
    LOG(INFO) << "bitmap bytes is " << bitmapBytes;
    return bitmapBytes <= kMaximumBitmapBytes;
}

// TODO(tongguangxun): Adding unit tests
int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, false);
    google::InitGoogleLogging(argv[0]);

    if (!is_aligned(FLAGS_fileSize, FLAGS_blockSize)) {
        LOG(ERROR) << "chunk file size doesn't align to block size";
        return -1;
    }

    if (!CanBitmapFitInMetaPage()) {
        LOG(ERROR) << "bitmap can't fit into meta page, chunk size: "
                   << FLAGS_fileSize << ", block size: " << FLAGS_blockSize
                   << ", meta page size: "
                   << curve::chunkserver::kChunkfileMetaPageSize;
        return -1;
    }

    // load current chunkfile pool
    std::mutex mtx;
    auto fsptr = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
    std::set<std::string, CompareInternal> tmpChunkSet_;
    std::atomic<uint64_t> allocateChunknum_(0);
    std::vector<std::string> tmpvec;

    constexpr size_t metaPageSize = curve::chunkserver::kChunkfileMetaPageSize;

    if (fsptr->Mkdir(FLAGS_filePoolDir) < 0) {
        LOG(ERROR) << "mkdir failed!, " << FLAGS_filePoolDir;
        return -1;
    }
    if (fsptr->List(FLAGS_filePoolDir, &tmpvec) < 0) {
        LOG(ERROR) << "list dir failed!, " << FLAGS_filePoolDir;
        return -1;
    }

    tmpChunkSet_.insert(tmpvec.begin(), tmpvec.end());
    uint64_t size = tmpChunkSet_.size()
                        ? atoi((*(--tmpChunkSet_.end())).c_str())
                        : 0;  // NOLINT
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
        preAllocateChunkNum = preAllocateSize / (FLAGS_fileSize + metaPageSize);
    } else {
        preAllocateChunkNum = FLAGS_preAllocateNum;
    }

    bool checkwrong = false;
    // two threads concurrent, can reach the bandwidth of disk.
    uint64_t threadAllocateNum = preAllocateChunkNum / 2;
    std::vector<std::thread> thvec;
    AllocateStruct allocateStruct;
    allocateStruct.fsptr = fsptr;
    allocateStruct.allocateChunknum = &allocateChunknum_;
    allocateStruct.checkwrong = &checkwrong;
    allocateStruct.mtx = &mtx;
    allocateStruct.chunknum = threadAllocateNum;
    allocateStruct.cleanChunkSuffix =
        curve::chunkserver::FilePool::GetCleanChunkSuffix();
    allocateStruct.actualFileSize = FLAGS_fileSize + metaPageSize;

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
    meta.metaPageSize = metaPageSize;
    meta.hasBlockSize = true;
    meta.blockSize = FLAGS_blockSize;
    meta.filePoolPath = FLAGS_filePoolDir;
    int ret = curve::chunkserver::FilePoolHelper::PersistEnCodeMetaInfo(
        fsptr, meta, FLAGS_filePoolMetaPath);

    if (ret == -1) {
        LOG(ERROR) << "persist chunkfile pool meta info failed!";
        return -1;
    }

    // Read the meta file and check if it is written correctly
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

        if (recordMeta.metaPageSize != metaPageSize) {
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
