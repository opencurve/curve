/*
 * Project: curve
 * File Created: Monday, 28th January 2019 2:59:53 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
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
#include "src/common/curve_define.h"
#include "src/chunkserver/datastore/chunkfile_pool.h"

/**
 * chunkfile pool预分配工具，提供两种分配方式
 * 1. 以磁盘空间百分比方式，指定需要分配的百分比
 * 2. 指定以chunk数量分配
 * 默认的分配方式是以磁盘空间百分比作为分配方式，可以通过-allocateByPercent=false/true
 * 调整分配方式。
 */
DEFINE_bool(allocateByPercent,
            true,
            "allocate chunkfilepool by percent of disk size or by chunk num!");

DEFINE_uint32(chunksize,
              16 * 1024 * 1024,
              "chunk size");

DEFINE_uint32(metapagsize,
              4 * 1024,
              "metapage size for every chunk");

DEFINE_string(filesystem_path,
              "./",
              "chunkserver disk path");

DEFINE_string(chunkfilepool_dir,
              "./chunkfilepool/",
              "chunkfile pool dir");

DEFINE_string(chunkfilepool_metapath,
              "./chunkfilepool.meta",
              "chunkfile pool meta info file path.");

// preallocateNum仅在测试的时候使用，测试提前预分配固定数量的chunk
// 当设置这个值的时候可以不用设置allocatepercent
DEFINE_uint32(preallocateNum,
              0,
              "preallocate chunk nums, this is JUST for curve test");

// 在系统初始化的时候，管理员需要预先格式化磁盘，并进行预分配
// 这时候只需要指定allocatepercent，allocatepercent是占整个盘的空间的百分比
DEFINE_uint32(allocatepercent,
              80,
              "preallocate storage percent of total disk");


using curve::fs::FileSystemType;
using curve::fs::LocalFsFactory;
using curve::fs::FileSystemInfo;
using curve::fs::LocalFileSystem;
using curve::common::kChunkFilePoolMaigic;

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
};

int AllocateChunks(AllocateStruct* allocatestruct) {
    char* data = new(std::nothrow)char[FLAGS_chunksize + FLAGS_metapagsize];
    memset(data, 0, FLAGS_chunksize + FLAGS_metapagsize);

    uint64_t count = 0;
    while (count < allocatestruct->chunknum) {
        std::string filename;
        {
            std::unique_lock<std::mutex> lk(*allocatestruct->mtx);
            allocatestruct->allocateChunknum->fetch_add(1);
            filename = std::to_string(
                            allocatestruct->allocateChunknum->load());
        }
        std::string tmpchunkfilepath
                    = FLAGS_chunkfilepool_dir + "/" + filename;

        int ret = allocatestruct->fsptr->Open(tmpchunkfilepath.c_str(),
                                             O_RDWR | O_CREAT);
        if (ret < 0) {
            *allocatestruct->checkwrong = true;
            LOG(ERROR) << "file open failed, " << tmpchunkfilepath.c_str();
            break;
        }
        int fd = ret;

        ret = allocatestruct->fsptr->Fallocate(fd, 0, 0,
                                             FLAGS_chunksize+FLAGS_metapagsize);
        if (ret < 0) {
            allocatestruct->fsptr->Close(fd);
            *allocatestruct->checkwrong = true;
            LOG(ERROR) << "Fallocate failed, " << tmpchunkfilepath.c_str();
            break;
        }

        ret = allocatestruct->fsptr->Write(fd, data, 0,
                                           FLAGS_chunksize+FLAGS_metapagsize);
        if (ret < 0) {
            allocatestruct->fsptr->Close(fd);
            *allocatestruct->checkwrong = true;
            LOG(ERROR) << "write failed, " << tmpchunkfilepath.c_str();
            break;
        }

        ret = allocatestruct->fsptr->Fsync(fd);
        if (ret < 0) {
            allocatestruct->fsptr->Close(fd);
            *allocatestruct->checkwrong = true;
            LOG(ERROR) << "fsync failed, " << tmpchunkfilepath.c_str();
            break;
        }

        allocatestruct->fsptr->Close(fd);
        if (ret < 0) {
            *allocatestruct->checkwrong = true;
            LOG(ERROR) << "close failed, " << tmpchunkfilepath.c_str();
            break;
        }
        count++;
    }
    delete[] data;
    return *allocatestruct->checkwrong == true ? 0 : -1;
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

    if (fsptr->Mkdir(FLAGS_chunkfilepool_dir.c_str()) < 0) {
        LOG(ERROR) << "mkdir failed!, " << FLAGS_chunkfilepool_dir.c_str();
        return -1;
    }
    if (fsptr->List(FLAGS_chunkfilepool_dir.c_str(), &tmpvec) < 0) {
        LOG(ERROR) << "list dir failed!, " << FLAGS_chunkfilepool_dir.c_str();
        return -1;
    }

    tmpChunkSet_.insert(tmpvec.begin(), tmpvec.end());
    uint64_t size = tmpChunkSet_.size() ? atoi((*(--tmpChunkSet_.end())).c_str()) : 0;          // NOLINT
    allocateChunknum_.store(size + 1);

    FileSystemInfo finfo;
    int r = fsptr->Statfs(FLAGS_filesystem_path, &finfo);
    if (r != 0) {
        LOG(ERROR) << "get disk usage info failed!";
        return -1;
    }

    uint64_t freepercent = finfo.available * 100 / finfo.total;
    LOG(INFO) << "free space = " << finfo.available
              << ", total space = " << finfo.total
              << ", freepercent = " << freepercent;

    if (freepercent < FLAGS_allocatepercent && FLAGS_allocateByPercent) {
        LOG(ERROR) << "disk free space not enough.";
        return 0;
    }

    uint64_t preAllocateChunkNum = 0;
    uint64_t preAllocateSize = FLAGS_allocatepercent * finfo.total / 100;

    if (FLAGS_allocateByPercent) {
        preAllocateChunkNum = preAllocateSize
                              / (FLAGS_chunksize + FLAGS_metapagsize);
    } else {
        preAllocateChunkNum = FLAGS_preallocateNum;
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

    thvec.push_back(std::move(std::thread(AllocateChunks, &allocateStruct)));
    thvec.push_back(std::move(std::thread(AllocateChunks, &allocateStruct)));

    for (auto& iter : thvec) {
        iter.join();
    }

    if (checkwrong) {
        LOG(ERROR) << "allocate got something wrong, please check.";
        return -1;
    }

    int ret = curve::chunkserver::ChunkfilePoolHelper::PersistEnCodeMetaInfo(
                                                fsptr,
                                                FLAGS_chunksize,
                                                FLAGS_metapagsize,
                                                FLAGS_chunkfilepool_dir,
                                                FLAGS_chunkfilepool_metapath);

    if (ret == -1) {
        LOG(ERROR) << "persist chunkfile pool meta info failed!";
        return -1;
    }

    // 读取meta文件，检查是否写入正确
    uint32_t chunksize = 0;
    uint32_t metapagesize = 0;
    std::string chunkfilePath;

    ret = curve::chunkserver::ChunkfilePoolHelper::DecodeMetaInfoFromMetaFile(
                                                fsptr,
                                                FLAGS_chunkfilepool_metapath,
                                                4096,
                                                &chunksize,
                                                &metapagesize,
                                                &chunkfilePath);
    if (ret == -1) {
        LOG(ERROR) << "chunkfile pool meta info file got something wrong!";
        fsptr->Delete(FLAGS_chunkfilepool_metapath.c_str());
        return -1;
    }

    bool valid = false;
    do {
        if (chunksize != FLAGS_chunksize) {
            LOG(ERROR) << "chunksize meta info persistency wrong!";
            break;
        }

        if (metapagesize != FLAGS_metapagsize) {
            LOG(ERROR) << "metapagesize meta info persistency wrong!";
            break;
        }

        if (strcmp(chunkfilePath.c_str(),
            FLAGS_chunkfilepool_dir.c_str()) != 0) {
            LOG(ERROR) << "meta info persistency failed!"
                    << ", read chunkpath = " << chunkfilePath.c_str()
                    << ", real chunkpath = " << FLAGS_chunkfilepool_dir.c_str();
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
