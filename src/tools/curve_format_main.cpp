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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <json/json.h>

#include <fcntl.h>

#include <atomic>
#include <mutex> // NOLINT
#include <set>
#include <thread> // NOLINT
#include <vector>

#include "src/chunkserver/datastore/chunkfile_pool.h"
#include "src/common/crc32.h"
#include "src/common/curve_define.h"
#include "src/fs/fs_common.h"
#include "src/fs/local_filesystem.h"

/**
 * chunkfile pool预分配工具，提供两种分配方式
 * 1. 以磁盘空间百分比方式，指定需要分配的百分比
 * 2. 指定以chunk数量分配
 * 默认的分配方式是以磁盘空间百分比作为分配方式，可以通过-allocateByPercent=false/true
 * 调整分配方式。
 */
DEFINE_bool(allocateByPercent, true,
            "allocate chunkfilepool by percent of disk size or by chunk num!");

DEFINE_uint32(chunksize, 16 * 1024 * 1024, "chunk size");

DEFINE_uint32(metapagsize, 4 * 1024, "metapage size for every chunk");

DEFINE_string(filesystem_path, "./", "chunkserver disk path");

DEFINE_string(chunkfilepool_dir, "./chunkfilepool/", "chunkfile pool dir");

DEFINE_string(chunkfilepool_metapath, "./chunkfilepool.meta",
              "chunkfile pool meta info file path.");

// preallocateNum仅在测试的时候使用，测试提前预分配固定数量的chunk
// 当设置这个值的时候可以不用设置allocatepercent
DEFINE_uint32(preallocateNum, 0,
              "preallocate chunk nums, this is JUST for curve test");

// 在系统初始化的时候，管理员需要预先格式化磁盘，并进行预分配
// 这时候只需要指定allocatepercent，allocatepercent是占整个盘的空间的百分比
DEFINE_uint32(allocatepercent, 80, "preallocate storage percent of total disk");

// 测试情况下置为false，加快测试速度
DEFINE_bool(needWriteZero, true, "not write zero for test.");

/**
 * WAL file pool预分配参数
 */
DEFINE_bool(WAL_alloc_by_percent, true,
            "allocate WAL filepool by percent of disk size or by chunk num!");

DEFINE_uint32(WAL_segment_size, 8 * 1024 * 1024, "WAL segment size");

DEFINE_uint32(WAL_metapage_size, 4 * 1024, "metapage size for every WAL log");

DEFINE_string(WAL_filepool_dir, "./wal_filepool/", "WAL file pool dir");

DEFINE_string(WAL_filepool_metapath, "./walfilepool.meta",
              "WAL file pool meta info file path.");
// WAL_prealloc_num仅在测试的时候使用，测试提前预分配固定数量的chunk
// 当设置这个值的时候可以不用设置allocatepercent
DEFINE_uint32(WAL_prealloc_num, 0,
              "preallocate WAL log nums, this is JUST for curve test");

// WAL file pool预分配比例
DEFINE_uint32(WAL_alloc_percent, 10,
              "preallocate WAL storage percent of total disk");

using curve::common::kChunkFilePoolMaigic;
using curve::fs::FileSystemInfo;
using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;

class CompareInternal {
 public:
  bool operator()(std::string s1, std::string s2) {
    auto index1 = std::atoi(s1.c_str());
    auto index2 = std::atoi(s2.c_str());
    return index1 < index2;
  }
};

struct FilePoolConfig {
  bool allocByPercent;
  uint32_t fileSize;
  uint32_t metaPageSize;
  string workPath;
  string filePoolDir;
  string filePoolMetaPath;
  uint32_t preallocNum;
  uint32_t allocPercent;
  bool needZero;
};

struct AllocateStruct {
  std::shared_ptr<LocalFileSystem> fsptr;
  std::atomic<uint64_t> *allocateChunknum;
  bool *checkwrong;
  std::mutex *mtx;
  uint64_t chunknum;
  FilePoolConfig *config;
};

int AllocateChunks(AllocateStruct *allocatestruct) {
  FilePoolConfig *config = allocatestruct->config;
  char *data = new (std::nothrow) char[config->fileSize + config->metaPageSize];
  memset(data, 0, config->fileSize + config->metaPageSize);

  uint64_t count = 0;
  while (count < allocatestruct->chunknum) {
    std::string filename;
    {
      std::unique_lock<std::mutex> lk(*allocatestruct->mtx);
      allocatestruct->allocateChunknum->fetch_add(1);
      filename = std::to_string(allocatestruct->allocateChunknum->load());
    }
    std::string tmpchunkfilepath = config->filePoolDir + "/" + filename;

    int ret =
        allocatestruct->fsptr->Open(tmpchunkfilepath.c_str(), O_RDWR | O_CREAT);
    if (ret < 0) {
      *allocatestruct->checkwrong = true;
      LOG(ERROR) << "file open failed, " << tmpchunkfilepath.c_str();
      break;
    }
    int fd = ret;

    ret = allocatestruct->fsptr->Fallocate(
        fd, 0, 0, config->fileSize + config->metaPageSize);
    if (ret < 0) {
      allocatestruct->fsptr->Close(fd);
      *allocatestruct->checkwrong = true;
      LOG(ERROR) << "Fallocate failed, " << tmpchunkfilepath.c_str();
      break;
    }

    if (config->needZero) {
      ret = allocatestruct->fsptr->Write(
          fd, data, 0, config->fileSize + config->metaPageSize);
      if (ret < 0) {
        allocatestruct->fsptr->Close(fd);
        *allocatestruct->checkwrong = true;
        LOG(ERROR) << "write failed, " << tmpchunkfilepath.c_str();
        break;
      }
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

int CreateFilePool(FilePoolConfig *poolConfig) {
  // load current chunkfile pool
  std::mutex mtx;
  std::shared_ptr<LocalFileSystem> fsptr =
      LocalFsFactory::CreateFs(FileSystemType::EXT4, ""); // NOLINT
  std::set<std::string, CompareInternal> tmpChunkSet_;
  std::atomic<uint64_t> allocateChunknum_(0);
  std::vector<std::string> tmpvec;

  if (fsptr->Mkdir(poolConfig->filePoolDir.c_str()) < 0) {
    LOG(ERROR) << "mkdir failed!, " << poolConfig->filePoolDir.c_str();
    return -1;
  }
  if (fsptr->List(poolConfig->filePoolDir.c_str(), &tmpvec) < 0) {
    LOG(ERROR) << "list dir failed!, " << poolConfig->filePoolDir.c_str();
    return -1;
  }

  tmpChunkSet_.insert(tmpvec.begin(), tmpvec.end());
  uint64_t size = tmpChunkSet_.size() ? atoi((*(--tmpChunkSet_.end())).c_str())
                                      : 0; // NOLINT
  allocateChunknum_.store(size + 1);

  FileSystemInfo finfo;
  int r = fsptr->Statfs(poolConfig->workPath, &finfo);
  if (r != 0) {
    LOG(ERROR) << "get disk usage info failed!";
    return -1;
  }

  uint64_t freepercent = finfo.available * 100 / finfo.total;
  LOG(INFO) << "free space = " << finfo.available
            << ", total space = " << finfo.total
            << ", freepercent = " << freepercent;

  if (freepercent < poolConfig->allocPercent && poolConfig->allocByPercent) {
    LOG(ERROR) << "disk free space not enough.";
    return 0;
  }

  uint64_t preAllocateChunkNum = 0;
  uint64_t preAllocateSize = poolConfig->allocPercent * finfo.total / 100;

  if (poolConfig->allocByPercent) {
    preAllocateChunkNum =
        preAllocateSize / (poolConfig->fileSize + poolConfig->metaPageSize);
  } else {
    preAllocateChunkNum = poolConfig->preallocNum;
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
  allocateStruct.config = poolConfig;

  thvec.push_back(std::move(std::thread(AllocateChunks, &allocateStruct)));
  thvec.push_back(std::move(std::thread(AllocateChunks, &allocateStruct)));

  for (auto &iter : thvec) {
    iter.join();
  }

  if (checkwrong) {
    LOG(ERROR) << "allocate got something wrong, please check.";
    return -1;
  }

  int ret = curve::chunkserver::ChunkfilePoolHelper::PersistEnCodeMetaInfo(
      fsptr, poolConfig->fileSize, poolConfig->metaPageSize,
      poolConfig->filePoolDir, poolConfig->filePoolMetaPath);

  if (ret == -1) {
    LOG(ERROR) << "persist file pool meta info failed!";
    return -1;
  }

  // 读取meta文件，检查是否写入正确
  uint32_t chunksize = 0;
  uint32_t metapagesize = 0;
  std::string chunkfilePath;

  ret = curve::chunkserver::ChunkfilePoolHelper::DecodeMetaInfoFromMetaFile(
      fsptr, poolConfig->filePoolMetaPath, 4096, &chunksize, &metapagesize,
      &chunkfilePath);
  if (ret == -1) {
    LOG(ERROR) << "file pool meta info file got something wrong!";
    fsptr->Delete(poolConfig->filePoolMetaPath.c_str());
    return -1;
  }

  bool valid = false;
  do {
    if (chunksize != poolConfig->fileSize) {
      LOG(ERROR) << "filesize meta info persistency wrong!";
      break;
    }

    if (metapagesize != poolConfig->metaPageSize) {
      LOG(ERROR) << "metapagesize meta info persistency wrong!";
      break;
    }

    if (strcmp(chunkfilePath.c_str(), poolConfig->filePoolDir.c_str()) != 0) {
      LOG(ERROR) << "meta info persistency failed!"
                 << ", read filepath = " << chunkfilePath.c_str()
                 << ", real filepath = " << poolConfig->filePoolDir.c_str();
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

// TODO(tongguangxun) :添加单元测试
int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, false);
  google::InitGoogleLogging(argv[0]);

  // create chunk file pool
  FilePoolConfig poolConfig;
  poolConfig.allocByPercent = FLAGS_allocateByPercent;
  poolConfig.fileSize = FLAGS_chunksize;
  poolConfig.metaPageSize = FLAGS_metapagsize;
  poolConfig.workPath = FLAGS_filesystem_path;
  poolConfig.filePoolDir = FLAGS_chunkfilepool_dir;
  poolConfig.filePoolMetaPath = FLAGS_chunkfilepool_metapath;
  poolConfig.preallocNum = FLAGS_preallocateNum;
  poolConfig.allocPercent = FLAGS_allocatepercent;
  poolConfig.needZero = FLAGS_needWriteZero;

  int ret = CreateFilePool(&poolConfig);
  if (ret)
    return -1;

  poolConfig.allocByPercent = FLAGS_WAL_alloc_by_percent;
  poolConfig.fileSize = FLAGS_WAL_segment_size;
  poolConfig.metaPageSize = FLAGS_WAL_metapage_size;
  poolConfig.workPath = FLAGS_filesystem_path;  // shared arg with chunkfilepool
  poolConfig.filePoolDir = FLAGS_WAL_filepool_dir;
  poolConfig.filePoolMetaPath = FLAGS_WAL_filepool_metapath;
  poolConfig.preallocNum = FLAGS_WAL_prealloc_num;
  poolConfig.allocPercent = FLAGS_WAL_alloc_percent;
  poolConfig.needZero = FLAGS_needWriteZero;  // shared arg with chunkfilepool

  return CreateFilePool(&poolConfig);
}
