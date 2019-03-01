/*
 * Project: curve
 * File Created: Monday, 28th January 2019 2:59:53 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <glog/logging.h>
#include <gflags/gflags.h>

#include <fcntl.h>

#include <set>
#include <mutex>    // NOLINT
#include <thread>   // NOLINT
#include <atomic>
#include <vector>

#include "src/fs/fs_common.h"
#include "src/fs/local_filesystem.h"
#include "src/common/crc32.h"

DEFINE_string(filesystem_path, "./", "chunkserver disk path");
DEFINE_string(chunkfilepool_metapath, "./chunkfilepool.meta", "chunkfile pool meta info file path.");   // NOLINT
DEFINE_string(chunkfilepool_dir, "./chunkfilepool/", "chunkfile pool dir");
DEFINE_uint32(chunksize, 16 * 1024 * 1024, "chunk size");
DEFINE_uint32(metapagsize, 4 * 1024, "metapage size for every chunk");
DEFINE_uint32(allocatepercent, 80, "preallocate storage percent of total disk");

using curve::fs::FileSystemType;
using curve::fs::LocalFsFactory;
using curve::fs::FileSystemInfo;
using curve::fs::LocalFileSystem;

class CompareInternal {
 public:
    bool operator()(std::string s1, std::string s2) {
        auto index1 = std::atoi(s1.c_str());
        auto index2 = std::atoi(s2.c_str());
        return index1 < index2;
    }
};

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);

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
    int16_t canbeallocateperc = freepercent - (100 - FLAGS_allocatepercent);
    LOG(INFO) << "free space = " << finfo.available
              << ", total space = " << finfo.total
              << ", freepercent = " << freepercent
              << ", canbeallocateperc = " << canbeallocateperc;

    if (canbeallocateperc < 0) {
        LOG(ERROR) << "disk space already allocated.";
        return 0;
    }

    uint64_t preallocatesize = canbeallocateperc * finfo.available / 100;
    uint64_t preallocatechunknum = preallocatesize / (FLAGS_chunksize + FLAGS_metapagsize);     // NOLINT

    bool checkwrong = false;
    // allocate chunk
    auto allocateChunk = [&](uint64_t chunknum) {
        char* data = new (std::nothrow) char[FLAGS_chunksize + FLAGS_metapagsize];              // NOLINT
        memset(data, '0', FLAGS_chunksize + FLAGS_metapagsize);

        uint64_t count = 0;
        while (count < chunknum) {
            std::string filename;
            {
                std::unique_lock<std::mutex> lk(mtx);
                allocateChunknum_.fetch_add(1, std::memory_order_relaxed);
                filename = std::to_string(allocateChunknum_.load(std::memory_order_relaxed));   // NOLINT
            }
            std::string tmpchunkfilepath = FLAGS_chunkfilepool_dir + "/" + filename;            // NOLINT

            int ret = fsptr->Open(tmpchunkfilepath.c_str(), O_RDWR | O_CREAT);                  //NOLINT
            if (ret < 0) {
                checkwrong = true;
                LOG(ERROR) << "file open failed, " << tmpchunkfilepath.c_str();
                break;
            }
            int fd = ret;

            ret = fsptr->Fallocate(fd, 0, 0, FLAGS_chunksize + FLAGS_metapagsize);              //NOLINT
            if (ret < 0) {
                fsptr->Close(fd);
                checkwrong = true;
                LOG(ERROR) << "Fallocate failed, " << tmpchunkfilepath.c_str();
                break;
            }

            ret = fsptr->Write(fd, data, 0, FLAGS_chunksize + FLAGS_metapagsize);               //NOLINT
            if (ret < 0) {
                fsptr->Close(fd);
                checkwrong = true;
                LOG(ERROR) << "write failed, " << tmpchunkfilepath.c_str();
                break;
            }

            ret = fsptr->Fsync(fd);
            if (ret < 0) {
                fsptr->Close(fd);
                checkwrong = true;
                LOG(ERROR) << "fsync failed, " << tmpchunkfilepath.c_str();
                break;
            }

            fsptr->Close(fd);
            if (ret < 0) {
                checkwrong = true;
                LOG(ERROR) << "close failed, " << tmpchunkfilepath.c_str();
                break;
            }
            count++;
        }
        delete[] data;
    };

    // two threads concurrent, can reach the bandwidth of disk.
    std::vector<std::thread> thvec;
    thvec.push_back(std::move(std::thread(allocateChunk, preallocatechunknum/2)));  // NOLINT
    thvec.push_back(std::move(std::thread(allocateChunk, preallocatechunknum/2)));  // NOLINT

    for (auto& iter : thvec) {
        iter.join();
    }

    if (checkwrong) {
        LOG(ERROR) << "allocate got something wrong, please check.";
        return -1;
    }

    // persistency: chunksize, metapagesize, chunkfilepool path, allocate percent                                   // NOLINT
    char persistency[4096] = {0};
    ::memcpy(persistency, &FLAGS_chunksize, sizeof(uint32_t));
    ::memcpy(persistency + sizeof(uint32_t), &FLAGS_metapagsize, sizeof(uint32_t));                                 // NOLINT
    ::memcpy(persistency + 2*sizeof(uint32_t), &FLAGS_allocatepercent, sizeof(uint32_t));                           // NOLINT
    ::memcpy(persistency + 3*sizeof(uint32_t), FLAGS_chunkfilepool_dir.c_str(), FLAGS_chunkfilepool_dir.size());    // NOLINT

    uint32_t len = 3 * sizeof(uint32_t) + FLAGS_chunkfilepool_dir.size();
    uint32_t crc = ::curve::common::CRC32(persistency, len);
    ::memcpy(persistency + len, &crc, sizeof(uint32_t));

    int fd = fsptr->Open(FLAGS_chunkfilepool_metapath.c_str(), O_RDWR | O_CREAT);                                   // NOLINT
    if (fd < 0) {
        LOG(ERROR) << "meta file open failed, " << FLAGS_chunkfilepool_metapath.c_str();                                 // NOLINT
        return -1;
    }
    int ret = fsptr->Write(fd, persistency, 0, 4096);
    if (ret != 4096) {
        LOG(ERROR) << "meta file write failed, " << FLAGS_chunkfilepool_metapath.c_str();                                // NOLINT
        return -1;
    }

    char readvalid[4096] = {0};
    ret = fsptr->Read(fd, readvalid, 0, 4096);
    if (ret != 4096) {
        LOG(ERROR) << "meta file read failed, " << FLAGS_chunkfilepool_metapath.c_str();                                 // NOLINT
        return -1;
    }

    uint32_t chunksize = 0;
    uint32_t metapagesize = 0;
    uint32_t allocateper = 0;
    char path[256];
    memcpy(&chunksize, readvalid + 0, sizeof(uint32_t));
    memcpy(&metapagesize, readvalid + sizeof(uint32_t) , sizeof(uint32_t));
    memcpy(&allocateper, readvalid + 2*sizeof(uint32_t), sizeof(uint32_t));
    memcpy(path, readvalid + 3*sizeof(uint32_t), 256);
    std::string chunkpath(path);

    bool valid = false;
    do {
        if (chunksize != FLAGS_chunksize) {
            LOG(ERROR) << "chunksize meta info persistency failed!";
            break;
        }

        if (metapagesize != FLAGS_metapagsize) {
            LOG(ERROR) << "metapagesize meta info persistency failed!";
            break;
        }

        if (allocateper != FLAGS_allocatepercent) {
            LOG(ERROR) << "allocatepercent meta info persistency failed!";
            break;
        }

        if (strcmp(chunkpath.c_str(), FLAGS_chunkfilepool_dir.c_str()) != 0) {
            LOG(ERROR) << "meta info persistency failed!"
                    << ", read chunkpath = " << chunkpath.c_str()
                    << ", real chunkpath = " << FLAGS_chunkfilepool_dir.c_str();
            break;
        }
        valid = true;
        fsptr->Close(fd);
    } while (0);

    if (!valid) {
        fsptr->Close(fd);
        fsptr->Delete(FLAGS_chunkfilepool_metapath.c_str());
        LOG(ERROR) << "meta file has something wrong, already deleted!";
        return -1;
    }

    return 0;
}
