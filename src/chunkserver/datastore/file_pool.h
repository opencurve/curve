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
 * File Created: Monday, 10th December 2018 9:54:34 am
 * Author: tongguangxun
 */

#ifndef SRC_CHUNKSERVER_DATASTORE_FILE_POOL_H_
#define SRC_CHUNKSERVER_DATASTORE_FILE_POOL_H_

#include <glog/logging.h>

#include <set>
#include <mutex>  // NOLINT
#include <vector>
#include <string>
#include <memory>
#include <deque>
#include <atomic>

#include "src/fs/local_filesystem.h"
#include "include/curve_compiler_specific.h"

using curve::fs::LocalFileSystem;
namespace curve {
namespace chunkserver {

struct FilePoolOptions {
    bool        getFileFromPool;
    // it should be set when getFileFromPool=false
    char        filePoolDir[256];
    uint32_t    fileSize;
    uint32_t    metaPageSize;
    uint32_t    blockSize;
    char        metaPath[256];
    uint32_t    metaFileSize;
    // retry times for get file
    uint16_t    retryTimes;

    FilePoolOptions() {
        getFileFromPool = true;
        metaFileSize = 4096;
        fileSize = 0;
        metaPageSize = 0;
        retryTimes = 5;
        blockSize = 0;
        ::memset(metaPath, 0, 256);
        ::memset(filePoolDir, 0, 256);
    }
};

struct FilePoolState {
    // How many pre-allocated chunks are not used by the datastore
    uint64_t    preallocatedChunksLeft;
    // chunksize
    uint32_t    chunkSize = 0;
    // metapage size
    uint32_t    metaPageSize = 0;
    // io alignment
    uint32_t    blockSize = 0;
};

struct FilePoolMeta {
    uint32_t chunkSize = 0;
    uint32_t metaPageSize = 0;
    bool hasBlockSize = false;
    uint32_t blockSize = 0;
    std::string filePoolPath;

    FilePoolMeta() = default;

    FilePoolMeta(uint32_t chunksize,
                 uint32_t metapagesize,
                 uint32_t blocksize,
                 const std::string& filepool)
        : chunkSize(chunksize),
          metaPageSize(metapagesize),
          hasBlockSize(true),
          blockSize(blocksize),
          filePoolPath(filepool) {}

    FilePoolMeta(uint32_t chunksize,
                 uint32_t metapagesize,
                 const std::string& filepool)
        : chunkSize(chunksize),
          metaPageSize(metapagesize),
          hasBlockSize(false),
          filePoolPath(filepool) {}

    uint32_t Crc32() const;
};

class FilePoolHelper {
 public:
    static const char* kFileSize;
    static const char* kMetaPageSize;
    static const char* kFilePoolPath;
    static const char* kCRC;
    static const char* kBlockSize;
    static const uint32_t kPersistSize;

    /**
     * Persistent chunkfile pool meta information
     * @param[in]: File system used for persistence
     * @param[in]: chunkSize The size of each chunk
     * @param[in]: metaPageSize The metapage size of each chunkfile
     * @param[in]: FilePool_path is the path of the chunk pool
     * @param[in]: The path where persistPathmeta information is to be persisted
     * @return: success 0, otherwise -1
     */
    static int PersistEnCodeMetaInfo(std::shared_ptr<LocalFileSystem> fsptr,
                                     const FilePoolMeta& meta,
                                     const std::string& persistPath);

    /**
     * Parse the current chunk pool information from the persistent meta data
     * @param[in]: File system used for persistence
     * @param[in]: metafile path
     * @param[in]: meta file size
     * @param[out]: chunkSize The size of each chunk
     * @param[out]: metaPageSize The metapage size of each chunkfile
     * @param[out]: FilePool_path is the path of the chunk pool
     * @return: success 0, otherwise -1
     */
    static int DecodeMetaInfoFromMetaFile(
        std::shared_ptr<LocalFileSystem> fsptr,
        const std::string& metaFilePath,
        uint32_t metaFileSize,
        FilePoolMeta* meta);
};

class CURVE_CACHELINE_ALIGNMENT FilePool {
 public:
    explicit FilePool(std::shared_ptr<LocalFileSystem> fsptr);
    virtual ~FilePool() = default;

    /**
     * Initialization function
     * @param: cfop is a configuration option
     */
    virtual bool Initialize(const FilePoolOptions& cfop);
    /**
     * The datastore obtains a new chunk through the GetChunk interface,
     * and GetChunk internally assigns the metapage atom and returns it.
     * @param: chunkpath is the new chunkfile path
     * @param: metapage is the metapage information of the new chunk
     */
    virtual int GetFile(const std::string& chunkpath, char* metapage);
    /**
     * Datastore deletes chunks and recycles directly, not really deleted
     * @param: chunkpath is the chunk path that needs to be recycled
     */
    virtual int RecycleFile(const std::string& chunkpath);
    /**
     * Get the current chunkfile pool size
     */
    virtual size_t Size();

    /**
     * Get the allocation status of FilePool
     */
    virtual FilePoolState GetState() const;
    /**
     * Get the option configuration information of the current FilePool
     */
    virtual FilePoolOptions GetFilePoolOpt() {
        return poolOpt_;
    }
    /**
     * Deconstruction, release resources
     */
    virtual void UnInitialize();

    /**
     * Test use
     */
    virtual void SetLocalFileSystem(std::shared_ptr<LocalFileSystem> fs) {
        CHECK(fs != nullptr) << "fs ptr allocate failed!";
        fsptr_ = fs;
    }

 private:
    // Traverse the pre-allocated chunk information from the
    // chunkfile pool directory
    bool ScanInternal();
    // Check whether the chunkfile pool pre-allocation is legal
    bool CheckValid();
    /**
     * Perform metapage assignment for the new chunkfile
     * @param: sourcepath is the file path to be written
     * @param: page is the metapage information to be written
     * @return: returns true if successful, otherwise false
     */
    bool WriteMetaPage(const std::string& sourcepath, char* page);
    /**
     * Directly allocate chunks, not from FilePool
     * @param: chunkpath is the path of the chunk file in the datastore
     * @return: return 0 if successful, otherwise return less than 0
     */
    int AllocateChunk(const std::string& chunkpath);

 private:
    // Protect tmpChunkvec_
    std::mutex mtx_;

    // Current FilePool pre-allocated files, folder path
    std::string currentdir_;

    // The underlying file system interface encapsulated by the chunkserver,
    // which provides the basic interface for manipulating files
    std::shared_ptr<LocalFileSystem> fsptr_;

    // The numeric format of the file name in the chunkfile pool held in memory
    std::vector<uint64_t> tmpChunkvec_;

    // The current largest file name number format
    std::atomic<uint64_t> currentmaxfilenum_;

    // FilePool configuration options
    FilePoolOptions poolOpt_;

    // FilePool allocation status
    FilePoolState currentState_;
};
}   // namespace chunkserver
}   // namespace curve

#endif  // SRC_CHUNKSERVER_DATASTORE_FILE_POOL_H_
