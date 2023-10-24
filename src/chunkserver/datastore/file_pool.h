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

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <string>
#include <vector>

#include "include/curve_compiler_specific.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"
#include "src/common/throttle.h"
#include "src/fs/local_filesystem.h"

using curve::fs::LocalFileSystem;
using curve::common::Thread;
using curve::common::Atomic;
using curve::common::InterruptibleSleeper;
using curve::common::ReadWriteThrottleParams;
using curve::common::ThrottleParams;
using curve::common::Throttle;
namespace curve {
namespace chunkserver {

struct FilePoolOptions {
    bool        getFileFromPool;
    bool        needClean;
    // Bytes per write for cleaning chunk (4096)
    uint32_t    bytesPerWrite;
    uint32_t    iops4clean;
    // it should be set when getFileFromPool=false
    char        filePoolDir[256];
    uint32_t    fileSize;
    uint32_t    metaPageSize;
    uint32_t    blockSize;
    char        metaPath[256];
    uint32_t    metaFileSize;
    // retry times for get file
    uint16_t    retryTimes;

    bool allocatedByPercent;
    uint32_t allocatedPercent;
    uint32_t preAllocateNum;
    uint64_t filePoolSize;
    uint32_t formatThreadNum;

    std::string copysetDir;
    std::string recycleDir;

    std::function<bool(const std::string&)> isAllocated;

    FilePoolOptions() {
        getFileFromPool = true;
        needClean = false;
        bytesPerWrite = 4096;
        iops4clean = -1;
        metaFileSize = 4096;
        fileSize = 0;
        metaPageSize = 0;
        retryTimes = 5;
        blockSize = 0;
        allocatedByPercent = false;
        allocatedPercent = 0;
        preAllocateNum = 0;
        filePoolSize = 0;
        formatThreadNum = 1;
        ::memset(metaPath, 0, 256);
        ::memset(filePoolDir, 0, 256);
    }
};

struct FilePoolState {
    // How many dirty chunks are not used by the datastore
    uint64_t    dirtyChunksLeft = 0;
    // How many clean chunks are not used by the datastore
    uint64_t    cleanChunksLeft = 0;
    // How many pre-allocated chunks are not used by the datastore
    uint64_t    preallocatedChunksLeft = 0;
    // Total num of chunks in the datastore
    uint64_t chunkNum;
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

struct ChunkFormatStat {
    std::atomic<bool> isWrong{false};
    std::atomic<uint32_t> allocateChunkNum{0};
    uint32_t preAllocateNum = 0;
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
    virtual ~FilePool();

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
     * @param: needClean is whether chunk need fill zero
     */
    virtual int GetFile(const std::string& chunkpath,
                        const char* metapage,
                        bool needClean = false);
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
     * @brief: Return the suffix of clean chunk
     */
    static std::string GetCleanChunkSuffix() {
        return kCleanChunkSuffix_;
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

    /**
     * @brief: Start thread for cleaning chunk
     * @return: Return true if success, otherwise return false
     */
    bool StartCleaning();

    /**
     * @brief: Stop thread for cleaning chunk
     * @return: Return true if success, otherwise return false
     */
    bool StopCleaning();

    /**
     * @brief: Wait for format thread done.
     * @return: Return true if success.
     */
    bool WaitoFormatDoneForTesting();

    /**
     * @brief: Get the format status of FilePool
     * @return: Return the format status.
     */
    virtual const ChunkFormatStat& GetChunkFormatStat() const;

 private:
    // Traverse the pre-allocated chunk information from the
    // chunkfile pool directory
    bool ScanInternal();
    // Prepare for format.
    bool PrepareFormat();
    //
    bool CheckValid();
    // Count the num of files that has been allocated.
    uint64_t CountAllocatedNum(const std::string& path);
    // Check whether pool file is legal.
    bool CheckPoolFile(const std::string& file);
    /**
     * Perform metapage assignment for the new chunkfile
     * @param: sourcepath is the file path to be written
     * @param: page is the metapage information to be written
     * @return: returns true if successful, otherwise false
     */
    bool WriteMetaPage(const std::string& sourcepath, const char* page);
    /**
     * Directly allocate chunks, not from FilePool
     * @param: chunkpath is the path of the chunk file in the datastore
     * @return: return 0 if successful, otherwise return less than 0
     */
    int AllocateChunk(const std::string& chunkpath);

    /**
     * @brief: Get chunk
     * @param needClean: Whether need the zeroed chunk
     * @param chunkid: The return chunk's id
     * @param isCleaned: Whether the return chunk is zeroed
     * @return: Return false if there is no valid chunk, else return true
     */
    bool GetChunk(bool needClean, uint64_t* chunkid, bool* isCleaned);

    /**
     * @brief: Zeroing specify chunk file
     * @param chunkid: The chunk id
     * @param onlyMarked: Use fallocate() to zeroing chunk file 
     *                    if onlyMarked is ture, otherwise 
     *                    write all bytes in chunk to zero
     * @return: Return true if success, else return false
     */
    bool CleanChunk(uint64_t chunkid, bool onlyMarked);

    /**
     * @brief: Clean chunk one by one
     * @return: Return true if clean chunk success, otherwise retrun false
     */
    bool CleaningChunk();

    int FormatTask(uint64_t indexOffset, std::atomic<uint32_t>* allocatIndex);

    /**
     * @brief: The function of thread for formatting chunk
     */
    int FormatWorker();

    /**
     * @brief: Stop thread for formatting chunk
     * @return: Return true if success, otherwise return false
     */
    bool StopFormatting();

    /**
     * @brief: The function of thread for cleaning chunk
     */
    void CleanWorker();

 private:
    // The suffix of clean chunk file (".0")
    static const std::string kCleanChunkSuffix_;

    // Sets a pause between cleaning when clean chunk success
    static const std::chrono::milliseconds kSuccessSleepMsec_;

    // Sets a pause between cleaning when clean chunk fail
    static const std::chrono::milliseconds kFailSleepMsec_;

    // Protect dirtyChunks_, cleanChunks_
    std::mutex mtx_;

    // Wait for GetChunk
    std::condition_variable cond_;

    // Current FilePool pre-allocated files, folder path
    std::string currentdir_;

    // The underlying file system interface encapsulated by the chunkserver,
    // which provides the basic interface for manipulating files
    std::shared_ptr<LocalFileSystem> fsptr_;

    // The numeric format of the file name for all dirty chunk
    std::vector<uint64_t> dirtyChunks_;

    // The numeric format of the file name for all clean chunk
    std::vector<uint64_t> cleanChunks_;

    // The current largest file name number format
    std::atomic<uint64_t> currentmaxfilenum_;

    // FilePool configuration options
    FilePoolOptions poolOpt_;

    // FilePool allocation status
    FilePoolState currentState_;

    // Whether the clean thread is alive
    Atomic<bool> cleanAlived_;

    // Thread for cleaning chunk
    Thread cleanThread_;

    // The throttle iops for cleaning chunk (4KB/IO)
    Throttle cleanThrottle_;

    // Whether the format thread is alive
    Atomic<bool> formatAlived_{true};

    // Thread for format chunks.
    Thread formatThread_;

    // Stat for format chunks.
    ChunkFormatStat formatStat_;

    // Sleeper for cleaning chunk thread
    InterruptibleSleeper cleanSleeper_;

    // Sleeper for formatting chunk thread
    InterruptibleSleeper formatSleeper_;

    // The buffer for write chunk file
    std::unique_ptr<char[]> writeBuffer_;
};
}   // namespace chunkserver
}   // namespace curve

#endif  // SRC_CHUNKSERVER_DATASTORE_FILE_POOL_H_
