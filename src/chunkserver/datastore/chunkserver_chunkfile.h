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
 * File Created: Thursday, 6th September 2018 10:49:30 am
 * Author: yangyaokai
 */
#ifndef SRC_CHUNKSERVER_DATASTORE_CHUNKSERVER_CHUNKFILE_H_
#define SRC_CHUNKSERVER_DATASTORE_CHUNKSERVER_CHUNKFILE_H_

#include <glog/logging.h>
#include <butil/iobuf.h>
#include <string>
#include <vector>
#include <set>
#include <atomic>
#include <functional>
#include <memory>
#include <condition_variable>

#include "include/curve_compiler_specific.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/crc32.h"
#include "src/common/timeutility.h"
#include "src/fs/local_filesystem.h"
#include "src/chunkserver/datastore/filename_operator.h"
#include "src/chunkserver/datastore/chunkserver_snapshot.h"
#include "src/chunkserver/datastore/define.h"
#include "src/chunkserver/datastore/file_pool.h"
#include "src/common/fast_align.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::common::RWLock;
using curve::common::WriteLockGuard;
using curve::common::ReadLockGuard;
using curve::common::BitRange;
using curve::common::TimeUtility;

class FilePool;
class CSSnapshot;
struct DataStoreMetric;
class CSDataStore;

#define CLONEINFOS_VECTOR_SIZE 16
struct CloneInfos {
    uint64_t cloneNo;
    uint64_t cloneSn;

    CloneInfos() {}

    CloneInfos(uint64_t no, uint64_t sn) {
        cloneNo = no;
        cloneSn = sn;
    }
};

struct CloneContext {
    ChunkID rootId;
    uint64_t cloneNo;
    uint64_t virtualId;
    std::vector<struct CloneInfos> clones;
};

using CloneContextPtr = std::unique_ptr<struct CloneContext>;

/**
 * Chunkfile Metapage Format
 * version: 1 byte
 * sn: 8 bytes
 * correctedSn: 8 bytes
 * crc: 4 bytes
 * padding: 4075 bytes
 */
struct ChunkFileMetaPage {
    // File format version
    uint8_t version;
    // The sequence number of the chunk file
    SequenceNum sn;
    // The revised sequence number of the chunk
    SequenceNum correctedSn;
    // Indicates the location information of the data source,
    // if it is not CloneChunk, it is empty
    string location;

    // Indicates the state of the page in the current Chunk,
    // if it is not CloneChunk, it is nullptr
    std::shared_ptr<Bitmap> bitmap;

    ChunkFileMetaPage() : version(FORMAT_VERSION)
                        , sn(0)
                        , correctedSn(0)
                        , location("")
                        , bitmap(nullptr) 
                        , cloneNo(0)
                        , virtualId(0)
                        , fileId(0) {}
    ChunkFileMetaPage(const ChunkFileMetaPage& metaPage);
    ChunkFileMetaPage& operator = (const ChunkFileMetaPage& metaPage);

    void encode(char* buf);
    CSErrorCode decode(const char* buf);

    // if it is a clone chunk the cloneNo indicate the clone no in the root chunk
    // if it is not a clone chunk, then the cloneNo = 0
    // if it a clone chunk then the virtualId indicate that the chunkid of the root
    uint64_t    cloneNo;
    //chunk Index of the clone chunk or the original chunk
    uint64_t    virtualId;
    //the fileid of the chunk itself
    uint64_t    fileId;
    //bool isclone;
    //just for clone chunk, the parent chunk's sn and the parent chunk's id
    //if the parentChunkId is -1 then that it is not a CloneChunk
    //std::vector<struct CloneInfos> clones;

    //just remember the snap vector Ctx of the ChunkFile
    std::vector<SequenceNum> snapCtx; 

};

struct ChunkOptions {
    // The id of the chunk, used as the file name of the chunk
    ChunkID         id;
    // The sequence number of the chunk
    SequenceNum     sn;
    // The corrected sequence number of the chunk
    SequenceNum     correctedSn;
    // The directory where the chunk is located
    std::string     baseDir;
    // If you want to create a CloneChunk, need to specify this parameter to
    // indicate the location of the data source
    std::string     location;
    // chunk size
    ChunkSizeType   chunkSize;
    // The size of the block, each bit in the bitmap represents 1 block,
    ChunkSizeType   blockSize;
    // The size of the meta page, each bit in the bitmap represents 1 block
    PageSizeType    metaPageSize;
    // enable O_DSYNC When Open ChunkFile
    bool enableOdsyncWhenOpenChunkFile;
    // datastore internal statistical metric
    std::shared_ptr<DataStoreMetric> metric;

    uint64_t        cloneNo;
    uint64_t        virtualId;
    uint64_t        fileId;
    uint32_t        blockSize_shift;

    ChunkOptions() : id(0)
                   , sn(0)
                   , correctedSn(0)
                   , baseDir("")
                   , location("")
                   , chunkSize(0)
                   , blockSize(0)
                   , metaPageSize(0)
                   , metric(nullptr) 
                   , cloneNo(0)
                   , virtualId (0)
                   , fileId(0)
                   , blockSize_shift(0) {}
};

class CSChunkFile {
 public:
    friend class CSSnapshots;
 public:
    CSChunkFile(std::shared_ptr<LocalFileSystem> lfs,
                std::shared_ptr<FilePool> chunkFilePool,
                const ChunkOptions& options);
    virtual ~CSChunkFile();

    /**
     * When a Chunk file is found when Datastore is initialized, this
     * interface will be called to initialize the Chunk file
     * Normally, there is no concurrency, mutually exclusive with other
     * operations, add write lock
     * @createFile: true means to create a new file, false not to create a
     *              file
     * @return returns the error code
     */
    CSErrorCode Open(bool createFile);

    //CSErrorCode Open(bool createFile, uint64_t cloneNo);
    /**
     * Called when a snapshot file is found during Datastore initialization
     * Load the metapage of the snapshot file into the memory inside the
     * function.
     * Under normal circumstances, there is no concurrency, mutually exclusive
     * with other operations, add write lock.
     * @param sn: the sequence number of the snapshot file to be loaded
     * @return: return error code
     */
    CSErrorCode LoadSnapshot(SequenceNum sn);
    /**
     * Write chunk files
     * The Write interface is called when raft apply, and there is no multiple
     * concurrency between Writes.
     * But it may be concurrent with other operations such as Read and Delete,
     * add write lock
     * @param sn: The file sequence number of the current write request
     * @param buf: data requested to be written
     * @param offset: The offset position of the request to write
     * @param length: The length of the data requested to be written
     * @param cost: The actual number of IOs generated by this request,
     * used for QOS control
     * @return: return error code
     */
    CSErrorCode Write(SequenceNum sn,
                      const butil::IOBuf& buf,
                      off_t offset,
                      size_t length,
                      uint32_t* cost,
                      std::shared_ptr<SnapContext> ctx = nullptr);
    
    CSErrorCode cloneWrite(SequenceNum sn,
                        const butil::IOBuf& buf,
                        off_t offset,
                        size_t length,
                        uint32_t* cost,
                        std::unique_ptr<CloneContext>& cloneCtx,
                        CSDataStore& datastore,
                        std::shared_ptr<SnapContext> ctx = nullptr);

    CSErrorCode flattenWrite(SequenceNum sn,
                        off_t offset, size_t length,
                        std::unique_ptr<CloneContext>& cloneCtx,
                        CSDataStore& datastore);


    CSErrorCode Sync();

    /**
     * Write the copied data into Chunk
     * Only write areas that have not been written, and will not overwrite
     * areas that have been written
     * There may be concurrency, add write lock
     * @param buf: request Paste data
     * @param offset: the starting offset of the data requesting Paste
     * @param length: the length of the data requested for Paste
     * @return: return error code
     */
    CSErrorCode Paste(const char * buf, off_t offset, size_t length);
    /**
     * Read chunk files
     * There may be concurrency, add read lock
     * @param buf: the data read
     * @param offset: the starting offset of the data requested to be read
     * @param length: The length of the data requested to be read
     * @return: return error code
     */
    CSErrorCode Read(char * buf, off_t offset, size_t length);

    /**
     * Read chunk meta data
     * There may be concurrency, add read lock
     * @param buf: the data read
     * @return: return error code
     */
    CSErrorCode ReadMetaPage(char * buf);

    /**
     * Read the chunk of the specified Sequence
     * There may be concurrency, add read lock
     * @param sn: SequenceNum of the specified chunk
     * @param buf: Snapshot data read
     * @param offset: the starting offset of the snapshot data requested to be
     *                read
     * @param length: The length of the snapshot data requested to be read
     * @return: return error code
     */
    CSErrorCode ReadSpecifiedChunk(SequenceNum sn,
                                   char * buf,
                                   off_t offset,
                                   size_t length);
    /**
     * Delete chunk files.
     * Normally there is no concurrency, mutually exclusive with other
     * operations, add write lock.
     * @param: The file sequence number when calling the DeleteChunk interface
     * @return: return error code
     */
    CSErrorCode Delete(SequenceNum sn);
    /**
     * Delete snapshots generated during this dump or left over from history.
     * If no snapshot is generated during the dump, modify the correctedSn of
     *  the chunk.
     * Normally there is no concurrency, mutually exclusive with other
     * operations, add write lock.
     * @param correctedSn: The sequence number of the chunk that needs to be
     * corrected, essentially the sequence number of the file after the
     * snapshot.
     * Corrected to this parameter value when the chunk does not have a
     *  snapshot.
     * @return: return error code
     */
    CSErrorCode DeleteSnapshot(SequenceNum snapSn, std::shared_ptr<SnapContext> ctx = nullptr);
    /**
     * Get chunk info
     * @param[out]: the chunk info getted
     */
    void GetInfo(CSChunkInfo* info);

    void GetCloneInfo(uint64_t& virtualId, uint64_t& cloneNo);
    /**
     * Get the hash value of the chunk, this interface is used for test
     * @param[out]: chunk hash value
     * @return: error code
     */
    CSErrorCode GetHash(off_t offset,
                        size_t length,
                        std::string *hash);
    /**
     * Get chunkFileMetaPage
     * @return: metapage
     */
    virtual const ChunkFileMetaPage& GetChunkFileMetaPage() {
        return metaPage_;
    }

    void SetChunkFileMetaPage(ChunkFileMetaPage metaPage) {
        metaPage_ = metaPage;
    }

    void SetSyncInfo(std::shared_ptr<std::atomic<uint64_t>> rate,
        std::shared_ptr<std::condition_variable> cond) {
        chunkrate_ = rate;
        cvar_ = cond;
    }

    ChunkID getChunkId() {
        return chunkId_;
    }

    uint64_t getCloneNumber() {
        return metaPage_.cloneNo;
    }

    ChunkID getVirtualId() {
        return metaPage_.virtualId; 
    }

    uint64_t getFileID() {
        return metaPage_.fileId;
    }

    CSErrorCode writeDataDirect(const butil::IOBuf& buf, off_t offset, size_t length); 

    bool DivideObjInfoByIndex (SequenceNum sn, 
                        std::vector<BitRange>& range, 
                        std::vector<BitRange>& notInRanges, 
                        std::vector<ObjectInfo>& objInfos);

    bool DivideObjInfoByIndexLockless (SequenceNum sn, 
                        std::vector<BitRange>& range, 
                        std::vector<BitRange>& notInRanges, 
                        std::vector<ObjectInfo>& objInfos);

    bool DivideSnapshotObjInfoByIndex (SequenceNum sn, 
                        std::vector<BitRange>& range, 
                        std::vector<BitRange>& notInRanges, 
                        std::vector<ObjectInfo>& objInfos);

    CSErrorCode ReadSpecifiedSnap (SequenceNum sn, 
                        CSSnapshot* snap, 
                        char* buff, 
                        off_t offset, 
                        size_t length);

    // default synclimit
    static uint64_t syncChunkLimits_;
    // high threshold limit
    static uint64_t syncThreshold_;

    int FindExtraReadFromParent(std::vector<File_ObjectInfoPtr>& objIns, 
                                CSChunkFilePtr& chunkFile, 
                                off_t offset, 
                                size_t length);

    void MergeObjectForRead(std::map<int32_t, Offset_InfoPtr>& objmap, 
                            std::vector<File_ObjectInfoPtr>& objIns, 
                            CSChunkFilePtr& chunkFile);

    CSErrorCode writeSnapData(SequenceNum sn,
                            const butil::IOBuf& buf,
                            off_t offset,
                            size_t length,
                            uint32_t* cost,
                            std::shared_ptr<SnapContext> ctx = nullptr);
 private:
     /**
     * Called when a snapshot file is deleted and merged to a non-existent snapshot file.
     * Write lock should NOT be added
     * @param sn: the sequence number of the snapshot file to be loaded
     * @return: return error code
     */
    CSErrorCode loadSnapshot(SequenceNum sn);
    /**
     * Determine whether you need to create a new snapshot
     * @param sn: write request sequence number
     * @return: true means to create a snapshot;
     *          false means no need to create a snapshot
     */
    bool needCreateSnapshot(SequenceNum sn, std::shared_ptr<SnapContext> ctx);
    /**
     * To create a snapshot chunk
     * @param sn: sequence number of the snapshot
     */
    CSErrorCode createSnapshot(SequenceNum sn);
    /**
     * Determine whether to copy on write
     * @param sn: write request sequence number
     * @return: true means cow is required; false means cow is not required
     */
    bool needCow(SequenceNum sn, std::shared_ptr<SnapContext> ctx);
    /**
     * Persist metapage
     * @param metaPage: the metapage that needs to be persisted to disk,
     * If it is successfully persisted, the metapage of the current memory
     * will be changed
     * If it fails, it will not be changed
     */
    CSErrorCode updateMetaPage(ChunkFileMetaPage* metaPage);
    /**
     * Load metapage into memory
     */
    CSErrorCode loadMetaPage();
    /**
     * Copy the uncopied data in the specified area from the chunk file
     * to the snapshot file
     * @param offset: the starting offset of the write data area
     * @param length: the length of the write data area
     * @return: return error code
     */
    CSErrorCode copy2Snapshot(off_t offset, size_t length);
    /**
     * Update the bitmap of the clone chunk
     * If all pages have been written, the clone chunk will be converted
     * to a normal chunk
     */
    CSErrorCode flush();

    inline string path() {
        return baseDir_ + "/" +
                FileNameOperator::GenerateChunkFileName(chunkId_);
    }

    inline uint32_t fileSize() const {
        return metaPageSize_ + size_;
    }

    inline int readMetaPage(char* buf) {
        return lfs_->Read(fd_, buf, 0, metaPageSize_);
    }

    inline int writeMetaPage(const char* buf) {
        return lfs_->Write(fd_, buf, 0, metaPageSize_);
    }

    inline int readData(char* buf, off_t offset, size_t length) {
        return lfs_->Read(fd_, buf, offset + metaPageSize_, length);
    }

    inline int writeData(const char* buf, off_t offset, size_t length) {
        int rc = lfs_->Write(fd_, buf, offset + metaPageSize_, length);
        if (rc < 0) {
            return rc;
        }
        // If it is a clone chunk, you need to determine whether you need to
        // change the bitmap and update the metapage
        if (isCloneChunk_) {
            uint32_t beginIndex = offset / blockSize_;
            uint32_t endIndex = (offset + length - 1) / blockSize_;
            for (uint32_t i = beginIndex; i <= endIndex; ++i) {
                // record dirty page
                if (!metaPage_.bitmap->Test(i)) {
                    dirtyPages_.insert(i);
                }
            }
        }
        return rc;
    }

    inline int writeData(const butil::IOBuf& buf, off_t offset, size_t length) {
        int rc = lfs_->Write(fd_, buf, offset + metaPageSize_, length);
        if (rc < 0) {
            return rc;
        }
        // If it is a clone chunk, you need to determine whether you need to
        // change the bitmap and update the metapage
        // page size to alignment
        if (isCloneChunk_) {
            uint32_t beginIndex = offset / blockSize_;
            uint32_t endIndex = (offset + length - 1) / blockSize_;
            for (uint32_t i = beginIndex; i <= endIndex; ++i) {
                // record dirty page
                if (!metaPage_.bitmap->Test(i)) {
                    dirtyPages_.insert(i);
                }
            }
        }
        return rc;
    }

    inline int SyncData() {
        return lfs_->Sync(fd_);
    }

    inline bool CheckOffsetAndLength(off_t offset, size_t len) {
        // Check if offset+len is out of bounds
        if (offset + len > size_) {
            return false;
        }

        return common::is_aligned(offset, blockSize_) &&
               common::is_aligned(len, blockSize_);
    }

    uint64_t MayUpdateWriteLimits(uint64_t write_len) {
        if (write_len > syncThreshold_) {
            return syncChunkLimits_ * 2;
        }
        return syncChunkLimits_;
    }

 private:
    // to notify syncThread
    std::shared_ptr<std::condition_variable> cvar_;
    // the sum of every chunkfile length
    std::shared_ptr<std::atomic<uint64_t>> chunkrate_;
    // file descriptor of chunk file
    int fd_;
    // The logical size of the chunk, not including metapage
    ChunkSizeType   size_;
    ChunkSizeType   blockSize_;
    PageSizeType    metaPageSize_;
    uint32_t        blockSize_shift_;
    // chunk id
    ChunkID chunkId_;
    // The directory where the chunk is located
    std::string baseDir_;
    // Is it a clone chunk
    bool isCloneChunk_;
    // chunk metapage
    ChunkFileMetaPage metaPage_;
    // has been written but has not yet been updated to the
    // page index in the metapage
    std::set<uint32_t> dirtyPages_;
    // read-write lock
    RWLock rwLock_;
    // Snapshot file pointer
    std::shared_ptr<CSSnapshots> snapshots_;
    // Rely on FilePool to create and delete files
    std::shared_ptr<FilePool> chunkFilePool_;
    // Rely on the local file system to manipulate files
    std::shared_ptr<LocalFileSystem> lfs_;
    // datastore internal statistical indicators
    std::shared_ptr<DataStoreMetric> metric_;
    // enable O_DSYNC When Open ChunkFile
    bool enableOdsyncWhenOpenChunkFile_;
};
}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_DATASTORE_CHUNKSERVER_CHUNKFILE_H_
