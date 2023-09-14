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
 * Created Date: Wednesday November 28th 2018
 * Author: yangyaokai
 */

#ifndef SRC_CHUNKSERVER_DATASTORE_CHUNKSERVER_SNAPSHOT_H_
#define SRC_CHUNKSERVER_DATASTORE_CHUNKSERVER_SNAPSHOT_H_

#include <glog/logging.h>
#include <string>
#include <memory>
#include <set>

#include "src/common/bitmap.h"
#include "src/common/crc32.h"
#include "src/chunkserver/datastore/filename_operator.h"
#include "src/chunkserver/datastore/define.h"
#include "src/fs/local_filesystem.h"
#include "src/chunkserver/datastore/file_pool.h"

namespace curve {
namespace chunkserver {

using curve::common::Bitmap;
using curve::fs::LocalFileSystem;

class FilePool;
class CSChunkFile;
struct ChunkOptions;
struct DataStoreMetric;

/**
 * Snapshot Metapage Format
 * version: 1 byte
 * damaged: 1 bytes
 * sn: 8 bytes
 * bits: 4 bytes
 * bitmap: (bits + 8 - 1) / 8 bytes
 * crc: 4 bytes
 * padding: (4096 - 18 - (bits + 8 - 1) / 8) bytes
 */
struct SnapshotMetaPage {
    // File format version number
    uint8_t version;
    // Indicates whether the current snapshot is damaged
    bool damaged;
    // Snapshot sequence number
    SequenceNum sn;
    // bitmap  representing the current snapshot page status
    std::shared_ptr<Bitmap> bitmap;

    SnapshotMetaPage()  
        : version(FORMAT_VERSION), damaged(false), bitmap(nullptr) {}
    SnapshotMetaPage(const SnapshotMetaPage &metaPage);
    SnapshotMetaPage &operator=(const SnapshotMetaPage &metaPage);

    void encode(char* buf);
    CSErrorCode decode(const char* buf);
};

class CSSnapshot {
 public:
    CSSnapshot(std::shared_ptr<LocalFileSystem> lfs,
               std::shared_ptr<FilePool> chunkFilePool,
               const ChunkOptions& options);
    virtual ~CSSnapshot();
    /**
     * open snapshot file, called when starting to load snapshot file or create
     * new snapshot file
     * @param createFile: true means to create a new file,
     *                    false to not create a file
     * @return: return error code
     */
    CSErrorCode Open(bool createFile);
    /**
     * Write the data into the snapshot file, the bitmap will not be updated
     * immediately after the data is written,
     * Need to be updated by calling Flush
     * @param buf: data requested to be written
     * @param offset: The actual offset requested to be written
     * @param length: The length of the data requested to be written
     * @return: return error code
     */
    CSErrorCode Write(const char* buf, off_t offset, size_t length);
    /**
     * Read the snapshot data, according to the bitmap to determine whether to
     * read the data from the chunk file
     * @param buf: Snapshot data read
     * @param offset: the starting offset of the request to read
     * @param length: The length of the data requested to be read
     * @return: return error code
     */
    CSErrorCode Read(char* buf, off_t offset, size_t length);
    /**
     * Delete snapshot files
     * @return: return error code
     */
    CSErrorCode Delete();
    /**
     * Write the metapage of the snapshot to pagecache, and add the
     * snapshot to the sync queue
     * @return: Returns 0 if successful, returns an error code on failure,
     * and the error code is a negative number
     */
    CSErrorCode Flush();
    /**
     * Get the snapshot sequence number
     * @return: Return the snapshot sequence number
     */
    SequenceNum GetSn() const;
    /**
     * Get a bitmap representing the page status of the snapshot file
     * @return: return bitmap
     */
    std::shared_ptr<const Bitmap> GetPageStatus() const;

 private:
    /**
     * Persist metapage
     * @param metaPage: the metapage that needs to be persisted to disk,
     * If it is successfully persisted, the metapage of the current memory
     * will be changed
     * If it fails, it will not be changed
     */
    CSErrorCode updateMetaPage(SnapshotMetaPage* metaPage);
    /**
     * Load metapage into memory
     */
    CSErrorCode loadMetaPage();

    inline string path() {
        return baseDir_ + "/" +
               FileNameOperator::GenerateSnapshotName(chunkId_, metaPage_.sn);
    }

    uint32_t fileSize() const {return metaPageSize_ + size_;}

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
        return lfs_->Write(fd_, buf, offset + metaPageSize_, length);
    }

 private:
    // Snapshot file descriptor
    int fd_;
    // The id of the chunk to which the snapshot belongs
    ChunkID chunkId_;
    // Logical size of the snapshot file, excluding metapage
    ChunkSizeType size_;
    // The smallest atomic read and write unit, which is also the size of
    // the metapage
    PageSizeType pageSize_;
    // The directory where the snapshot file is located
    std::string baseDir_;
    // The metapage of the snapshot file
    SnapshotMetaPage metaPage_;
    // page index has been written but has not yet been updated to the in
    // the metapage
    std::set<uint32_t> dirtyPages_;
    // Rely on the local file system to manipulate files
    std::shared_ptr<LocalFileSystem> lfs_;
    // Rely on FilePool to create and delete files
    std::shared_ptr<FilePool> chunkFilePool_;
    // datastore internal statistical indicators
    std::shared_ptr<DataStoreMetric> metric_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_DATASTORE_CHUNKSERVER_SNAPSHOT_H_
