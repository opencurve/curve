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
 * Created Date: Tuesday December 4th 2018
 * Author: yangyaokai
 */

#ifndef SRC_CHUNKSERVER_DATASTORE_DEFINE_H_
#define SRC_CHUNKSERVER_DATASTORE_DEFINE_H_

#include <string>
#include <memory>
#include <vector>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/bitmap.h"

// switch on/off memory sanity check
#define MEMORY_SANITY_CHECK

namespace curve {
namespace chunkserver {

class ChunkRequest;
class CSChunkFile;
class CSSnapshot;

using CSChunkFilePtr = std::shared_ptr<CSChunkFile>;
using CSSnapshotPtr = std::shared_ptr<CSSnapshot>;

using curve::common::Bitmap;

const uint8_t FORMAT_VERSION = 1;
const SequenceNum kInvalidSeq = 0;

DECLARE_uint32(minIoAlignment);

/*
#define OBJ_SIZE 65536
#define OBJ_SIZE_SHIFT 16
#define OBJ_SIZE_MASK 0xFFFF
*/
#define OBJ_SIZE 4096
#define OBJ_SIZE_SHIFT 12
#define OBJ_SIZE_MASK 0xFFF

#define PAGE_SIZE_SHIFT 12
#define PAGE_SIZE_MASK 0xFFF

#define OBJECTINFO_SIZE 16
struct ObjectInfo {
    ObjectInfo() : snapptr(nullptr)
                 , sn(0)
                 , offset(0)
                 , length(0) {}

    CSSnapshot* snapptr;
    SequenceNum sn;
    uint32_t offset;
    uint32_t length;
};

struct File_ObjectInfo {
    CSChunkFilePtr fileptr;
    std::vector<struct ObjectInfo> obj_infos;
};

using File_ObjectInfoPtr = std::unique_ptr<File_ObjectInfo>;

struct File_Object {
    File_Object(CSChunkFilePtr& ptr, struct ObjectInfo info)  // NOLINT
      : fileptr(ptr) , obj(info) { }

    CSChunkFilePtr& fileptr;
    struct ObjectInfo obj;
};

struct Offset_Info {
    uint32_t offset;
    uint32_t length;
    std::vector<File_Object> objs;
};

using Offset_InfoPtr = std::unique_ptr<Offset_Info>;

// define error code
enum CSErrorCode {
    // success
    Success = 0,
    // Internal error, usually an error during system call
    InternalError = 1,
    // Version is not compatible
    IncompatibleError = 2,
    // crc verification failed
    CrcCheckError = 3,
    // The file format is incorrect, for example the file length is incorrect
    FileFormatError = 4,
    // Snapshot conflict, there are multiple snapshot files
    SnapshotConflictError = 5,
    // For requests with outdated sequences, it is normal if the error is thrown
    // during log recovery
    BackwardRequestError = 6,
    // Thrown when there is a snapshot file when deleting a chunk, it is not
    // allowed to delete a chunk with a snapshot
    SnapshotExistError = 7,
    // The chunk requested to read and write does not exist
    ChunkNotExistError = 8,
    // The area requested to read and write exceeds the size of the file
    OutOfRangeError = 9,
    // Parameter error
    InvalidArgError = 10,
    // There are conflicting chunks when creating chunks
    ChunkConflictError = 11,
    // Status conflict
    StatusConflictError = 12,
    // The page has not been written, it will appear when the page that has not
    // been written is read when the clone chunk is read
    PageNerverWrittenError = 13,
    // Thrown when given snapshot is not found for a chunk.
    SnapshotNotExistError = 14,
    // Flatten Again Flag
    FlattenAgain = 15,
};

// Chunk details
struct CSChunkInfo {
    // the id of the chunk
    ChunkID chunkId;
    // page size
    uint32_t metaPageSize;
    // The size of the chunk
    uint32_t chunkSize;
    // The size of the block, smallest read/write granularity and alignment
    uint32_t blockSize;
    // The sequence number of the chunk file
    SequenceNum curSn;
    // The sequence number of the chunk snapshot,
    // if the snapshot does not exist, it is 0
    SequenceNum snapSn;
    // The revised sequence number of the chunk
    SequenceNum correctedSn;
    // Indicates whether the chunk is CloneChunk
    bool isClone;
    // If it is CloneChunk, it indicates the location of the data source;
    // otherwise it is empty
    std::string location;
    // If it is CloneChunk, it means the state of the current Chunk page,
    // otherwise it is nullptr
    std::shared_ptr<Bitmap> bitmap;
    CSChunkInfo() : chunkId(0)
                  , metaPageSize(4096)
                  , chunkSize(16 * 4096 * 4096)
                  , blockSize(4096)
                  , curSn(0)
                  , snapSn(0)
                  , correctedSn(0)
                  , isClone(false)
                  , location("")
                  , bitmap(nullptr) {}

    bool operator== (const CSChunkInfo& rhs) const {
        if (chunkId != rhs.chunkId ||
            metaPageSize != rhs.metaPageSize ||
            chunkSize != rhs.chunkSize ||
            blockSize != rhs.blockSize ||
            curSn != rhs.curSn ||
            snapSn != rhs.snapSn ||
            correctedSn != rhs.correctedSn ||
            isClone != rhs.isClone ||
            location != rhs.location) {
            return false;
        }
        // If the bitmap is not nullptr, compare whether the contents are equal
        if (bitmap != nullptr && rhs.bitmap != nullptr) {
            if (*bitmap != *rhs.bitmap)
                return false;
        } else {
            // Determine whether both are nullptr
            if (bitmap != rhs.bitmap)
                return false;
        }

        return true;
    }

    bool operator!= (const CSChunkInfo& rhs) const {
        return !(*this == rhs);
    }
};

class SnapContext {
 public:
    explicit SnapContext(const std::vector<SequenceNum>& snaps);
    virtual ~SnapContext() = default;

    static std::shared_ptr<SnapContext> build_empty() {
        std::shared_ptr<SnapContext> ptr(new SnapContext());
        return ptr;
    }

    SequenceNum getNext(SequenceNum snapSn) const;
    SequenceNum getPrev(SequenceNum snapSn) const;
    SequenceNum getLatest() const;
    bool contains(SequenceNum snapSn) const;
    bool empty() const;

 private:
    SnapContext() = default;
    // existing snapshot sequences, in descending order.
    std::vector<SequenceNum> snaps;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_DATASTORE_DEFINE_H_
