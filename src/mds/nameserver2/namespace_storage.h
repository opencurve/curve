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
 * Created Date: Friday September 7th 2018
 * Author: hzsunjianliang
 */
#ifndef SRC_MDS_NAMESERVER2_NAMESPACE_STORAGE_H_
#define SRC_MDS_NAMESERVER2_NAMESPACE_STORAGE_H_

#include <string>
#include <tuple>
#include <vector>
#include <iostream>
#include <map>
#include <memory>
#include "proto/nameserver2.pb.h"

#include "src/common/encode.h"
#include "src/mds/common/mds_define.h"
#include "src/kvstorageclient/etcd_client.h"
#include "src/mds/nameserver2/metric.h"
#include "src/common/lru_cache.h"

namespace curve {
namespace mds {

using ::curve::kvstorage::EtcdClientImp;
////using ::curve::kvstorage::StorageClient;
using Cache =
    ::curve::common::LRUCacheInterface<std::string, std::string>;

enum class StoreStatus {
    OK = 0,
    KeyNotExist,
    InternalError,
};
std::ostream& operator << (std::ostream & os, StoreStatus &s);

// TODO(hzsunjianliang): may be storage need high level abstraction
// put the encoding internal, not external


// kv value storage for namespace and segment
class NameServerStorage {
 public:
  virtual ~NameServerStorage(void) {}

    /**
     * @brief PutFile Store fileInfo
     *
     * @param[in] fileInfo
     *
     * @return StoreStatus: error code
     */
    virtual StoreStatus PutFile(const FileInfo & fileInfo) = 0;

    /**
     * @brief GetFile Get metadata of the specified file
     *
     * @param[in] id: Parent inode ID of the file to be obtained
     * @param[in] filename
     * @param[out] file info obtained
     *
     * @return StoreStatus: error code
     */
    virtual StoreStatus GetFile(InodeID id,
                                const std::string &filename,
                                FileInfo * fileInfo) = 0;

    /**
     * @brief DeleteFile
     *
     * @param[in] id: Parent inode ID of the file to be deleted
     * @param[in] filename
     *
     * @return StoreStatus: error code
     */
    virtual StoreStatus DeleteFile(InodeID id,
                                const std::string &filename) = 0;

    /**
     * @brief DeleteSnapshotFile
     *
     * @param[in] id: Parent inode ID of the target file
     * @param[in] filename
     *
     * @return StoreStatus: error code
     */
    virtual StoreStatus DeleteSnapshotFile(InodeID id,
                                const std::string &filename) = 0;

    /**
     * @brief RenameFile: Transaction for storing metadata of new file and
     *                    delete old metadata
     *
     * @param[in] oldFileInfo
     * @param[in] newFileInfo
     *
     * @return StoreStaus: error code
     */
    virtual StoreStatus RenameFile(const FileInfo &oldfileInfo,
                                    const FileInfo &newfileInfo) = 0;
    /**
     * @brief ReplaceFileAndRecycleOldFile Transaction for storing the metadata
     *                                     of the new file and delete the old
     *                                     one. The new file has been occupied
     *                                     by conflictFInfo, and the occupied
     *                                     file needs to be moved to the recycle
     *                                     bin.
     *
     * @param[in] oldFileInfo
     * @param[in] newFileInfo
     * @param[in] conflictFInfo
     * @param[in] recycleFInfo
     *
     * @return StoreStaus: error code
     */
    virtual StoreStatus ReplaceFileAndRecycleOldFile(
        const FileInfo &oldFInfo, const FileInfo &newFInfo,
        const FileInfo &conflictFInfo, const FileInfo &recycleFInfo) = 0;

    /**
     * @brief MoveFileToRecycle Transaction for deleting the old metadata, and
     *                          the original file will becomes recycle file
     *
     * @param[in] originFileInfo: Files to be deleted
     * @param[in] recycleFileInfo: Files to be placed in recycle bin (same file)
     *
     * @return StoreStaus: error code
     */
    virtual StoreStatus MoveFileToRecycle(
        const FileInfo &originFileInfo, const FileInfo &recycleFileInfo) = 0;

    /**
     * @brief ListFile: Get all files between [startid, endid)
     *
     * @param[in] startidid
     * @param[in] endid
     * @param[out] files
     *
     * @return StoreStatus: error code
     */
    virtual StoreStatus ListFile(InodeID startid,
                                InodeID endid,
                                std::vector<FileInfo> * files) = 0;

    /**
     * @brief ListSegment: Get all the segments between [startid, endid)
     *
     * @param[in] id: Inode ID of the file
     * @param[out] segments: Segment list
     *
     * @return StoreStatus: error code
     */
    virtual StoreStatus ListSegment(InodeID id,
                                    std::vector<PageFileSegment> *segments) = 0;

    /**
     * @brief ListSnapshotFile: Get all snapshot files between [startid, endid)
     *
     * @param[in] startidid
     * @param[in] endid
     * @param[out] files
     *
     * @return StoreStatus: error code
     */
    virtual StoreStatus ListSnapshotFile(InodeID startid,
                                InodeID endid,
                                std::vector<FileInfo> * files) = 0;

    /**
     * @brief GetSegment: Obtain specified segment information
     *
     * @param[in] id: Inode ID of the target file
     * @param[in] off: Offset of the target segment
     * @param[out] segment: Segment info
     *
     * @return StoreStatus: error code
     */
    virtual StoreStatus GetSegment(InodeID id,
                                    uint64_t off,
                                    PageFileSegment *segment) = 0;

    /**
     * @brief PutSegment: Store specified segment information
     *
     * @param[in] id: Inode ID of the target file
     * @param[in] off: Offset of the target segment
     * @param[out] segment: Segment info
     * @param[out] revision: The version number of this operation
     *
     * @return StoreStatus: error code
     */
    virtual StoreStatus PutSegment(InodeID id,
                                    uint64_t off,
                                    const PageFileSegment * segment,
                                    int64_t *revision) = 0;

    /**
     * @brief DeleteSegment: Delete the specified segment metadata
     *
     * @param[in] id: Inode ID of the target file
     * @param[in] off: Offset of the target segment
     * @param[out] revision: The version number of this operation
     *
     * @return StoreStatus: error code
     */
    virtual StoreStatus DeleteSegment(
        InodeID id, uint64_t off, int64_t *revision) = 0;

    /**
     * @brief Move segment metadata from SegmentTable to DiscardSegmentTable,
     *        another background task will delete all chunks and delete segment
     *        in DiscardSegmentTable
     * @param[in] id: Inode ID of the target file
     * @param[in] off: Offset of the target segment
     *
     * @return StoreStatus: error code
     */
    virtual StoreStatus DiscardSegment(const FileInfo& fileInfo,
                                       const PageFileSegment& segment) = 0;

    /**
     * @brief Remove discard segment from DiscardSegmentTable
     * @param[in] segmentSize segment's size
     * @param[in] key discard segment's key
     * @param[out] revision: the version number of this operation
     * @return On success, return StoreStatus::OK
     */
    virtual StoreStatus CleanDiscardSegment(uint64_t segmentSize,
                                            const std::string& key,
                                            int64_t* revision) = 0;

    /**
     * @brief list all segment from DiscardSegmentTable
     * @param[out] store key and values
     * @return On success, return StoreStatus::OK
     */
    virtual StoreStatus ListDiscardSegment(
        std::map<std::string, DiscardSegmentInfo>* out) = 0;

    /**
     * @brief SnapShotFile: Transaction for storing metadata of snapshotFile,
     *                      and update source file metadata
     *
     * @param[in] originalFileInfo: Metadata of the source file to take snapshot
     * @param[in] snapshotFileInfo: Metadata of the snapshot file
     *
     * @return StoreStatus: error code
     */
    virtual StoreStatus SnapShotFile(const FileInfo *originalFileInfo,
                                    const FileInfo *snapshotFileInfo) = 0;

    /**
     * @brief LoadSnapShotFile: Load all snapshotFile metadata
     *
     * @param[out] snapshotFiles: Snapshot metadata list
     *
     * @retrun StoreStatus: error code
     */
    virtual StoreStatus LoadSnapShotFile(
                                    std::vector<FileInfo> *snapShotFiles) = 0;
};

class NameServerStorageImp : public NameServerStorage {
 public:
    explicit NameServerStorageImp(
        std::shared_ptr<StorageClient> client, std::shared_ptr<Cache> cache);
    ~NameServerStorageImp() {}

    StoreStatus PutFile(const FileInfo & fileInfo) override;

    StoreStatus GetFile(InodeID id,
                        const std::string &filename,
                        FileInfo * fileInfo) override;

    StoreStatus DeleteFile(InodeID id,
                            const std::string &filename) override;

    StoreStatus DeleteSnapshotFile(InodeID id,
                         const std::string &filename) override;

    StoreStatus RenameFile(const FileInfo &oldfileInfo,
                            const FileInfo &newfileInfo) override;

    StoreStatus ReplaceFileAndRecycleOldFile(const FileInfo &oldFInfo,
                                        const FileInfo &newFInfo,
                                        const FileInfo &conflictFInfo,
                                        const FileInfo &recycleFInfo) override;

    StoreStatus MoveFileToRecycle(const FileInfo &originFileInfo,
                                const FileInfo &recycleFileInfo) override;

    StoreStatus ListFile(InodeID startid,
                        InodeID endid,
                        std::vector<FileInfo> * files) override;

    StoreStatus ListSegment(InodeID id,
                            std::vector<PageFileSegment> *segments) override;

    StoreStatus ListSnapshotFile(InodeID startid,
                        InodeID endid,
                        std::vector<FileInfo> * files) override;

    StoreStatus GetSegment(InodeID id,
                            uint64_t off,
                            PageFileSegment *segment) override;

    StoreStatus PutSegment(InodeID id,
                            uint64_t off,
                            const PageFileSegment * segment,
                            int64_t *revision) override;

    StoreStatus DeleteSegment(
        InodeID id, uint64_t off, int64_t *revision) override;

    StoreStatus DiscardSegment(const FileInfo& fileInfo,
                             const PageFileSegment& segment) override;

    StoreStatus CleanDiscardSegment(uint64_t segmentSize,
                                    const std::string& key,
                                    int64_t* revision) override;

    StoreStatus ListDiscardSegment(
        std::map<std::string, DiscardSegmentInfo>* out) override;

    StoreStatus SnapShotFile(const FileInfo *originalFileInfo,
                            const FileInfo * snapshotFileInfo) override;

    StoreStatus LoadSnapShotFile(std::vector<FileInfo> *snapShotFiles) override;

 private:
    StoreStatus ListFileInternal(const std::string& startStoreKey,
                                 const std::string& endStoreKey,
                                 std::vector<FileInfo> *files);
    StoreStatus GetStoreKey(FileType filetype,
                            InodeID id,
                            const std::string& filename,
                            std::string* storekey);
    StoreStatus getErrorCode(int errCode);

 private:
    // namespace-meta cache
    std::shared_ptr<Cache> cache_;

    // underlying storage
    std::shared_ptr<StorageClient> client_;

    // metric for discard
    SegmentDiscardMetric discardMetric_;
};
}  // namespace mds
}  // namespace curve


#endif   // SRC_MDS_NAMESERVER2_NAMESPACE_STORAGE_H_