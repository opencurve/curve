/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#ifndef CURVEFS_SRC_METASERVER_INODE_STORAGE_H_
#define CURVEFS_SRC_METASERVER_INODE_STORAGE_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/container/btree_map.h"
#include "absl/container/btree_set.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/storage/converter.h"
#include "curvefs/src/metaserver/storage/status.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/utils.h"
#include "src/common/concurrent/rw_lock.h"

#define DELETING_PREFIX "deleting_"
namespace curvefs {
namespace metaserver {

using ::curve::common::RWLock;
using ::curvefs::metaserver::storage::Converter;
using ::curvefs::metaserver::storage::Iterator;
using ::curvefs::metaserver::storage::Key4Inode;
using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::NameGenerator;
using ::curvefs::metaserver::storage::StorageTransaction;

using S3ChunkInfoMap = google::protobuf::Map<uint64_t, S3ChunkInfoList>;
using DeallocatableBlockGroupVec =
    google::protobuf::RepeatedPtrField<DeallocatableBlockGroup>;
using Transaction = std::shared_ptr<StorageTransaction>;

class InodeStorage {
 public:
    InodeStorage(std::shared_ptr<KVStorage> kvStorage,
                 std::shared_ptr<NameGenerator> nameGenerator, uint64_t nInode);

    MetaStatusCode GetAppliedIndex(int64_t* index);

    bool Init();

    /**
     * @brief insert inode to storage
     * @param[in] inode: the inode want to insert
     * @param[in] logIndex: the index of raft log
     * @return If inode exist, return INODE_EXIST; else insert and return OK
     */
    MetaStatusCode Insert(const Inode& inode, int64_t logIndex);

    /**
     * @brief update deleting inode key in storage
     * @param[in] inode: the inode want to update
     * @param[in] logIndex: the index of raft log
     * @return
     */
    MetaStatusCode UpdateDeletingKey(const Inode& inode, int64_t logIndex);

    MetaStatusCode ClearDelKey(const Key4Inode& key);
    /**
     * @brief get inode from storage
     * @param[in] key: the key of inode want to get
     * @param[out] inode: the inode got
     * @return If inode not exist, return NOT_FOUND; else return OK
     */
    MetaStatusCode Get(const Key4Inode& key, Inode* inode);

    /**
     * @brief get inode attribute from storage
     * @param[in] key: the key of inode want to get
     * @param[out] attr: the inode attribute got
     * @return If inode not exist, return NOT_FOUND; else return OK
     */
    MetaStatusCode GetAttr(const Key4Inode& key, InodeAttr* attr);

    /**
     * @brief get inode extended attributes from storage
     * @param[in] key: the key of inode want to get
     * @param[out] attr: the inode extended attribute got
     * @return If inode not exist, return NOT_FOUND; else return OK
     */
    MetaStatusCode GetXAttr(const Key4Inode& key, XAttr* xattr);

    /**
     * @brief delete inode from storage
     * @param[in] key: the key of inode want to delete
     * @param[in] logIndex: the index of raft log
     * @return If inode not exist, return NOT_FOUND; else return OK
     */
    MetaStatusCode Delete(const Key4Inode& key, int64_t logIndex);

    MetaStatusCode ForceDelete(const Key4Inode& key);

    /**
     * @brief update inode from storage
     * @param[in] inode: the inode want to update
     * @param[in] inodeDeallocate: Whether the inode needs to deallocate space
     * @return If inode not exist, return NOT_FOUND; else replace and return OK
     */
    MetaStatusCode Update(const Inode& inode, int64_t logIndex,
                          bool inodeDeallocate = false);

    MetaStatusCode Update(std::shared_ptr<storage::StorageTransaction>* txn,
                          const Inode& inode, int64_t logIndex,
                          bool inodeDeallocate = false);

    std::shared_ptr<Iterator> GetAllInode();

    bool GetAllInodeId(std::list<uint64_t>* ids);

    // NOTE: the return size is accurate under normal cluster status,
    // but under abnormal status, the return size maybe less than
    // the real value for delete the unexist inode multi-times.
    size_t Size();

    bool Empty();

    MetaStatusCode Clear();

    // s3chunkinfo
    MetaStatusCode ModifyInodeS3ChunkInfoList(uint32_t fsId, uint64_t inodeId,
                                              uint64_t chunkIndex,
                                              const S3ChunkInfoList* list2add,
                                              const S3ChunkInfoList* list2del,
                                              int64_t logIndex);

    MetaStatusCode ModifyInodeS3ChunkInfoList(
        std::shared_ptr<StorageTransaction>* txn, uint32_t fsId,
        uint64_t inodeId, uint64_t chunkIndex, const S3ChunkInfoList* list2add,
        const S3ChunkInfoList* list2del, int64_t logIndex);

    MetaStatusCode PaddingInodeS3ChunkInfo(int32_t fsId, uint64_t inodeId,
                                           S3ChunkInfoMap* m,
                                           uint64_t limit = 0);

    std::shared_ptr<Iterator> GetInodeS3ChunkInfoList(uint32_t fsId,
                                                      uint64_t inodeId);

    std::shared_ptr<Iterator> GetAllS3ChunkInfoList();

    // volume extent
    std::shared_ptr<Iterator> GetAllVolumeExtentList();

    MetaStatusCode UpdateVolumeExtentSlice(uint32_t fsId, uint64_t inodeId,
                                           const VolumeExtentSlice& slice,
                                           int64_t logIndex);

    MetaStatusCode UpdateVolumeExtentSlice(
        std::shared_ptr<storage::StorageTransaction>* txn, uint32_t fsId,
        uint64_t inodeId, const VolumeExtentSlice& slice, int64_t logIndex);

    MetaStatusCode GetAllVolumeExtent(uint32_t fsId, uint64_t inodeId,
                                      VolumeExtentSliceList* extents);

    std::shared_ptr<Iterator> GetAllVolumeExtent(uint32_t fsId,
                                                 uint64_t inodeId);

    MetaStatusCode GetVolumeExtentByOffset(uint32_t fsId, uint64_t inodeId,
                                           uint64_t offset,
                                           VolumeExtentSlice* slice);

    // use the transaction to delete {inodes} in the deallocatable_inode_list
    // and update the statistics of each item of blockgroup_list
    MetaStatusCode UpdateDeallocatableBlockGroup(
        uint32_t fsId, const DeallocatableBlockGroupVec& update,
        int64_t logIndex);

    MetaStatusCode GetAllBlockGroup(
        std::vector<DeallocatableBlockGroup>* deallocatableBlockGroupVec);

    std::shared_ptr<KVStorage> GetKVStorage() {
        return kvStorage_;
    }

 private:
    MetaStatusCode UpdateInodeS3MetaSize(Transaction txn, uint32_t fsId,
                                         uint64_t inodeId, uint64_t size4add,
                                         uint64_t size4del);

    uint64_t GetInodeS3MetaSize(uint32_t fsId, uint64_t inodeId);

    MetaStatusCode DelS3ChunkInfoList(Transaction txn, uint32_t fsId,
                                      uint64_t inodeId, uint64_t chunkIndex,
                                      const S3ChunkInfoList* list2del);

    MetaStatusCode AddS3ChunkInfoList(Transaction txn, uint32_t fsId,
                                      uint64_t inodeId, uint64_t chunkIndex,
                                      const S3ChunkInfoList* list2add);

    MetaStatusCode Increase(Transaction txn, uint32_t fsId,
                            const IncreaseDeallocatableBlockGroup& increase,
                            DeallocatableBlockGroup* out);

    MetaStatusCode Decrease(const DecreaseDeallocatableBlockGroup& decrease,
                            DeallocatableBlockGroup* out);

    MetaStatusCode Mark(const MarkDeallocatableBlockGroup& mark,
                        DeallocatableBlockGroup* out);

    storage::Status SetAppliedIndex(storage::StorageTransaction* transaction,
                                    int64_t index);

    storage::Status DelAppliedIndex(storage::StorageTransaction* transaction);

    storage::Status GetInodeCount(std::size_t* count);

    storage::Status SetInodeCount(storage::StorageTransaction* transaction,
                                  std::size_t count);

    storage::Status DelInodeCount(storage::StorageTransaction* transaction);

    // NOTE: if transaction success
    // we will commit transaction
    // it should be the last step of your operations
    storage::Status DeleteInternal(storage::StorageTransaction* transaction,
                                   const Key4Inode& key);

 private:
    // FIXME: please remove this lock, because we has locked each inode
    // in inode manager, this lock only for proetct storage, but now we
    // use rocksdb storage, it support write in parallel.
    RWLock rwLock_;
    std::shared_ptr<KVStorage> kvStorage_;
    std::string table4Inode_;
    std::string table4S3ChunkInfo_;
    std::string table4VolumeExtent_;
    std::string table4InodeAuxInfo_;
    std::string table4DeallocatableBlockGroup_;
    std::string table4DeallocatableInode_;
    std::string table4AppliedIndex_;
    std::string table4InodeCount_;

    size_t nInode_;
    Converter conv_;

    static const char* kInodeCountKey;
    static const char* kInodeAppliedKey;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_INODE_STORAGE_H_
