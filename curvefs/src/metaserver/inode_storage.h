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

#include <functional>
#include <utility>
#include <unordered_map>
#include <unordered_set>
#include <list>
#include <string>
#include <memory>

#include "absl/container/btree_set.h"
#include "absl/container/btree_map.h"
#include "src/common/concurrent/rw_lock.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/storage/converter.h"
#include "curvefs/src/metaserver/storage/utils.h"
#include "curvefs/src/metaserver/storage/storage.h"

using curve::common::RWLock;
using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;
using ::curvefs::metaserver::storage::Status;
using ::curvefs::metaserver::storage::Iterator;
using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::StorageTransaction;

namespace curvefs {
namespace metaserver {

using ::curvefs::metaserver::storage::Key4Inode;
using ::curvefs::metaserver::storage::Converter;
using ::curvefs::metaserver::storage::NameGenerator;
using S3ChunkInfoMap = google::protobuf::Map<uint64_t, S3ChunkInfoList>;

class InodeStorage {
 public:
    InodeStorage(std::shared_ptr<KVStorage> kvStorage,
                 std::shared_ptr<NameGenerator> tableName,
                 uint64_t nInode);

    /**
     * @brief insert inode to storage
     * @param[in] inode: the inode want to insert
     * @return If inode exist, return INODE_EXIST; else insert and return OK
     */
    MetaStatusCode Insert(const Inode& inode);

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
    MetaStatusCode GetAttr(const Key4Inode& key, InodeAttr *attr);

    /**
     * @brief get inode extended attributes from storage
     * @param[in] key: the key of inode want to get
     * @param[out] attr: the inode extended attribute got
     * @return If inode not exist, return NOT_FOUND; else return OK
     */
    MetaStatusCode GetXAttr(const Key4Inode& key, XAttr *xattr);

    /**
     * @brief delete inode from storage
     * @param[in] key: the key of inode want to delete
     * @return If inode not exist, return NOT_FOUND; else return OK
     */
    MetaStatusCode Delete(const Key4Inode& key);

    /**
     * @brief update inode from storage
     * @param[in] inode: the inode want to update
     * @return If inode not exist, return NOT_FOUND; else replace and return OK
     */
    MetaStatusCode Update(const Inode& inode);

    std::shared_ptr<Iterator> GetAllInode();

    bool GetAllInodeId(std::list<uint64_t>* ids);

    // NOTE: the return size is accurate under normal cluster status,
    // but under abnormal status, the return size maybe less than
    // the real value for delete the unexist inode multi-times.
    size_t Size();

    bool Empty();

    MetaStatusCode Clear();

    // s3chunkinfo
    MetaStatusCode ModifyInodeS3ChunkInfoList(uint32_t fsId,
                                              uint64_t inodeId,
                                              uint64_t chunkIndex,
                                              const S3ChunkInfoList* list2add,
                                              const S3ChunkInfoList* list2del);

    MetaStatusCode PaddingInodeS3ChunkInfo(int32_t fsId,
                                           uint64_t inodeId,
                                           S3ChunkInfoMap* m,
                                           uint64_t limit = 0);

    std::shared_ptr<Iterator> GetInodeS3ChunkInfoList(uint32_t fsId,
                                                      uint64_t inodeId);

    std::shared_ptr<Iterator> GetAllS3ChunkInfoList();

    // volume extent
    std::shared_ptr<Iterator> GetAllVolumeExtentList();

    MetaStatusCode UpdateVolumeExtentSlice(uint32_t fsId,
                                           uint64_t inodeId,
                                           const VolumeExtentSlice& slice);

    MetaStatusCode GetAllVolumeExtent(uint32_t fsId,
                                      uint64_t inodeId,
                                      VolumeExtentList* extents);

    MetaStatusCode GetVolumeExtentByOffset(uint32_t fsId,
                                           uint64_t inodeId,
                                           uint64_t offset,
                                           VolumeExtentSlice* slice);

 private:
    bool UpdateInodeS3MetaSize(uint32_t fsId, uint64_t inodeId,
                               uint64_t size4add, uint64_t size4del);

    uint64_t GetInodeS3MetaSize(uint32_t fsId, uint64_t inodeId);

    MetaStatusCode AddS3ChunkInfoList(
        std::shared_ptr<StorageTransaction> txn,
        uint32_t fsId,
        uint64_t inodeId,
        uint64_t chunkIndex,
        const S3ChunkInfoList* list2add);

    MetaStatusCode DelS3ChunkInfoList(
        std::shared_ptr<StorageTransaction> txn,
        uint32_t fsId,
        uint64_t inodeId,
        uint64_t chunkIndex,
        const S3ChunkInfoList* list2del);

 private:
    RWLock rwLock_;
    Converter conv_;
    size_t nInode_;
    std::string table4Inode_;
    std::string table4S3ChunkInfo_;
    std::string table4VolumeExtent_;
    std::string table4InodeAuxInfo_;
    std::shared_ptr<KVStorage> kvStorage_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_INODE_STORAGE_H_
