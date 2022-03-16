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
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <list>
#include <string>
#include <memory>

#include "absl/container/btree_set.h"
#include "src/common/concurrent/rw_lock.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/storage/storage.h"

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;
using curve::common::RWLock;
using ::curvefs::metaserver::storage::Status;
using ::curvefs::metaserver::storage::Iterator;
using KVStorage = ::curvefs::metaserver::storage::KVStorage;

namespace curvefs {
namespace metaserver {

struct InodeKey {
    uint32_t fsId;
    uint64_t inodeId;

    InodeKey(uint32_t fs, uint64_t inode) : fsId(fs), inodeId(inode) {}

    explicit InodeKey(const Inode &inode)
        : fsId(inode.fsid()), inodeId(inode.inodeid()) {}

    explicit InodeKey(const std::shared_ptr<Inode> &inode)
        : fsId(inode->fsid()), inodeId(inode->inodeid()) {}

    bool operator==(const InodeKey &k1) const {
        return k1.fsId == fsId && k1.inodeId == inodeId;
    }
};

class InodeStorage {
 public:
    InodeStorage(std::shared_ptr<KVStorage> kvStorage,
                 const std::string& tablename); // TODO(@Wine93): 这里需要一个 INDOE/DENTRY 来做区分

    /**
     * @brief insert inode to storage
     *
     * @param[in] inode: the inode want to insert
     *
     * @return If inode exist, return INODE_EXIST; else insert and return OK
     */
    MetaStatusCode Insert(const Inode& inode);

    /**
     * @brief get inode from storage
     *
     * @param[in] key: the key of inode want to get
     * @param[out] inode: the inode got
     *
     * @return If inode not exist, return NOT_FOUND; else return OK
     */
    MetaStatusCode Get(const InodeKey& inodeKey, Inode* inode);

    /**
     * @brief get inode attribute from storage
     *
     * @param[in] key: the key of inode want to get
     * @param[out] attr: the inode attribute got
     *
     * @return If inode not exist, return NOT_FOUND; else return OK
     */
    MetaStatusCode GetAttr(const InodeKey& inodeKey, InodeAttr *attr);

    /**
     * @brief get inode extended attributes from storage
     *
     * @param[in] key: the key of inode want to get
     * @param[out] attr: the inode extended attribute got
     *
     * @return If inode not exist, return NOT_FOUND; else return OK
     */
    MetaStatusCode GetXAttr(const InodeKey& inodeKey, XAttr *xattr);

    /**
     * @brief delete inode from storage
     *
     * @param[in] key: the key of inode want to delete
     *
     * @return If inode not exist, return NOT_FOUND; else return OK
     */
    MetaStatusCode Delete(const InodeKey& inodeKey);

    /**
     * @brief update inode from storage
     * @param[in] inode: the inode want to update
     *
     * @return If inode not exist, return NOT_FOUND; else replace and return OK
     */
    MetaStatusCode Update(const Inode& inode);

    MetaStatusCode AppendS3ChunkInfoList(uint32_t fsId,
                                         uint64_t inodeId,
                                         uint64_t chunkIndex,
                                         S3ChunkInfoList& list2add,
                                         bool compaction);

    std::shared_ptr<Iterator> GetAllS3ChunkInfo(uint32_t fsId,
                                                uint64_t inodeId);

    MetaStatusCode PaddingInodeS3ChunkInfo(int32_t fsId,
                                           uint64_t inodeId,
                                           Inode* inode);

    std::shared_ptr<Iterator> GetAll();

    std::shared_ptr<Iterator> GetAllS3ChunkInfoList();

    MetaStatusCode Clear();

    size_t Size();

    void GetInodeIdList(std::list<uint64_t>* inodeIdList);

 private:
    std::string Key2Str(const InodeKey& key);

    std::pair<uint32_t, uint64_t> ExtractKey(const std::string& key);

    bool Inode2Str(const Inode& inode, std::string* value);

    bool Str2Inode(const std::string& value, Inode* inode);

    void AddInodeId(const std::string& key);

    void DeleteInodeId(const std::string& key);

    bool InodeIdExist(const std::string& key);

    MetaStatusCode AddS3ChunkInfoList(
        std::shared_ptr<StorageTransaction> txn,
        uint32_t fsId,
        uint64_t inodeId,
        uint64_t chunkIndex,
        S3ChunkInfoList& list2add):

    MetaStatusCode RemoveS3ChunkInfoList(
        std::shared_ptr<StorageTransaction> txn,
        uint32_t fsId,
        uint64_t inodeId,
        uint64_t chunkIndex,
        uint64_t minChunkId);

 private:
    RWLock rwLock_;
    std::string tablename_;
    std::unordered_set<std::string> counter_;
    std::shared_ptr<KVStorage> kvStorage_;
    std::shared_ptr<Converter> converter_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_INODE_STORAGE_H_
