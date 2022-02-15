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
#include <utility>
#include <list>

#include "curvefs/proto/metaserver.pb.h"
#include "src/common/concurrent/rw_lock.h"

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;
using curve::common::RWLock;
namespace curvefs {
namespace metaserver {

class InodeStorage {
 public:
    using CounterType = absl::btree_set<uint64_t>;

 public:
    InodeStorage(std::shared_ptr<KVStorage> kvStorage, uint32_t partitionId);

    std::string Name();

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
    MetaStatusCode Get(const std::string& key, Inode* inode);

    /**
     * @brief delete inode from storage
     * @param[in] key: the key of inode want to delete
     * @return If inode not exist, return NOT_FOUND; else return OK
     */
    MetaStatusCode Delete(const std:;string& key);

    /**
     * @brief update inode from storage
     * @param[in] inode: the inode want to update
     * @return If inode not exist, return NOT_FOUND; else replace and  return OK
     */
    MetaStatusCode Update(const Inode &inode);

    Iterator GetAll();

    MetaStatusCode Clear();

    void GetInodeIdList(std::list<uint64_t>* inodeIdList);

 private:
    void AddInode(uint64_t fsId, uint64_t inodeId);
    void DelInode(uint64_t fsId, uint64_t inodeId);
    bool InodeExist(uint64_t fsId, uint64_t inodeId);

 private:
    RWLock rwLock_;
    uint32_t partiionId_;
    std::string tableName_;
    std::unordered_map<uint64_t, std::shared_ptr<CounterType>> inodeIds_;
    std::shared_ptr<KVStorage> kvStorage_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_INODE_STORAGE_H_
