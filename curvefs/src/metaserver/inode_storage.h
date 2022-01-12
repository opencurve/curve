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

struct InodeKey {
    uint32_t fsId;
    uint64_t inodeId;

    InodeKey(uint32_t fs, uint64_t inode) : fsId(fs), inodeId(inode) {}

    explicit InodeKey(const Inode &inode)
        : fsId(inode.fsid()), inodeId(inode.inodeid()) {}

    bool operator==(const InodeKey &k1) const {
        return k1.fsId == fsId && k1.inodeId == inodeId;
    }
};

struct hashInode {
    size_t operator()(const InodeKey &key) const {
        return std::hash<uint64_t>()(key.inodeId) ^
               std::hash<uint32_t>()(key.fsId);
    }
};

class InodeStorage {
 public:
    using ContainerType = std::unordered_map<InodeKey, Inode, hashInode>;

 public:
    virtual MetaStatusCode Insert(const Inode &inode) = 0;
    virtual MetaStatusCode Get(const InodeKey &key, Inode *inode) = 0;
    virtual MetaStatusCode Delete(const InodeKey &key) = 0;
    virtual MetaStatusCode Update(const Inode &inode) = 0;
    virtual int Count() = 0;
    virtual ContainerType* GetContainer() = 0;
    virtual ContainerType GetContainerData() = 0;
    virtual void GetInodeIdList(std::list<uint64_t>* InodeIdList) = 0;
    virtual ~InodeStorage() = default;
};

class MemoryInodeStorage : public InodeStorage {
 public:
    /**
     * @brief insert inode to storage
     *
     * @param[in] inode: the inode want to insert
     *
     * @return If inode exist, return INODE_EXIST; else insert and return OK
     */
    MetaStatusCode Insert(const Inode &inode) override;

    /**
     * @brief get inode from storage
     *
     * @param[in] key: the key of inode want to get
     * @param[out] inode: the inode got
     *
     * @return If inode not exist, return NOT_FOUND; else return OK
     */
    MetaStatusCode Get(const InodeKey &key, Inode *inode) override;

    /**
     * @brief delete inode from storage
     *
     * @param[in] key: the key of inode want to delete
     *
     * @return If inode not exist, return NOT_FOUND; else return OK
     */
    MetaStatusCode Delete(const InodeKey &key) override;

    /**
     * @brief update inode from storage
     *
     * @param[in] inode: the inode want to update
     *
     * @return If inode not exist, return NOT_FOUND; else replace and  return OK
     */
    MetaStatusCode Update(const Inode &inode) override;

    int Count() override;

    /**
     * @brief get Inode container
     *
     * @return Inode container, here returns inodeMap_ pointer
     */
    ContainerType* GetContainer() override;
    ContainerType GetContainerData() override;


    void GetInodeIdList(std::list<uint64_t>* inodeIdList) override;

 private:
    RWLock rwLock_;
    // use fsid + inodeid as key
    ContainerType inodeMap_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_INODE_STORAGE_H_
