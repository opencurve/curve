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

#ifndef CURVEFS_SRC_METASERVER_DENTRY_STORAGE_H_
#define CURVEFS_SRC_METASERVER_DENTRY_STORAGE_H_

#include <functional>
#include <list>
#include <string>
#include <unordered_map>
#include "curvefs/proto/metaserver.pb.h"
#include "src/common/concurrent/rw_lock.h"

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;
using curve::common::RWLock;

namespace curvefs {
namespace metaserver {

struct DentryKey {
    uint32_t fsId;
    uint64_t parentId;
    std::string name;
    DentryKey(uint32_t fs, uint64_t parent, const std::string &fsname)
        : fsId(fs), parentId(parent), name(fsname) {}

    explicit DentryKey(const Dentry &dentry)
        : fsId(dentry.fsid()),
          parentId(dentry.parentinodeid()),
          name(dentry.name()) {}

    bool operator==(const DentryKey &k1) const {
        return k1.fsId == fsId && k1.parentId == parentId && k1.name == name;
    }
};

struct DentryParentKey {
    uint32_t fsId;
    uint64_t parentId;
    DentryParentKey(uint32_t fs, uint64_t parent)
        : fsId(fs), parentId(parent) {}

    explicit DentryParentKey(const Dentry &dentry)
        : fsId(dentry.fsid()), parentId(dentry.parentinodeid()) {}

    explicit DentryParentKey(const DentryKey &key)
        : fsId(key.fsId), parentId(key.parentId) {}

    bool operator==(const DentryParentKey &k1) const {
        return k1.fsId == fsId && k1.parentId == parentId;
    }
};

struct HashDentry {
    size_t operator()(const DentryKey &key) const {
        return std::hash<uint64_t>()(key.parentId) ^
               std::hash<uint32_t>()(key.fsId) ^
               std::hash<std::string>()(key.name);
    }
};

struct HashParentDentry {
    size_t operator()(const DentryParentKey &key) const {
        return std::hash<uint64_t>()(key.parentId) ^
               std::hash<uint32_t>()(key.fsId);
    }
};

using DentryContainerType = std::unordered_map<DentryKey, Dentry, HashDentry>;

class DentryStorage {
 public:
    virtual MetaStatusCode Insert(const Dentry &dentry) = 0;
    virtual MetaStatusCode Get(const DentryKey &key, Dentry *dentry) = 0;
    virtual MetaStatusCode List(const DentryParentKey &key,
                                std::list<Dentry> *dentry) = 0;
    virtual MetaStatusCode Delete(const DentryKey &key) = 0;
    virtual int Count() = 0;
    virtual DentryContainerType *GetDentryContainer() = 0;
    virtual ~DentryStorage() = default;
};

class MemoryDentryStorage : public DentryStorage {
 public:
    /**
     * @brief insert dentry to storage
     *
     * @param[in] dentry: the dentry want to insert
     *
     * @return If dentry exist, return DENTRY_EXIST; else insert and return OK
     *         else return error code
     */
    MetaStatusCode Insert(const Dentry &dentry) override;

    /**
     * @brief get dentry from storage
     *
     * @param[in] key: the dentry key
     * @param[out] dentry: the dentry want to get
     *
     * @return If not found, return NOT_FOUND; else return OK
     */
    MetaStatusCode Get(const DentryKey &key, Dentry *dentry) override;

    /**
     * @brief get dentry list from storage, name lexicographically sorted
     *
     * @param[in] key: the DentryParent key
     * @param[out] dentryList: the dentry list want to get
     *
     * @return If not found, return NOT_FOUND; else return OK
     */
    MetaStatusCode List(const DentryParentKey &key,
                        std::list<Dentry> *dentryList) override;

    /**
     * @brief Delete dentry from storage
     *
     * @param[in] key: the dentry key
     *
     * @return If not found, return NOT_FOUND; else delete and return OK
     */
    MetaStatusCode Delete(const DentryKey &key) override;

    int Count() override;

    DentryContainerType *GetDentryContainer() override;

 private:
    static bool CompareDentry(const Dentry &first, const Dentry &second) {
        return first.name() < second.name();
    }

 private:
    RWLock rwLock_;
    // use fsid + parentid + name as key
    DentryContainerType dentryMap_;
    // use fsid + parentid as key，
    // for list search
    std::unordered_map<DentryParentKey, std::list<Dentry *>, HashParentDentry>
        dentryListMap_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_DENTRY_STORAGE_H_
