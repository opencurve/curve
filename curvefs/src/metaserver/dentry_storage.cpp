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

#include "curvefs/src/metaserver/dentry_storage.h"

namespace curvefs {
namespace metaserver {
MetaStatusCode MemoryDentryStorage::Insert(const Dentry &dentry) {
    WriteLockGuard writeLockGuard(rwLock_);
    auto it = dentryMap_.emplace(DentryKey(dentry), dentry);
    if (it.second == false) {
        return MetaStatusCode::DENTRY_EXIST;
    }

    Dentry *dentryPtr = &(it.first->second);
    DentryParentKey parentKey(dentry);
    auto itList = dentryListMap_.find(parentKey);
    if (itList == dentryListMap_.end()) {
        std::list<Dentry *> list;
        list.push_back(dentryPtr);
        auto it2 = dentryListMap_.emplace(parentKey, list);
        if (it2.second == false) {
            return MetaStatusCode::DENTRY_EXIST;
        }
    } else {
        itList->second.push_back(dentryPtr);
    }

    return MetaStatusCode::OK;
}

MetaStatusCode MemoryDentryStorage::Get(const DentryKey &key, Dentry *dentry) {
    ReadLockGuard readLockGuard(rwLock_);
    auto it = dentryMap_.find(key);
    if (it == dentryMap_.end()) {
        return MetaStatusCode::NOT_FOUND;
    }
    *dentry = it->second;
    return MetaStatusCode::OK;
}

MetaStatusCode MemoryDentryStorage::List(const DentryParentKey &key,
                                         std::list<Dentry> *dentry) {
    ReadLockGuard readLockGuard(rwLock_);
    auto itList = dentryListMap_.find(key);
    if (itList == dentryListMap_.end()) {
        return MetaStatusCode::NOT_FOUND;
    }

    dentry->clear();
    for (auto itPtr : itList->second) {
        dentry->push_back(*itPtr);
    }
    dentry->sort(CompareDentry);

    return MetaStatusCode::OK;
}

MetaStatusCode MemoryDentryStorage::Delete(const DentryKey &key) {
    WriteLockGuard writeLockGuard(rwLock_);
    auto it = dentryMap_.find(key);
    if (it == dentryMap_.end()) {
        return MetaStatusCode::NOT_FOUND;
    }

    auto itList = dentryListMap_.find(DentryParentKey(key));
    if (itList != dentryListMap_.end()) {
        itList->second.remove(&(it->second));
        if (itList->second.empty()) {
            dentryListMap_.erase(itList);
        }
    }
    dentryMap_.erase(it);
    return MetaStatusCode::OK;
}

int MemoryDentryStorage::Count() {
    ReadLockGuard readLockGuard(rwLock_);
    return dentryMap_.size();
}

}  // namespace metaserver
}  // namespace curvefs
