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

#include <vector>

#include "curvefs/src/metaserver/dentry_storage.h"

namespace curvefs {
namespace metaserver {

bool operator==(const Dentry& lhs, const Dentry& rhs) {
    return EQUAL(fsid) && EQUAL(parentinodeid) && EQUAL(name) &&
           EQUAL(txid) && EQUAL(inodeid) && EQUAL(flag);
}

bool operator<(const Dentry& lhs, const Dentry& rhs) {
    return LESS(fsid) ||
           LESS2(fsid, parentinodeid) ||
           LESS3(fsid, parentinodeid, name) ||
           LESS4(fsid, parentinodeid, name, txid);
}

bool MemoryDentryStorage::BelongSameOne(const Dentry& lhs, const Dentry& rhs) {
    return EQUAL(fsid) && EQUAL(parentinodeid) &&
           EQUAL(name) && lhs.txid() <= rhs.txid();
}

inline bool MemoryDentryStorage::HasDeleteMarkFlag(const Dentry& dentry) {
    return (dentry.flag() & DentryFlag::DELETE_MARK_FLAG) != 0;
}

// NOTE: Find() return the iterator of dentry which has the latest txid,
// and it will clean the old txid's dentry if you specify compress to true
Btree::iterator MemoryDentryStorage::Find(const Dentry& dentry, bool compress) {
    auto ikey = dentry;
    ikey.set_txid(0);

    std::vector<Btree::iterator> its;
    for (auto iter = dentryTree_.lower_bound(ikey);
         iter != dentryTree_.end() && BelongSameOne(*iter, dentry);
         iter++) {
        its.emplace_back(iter);
    }

    auto size = its.size();  // NOTE: size must belong [0, 2]
    if (size > 2) {
        LOG(ERROR) << "There are more than 2 dentrys";
        return dentryTree_.end();
    } else if (size == 0) {
        return dentryTree_.end();
    }

    // size == 1 || size == 2
    auto first = its[0];
    auto second = its[size - 1];
    if (HasDeleteMarkFlag(*second)) {
        if (compress) {
            second++;
            dentryTree_.erase(first, second);
        }
        return dentryTree_.end();
    }

    return compress ? dentryTree_.erase(first, second) : second;
}

MetaStatusCode MemoryDentryStorage::Insert(const Dentry& dentry) {
    WriteLockGuard w(rwLock_);

    auto iter = Find(dentry, true);
    if (iter != dentryTree_.end()) {
        // Idempotence
        if (*iter == dentry) {
            return MetaStatusCode::OK;
        }
        return MetaStatusCode::DENTRY_EXIST;
    }

    dentryTree_.emplace(dentry);
    return MetaStatusCode::OK;
}

MetaStatusCode MemoryDentryStorage::Delete(const Dentry& dentry) {
    WriteLockGuard w(rwLock_);

    auto iter = Find(dentry, true);
    if (iter == dentryTree_.end()) {
        return MetaStatusCode::NOT_FOUND;
    }

    dentryTree_.erase(iter);
    return MetaStatusCode::OK;
}

MetaStatusCode MemoryDentryStorage::Get(Dentry* dentry) {
    ReadLockGuard r(rwLock_);

    auto iter = Find(*dentry, false);
    if (iter == dentryTree_.end()) {
        return MetaStatusCode::NOT_FOUND;
    }

    dentry->set_inodeid(iter->inodeid());
    return MetaStatusCode::OK;
}

MetaStatusCode MemoryDentryStorage::List(const Dentry& dentry,
                                         std::vector<Dentry>* dentrys,
                                         uint32_t limit) {
    auto parentId = dentry.parentinodeid();
    auto exclude = dentry.name();
    auto txId = dentry.txid();

    // range = [lower, upper)
    uint32_t count = 0;
    auto ukey = dentry;
    ukey.set_parentinodeid(parentId + 1);
    ukey.set_name("");
    auto lower = dentryTree_.lower_bound(dentry);
    auto upper = dentryTree_.upper_bound(ukey);
    for (auto first = lower; first != upper; first++) {
        auto exist = false;
        auto iter = first;
        auto second = first;
        while (second != upper && second->name() == first->name()) {
            if (second->name() != exclude && second->txid() <= txId) {
                if (HasDeleteMarkFlag(*second)) {
                    exist = false;
                } else {
                    exist = true;
                    iter = second;
                }
            }
            second++;
        }

        // dentry belong [first, second)
        if (exist) {
            dentrys->push_back(*iter);
            VLOG(1) << "ListDentry, dentry = ("
                    << iter->ShortDebugString() << ")";
            if (limit != 0 && ++count >= limit) {
                break;
            }
        }

        second--;
        first = second;
    }

    return dentrys->empty() ? MetaStatusCode::NOT_FOUND : MetaStatusCode::OK;
}

MetaStatusCode MemoryDentryStorage::HandleTx(TX_OP_TYPE type,
                                             const Dentry& dentry) {
    WriteLockGuard w(rwLock_);

    auto rc = MetaStatusCode::OK;
    switch (type) {
        case TX_OP_TYPE::PREPARE:
            // For idempotence, do not judge the return value
            dentryTree_.emplace(dentry);
            break;

        case TX_OP_TYPE::COMMIT:
            Find(dentry, true);
            break;

        case TX_OP_TYPE::ROLLBACK:
            dentryTree_.erase(dentry);
            break;

        default:
            rc = MetaStatusCode::PARAM_ERROR;
    }

    return rc;
}

size_t MemoryDentryStorage::Size() {
    ReadLockGuard r(rwLock_);
    return dentryTree_.size();
}

void MemoryDentryStorage::Clear() {
    ReadLockGuard w(rwLock_);
    dentryTree_.clear();
}

DentryStorage::ContainerType* MemoryDentryStorage::GetContainer() {
    return &dentryTree_;
}

}  // namespace metaserver
}  // namespace curvefs
