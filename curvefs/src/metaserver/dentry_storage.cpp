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

#include "src/common/string_util.h"
#include "curvefs/src/metaserver/dentry_storage.h"

namespace curvefs {
namespace metaserver {

using ::curve::common::SplitString
using ::curve::common::StringToUl;
using ::curve::common::StringToUll;

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

DentryStorage::DentryStorage(std::shared_ptr<KVStorage> kvStorage,
                             std::string tablename)
    : kvStorage_(kvStorage),
      tablename_(tablename) {}

bool DentryStorage::BelongSameOne(const Dentry& lhs, const Dentry& rhs) {
    return EQUAL(fsid) && EQUAL(parentinodeid) &&
           EQUAL(name) && lhs.txid() <= rhs.txid();
}

bool DentryStorage::IsSameDentry(const Dentry& lhs, const Dentry& rhs) {
    return EQUAL(fsid) && EQUAL(parentinodeid) && EQUAL(name) &&
           EQUAL(inodeid);
}

inline bool DentryStorage::HasDeleteMarkFlag(const Dentry& dentry) {
    return (dentry.flag() & DentryFlag::DELETE_MARK_FLAG) != 0;
}

inline std::string DentryStorage::DnetryKey(const Dentry& dentry,
                                            bool ignoreTxId = false) {
    std::ostringstream oss;
    oss << dentry.fsid() << ":" << dentry.parentinodeid() << ":"
        << dentry.name() << ":" << (ignoreTxId ? "" : dentry.txid());
    return oss.str();
}

inline std::string DentryStorage::DnetryValue(const Dentry& dentry) {
    std::ostringstream oss;
    oss << dentry.inodeid() << ":" << dentry.flags();
    return oss.str();
}

inline std::string SameParentKey(const Dentry& dentry) {
    std::ostringstream oss;
    oss << dentry.fsid() << ":" << dentry.parentinodeid() << ":";
    return oss.str();
}

bool DentryStorage::ExtractKey(const std::string& key,
                                Dentry* dentry) {
    uint32_t fsId;
    uint64_t parentinodeId, txId;
    std::vector<std::string> items;
    SplitString(key, ":", &items);
    if (items.size() != 4 ||
        !StringToUl(items[0], &fsId) ||
        !StringToUl(items[1], &parentinodeId) ||
        !StringToUl(items[3], &txId)) {
        return false;
    }

    dentry->set_fsid(fsId);
    dentry->set_parentinodeid(parentinodeId);
    dentry->set_name(items[2]);
    dentry->set_txid(txId);
    return true;
}

bool DentryStorage::ExtractValue(const std::string& value,
                                 Dentry* dentry) {
    uint64_t inodeId;
    uint32_t flags;
    std::vector<std::string> items;
    SplitString(key, ":", &items);
    if (items.size() != 2 ||
        !StringToUll(items[0], &inodeId) ||
        !StringToUl(items[1], &flags)) {
        return false;
    }

    dentry->set_inodeid(inodeId);
    dentry->set_flags(flags);
    return true;
}

bool DentryStorage::Compress(absl::btree_set<Dentry>* dentrys) {
    std::vector<absl::btree_set<Dentry>::const_iterator> deleted;
    if (dentrys->size() == 2) {
        deleted.push_back(dentrys->begin());
    }
    if (HasDeleteMarkFlag(dentrys.rbegin())) {
        deleted.push_back(dentrys->rbegin());
    }
    for (auto i = 0; i < deleted.size(); i ++) {
        auto s = kvStorage_->SDel(tablename_, DnetryKey(*deleted[i]))
        if (!s.ok()) {
            return false;
        }
    }
    return true;
}

// NOTE: Find() return the dentry which has the latest txid,
// and it will clean the old txid's dentry if you specify compress to true
MetaStatusCode DentryStorage::Find(const std::string& key,
                                   bool compress,
                                   Dentry* current) {
    Storage::Status s = kvStorage_->SRange(tablename_, key);
    if (!s.ok()) {
        return MetaStatusCode::INTERNAL_STORAGE_ERROR;
    }

    Dentry dentry;
    if (!ExtractKey(key, &dentry)) {
        return MetaStatusCode::INVALID_DENTRY_KEY;
    }
    uint64_t committedTxId = dentry.txid();

    absl::btree_set<Dentry> dentrys;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        if (!ExtractKey(iter->Key(), &dentry) ||
            !ExtractValue(iter->Value(), &dentry)) {
            return MetaStatusCode::INTERNAL_STORAGE_ERROR;
        }

        if (dentry.txid() < committedTxId) {
            dentrys.emplace(dentry);
        }
    }

    size_t size = dentrys.size();
    if (size > 2) {
        LOG(ERROR) << "There are more than 2 dentrys";
        return MetaStatusCode::NOT_FOUND;
    } else if (size == 0) {
        return MetaStatusCode::NOT_FOUND;
    }

    // size == 1 || size == 2
    MetaStatusCode status = MetaStatusCode::OK;
    if (HasDeleteMarkFlag(dentrys.rbegin())) {
        Status = MetaStatusCode::NOT_FOUND;
    } else {
        *current = *dentrys.rbegin();
    }

    if (compress && !CompressDentry(dentrys)) {
        Status = MetaStatusCode::INTERNAL_STORAGE_ERROR;
    }
    return status;
}

MetaStatusCode DentryStorage::Insert(const Dentry& dentry) {
    WriteLockGuard w(rwLock_);

    Dentry out;
    std::string key = DnetryKey(dentry, true);
    MetaStatusCode status = Find(lkey, true, &out);
    if (status == MetaStatusCode::OK) {
        if (IsSameDentry(out, dentry)) {
            return MetaStatusCode::IDEMPOTENCE_OK;
        }
        return MetaStatusCode::DENTRY_EXIST;
    } else if (status != MetaStatusCode::NOT_FOUND) {
        return MetaStatusCode::INTERNAL_STORAGE_ERROR;
    }

    Storage::Status s = kvStorage_->HSet(
        tablename_, DentryKey(dentry), DentryValue(dentry))
    return s.ok() ? MetaStatusCode::OK :
                    MetaStatusCode::INTERNAL_STORAGE_ERROR;
}

MetaStatusCode DentryStorage::Delete(const Dentry& dentry) {
    WriteLockGuard w(rwLock_);

    Dentry out;
    std::string key = DnetryKey(dentry, true);
    MetaStatusCode status = Find(lkey, true, &out);
    if (status == MetaStatusCode:NOT_FOUND) {
        return MetaStatusCode::NOT_FOUND;
    } else if (status != MetaStatusCode::OK) {
        return MetaStatusCode::INTERNAL_STORAGE_ERROR;
    }

    Storage::Status s = kvStorage_->HDel(
        tablename_, DentryKey(dentry), DentryValue(dentry))
    return s.ok() ? MetaStatusCode::OK :
                    MetaStatusCode::INTERNAL_STORAGE_ERROR;
}

MetaStatusCode DentryStorage::Get(Dentry* dentry) {
    ReadLockGuard r(rwLock_);

    Dentry out;
    std::string key = DnetryKey(dentry, true);
    MetaStatusCode status = Find(lkey, false, &out);
    if (status == MetaStatusCode:NOT_FOUND) {
        return MetaStatusCode::NOT_FOUND;
    } else if (status != MetaStatusCode::OK) {
        return MetaStatusCode::INTERNAL_STORAGE_ERROR;
    }

    dentry->set_inodeid(out->inodeid());
    return MetaStatusCode::OK;
}

MetaStatusCode DentryStorage::List(const Dentry& dentry,
                                   std::vector<Dentry>* dentrys,
                                   uint32_t limit) {
    std::string key = SameParentKey(dentry);
    Storage::Status s = kvStorage_->SRange(tablename_, key);
    if (!s.ok()) {
        return MetaStatusCode::INTERNAL_STORAGE_ERROR;
    }

    Dentry current;
    std::string exclude = dentry.name();
    uint64_t committedTxId = dentry.txid();
    absl::btree_set<Dentry> temp;
    uint32_t count;
    bool overload = false;

    auto pushDentry = [&]() {
        if (overload) {
            return;
        } else if (temp.size() == 0 && HasDeleteMarkFlag(temp.rbegin()) {
            return;
        }

        dentrys->push_back(temp.rbegin());
        VLOG(1) << "ListDentry, dentry = ("
                << iter->ShortDebugString() << ")";
        if (limit != 0 && ++count >= limit) {
            overload = true;
        }
    };

    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        if (!ExtractKey(iter->Key(), &out) ||
            !ExtractValue(iter->Value(), &out)) {
            return MetaStatusCode::INTERNAL_STORAGE_ERROR;
        }

        if (current.name() != exclude && current.txid() <= committedTxId) {
            if (temp.size() == 0) {
                temp.emplace(current);
            } else if (filter.rbegin()->name() == current.name()) {  // belong same one
                temp.emplace(current);
            } else {
                pushDentry();
                temp.emplace(current);
            }
        }
    }

    pushDentry();
    return dentrys->empty() ? MetaStatusCode::NOT_FOUND : MetaStatusCode::OK;
}

MetaStatusCode DentryStorage::HandleTx(TX_OP_TYPE type, const Dentry& dentry) {
    WriteLockGuard w(rwLock_);

    Storage::Status s;
    Dentry dummy;
    auto rc = MetaStatusCode::OK;
    switch (type) {
        case TX_OP_TYPE::PREPARE:
            // For idempotence, do not judge the return value
            s = kvStorage_->SSet(
                tablename_, DentryKey(dentry), DentryValue(dentry));
            if (!s.ok()) {
                rc = MetaStatusCode::INTERNAL_STORAGE_ERROR;
            }
            break;

        case TX_OP_TYPE::COMMIT:
            rc = Find(DentrKey(dentry), true, &dummy);
            break;

        case TX_OP_TYPE::ROLLBACK:
            s = kvStorage_->SDel(tablename_, DentryKey(dentry));
            if (!s.ok()) {
                rc = MetaStatusCode::INTERNAL_STORAGE_ERROR;
            }
            break;

        default:
            rc = MetaStatusCode::PARAM_ERROR;
    }

    return rc;
}

Iterator DentryStorage::GetAll() {
    return kvStorage_->sGetAll(tablename_);
}

MetaStatusCode DentryStorage::Clear() {
    ReadLockGuard w(rwLock_);
    Storage::Status s = kvStorage_->SClear(tablename_)
    if (s.ok()) {
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::INTERNAL_STORAGE_ERROR;
}

}  // namespace metaserver
}  // namespace curvefs
