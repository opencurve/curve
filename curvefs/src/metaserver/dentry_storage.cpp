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
#include <memory>

#include "src/common/string_util.h"
#include "curvefs/src/metaserver/dentry_storage.h"

namespace curvefs {
namespace metaserver {

using ::curve::common::SplitString;
using ::curve::common::StringToUl;
using ::curve::common::StringToUll;
using ::curve::common::StringStartWith;

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
                             const std::string& tablename)
    : kvStorage_(kvStorage),
      tablename_(tablename) {}

inline std::string DentryStorage::DentryKey(const Dentry& dentry,
                                            bool ignoreTxId) {
    std::ostringstream oss;
    std::string txId = ignoreTxId ? "" : std::to_string(dentry.txid());
    oss << dentry.fsid() << ":" << dentry.parentinodeid() << ":"
        << dentry.name() << ":" << txId;
    return oss.str();
}

inline std::string DentryStorage::DentryValue(const Dentry& dentry) {
    std::ostringstream oss;
    oss << dentry.inodeid() << ":" << dentry.flag();
    return oss.str();
}

inline std::string DentryStorage::SameParentKey(const Dentry& dentry) {
    std::ostringstream oss;
    oss << dentry.fsid() << ":" << dentry.parentinodeid() << ":";
    return oss.str();
}

bool DentryStorage::ExtractKey(const std::string& key,
                               Dentry* dentry) {
    uint32_t fsId;
    uint64_t parentInodeId, txId;
    std::vector<std::string> items;
    SplitString(key, ":", &items);
    if (items.size() != 4 ||
        !StringToUl(items[0], &fsId) ||
        !StringToUll(items[1], &parentInodeId) ||
        !StringToUll(items[3], &txId)) {
        return false;
    }

    dentry->set_fsid(fsId);
    dentry->set_parentinodeid(parentInodeId);
    dentry->set_name(items[2]);
    dentry->set_txid(txId);
    return true;
}

bool DentryStorage::ExtractValue(const std::string& value,
                                 Dentry* dentry) {
    uint64_t inodeId;
    uint32_t flag;
    std::vector<std::string> items;
    SplitString(value, ":", &items);
    if (items.size() != 2 ||
        !StringToUll(items[0], &inodeId) ||
        !StringToUl(items[1], &flag)) {
        return false;
    }

    dentry->set_inodeid(inodeId);
    dentry->set_flag(flag);
    return true;
}

inline bool DentryStorage::BelongSameOne(const Dentry& lhs, const Dentry& rhs) {
    return EQUAL(fsid) && EQUAL(parentinodeid) &&
           EQUAL(name) && lhs.txid() <= rhs.txid();
}

inline bool DentryStorage::IsSameDentry(const Dentry& lhs, const Dentry& rhs) {
    return EQUAL(fsid) && EQUAL(parentinodeid) && EQUAL(name) &&
           EQUAL(inodeid);
}

inline bool DentryStorage::HasDeleteMarkFlag(const Dentry& dentry) {
    return (dentry.flag() & DentryFlag::DELETE_MARK_FLAG) != 0;
}

bool DentryStorage::CompressDentry(BTree* dentrys) {
    std::vector<Dentry> deleted;
    if (dentrys->size() == 2) {
        deleted.push_back(*dentrys->begin());
    }
    if (HasDeleteMarkFlag(*dentrys->rbegin())) {
        deleted.push_back(*dentrys->rbegin());
    }
    for (const auto& item : deleted) {
        Status s = kvStorage_->SDel(tablename_, DentryKey(item));
        if (!s.ok() && !s.IsNotFound()) {
            return false;
        }
    }
    return true;
}

// NOTE: Find() return the dentry which has the latest txid,
// and it will clean the old txid's dentry if you specify compress to true
MetaStatusCode DentryStorage::Find(const Dentry& in,
                                   Dentry* out,
                                   bool compress) {
    uint64_t maxTxId = in.txid();
    std::string prefix = DentryKey(in, true);
    auto iterator = kvStorage_->SSeek(tablename_, prefix);
    if (iterator->Status() < 0) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    // find dentry which belongs to one
    Dentry dentry;
    BTree dentrys;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        std::string key = iterator->Key();
        std::string value = iterator->Value();
        if (!StringStartWith(key, prefix)) {
            break;
        }

        if (!ExtractKey(key, &dentry)) {
            return MetaStatusCode::INVALID_DENTRY_KEY;
        } else if (!ExtractValue(value, &dentry)) {
            return MetaStatusCode::INVALID_DENTRY_VALUE;
        }

        if (dentry.txid() <= maxTxId) {
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
    MetaStatusCode rc;
    if (HasDeleteMarkFlag(*dentrys.rbegin())) {
        rc = MetaStatusCode::NOT_FOUND;
    } else {
        rc = MetaStatusCode::OK;
        *out = *dentrys.rbegin();
    }

    if (compress && !CompressDentry(&dentrys)) {
        rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    return rc;
}

MetaStatusCode DentryStorage::Insert(const Dentry& dentry) {
    WriteLockGuard w(rwLock_);

    Dentry out;
    MetaStatusCode rc = Find(dentry, &out, true);
    if (rc == MetaStatusCode::OK) {
        if (IsSameDentry(out, dentry)) {
            return MetaStatusCode::IDEMPOTENCE_OK;
        }
        return MetaStatusCode::DENTRY_EXIST;
    } else if (rc != MetaStatusCode::NOT_FOUND) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    // MetaStatusCode::NOT_FOUND
    Status s = kvStorage_->SSet(
        tablename_, DentryKey(dentry), DentryValue(dentry));
    return s.ok() ? MetaStatusCode::OK :
                    MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode DentryStorage::Delete(const Dentry& dentry) {
    WriteLockGuard w(rwLock_);

    Dentry out;
    MetaStatusCode rc = Find(dentry, &out, true);
    if (rc == MetaStatusCode::NOT_FOUND) {
        return MetaStatusCode::NOT_FOUND;
    } else if (rc != MetaStatusCode::OK) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    // MetaStatusCode::OK
    Status s = kvStorage_->SDel(tablename_, DentryKey(out));
    return s.ok() ? MetaStatusCode::OK :
                    MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode DentryStorage::Get(Dentry* dentry) {
    ReadLockGuard r(rwLock_);

    Dentry out;
    MetaStatusCode rc = Find(*dentry, &out, false);
    if (rc == MetaStatusCode::NOT_FOUND) {
        return MetaStatusCode::NOT_FOUND;
    } else if (rc != MetaStatusCode::OK) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    // MetaStatusCode::OK
    dentry->set_inodeid(out.inodeid());
    return MetaStatusCode::OK;
}

MetaStatusCode DentryStorage::List(const Dentry& dentry,
                                   std::vector<Dentry>* dentrys,
                                   uint32_t limit) {
    std::string prefix = SameParentKey(dentry);
    std::string lkey = prefix;
    if (dentry.name().size() > 0) {
        lkey = lkey + dentry.name() + ":";
    }

    auto iter = kvStorage_->SSeek(tablename_, lkey);
    if (iter->Status() < 0) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    uint32_t count = 0;
    auto push = [&](BTree* temp) {
        if (limit != 0 && count >= limit) {
            return;
        } else if (temp->size() == 0 || HasDeleteMarkFlag(*temp->rbegin())) {
            return;
        }

        count++;
        auto iter = temp->rbegin();
        dentrys->push_back(*iter);
        VLOG(1) << "ListDentry, dentry = ("
                << iter->ShortDebugString() << ")";
    };

    Dentry current;
    BTree temp;
    std::string exclude = dentry.name();
    uint64_t maxTxId = dentry.txid();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        std::string key = iter->Key();
        std::string value = iter->Value();
        if (!StringStartWith(key, prefix)) {
            break;
        }

        if (!ExtractKey(key, &current)) {
            return MetaStatusCode::INVALID_DENTRY_KEY;
        } else if (!ExtractValue(value, &current)) {
            return MetaStatusCode::INVALID_DENTRY_VALUE;
        }

        if (current.name() != exclude && current.txid() <= maxTxId) {
            if (temp.size() == 0) {
                temp.emplace(current);
            } else if (temp.rbegin()->name() == current.name()) {
                // belong same dentry
                temp.emplace(current);
            } else {
                push(&temp);
                temp.emplace(current);
            }
        }
    }

    push(&temp);
    return dentrys->empty() ? MetaStatusCode::NOT_FOUND : MetaStatusCode::OK;
}

MetaStatusCode DentryStorage::HandleTx(TX_OP_TYPE type, const Dentry& dentry) {
    WriteLockGuard w(rwLock_);

    Status s;
    Dentry dummy;
    auto rc = MetaStatusCode::OK;
    switch (type) {
        case TX_OP_TYPE::PREPARE:
            // For idempotence, do not judge the return value
            s = kvStorage_->SSet(tablename_,
                                 DentryKey(dentry),
                                 DentryValue(dentry));
            if (!s.ok()) {
                rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
            }
            break;

        case TX_OP_TYPE::COMMIT:
            rc = Find(dentry, &dummy, true);
            if (rc == MetaStatusCode::OK ||
                rc == MetaStatusCode::NOT_FOUND) {
                rc = MetaStatusCode::OK;
            }
            break;

        case TX_OP_TYPE::ROLLBACK:
            s = kvStorage_->SDel(tablename_, DentryKey(dentry));
            if (!s.ok() && !s.IsNotFound()) {
                rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
            }
            break;

        default:
            rc = MetaStatusCode::PARAM_ERROR;
    }

    return rc;
}

std::shared_ptr<Iterator> DentryStorage::GetAll() {
    return kvStorage_->SGetAll(tablename_);
}

size_t DentryStorage::Size() {
    return kvStorage_->SSize(tablename_);
}

MetaStatusCode DentryStorage::Clear() {
    ReadLockGuard w(rwLock_);
    Status s = kvStorage_->SClear(tablename_);
    return s.ok() ? MetaStatusCode::OK : MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

}  // namespace metaserver
}  // namespace curvefs
