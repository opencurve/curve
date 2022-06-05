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
#include <algorithm>

#include "src/common/string_util.h"
#include "curvefs/src/metaserver/dentry_storage.h"
#include "curvefs/src/metaserver/storage/utils.h"
#include "curvefs/src/metaserver/storage/converter.h"

namespace curvefs {
namespace metaserver {

using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;
using ::curve::common::SplitString;
using ::curve::common::StringStartWith;
using ::curvefs::metaserver::storage::Status;
using ::curvefs::metaserver::storage::Key4Dentry;
using ::curvefs::metaserver::storage::Prefix4SameParentDentry;

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

static bool IsSameDentry(const Dentry& lhs, const Dentry& rhs) {
    return EQUAL(fsid) && EQUAL(parentinodeid) && EQUAL(name) &&
           EQUAL(inodeid);
}

static bool HasDeleteMarkFlag(const Dentry& dentry) {
    return (dentry.flag() & DentryFlag::DELETE_MARK_FLAG) != 0;
}

static void FilterDentry(const DentryVec& vec, BTree* btree, uint64_t maxTxId) {
    /*
    for (const Dentry& dentry : vec->dentrys()) {
        if (dentry.txid() <= maxTxId) {
            btree->insert(dentry);
        }
    }
    */
}

static void Insert2Vec(DentryVec* vec, const Dentry& dentry) {
    vec->add_dentrys()->CopyFrom(dentry);
}

static void Delete4Vec(DentryVec* vec, const Dentry& dentry) {
    /*
    auto dentrys = vec->mutable_dentrys();
    for (const auto iter = dentrys->begin(); iter != dentrys->end(); iter++) {
        if (*iter == dentry) {
            dentrys->erase(iter);
            return;
        }
    }
    */
}

static void MergeVec(DentryVec* dst, const DentryVec& src) {
    /*
    for (const auto& dentry : src->dentrys()) {
        dst->add_dentrys()->CopyFrom(dentry);
    }
    */
}

DentryList::DentryList(std::vector<Dentry>* list,
                       uint32_t limit,
                       const std::string& exclude,
                       uint64_t maxTxId,
                       bool onlyDir)
    : list_(list),
      size_(0),
      limit_(limit),
      exclude_(exclude),
      maxTxId_(maxTxId),
      onlyDir_(onlyDir) {}

void DentryList::PushBack(const DentryVec& vec) {
    // NOTE: it's a cheap operation becacuse the size of
    // dentryVec must less than 2
    BTree dentrys;
    FilterDentry(vec, &dentrys, maxTxId_);
    auto last = dentrys.rbegin();
    if (limit_ != 0 && size_ >= limit_) {
        return;
    } else if (dentrys.size() == 0 || HasDeleteMarkFlag(*last)) {
        return;
    } else if (onlyDir_ && last->type() != FsFileType::TYPE_DIRECTORY) {
        return;
    }

    size_++;
    list_->push_back(*last);
    VLOG(9) << "ListDentry, dentry = ("
            << last->ShortDebugString() << ")";
}

uint32_t DentryList::Size() {
    return size_;
}

bool DentryList::IsFull() {
    return limit_ != 0 && size_ >= limit_;
}

DentryStorage::DentryStorage(std::shared_ptr<KVStorage> kvStorage,
                             std::shared_ptr<NameGenerator> nameGenerator,
                             uint64_t nDentry)
    : kvStorage_(kvStorage),
      table4Dentry_(nameGenerator->GetDentryTableName()),
      nDentry_(nDentry) {}

std::string DentryStorage::DentryKey(const Dentry& dentry) {
    Key4Dentry key(dentry.fsid(), dentry.parentinodeid(), dentry.name());
    return conv_.SerializeToString(key);
}

bool DentryStorage::CompressDentry(DentryVec* vec, BTree* dentrys) {
    std::vector<Dentry> deleted;
    if (dentrys->size() == 2) {
        deleted.push_back(*dentrys->begin());
    }
    if (HasDeleteMarkFlag(*dentrys->rbegin())) {
        deleted.push_back(*dentrys->rbegin());
    }
    for (const auto& dentry : deleted) {
        Delete4Vec(vec, dentry);
    }

    Status s;
    std::string skey = DentryKey(*dentrys->begin());
    if (vec->dentrys_size() == 0) {  // delete directly
        s = kvStorage_->SDel(table4Dentry_, skey);
    } else {
        s = kvStorage_->SSet(table4Dentry_, skey, *vec);
    }

    return s.ok();
}

// NOTE: Find() return the dentry which has the latest txid,
// and it will clean the old txid's dentry if you specify compress to true
MetaStatusCode DentryStorage::Find(const Dentry& in,
                                   Dentry* out,
                                   DentryVec* vec,
                                   bool compress) {
    Key4Dentry key(in.fsid(), in.parentinodeid(), in.name());
    std::string skey = conv_.SerializeToString(key);
    Status s = kvStorage_->SGet(table4Dentry_, skey, vec);
    if (s.IsNotFound()) {
        return MetaStatusCode::NOT_FOUND;
    } else if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    BTree dentrys;
    FilterDentry(*vec, &dentrys, in.txid());
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

    if (compress && !CompressDentry(vec, &dentrys)) {
        rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    return rc;
}

MetaStatusCode DentryStorage::Insert(const Dentry& dentry) {
    WriteLockGuard w(rwLock_);
    Dentry out;
    DentryVec vec;
    MetaStatusCode rc = Find(dentry, &out, &vec, true);
    if (rc == MetaStatusCode::OK) {
        if (IsSameDentry(out, dentry)) {
            return MetaStatusCode::IDEMPOTENCE_OK;
        }
        return MetaStatusCode::DENTRY_EXIST;
    } else if (rc != MetaStatusCode::NOT_FOUND) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    // rc == MetaStatusCode::NOT_FOUND
    Key4Dentry key(dentry.fsid(), dentry.parentinodeid(), dentry.name());
    std::string skey = key.SerializeToString();
    Insert2Vec(&vec, dentry);
    Status s = kvStorage_->SSet(table4Dentry_, skey, vec);
    if (!s.ok()) {
        LOG(ERROR) << "Insert dentry failed, status = " << s.ToString();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    nDentry_ += 1;
    return MetaStatusCode::OK;
}

MetaStatusCode DentryStorage::Insert(const DentryVec& vec, bool merge) {
    WriteLockGuard w(rwLock_);
    Status s;
    DentryVec oldVec;
    std::string skey = DentryKey(vec.dentrys(0));
    if (merge) {  // for old version dumpfile (v1)
        s = kvStorage_->SGet(table4Dentry_, skey, &oldVec);
        if (s.IsNotFound()) {
            // do nothing
        } else if (!s.ok()) {
            return MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
    }

    MergeVec(&oldVec, vec);
    s = kvStorage_->SSet(table4Dentry_, skey, oldVec);
    if (!s.ok()) {
        LOG(ERROR) << "Insert dentry failed, status = " << s.ToString();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    nDentry_ += vec.dentrys_size();
    return MetaStatusCode::OK;
}

MetaStatusCode DentryStorage::Delete(const Dentry& dentry) {
    WriteLockGuard w(rwLock_);

    Dentry out;
    DentryVec vec;
    MetaStatusCode rc = Find(dentry, &out, &vec, true);
    if (rc == MetaStatusCode::NOT_FOUND) {
        return MetaStatusCode::NOT_FOUND;
    } else if (rc != MetaStatusCode::OK) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    Status s;
    Key4Dentry key(dentry.fsid(), dentry.parentinodeid(), dentry.name());
    std::string skey = key.SerializeToString();
    if (vec.dentrys_size() == 1) {
        s = kvStorage_->SDel(table4Dentry_, skey);
    } else {
        // delete from vector
        s = kvStorage_->SSet(table4Dentry_, skey, vec);
    }
    return s.ok() ? MetaStatusCode::OK :
                    MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode DentryStorage::Get(Dentry* dentry) {
    ReadLockGuard r(rwLock_);

    Dentry out;
    DentryVec vec;
    MetaStatusCode rc = Find(*dentry, &out, &vec, false);
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
                                   uint32_t limit,
                                   bool onlyDir) {
    ReadLockGuard r(rwLock_);
    // 1. prepare seek lower key
    uint32_t fsId = dentry.fsid();
    uint64_t parentInodeId = dentry.parentinodeid();
    std::string name = dentry.name();
    Prefix4SameParentDentry prefix(fsId, parentInodeId);
    std::string sprefix = prefix.SerializeToString();  // "1:1:"
    Key4Dentry key(fsId, parentInodeId, name);
    std::string lower = key.SerializeToString();  // "1:1:", "1:1:/a/b/c"

    // 2. iterator key/value pair one by one
    auto iterator = kvStorage_->SSeek(table4Dentry_, lower);
    iterator->DisablePrefixChecking();
    if (iterator->Status() < 0) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    DentryVec current;
    DentryList list(dentrys, limit, name, dentry.txid(), onlyDir);
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        std::string skey = iterator->Key();
        std::string svalue = iterator->Value();
        if (!StringStartWith(skey, sprefix)) {
            break;
        } else if (!iterator->ParseFromValue(&current)) {
            return MetaStatusCode::PARSE_FROM_STRING_FAILED;
        }

        list.PushBack(current);
        if (list.IsFull()) {
            break;
        }
    }

    if (list.Size() == 0) {
        return MetaStatusCode::NOT_FOUND;
    }
    return MetaStatusCode::OK;
}

MetaStatusCode DentryStorage::HandleTx(TX_OP_TYPE type, const Dentry& dentry) {
    WriteLockGuard w(rwLock_);

    Status s;
    Dentry out;
    DentryVec vec;
    std::string skey = DentryKey(dentry);
    MetaStatusCode rc = MetaStatusCode::OK;
    switch (type) {
        case TX_OP_TYPE::PREPARE:
            s = kvStorage_->SGet(table4Dentry_, skey, &vec);
            if (!s.ok() && !s.IsNotFound()) {
                rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
                break;
            }

            // OK || NOT_FOUND
            Insert2Vec(&vec, dentry);
            s = kvStorage_->SSet(table4Dentry_, skey, vec);
            if (!s.ok()) {
                rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
            }
            break;

        case TX_OP_TYPE::COMMIT:
            rc = Find(dentry, &out, &vec, true);
            if (rc == MetaStatusCode::OK ||
                rc == MetaStatusCode::NOT_FOUND) {
                rc = MetaStatusCode::OK;
            }
            break;

        case TX_OP_TYPE::ROLLBACK:
            s = kvStorage_->SGet(table4Dentry_, skey, &vec);
            if (!s.ok() && !s.IsNotFound()) {
                rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
                break;
            }

            // OK || NOT_FOUN
            Delete4Vec(&vec, dentry);
            if (vec.dentrys_size() == 0) {  // delete directly
                s = kvStorage_->SDel(table4Dentry_, skey);
            } else {
                s = kvStorage_->SSet(table4Dentry_, skey, vec);
            }
            if (!s.ok()) {
                rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
            }
            break;

        default:
            rc = MetaStatusCode::PARAM_ERROR;
    }

    return rc;
}

std::shared_ptr<Iterator> DentryStorage::GetAll() {
    return kvStorage_->SGetAll(table4Dentry_);
}

size_t DentryStorage::Size() {
    return kvStorage_->SSize(table4Dentry_);
}

MetaStatusCode DentryStorage::Clear() {
    ReadLockGuard w(rwLock_);
    Status s = kvStorage_->SClear(table4Dentry_);
    return s.ok() ? MetaStatusCode::OK : MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

}  // namespace metaserver
}  // namespace curvefs
