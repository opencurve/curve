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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */
#include "curvefs/src/client/dentry_manager.h"

#include <cstdint>
#include <string>
#include <list>
#include <vector>
#include <utility>
#include <unordered_map>
#include "curvefs/src/metaserver/storage/converter.h"

using ::curvefs::metaserver::MetaStatusCode_Name;
using ::curvefs::metaserver::storage::Key4Dentry;

namespace curvefs {
namespace client {
namespace common {
DECLARE_bool(enableCto);
}  // namespace common
}  // namespace client
}  // namespace curvefs

namespace curvefs {
namespace client {

using curve::common::WriteLockGuard;
using NameLockGuard = ::curve::common::GenericNameLockGuard<Mutex>;
using ::curvefs::client::filesystem::ToFSError;

MetaStatusCode DentryCacheManagerImpl::CheckTxStatus(
    const std::string primaryKey, uint64_t startTs, uint64_t curTimestamp) {
    Key4Dentry key4Dentry;
    if (!key4Dentry.ParseFromString(primaryKey)) {
        LOG(ERROR) << "CheckTxStatus parse primary key failed, primaryKey = "
                   << primaryKey;
        return MetaStatusCode::PARSE_FROM_STRING_FAILED;
    }
    return metaClient_->CheckTxStatus(key4Dentry.fsId, key4Dentry.parentInodeId,
        primaryKey, startTs, curTimestamp);
}

MetaStatusCode DentryCacheManagerImpl::ResolveTxLock(const Dentry &dentry,
    uint64_t startTs, uint64_t commitTs) {
    return metaClient_->ResolveTxLock(dentry, startTs, commitTs);
}

MetaStatusCode DentryCacheManagerImpl::CheckAndResolveTx(const Dentry& dentry,
    const TxLock& txLock, uint64_t timestamp, uint64_t commitTs) {
    auto rt = CheckTxStatus(txLock.primarykey(), txLock.startts(), timestamp);
    switch (rt) {
        case MetaStatusCode::TX_COMMITTED:
            return ResolveTxLock(dentry, txLock.startts(), commitTs);
        case MetaStatusCode::TX_ROLLBACKED:
        case MetaStatusCode::TX_TIMEOUT:
            return ResolveTxLock(dentry, txLock.startts());
        default:
            LOG(ERROR) << "CheckTxStatus unexpected rt = "
                       << MetaStatusCode_Name(rt);
            return rt;
    }
    return MetaStatusCode::OK;
}

CURVEFS_ERROR DentryCacheManagerImpl::GetDentry(uint64_t parent,
                                                const std::string &name,
                                                Dentry *out) {
    std::string key = GetDentryCacheKey(parent, name);
    NameLockGuard lock(nameLock_, key);
    TxLock txLockOut;
    MetaStatusCode ret = metaClient_->GetDentry(fsId_, parent, name, out,
        &txLockOut);
    while (ret == MetaStatusCode::TX_KEY_LOCKED) {
        uint64_t ts = 0;
        uint64_t timestamp = 0;
        if (mdsClient_->Tso(&ts, &timestamp) != FSStatusCode::OK) {
            LOG(ERROR) << "GetDentry Tso failed, parent = " << parent
                       << ", name = " << name;
            return CURVEFS_ERROR::INTERNAL;
        }
        Dentry dentry;
        dentry.set_fsid(fsId_);
        dentry.set_parentinodeid(parent);
        dentry.set_name(name);
        MetaStatusCode rc = CheckAndResolveTx(dentry, txLockOut, timestamp, ts);
        if (rc != MetaStatusCode::OK) {
            LOG(ERROR) << "GetDentry CheckAndResolveTx failed, rc = "
                       << MetaStatusCode_Name(rc)
                       << ", parent = " << parent << ", name = " << name;
            return CURVEFS_ERROR::INTERNAL;
        }
        ret = metaClient_->GetDentry(fsId_, parent, name, out, &txLockOut);
    }

    if (ret != MetaStatusCode::OK) {
        LOG_IF(ERROR, ret != MetaStatusCode::NOT_FOUND)
            << "metaClient_ GetDentry failed, MetaStatusCode = " << ret
            << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)
            << ", parent = " << parent << ", name = " << name;
        return ToFSError(ret);
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR DentryCacheManagerImpl::CreateDentry(const Dentry &dentry) {
    std::string key = GetDentryCacheKey(dentry.parentinodeid(), dentry.name());
    NameLockGuard lock(nameLock_, key);
    TxLock txLockOut;
    MetaStatusCode ret = metaClient_->CreateDentry(dentry, &txLockOut);
    while (ret == MetaStatusCode::TX_KEY_LOCKED) {
        uint64_t ts = 0;
        uint64_t timestamp = 0;
        if (mdsClient_->Tso(&ts, &timestamp) != FSStatusCode::OK) {
            LOG(ERROR) << "CreateDentry Tso failed, dentry = "
                       << dentry.ShortDebugString();
            return CURVEFS_ERROR::INTERNAL;
        }
        MetaStatusCode rc = CheckAndResolveTx(dentry, txLockOut, timestamp, ts);
        if (rc != MetaStatusCode::OK) {
            LOG(ERROR) << "CreateDentry CheckAndResolveTx failed, rc = "
                       << MetaStatusCode_Name(rc) << ", dentry = "
                       << dentry.ShortDebugString();
            return CURVEFS_ERROR::INTERNAL;
        }
        ret = metaClient_->CreateDentry(dentry, &txLockOut);
    }
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "metaClient_ CreateDentry failed"
                   << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)
                   << ", parent = " << dentry.parentinodeid()
                   << ", name = " << dentry.name();
        return ToFSError(ret);
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR DentryCacheManagerImpl::DeleteDentry(uint64_t parent,
                                                   const std::string &name,
                                                   FsFileType type) {
    std::string key = GetDentryCacheKey(parent, name);
    NameLockGuard lock(nameLock_, key);

    TxLock txLockOut;
    MetaStatusCode ret = metaClient_->DeleteDentry(
        fsId_, parent, name, type, &txLockOut);
    while (ret == MetaStatusCode::TX_KEY_LOCKED) {
        uint64_t ts = 0;
        uint64_t timestamp = 0;
        if (mdsClient_->Tso(&ts, &timestamp) != FSStatusCode::OK) {
            LOG(ERROR) << "DeleteDentry Tso failed, parent = " << parent
                       << ", name = " << name;
            return CURVEFS_ERROR::INTERNAL;
        }
        Dentry dentry;
        dentry.set_fsid(fsId_);
        dentry.set_parentinodeid(parent);
        dentry.set_name(name);
        MetaStatusCode rc = CheckAndResolveTx(dentry, txLockOut, timestamp, ts);
        if (rc != MetaStatusCode::OK) {
            LOG(ERROR) << "DeleteDentry CheckAndResolveTx failed, rc = "
                       << MetaStatusCode_Name(rc) << ", parent = " << parent
                       << ", name = " << name;
            return CURVEFS_ERROR::INTERNAL;
        }
        ret = metaClient_->DeleteDentry(fsId_, parent, name, type, &txLockOut);
    }

    if (ret != MetaStatusCode::OK && ret != MetaStatusCode::NOT_FOUND) {
        LOG(ERROR) << "metaClient_ DeleteInode failed, MetaStatusCode = " << ret
                   << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)
                   << ", parent = " << parent << ", name = " << name;
        return ToFSError(ret);
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR DentryCacheManagerImpl::ListDentry(uint64_t parent,
                                                 std::list<Dentry> *dentryList,
                                                 uint32_t limit,
                                                 bool onlyDir,
                                                 uint32_t nlink) {
    dentryList->clear();
    // means no dir under this dir
    if (onlyDir && nlink == 2) {
        LOG(INFO) << "ListDentry parent = " << parent
                  << ", onlyDir = 1 and nlink = 2, return directly";
        return CURVEFS_ERROR::OK;
    }

    MetaStatusCode ret = MetaStatusCode::OK;
    bool perceed = true;
    std::string last = "";
    TxLock txLockOut;
    do {
        std::list<Dentry> part;
        ret = metaClient_->ListDentry(fsId_, parent, last, limit, onlyDir,
                                      &part, &txLockOut);
        VLOG(6) << "ListDentry fsId = " << fsId_ << ", parent = " << parent
                << ", last = " << last << ", count = " << limit
                << ", onlyDir = " << onlyDir
                << ", ret = " << ret << ", part.size() = " << part.size();
        if (ret == MetaStatusCode::TX_KEY_LOCKED) {
            uint64_t ts = 0;
            uint64_t timestamp = 0;
            if (mdsClient_->Tso(&ts, &timestamp) != FSStatusCode::OK) {
                LOG(ERROR) << "ListDentry Tso failed, parent = " << parent;
                return CURVEFS_ERROR::INTERNAL;
            }
            Dentry dentry;
            dentry.set_fsid(fsId_);
            dentry.set_parentinodeid(parent);
            if (part.empty()) {
                LOG(ERROR) << "ListDentry tx key locked, but part is empty"
                           << ", parent = " << parent;
                return CURVEFS_ERROR::INTERNAL;
            }
            dentry.set_name(part.back().name());
            part.pop_back();
            MetaStatusCode rc = CheckAndResolveTx(
                dentry, txLockOut, timestamp, ts);
            if (rc != MetaStatusCode::OK) {
                LOG(ERROR) << "ListDentry CheckAndResolveTx failed, rc = "
                           << MetaStatusCode_Name(rc)
                           << ", parent = " << parent;
                return CURVEFS_ERROR::INTERNAL;
            }
        } else if (ret != MetaStatusCode::OK) {
            LOG(ERROR) << "metaClient_ ListDentry failed"
                       << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)
                       << ", parent = " << parent << ", last = " << last
                       << ", count = " << limit << ", onlyDir = " << onlyDir;
            return ToFSError(ret);
        }

        if (!onlyDir) {
            if (part.size() < limit && ret != MetaStatusCode::TX_KEY_LOCKED) {
                perceed = false;
            }
            if (!part.empty()) {
                last = part.back().name();
                dentryList->splice(dentryList->end(), part);
            }
        } else {
            // means iterate over the range
            if (part.empty() && ret != MetaStatusCode::TX_KEY_LOCKED) {
                perceed = false;
            } else {
                if (!part.empty()) {
                    last = part.back().name();
                    if (part.back().type() != FsFileType::TYPE_DIRECTORY) {
                        part.pop_back();
                    }
                    dentryList->splice(dentryList->end(), part);
                }
                // means already get all the dir under this dir
                if (nlink - dentryList->size() == 2) {
                    perceed = false;
                }
            }
        }
    } while (perceed);

    return CURVEFS_ERROR::OK;
}

}  // namespace client
}  // namespace curvefs
