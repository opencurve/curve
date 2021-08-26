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
 * Project: Curve
 * Created Date: 2021-08-19
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/metaserver/dentry_storage.h"
#include "curvefs/src/metaserver/transaction.h"

namespace curvefs {
namespace metaserver {

#define FOR_EACH_DENTRY(action) \
do { \
    for (const auto& dentry : dentrys_) { \
        auto rc = storage_->HandleTx( \
            DentryStorage::TX_OP_TYPE::action, dentry); \
        if (rc != MetaStatusCode::OK) { \
            return false; \
        } \
    } \
} while (0)

RenameTx::RenameTx(uint32_t fsId,
                   uint64_t txId,
                   const std::vector<Dentry>& dentrys,
                   std::shared_ptr<DentryStorage> storage)
    : fsId_(fsId), txId_(txId) , dentrys_(dentrys), storage_(storage) {}

bool RenameTx::Prepare() {
    FOR_EACH_DENTRY(PREPARE);
    return true;
}

bool RenameTx::Commit() {
    FOR_EACH_DENTRY(COMMIT);
    return true;
}

bool RenameTx::Rollback() {
    FOR_EACH_DENTRY(ROLLBACK);
    return true;
}

inline uint32_t RenameTx::GetFsId() const {
    return fsId_;
}

inline uint64_t RenameTx::GetTxId() const {
    return txId_;
}

TxManager::TxManager(std::shared_ptr<DentryStorage> storage)
    : storage_(storage) {}

MetaStatusCode TxManager::PreCheck(const std::vector<Dentry>& dentrys) {
    auto size = dentrys.size();
    if (size != 1 && size != 2) {
        return MetaStatusCode::PARAM_ERROR;
    } else if (size == 2) {
         if (dentrys[0].fsid() != dentrys[1].fsid() ||
             dentrys[0].txid() != dentrys[1].txid()) {
            return MetaStatusCode::PARAM_ERROR;
        }
    }

    return MetaStatusCode::OK;
}

MetaStatusCode TxManager::HandleRenameTx(const std::vector<Dentry>& dentrys) {
    auto rc = PreCheck(dentrys);
    if (rc != MetaStatusCode::OK) {
        return rc;
    }

    // Handle pending TX
    RenameTx pendingTx;
    auto fsId = dentrys[0].fsid();
    auto txId = dentrys[0].txid();
    if (FindPendingTx(fsId, &pendingTx)) {
        if (!HandlePendingTx(txId, &pendingTx)) {
            return MetaStatusCode::HANDLE_PENDING_TX_FAILED;
        }
        DeletePendingTx(fsId);
    }

    // Prepare for TX
    auto renameTx = RenameTx(fsId, txId, dentrys, storage_);
    if (!InsertPendingTx(fsId, renameTx) ||
        !renameTx.Prepare()) {
        return MetaStatusCode::HANDLE_TX_FAILED;
    }

    return MetaStatusCode::OK;
}

bool TxManager::InsertPendingTx(uint32_t fsId, const RenameTx& tx) {
    ReadLockGuard r(rwLock_);
    return pendingTx_.emplace(fsId, tx).second;
}

void TxManager::DeletePendingTx(uint32_t fsId) {
    WriteLockGuard w(rwLock_);
    pendingTx_.erase(fsId);
}

bool TxManager::FindPendingTx(uint32_t fsId, RenameTx* pendingTx) {
    ReadLockGuard r(rwLock_);
    auto iter = pendingTx_.find(fsId);
    if (iter == pendingTx_.end()) {
        return false;
    }

    *pendingTx = iter->second;
    return true;
}

bool TxManager::HandlePendingTx(uint64_t txId, RenameTx* pendingTx) {
    if (txId > pendingTx->GetTxId()) {
        return pendingTx->Commit();
    }
    return pendingTx->Rollback();
}

};  // namespace metaserver
};  // namespace curvefs
