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

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;

RenameTx::RenameTx(const std::vector<Dentry>& dentrys,
                   std::shared_ptr<DentryStorage> storage)
    : txId_(dentrys[0].txid()),
      txSequence_(dentrys[0].txsequence()),
      dentrys_(dentrys),
      storage_(storage) {}

bool RenameTx::Prepare(const std::string& txPayload, int64_t logIndex) {
    metaserver::TransactionRequest request;
    request.set_type(metaserver::TransactionRequest::Rename);
    request.set_rawpayload(txPayload);
    return storage_->PrepareTx(dentrys_, request, logIndex) ==
           MetaStatusCode::OK;
}

bool RenameTx::Commit(int64_t logIndex) {
    return storage_->CommitTx(dentrys_, logIndex) == MetaStatusCode::OK;
}

bool RenameTx::Rollback(int64_t logIndex) {
    return storage_->RollbackTx(dentrys_, logIndex) == MetaStatusCode::OK;
}

uint64_t RenameTx::GetTxId() { return txId_; }

uint64_t RenameTx::GetTxSequence() { return txSequence_; }

std::vector<Dentry>* RenameTx::GetDentrys() { return &dentrys_; }

const std::vector<Dentry>* RenameTx::GetDentrys() const { return &dentrys_; }

inline bool RenameTx::operator==(const RenameTx& rhs) {
    return dentrys_ == rhs.dentrys_;
}

std::ostream& operator<<(std::ostream& os, const RenameTx& renameTx) {
    auto dentrys = renameTx.dentrys_;
    os << "txId = " << renameTx.txId_;
    for (size_t i = 0; i < dentrys.size(); i++) {
        os << ", dentry[" << i << "] = (" << dentrys[i].ShortDebugString()
           << ")";
    }
    return os;
}

TxManager::TxManager(std::shared_ptr<DentryStorage> storage,
                     common::PartitionInfo partitionInfo)
    : storage_(std::move(storage)),
      conv_(),
      partitionInfo_(std::move(partitionInfo)) {}

MetaStatusCode TxManager::PreCheck(const std::vector<Dentry>& dentrys) {
    auto size = dentrys.size();
    if (size != 1 && size != 2) {
        return MetaStatusCode::PARAM_ERROR;
    } else if (size == 2) {
        if (dentrys[0].fsid() != dentrys[1].fsid() ||
            dentrys[0].txid() != dentrys[1].txid() ||
            dentrys[0].txsequence() != dentrys[1].txsequence()) {
            return MetaStatusCode::PARAM_ERROR;
        }
    }

    return MetaStatusCode::OK;
}

void TxManager::SerializeRenameTx(const RenameTx& in,
                                  PrepareRenameTxRequest* out) {
    const auto* dentrys = in.GetDentrys();
    out->set_poolid(partitionInfo_.poolid());
    out->set_copysetid(partitionInfo_.copysetid());
    out->set_partitionid(partitionInfo_.partitionid());
    *out->mutable_dentrys() = {dentrys->begin(), dentrys->end()};
}

bool TxManager::Init() {
    metaserver::TransactionRequest request;
    auto s = storage_->GetPendingTx(&request);
    if (s == MetaStatusCode::OK) {
        auto txType = request.type();
        if (txType == metaserver::TransactionRequest::None) {
            // NOTE: if tx type is none
            // means that pending tx is empty
            pendingTx_ = EMPTY_TX;
        } else if (txType == metaserver::TransactionRequest::Rename) {
            std::string txPayload = request.rawpayload();
            PrepareRenameTxRequest request;
            conv_.ParseFromString(txPayload, &request);
            RenameTx tx({request.dentrys().begin(), request.dentrys().end()},
                        storage_);
            pendingTx_ = tx;
        }
        return true;
    }
    return s == MetaStatusCode::NOT_FOUND;
}

MetaStatusCode TxManager::HandleRenameTx(const std::vector<Dentry>& dentrys,
                                         int64_t logIndex) {
    auto rc = PreCheck(dentrys);
    if (rc != MetaStatusCode::OK) {
        return rc;
    }

    // Handle pending TX
    RenameTx pendingTx;
    if (FindPendingTx(&pendingTx)) {
        auto txId = dentrys[0].txid();
        auto txSequence = dentrys[0].txsequence();
        if (txSequence != 0 && txSequence <= pendingTx.GetTxSequence()) {
            LOG(ERROR) << "HandlePendingTx failed, current transaction is stale"
                       << ", we will discard it. current tx sequence = "
                       << txSequence << ", pending tx sequence = "
                       << pendingTx.GetTxSequence();
            return MetaStatusCode::HANDLE_PENDING_TX_FAILED;
        } else if (!HandlePendingTx(txId, &pendingTx, logIndex)) {
            LOG(ERROR) << "HandlePendingTx failed, pendingTx: " << pendingTx;
            return MetaStatusCode::HANDLE_PENDING_TX_FAILED;
        }
        DeletePendingTx();
    }

    // Prepare for TX
    auto renameTx = RenameTx(dentrys, storage_);
    if (!InsertPendingTx(renameTx)) {
        LOG(ERROR) << "InsertPendingTx failed, renameTx: " << renameTx;
        return MetaStatusCode::HANDLE_TX_FAILED;
    } else {
        PrepareRenameTxRequest request;
        SerializeRenameTx(renameTx, &request);
        std::string txPayload;
        conv_.SerializeToString(request, &txPayload);
        if (!renameTx.Prepare(txPayload, logIndex)) {
            LOG(ERROR) << "Prepare for RenameTx failed, renameTx: " << renameTx;
            return MetaStatusCode::HANDLE_TX_FAILED;
        }
    }
    return MetaStatusCode::OK;
}

bool TxManager::InsertPendingTx(const RenameTx& tx) {
    WriteLockGuard w(rwLock_);
    if (pendingTx_ == EMPTY_TX) {
        pendingTx_ = tx;
        return true;
    }
    return false;
}

void TxManager::DeletePendingTx() {
    WriteLockGuard w(rwLock_);
    pendingTx_ = EMPTY_TX;
}

bool TxManager::FindPendingTx(RenameTx* pendingTx) {
    ReadLockGuard r(rwLock_);
    if (pendingTx_ == EMPTY_TX) {
        return false;
    }
    *pendingTx = pendingTx_;
    return true;
}

bool TxManager::HandlePendingTx(uint64_t txId, RenameTx* pendingTx,
                                int64_t logIndex) {
    if (txId > pendingTx->GetTxId()) {
        return pendingTx->Commit(logIndex);
    }
    return pendingTx->Rollback(logIndex);
}

};  // namespace metaserver
};  // namespace curvefs
