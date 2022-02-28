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

#ifndef CURVEFS_SRC_METASERVER_TRANSACTION_H_
#define CURVEFS_SRC_METASERVER_TRANSACTION_H_

#include <vector>
#include <memory>
#include <unordered_map>

#include "src/common/concurrent/rw_lock.h"
#include "curvefs/src/metaserver/dentry_storage.h"

namespace curvefs {
namespace metaserver {

class RenameTx {
 public:
    RenameTx() = default;

    RenameTx(const std::vector<Dentry>& dentrys,
             std::shared_ptr<DentryStorage> storage);

    bool Prepare();

    bool Commit();

    bool Rollback();

    uint64_t GetTxId();

    std::vector<Dentry>* GetDentrys();

    bool operator==(const RenameTx& rhs);

    friend std::ostream& operator<<(std::ostream& os, const RenameTx& renameTx);

 private:
    uint64_t txId_;

    std::vector<Dentry> dentrys_;

    std::shared_ptr<DentryStorage> storage_;
};

class TxManager {
 public:
    explicit TxManager(std::shared_ptr<DentryStorage> storage);

    MetaStatusCode HandleRenameTx(const std::vector<Dentry>& dentrys);

    MetaStatusCode PreCheck(const std::vector<Dentry>& dentrys);

    bool InsertPendingTx(const RenameTx& tx);

    bool FindPendingTx(RenameTx* pendingTx);

    void DeletePendingTx();

    bool HandlePendingTx(uint64_t txId, RenameTx* pendingTx);

 private:
    RWLock rwLock_;

    std::shared_ptr<DentryStorage> storage_;

    RenameTx EMPTY_TX, pendingTx_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_TRANSACTION_H_
