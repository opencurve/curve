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

#ifndef CURVEFS_SRC_METASERVER_DENTRY_MANAGER_H_
#define CURVEFS_SRC_METASERVER_DENTRY_MANAGER_H_

#include <list>
#include <memory>
#include <string>
#include <vector>
#include <atomic>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/transaction.h"
#include "curvefs/src/metaserver/dentry_storage.h"

namespace curvefs {
namespace metaserver {
class DentryManager {
 public:
    DentryManager(std::shared_ptr<DentryStorage> dentryStorage,
                  std::shared_ptr<TxManager> txManger);

    bool Init();

    MetaStatusCode CreateDentry(const Dentry& dentry, int64_t logIndex,
        TxLock* txLock = nullptr);

    // only invoked from snapshot loadding
    MetaStatusCode CreateDentry(const DentryVec& vec, bool merge,
                                int64_t logIndex);

    MetaStatusCode DeleteDentry(const Dentry& dentry, int64_t logIndex,
        TxLock* txLock = nullptr);

    MetaStatusCode GetDentry(Dentry* dentry, TxLock* txLock = nullptr);

    MetaStatusCode ListDentry(const Dentry& dentry,
                              std::vector<Dentry>* dentrys, uint32_t limit,
                              bool onlyDir = false, TxLock* txLock = nullptr);

    void ClearDentry();

    MetaStatusCode HandleRenameTx(const std::vector<Dentry>& dentrys,
                                  int64_t logIndex);

    MetaStatusCode PrewriteRenameTx(const std::vector<Dentry>& dentrys,
        const TxLock& txLock, int64_t logIndex, TxLock* out);

    MetaStatusCode CheckTxStatus(const std::string& primaryKey,
        uint64_t startTs, uint64_t curTimestamp, int64_t logIndex);

    MetaStatusCode ResolveTxLock(const Dentry& dentry,
        uint64_t startTs, uint64_t commitTs, int64_t logIndex);

    MetaStatusCode CommitTx(const std::vector<Dentry>& dentrys,
        uint64_t startTs, uint64_t commitTs, int64_t logIndex);

 private:
    void Log4Dentry(const std::string& request, const Dentry& dentry);
    void Log4Code(const std::string& request, MetaStatusCode rc);

 private:
    std::shared_ptr<DentryStorage> dentryStorage_;
    std::shared_ptr<TxManager> txManager_;
    int64_t appliedIndex_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_DENTRY_MANAGER_H_
