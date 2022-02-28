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

#ifndef CURVEFS_SRC_METASERVER_DENTRY_STORAGE_H_
#define CURVEFS_SRC_METASERVER_DENTRY_STORAGE_H_

#include <list>
#include <string>
#include <vector>
#include <memory>
#include <functional>

#include "absl/container/btree_set.h"
#include "src/common/concurrent/rw_lock.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/storage/storage.h"

namespace curvefs {
namespace metaserver {

using curve::common::RWLock;
using ::curvefs::metaserver::storage::Iterator;
using KVStorage = ::curvefs::metaserver::storage::KVStorage;

#define EQUAL(a) (lhs.a() == rhs.a())
#define LESS(a) (lhs.a() < rhs.a())
#define LESS2(a, b) (EQUAL(a) && LESS(b))
#define LESS3(a, b, c) (EQUAL(a) && LESS2(b, c))
#define LESS4(a, b, c, d) (EQUAL(a) && LESS3(b, c, d))

bool operator==(const Dentry& lhs, const Dentry& rhs);

bool operator<(const Dentry& lhs, const Dentry& rhs);

class DentryStorage {
 public:
    using BTree = absl::btree_set<Dentry>;

    enum class TX_OP_TYPE {
        PREPARE,
        COMMIT,
        ROLLBACK,
    };

 public:
    DentryStorage(std::shared_ptr<KVStorage> kvStorage,
                  const std::string& tablename);

    MetaStatusCode Insert(const Dentry& dentry);

    MetaStatusCode Delete(const Dentry& dentry);

    MetaStatusCode Get(Dentry* dentry);

    MetaStatusCode List(const Dentry& dentry,
                        std::vector<Dentry>* dentrys,
                        uint32_t limit,
                        bool onlyDir = false);

    MetaStatusCode HandleTx(TX_OP_TYPE type, const Dentry& dentry);

    std::shared_ptr<Iterator> GetAll();

    size_t Size();

    MetaStatusCode Clear();

 private:
    std::string DentryKey(const Dentry& dentry, bool ignoreTxId = false);

    std::string SameParentKey(const Dentry& dentry);

    bool BelongSameOne(const Dentry& lhs, const Dentry& rhs);

    bool IsSameDentry(const Dentry& lhs, const Dentry& rhs);

    bool HasDeleteMarkFlag(const Dentry& dentry);

    bool CompressDentry(BTree* dentrys);

    MetaStatusCode Find(const Dentry& kDentry, Dentry* vDentry, bool compress);

 private:
    RWLock rwLock_;
    std::string tablename_;
    std::shared_ptr<KVStorage> kvStorage_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_DENTRY_STORAGE_H_
