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
#include <functional>

#include "absl/container/btree_set.h"

#include "src/common/concurrent/rw_lock.h"
#include "curvefs/proto/metaserver.pb.h"

namespace curvefs {
namespace metaserver {

using curve::common::RWLock;
using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;
using Btree = absl::btree_set<Dentry>;

#define EQUAL(a) (lhs.a() == rhs.a())
#define LESS(a) (lhs.a() < rhs.a())
#define LESS2(a, b) (EQUAL(a) && LESS(b))
#define LESS3(a, b, c) (EQUAL(a) && LESS2(b, c))
#define LESS4(a, b, c, d) (EQUAL(a) && LESS3(b, c, d))

bool operator==(const Dentry& lhs, const Dentry& rhs);

class DentryStorage {
 public:
    using ContainerType = Btree;

    enum class TX_OP_TYPE {
        PREPARE,
        COMMIT,
        ROLLBACK,
    };

 public:
    virtual ~DentryStorage() = default;

    virtual MetaStatusCode Insert(const Dentry& dentry) = 0;

    virtual MetaStatusCode Delete(const Dentry& dentry) = 0;

    virtual MetaStatusCode Get(Dentry* dentry) = 0;

    virtual MetaStatusCode List(const Dentry& dentry,
                                std::vector<Dentry>* dentrys,
                                uint32_t limit,
                                bool onlyDir = false) = 0;

    virtual MetaStatusCode HandleTx(TX_OP_TYPE type, const Dentry& dentrys) = 0;

    virtual size_t Size() = 0;

    virtual void Clear() = 0;

    virtual ContainerType* GetContainer() = 0;
};

class MemoryDentryStorage : public DentryStorage {
 public:
    MetaStatusCode Insert(const Dentry& dentry) override;

    MetaStatusCode Delete(const Dentry& dentry) override;

    MetaStatusCode Get(Dentry* dentry) override;

    MetaStatusCode List(const Dentry& dentry,
                        std::vector<Dentry>* dentrys,
                        uint32_t limit,
                        bool onlyDir = false) override;

    MetaStatusCode HandleTx(TX_OP_TYPE type, const Dentry& dentry) override;

    size_t Size() override;

    void Clear() override;

    ContainerType* GetContainer() override;

 private:
    bool BelongSameOne(const Dentry& lhs, const Dentry& rhs);

    bool IsSameDentry(const Dentry& lhs, const Dentry& rhs);

    bool HasDeleteMarkFlag(const Dentry& dentry);

    Btree::iterator Find(const Dentry& dentry, bool compress);

 private:
    RWLock rwLock_;

    Btree dentryTree_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_DENTRY_STORAGE_H_
