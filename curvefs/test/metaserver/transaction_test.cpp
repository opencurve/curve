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
 * Created Date: 2021-08-30
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/metaserver/transaction.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "curvefs/src/metaserver/dentry_manager.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/memory_storage.h"

namespace curvefs {
namespace metaserver {

using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::StorageOptions;
using ::curvefs::metaserver::storage::MemoryStorage;
using TX_OP_TYPE = DentryStorage::TX_OP_TYPE;

class TransactionTest : public ::testing::Test {
 protected:
    void SetUp() override {
        tablename_ = "partition:1";
        kvStorage_ = std::make_shared<MemoryStorage>(options_);
        dentryStorage_ = std::make_shared<DentryStorage>(
            kvStorage_, tablename_);
        txManager_ = std::make_shared<TxManager>(dentryStorage_);
        dentryManager_ = std::make_shared<DentryManager>(
            dentryStorage_, txManager_);
    }

    void TearDown() override {}

    Dentry GenDentry(uint32_t fsId,
                     uint64_t parentId,
                     const std::string& name,
                     uint64_t txId,
                     uint64_t inodeId,
                     uint32_t flag) {
        Dentry dentry;
        dentry.set_fsid(fsId);
        dentry.set_parentinodeid(parentId);
        dentry.set_name(name);
        dentry.set_txid(txId);
        dentry.set_inodeid(inodeId);
        dentry.set_flag(flag);
        return dentry;
    }

    void InsertDentrys(std::shared_ptr<DentryStorage> storage,
                       const std::vector<Dentry>&& dentrys) {
        for (const auto& dentry : dentrys) {
            auto rc = storage->HandleTx(TX_OP_TYPE::PREPARE, dentry);
            ASSERT_EQ(rc, MetaStatusCode::OK);
        }
        ASSERT_EQ(storage->Size(), dentrys.size());
    }

    void ASSERT_DENTRYS_EQ(const std::vector<Dentry>& lhs,
                           const std::vector<Dentry>&& rhs) {
        ASSERT_EQ(lhs, rhs);
    }

 protected:
    static const uint32_t DELETE = DentryFlag::DELETE_MARK_FLAG;
    static const uint32_t FILE = DentryFlag::TYPE_FILE_FLAG;

    std::string tablename_;
    StorageOptions options_;
    std::shared_ptr<KVStorage> kvStorage_;
    std::shared_ptr<DentryStorage> dentryStorage_;
    std::shared_ptr<DentryManager> dentryManager_;
    std::shared_ptr<TxManager> txManager_;
};

TEST_F(TransactionTest, PreCheck) {
    // CASE 1: empty dentrys
    auto dentrys = std::vector<Dentry>();
    auto rc = txManager_->HandleRenameTx(dentrys);
    ASSERT_EQ(rc, MetaStatusCode::PARAM_ERROR);

    // CASE 2: sizeof(dentrys) > 2
    dentrys = std::vector<Dentry>{
        // { fsId, parentId, name, txId, inodeId, flag }
        GenDentry(1, 0, "A", 0, 1, 0),
        GenDentry(1, 0, "B", 0, 2, 0),
        GenDentry(1, 0, "C", 0, 3, 0),
    };
    rc = txManager_->HandleRenameTx(dentrys);
    ASSERT_EQ(rc, MetaStatusCode::PARAM_ERROR);

    // CASE 3: dentrys fsids are different
    dentrys = std::vector<Dentry>{
        // { fsId, parentId, name, txId, inodeId, flag }
        GenDentry(1, 0, "A", 0, 1, 0),
        GenDentry(2, 0, "B", 0, 2, 0),
    };
    rc = txManager_->HandleRenameTx(dentrys);
    ASSERT_EQ(rc, MetaStatusCode::PARAM_ERROR);

    // CASE 4: dentrys txids are different
    dentrys = std::vector<Dentry>{
        // { fsId, parentId, name, txId, inodeId, flag }
        GenDentry(1, 0, "A", 0, 1, 0),
        GenDentry(1, 0, "B", 1, 2, 0),
    };
    rc = txManager_->HandleRenameTx(dentrys);
    ASSERT_EQ(rc, MetaStatusCode::PARAM_ERROR);
}

TEST_F(TransactionTest, HandleTxWithCommit) {
    InsertDentrys(dentryStorage_, std::vector<Dentry>{
        // { fsId, parentId, name, txId, inodeId, flag }
        GenDentry(1, 0, "A", 0, 1, 0),
    });

    // step-1: prepare tx success (rename A B)
    auto dentrys = std::vector<Dentry> {
        // { fsId, parentId, name, txId, inodeId, flag }
        GenDentry(1, 0, "A", 1, 1, DELETE),
        GenDentry(1, 0, "B", 1, 1, 0),
    };
    auto rc = txManager_->HandleRenameTx(dentrys);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(dentryStorage_->Size(), 3);

    // step-2: get dentry with txid=0
    auto dentry = GenDentry(1, 0, "A", 0, 0, 0);
    ASSERT_EQ(dentryManager_->GetDentry(&dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentry.inodeid(), 1);

    dentry = GenDentry(1, 0, "B", 0, 0, 0);
    ASSERT_EQ(dentryManager_->GetDentry(&dentry), MetaStatusCode::NOT_FOUND);
    ASSERT_EQ(dentryStorage_->Size(), 3);

    // step-3: get dentry with txid=1
    dentry = GenDentry(1, 0, "A", 1, 0, 0);
    ASSERT_EQ(dentryManager_->GetDentry(&dentry), MetaStatusCode::NOT_FOUND);

    dentry = GenDentry(1, 0, "B", 1, 0, 0);
    ASSERT_EQ(dentryManager_->GetDentry(&dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentry.inodeid(), 1);
    ASSERT_EQ(dentryStorage_->Size(), 3);

    // step-4: prepare a new tx success with commit
    dentrys = std::vector<Dentry> {
        // { fsId, parentId, name, txId, inodeId, flag }
        GenDentry(1, 0, "C", 2, 2, 0),
    };
    rc = txManager_->HandleRenameTx(dentrys);
    ASSERT_EQ(rc, MetaStatusCode::OK);

    // step-5: check dentrys
    dentrys.clear();
    dentry = GenDentry(1, 0, "", 2, 0, 0);
    rc = dentryManager_->ListDentry(dentry, &dentrys, 0);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 2);
    ASSERT_DENTRYS_EQ(dentrys, std::vector<Dentry>{
        GenDentry(1, 0, "B", 1, 1, 0),
        GenDentry(1, 0, "C", 2, 2, 0),
    });
    ASSERT_EQ(dentryStorage_->Size(), 2);
}

TEST_F(TransactionTest, HandleTxWithRollback) {
    InsertDentrys(dentryStorage_, std::vector<Dentry>{
        // { fsId, parentId, name, txId, inodeId, flag }
        GenDentry(1, 0, "A", 0, 1, 0),
    });

    // step-1: prepare tx success (rename A B)
    auto dentrys = std::vector<Dentry> {
        // { fsId, parentId, name, txId, inodeId, flag }
        GenDentry(1, 0, "A", 1, 1, DELETE),
        GenDentry(1, 0, "B", 1, 1, 0),
    };
    auto rc = txManager_->HandleRenameTx(dentrys);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(dentryStorage_->Size(), 3);

    // step-2: get dentry with txid=0
    auto dentry = GenDentry(1, 0, "A", 0, 0, 0);
    ASSERT_EQ(dentryManager_->GetDentry(&dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentry.inodeid(), 1);

    dentry = GenDentry(1, 0, "B", 0, 0, 0);
    ASSERT_EQ(dentryManager_->GetDentry(&dentry), MetaStatusCode::NOT_FOUND);
    ASSERT_EQ(dentryStorage_->Size(), 3);

    // step-3: get dentry with txid=1
    dentry = GenDentry(1, 0, "A", 1, 0, 0);
    ASSERT_EQ(dentryManager_->GetDentry(&dentry), MetaStatusCode::NOT_FOUND);

    dentry = GenDentry(1, 0, "B", 1, 0, 0);
    ASSERT_EQ(dentryManager_->GetDentry(&dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentry.inodeid(), 1);
    ASSERT_EQ(dentryStorage_->Size(), 3);

    // step-4: prepare a new tx success with rollback
    dentrys = std::vector<Dentry> {
        // { fsId, parentId, name, txId, inodeId, flag }
        GenDentry(1, 0, "C", 1, 2, 0),
    };
    rc = txManager_->HandleRenameTx(dentrys);
    ASSERT_EQ(rc, MetaStatusCode::OK);

    // step-5: check dentrys
    dentrys.clear();
    dentry = GenDentry(1, 0, "", 1, 0, 0);
    rc = dentryManager_->ListDentry(dentry, &dentrys, 0);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 2);
    ASSERT_DENTRYS_EQ(dentrys, std::vector<Dentry>{
        GenDentry(1, 0, "A", 0, 1, 0),
        GenDentry(1, 0, "C", 1, 2, 0),
    });
    ASSERT_EQ(dentryStorage_->Size(), 2);
}

TEST_F(TransactionTest, HandleTxWithTargetExist) {
    /**
     *      /(0)
     *    /    \
     *   A(1)  B(2)
     *         \
     *         A(3)
     *
     * rename /A /B/A
     */
    InsertDentrys(dentryStorage_, std::vector<Dentry>{
        // { fsId, parentId, name, txId, inodeId, flag }
        GenDentry(1, 0, "A", 0, 1, 0),
        GenDentry(1, 0, "B", 0, 2, 0),
        GenDentry(1, 2, "A", 0, 3, 0),
    });

    // step-1: prepare tx success (rename A B)
    auto dentrys = std::vector<Dentry> {
        // { fsId, parentId, name, txId, inodeId, flag }
        GenDentry(1, 0, "A", 1, 1, FILE | DELETE),
        GenDentry(1, 2, "A", 1, 1, FILE),
    };
    auto rc = txManager_->HandleRenameTx(dentrys);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(dentryStorage_->Size(), 5);

    // step-2: get dentry with txid=0
    auto dentry = GenDentry(1, 0, "A", 0, 0, 0);
    ASSERT_EQ(dentryManager_->GetDentry(&dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentry.inodeid(), 1);

    dentry = GenDentry(1, 2, "A", 0, 0, 0);
    ASSERT_EQ(dentryManager_->GetDentry(&dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentry.inodeid(), 3);
    ASSERT_EQ(dentryStorage_->Size(), 5);

    // step-3: get dentry with txid=1
    dentry = GenDentry(1, 0, "A", 1, 0, 0);
    ASSERT_EQ(dentryManager_->GetDentry(&dentry), MetaStatusCode::NOT_FOUND);

    dentry = GenDentry(1, 2, "A", 1, 0, 0);
    ASSERT_EQ(dentryManager_->GetDentry(&dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentry.inodeid(), 1);
    ASSERT_EQ(dentryStorage_->Size(), 5);

    // step-4: prepare a new tx success with commit
    dentrys = std::vector<Dentry> {
        // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
        GenDentry(1, 0, "C", 2, 4, 0),
    };
    rc = txManager_->HandleRenameTx(dentrys);
    ASSERT_EQ(rc, MetaStatusCode::OK);

    // step-5: check dentrys
    dentrys.clear();
    dentry = GenDentry(1, 0, "", 1, 0, 0);
    rc = dentryManager_->ListDentry(dentry, &dentrys, 0);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 1);
    ASSERT_DENTRYS_EQ(dentrys, std::vector<Dentry>{
        GenDentry(1, 0, "B", 0, 2, 0),
    });

    dentrys.clear();
    dentry = GenDentry(1, 2, "", 1, 0, 0);
    rc = dentryManager_->ListDentry(dentry, &dentrys, 0);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 1);
    ASSERT_DENTRYS_EQ(dentrys, std::vector<Dentry>{
        GenDentry(1, 2, "A", 1, 1, FILE),
    });

    ASSERT_EQ(dentryStorage_->Size(), 3);  // /B /B/A /C(pending)
}

}  // namespace metaserver
}  // namespace curvefs
