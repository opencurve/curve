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
 * @Project: curve
 * @Date: 2021-09-01
 * @Author: xuchaojie
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "curvefs/src/metaserver/trash_manager.h"
#include "curvefs/test/metaserver/mock_inode_storage.h"

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SaveArg;

namespace curvefs {
namespace metaserver {

class TestTrash : public ::testing::Test {
 protected:
    void SetUp() override {
        inodeStorage_ = std::make_shared<MockInodeStorage>();
        trashManager_ = std::make_shared<TrashManager>();
    }

    void TearDown() override {
        inodeStorage_ = nullptr;
        trashManager_ = nullptr;
    }

 protected:
    std::shared_ptr<MockInodeStorage> inodeStorage_;
    std::shared_ptr<TrashManager> trashManager_;
};

TEST_F(TestTrash, testAdd3ItemAndDelete) {
    TrashOption option;
    option.scanPeriodSec = 1;
    option.expiredAfterSec = 1;

    trashManager_->Init(option);
    trashManager_->Run();

    auto trash1 = std::make_shared<TrashImpl>(inodeStorage_);
    auto trash2 = std::make_shared<TrashImpl>(inodeStorage_);
    trashManager_->Add(1, trash1);
    trashManager_->Add(2, trash2);

    EXPECT_CALL(*inodeStorage_, Delete(_))
        .Times(3)
        .WillRepeatedly(Return(MetaStatusCode::OK));

    trash1->Add(1, 1, 0);
    trash1->Add(1, 2, 0);
    trash2->Add(2, 1, 0);

    std::this_thread::sleep_for(std::chrono::seconds(5));

    std::list<TrashItem> list;

    trashManager_->ListItems(&list);

    ASSERT_EQ(0, list.size());

    trashManager_->Fini();
}

}  // namespace metaserver
}  // namespace curvefs
