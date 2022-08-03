/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: Mon Aug 30 2021
 * Author: hzwuhongsong
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "curvefs/test/client/mock_disk_cache_write.h"
#include "curvefs/test/client/mock_disk_cache_read.h"
#include "curvefs/test/client/mock_test_posix_wapper.h"
#include "curvefs/src/client/cache/disk_cache_base.h"

namespace curvefs {
namespace client {

using ::testing::_;
using ::testing::Contains;
using ::curve::common::Configuration;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::Mock;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::ReturnNull;
using ::testing::ReturnPointee;
using ::testing::NotNull;
using ::testing::StrEq;
using ::testing::ElementsAre;
using ::testing::SetArgPointee;
using ::testing::ReturnArg;

class TestDiskCacheBase : public ::testing::Test {
 protected:
    TestDiskCacheBase() {}
    ~TestDiskCacheBase() {}

    virtual void SetUp() {
        wrapper_ = std::make_shared<MockPosixWrapper>();
        diskCacheBase_ = std::make_shared<DiskCacheBase>();
        diskCacheBase_->Init(wrapper_, "/mnt/test");
    }

    virtual void TearDown() {
        // allows the destructor of lfs_ to be invoked correctly
        Mock::VerifyAndClear(wrapper_.get());
    }

    std::shared_ptr<DiskCacheBase> diskCacheBase_;
    std::shared_ptr<MockPosixWrapper> wrapper_;
};

TEST_F(TestDiskCacheBase, CreateIoDir) {
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .WillOnce(Return(-1));
    EXPECT_CALL(*wrapper_, mkdir(_, _))
        .WillOnce(Return(-1));
    int ret = diskCacheBase_->CreateIoDir(true);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .WillOnce(Return(-1));
    EXPECT_CALL(*wrapper_, mkdir(_, _))
        .WillOnce(Return(0));
    ret = diskCacheBase_->CreateIoDir(true);
    ASSERT_EQ(0, ret);

    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .WillOnce(Return(0));
    ret = diskCacheBase_->CreateIoDir(true);
    ASSERT_EQ(0, ret);
}

TEST_F(TestDiskCacheBase, IsFileExist) {
    std::string fileName = "test";
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .WillOnce(Return(-1));
    bool ret = diskCacheBase_->IsFileExist(fileName);
    ASSERT_EQ(false, ret);

    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .WillOnce(Return(0));
    ret = diskCacheBase_->IsFileExist(fileName);
    ASSERT_EQ(true, ret);
}


}  // namespace client
}  // namespace curvefs
