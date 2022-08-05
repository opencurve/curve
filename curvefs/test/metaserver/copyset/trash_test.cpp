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
 * Created Date: Tue Aug 31 15:25:38 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/trash.h"

#include <gtest/gtest.h>

#include <memory>

#include "test/fs/mock_local_filesystem.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::curve::fs::MockLocalFileSystem;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

class TrashTest : public testing::Test {
 protected:
    void SetUp() override {
        mocklfs_.reset(new MockLocalFileSystem());
        trash_.reset(new CopysetTrash());

        options.trashUri = "local://./runlog/curvefs/trash_test0/trash";
        options.expiredAfterSec = 1;
        options.scanPeriodSec = 1;

        ASSERT_TRUE(trash_->Init(options, mocklfs_.get()));
    }

    void TearDown() override {}

 protected:
    std::unique_ptr<MockLocalFileSystem> mocklfs_;
    CopysetTrashOptions options;
    std::unique_ptr<CopysetTrash> trash_;
};

TEST_F(TrashTest, RecycleCopysetTest) {
    // trash dir not exist, and create failed
    {
        EXPECT_CALL(*mocklfs_, DirExists(_))
            .WillOnce(Return(false));

        EXPECT_CALL(*mocklfs_, Mkdir(_, _))
            .WillOnce(Return(-1));

        EXPECT_FALSE(trash_->RecycleCopyset("/mnt/data/copysets/123456789"));
    }

    // dest path conflict
    {
        EXPECT_CALL(*mocklfs_, DirExists(_))
            .Times(2)
            .WillRepeatedly(Return(true));
        EXPECT_FALSE(trash_->RecycleCopyset("/mnt/data/copysets/123456789"));
    }

    // rename failed
    {
        EXPECT_CALL(*mocklfs_, DirExists(_))
            .Times(2)
            .WillOnce(Return(true))
            .WillOnce(Return(false));

        EXPECT_CALL(*mocklfs_, Rename(_, _, _))
            .WillOnce(Return(-1));

        EXPECT_FALSE(trash_->RecycleCopyset("/mnt/data/copysets/123456789"));
    }

    // success
    {
        EXPECT_CALL(*mocklfs_, DirExists(_))
            .Times(2)
            .WillOnce(Return(true))
            .WillOnce(Return(false));

        EXPECT_CALL(*mocklfs_, Rename(_, _, _))
            .WillOnce(Return(0));

        EXPECT_TRUE(trash_->RecycleCopyset("/mnt/data/copysets/123456789"));
    }
}

TEST_F(TrashTest, DeleteExpiredCopysetsTest_TrashPathNotExist) {
    EXPECT_CALL(*mocklfs_, DirExists(_))
        .WillRepeatedly(Return(false));

    EXPECT_CALL(*mocklfs_, List(_, _))
        .Times(0);

    trash_->Start();
    std::this_thread::sleep_for(std::chrono::seconds(10));
    trash_->Stop();
}

TEST_F(TrashTest, DeleteExpiredCopysetsTest_ListDirFailed) {
    EXPECT_CALL(*mocklfs_, DirExists(_))
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*mocklfs_, List(_, _))
        .WillRepeatedly(Return(-1));

    EXPECT_CALL(*mocklfs_, Open(_, _))
        .Times(0);

    trash_->Start();
    std::this_thread::sleep_for(std::chrono::seconds(10));
    trash_->Stop();
}

TEST_F(TrashTest, DeleteExpiredCopysetsTest_ListDirEmpty) {
    EXPECT_CALL(*mocklfs_, DirExists(_))
        .WillRepeatedly(Return(true));

    std::vector<std::string> dirs;
    EXPECT_CALL(*mocklfs_, List(_, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(dirs), Return(0)));

    EXPECT_CALL(*mocklfs_, Open(_, _))
        .Times(0);

    trash_->Start();
    std::this_thread::sleep_for(std::chrono::seconds(10));
    trash_->Stop();
}

TEST_F(TrashTest, DeleteExpiredCopysetsTest_InvalidCopysetPath) {
    EXPECT_CALL(*mocklfs_, DirExists(_))
        .WillRepeatedly(Return(true));

    std::vector<std::string> dirs;
    dirs.push_back("/mnt/hello/world");
    dirs.push_back("1.2");
    EXPECT_CALL(*mocklfs_, List(_, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(dirs), Return(0)));

    EXPECT_CALL(*mocklfs_, Open(_, _))
        .Times(0);

    trash_->Start();
    std::this_thread::sleep_for(std::chrono::seconds(10));
    trash_->Stop();
}

TEST_F(TrashTest, DeleteExpiredCopysetsTest_NotExpired) {
    EXPECT_CALL(*mocklfs_, DirExists(_))
        .WillRepeatedly(Return(true));

    std::vector<std::string> dirs;
    time_t now = time(nullptr);
    dirs.push_back("4294967297." + std::to_string(now));
    EXPECT_CALL(*mocklfs_, List(_, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(dirs), Return(0)));

    EXPECT_CALL(*mocklfs_, Open(_, _))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*mocklfs_, Close(_))
        .WillRepeatedly(Return(0));

    EXPECT_CALL(*mocklfs_, Fstat(_, _))
        .WillRepeatedly(Invoke([](int fd, struct stat* dirInfo) {
            dirInfo->st_ctime = time(nullptr) * 2;
            return 0;
        }));

    EXPECT_CALL(*mocklfs_, Delete(_))
        .Times(0);

    trash_->Start();
    std::this_thread::sleep_for(std::chrono::seconds(10));
    trash_->Stop();
}

TEST_F(TrashTest, DeleteExpiredCopysetsTest_Expired) {
    EXPECT_CALL(*mocklfs_, DirExists(_))
        .WillRepeatedly(Return(true));

    std::vector<std::string> dirs;
    time_t now = time(nullptr);
    dirs.push_back("4294967297." + std::to_string(now));
    EXPECT_CALL(*mocklfs_, List(_, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(dirs), Return(0)));

    EXPECT_CALL(*mocklfs_, Open(_, _))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*mocklfs_, Close(_))
        .WillRepeatedly(Return(0));

    EXPECT_CALL(*mocklfs_, Fstat(_, _))
        .WillRepeatedly(Invoke([](int fd, struct stat* dirInfo) {
            dirInfo->st_ctime = 0;
            return 0;
        }));

    EXPECT_CALL(*mocklfs_, Delete(_))
        .WillRepeatedly(Return(0));

    trash_->Start();
    std::this_thread::sleep_for(std::chrono::seconds(10));
    trash_->Stop();
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
