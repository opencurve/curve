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
 * Created Date: Sat Sep  4 18:14:22 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/conf_epoch_file.h"

#include <gtest/gtest.h>

#include "absl/memory/memory.h"
#include "test/fs/mock_local_filesystem.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::curve::fs::MockLocalFileSystem;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Matcher;
using ::testing::Return;
using ::testing::SetArrayArgument;

const char* kEpochFilePath = "./runlog/conf_epoch_file";

class ConfEpochFileTest : public testing::Test {
 protected:
    void SetUp() override {
        mockfs_ = std::make_shared<MockLocalFileSystem>();
        confFile_ = absl::make_unique<ConfEpochFile>(mockfs_.get());
    }

 protected:
    std::shared_ptr<MockLocalFileSystem> mockfs_;
    std::unique_ptr<ConfEpochFile> confFile_;

    PoolId poolId_ = 1;
    CopysetId copysetId_ = 1;
    uint64_t epoch_ = 1;
};

TEST_F(ConfEpochFileTest, LoadTest_OpenFailed) {
    EXPECT_CALL(*mockfs_, Open(_, _))
        .WillOnce(Return(-1));

    EXPECT_NE(0,
              confFile_->Load(kEpochFilePath, &poolId_, &copysetId_, &epoch_));
}

TEST_F(ConfEpochFileTest, LoadTest_ReadFailed) {
    EXPECT_CALL(*mockfs_, Open(_, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*mockfs_, Close(_))
        .Times(1);
    EXPECT_CALL(*mockfs_, Read(_, Matcher<char*>(_), _, _))
        .WillOnce(Return(0));

    EXPECT_NE(0,
              confFile_->Load(kEpochFilePath, &poolId_, &copysetId_, &epoch_));
}

TEST_F(ConfEpochFileTest, LoadTest_DecodeFailed) {
    EXPECT_CALL(*mockfs_, Open(_, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*mockfs_, Close(_))
        .Times(1);

    const char* data = "{\"hello\", \"world\"}";
    size_t datasize = strlen(data);
    EXPECT_CALL(*mockfs_, Read(_, Matcher<char*>(_), _, _))
        .WillOnce(DoAll(SetArrayArgument<1>(data, data + datasize),
                        Return(datasize)));

    EXPECT_NE(0,
              confFile_->Load(kEpochFilePath, &poolId_, &copysetId_, &epoch_));
}

TEST_F(ConfEpochFileTest, LoadTest_CrcFailed) {
    EXPECT_CALL(*mockfs_, Open(_, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*mockfs_, Close(_))
        .Times(1);

    const char* data =
        "{\"poolId\": 123, \"copysetId\": 431, \"epoch\": 3, \"checksum\": 12}";
    size_t datasize = strlen(data);
    EXPECT_CALL(*mockfs_, Read(_, Matcher<char*>(_), _, _))
        .WillOnce(DoAll(SetArrayArgument<1>(data, data + datasize),
                        Return(datasize)));

    EXPECT_NE(0,
              confFile_->Load(kEpochFilePath, &poolId_, &copysetId_, &epoch_));
}

TEST_F(ConfEpochFileTest, LoadTest_Success) {
    EXPECT_CALL(*mockfs_, Open(_, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*mockfs_, Close(_))
        .Times(1);

    const char* data =
        "{\"poolId\": 123, \"copysetId\": 431, \"epoch\": 3, \"checksum\": 1347065312}";  // NOLINT
    size_t datasize = strlen(data);
    EXPECT_CALL(*mockfs_, Read(_, Matcher<char*>(_), _, _))
        .WillOnce(DoAll(SetArrayArgument<1>(data, data + datasize),
                        Return(datasize)));

    EXPECT_EQ(0,
              confFile_->Load(kEpochFilePath, &poolId_, &copysetId_, &epoch_));
}

TEST_F(ConfEpochFileTest, SaveTest_OpenFailed) {
    EXPECT_CALL(*mockfs_, Open(_, _))
        .WillOnce(Return(-1));

    EXPECT_NE(0, confFile_->Save(kEpochFilePath, poolId_, copysetId_, epoch_));
}

TEST_F(ConfEpochFileTest, SaveTest_WriteFailed) {
    EXPECT_CALL(*mockfs_, Open(_, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*mockfs_, Close(_))
        .Times(1);

    EXPECT_CALL(*mockfs_, Write(_, Matcher<const char*>(_), _, _))
        .WillOnce(Return(-1));

    EXPECT_NE(0, confFile_->Save(kEpochFilePath, poolId_, copysetId_, epoch_));
}

TEST_F(ConfEpochFileTest, SaveTest_FsyncFailed) {
    EXPECT_CALL(*mockfs_, Open(_, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*mockfs_, Close(_))
        .Times(1);

    EXPECT_CALL(*mockfs_, Write(_, Matcher<const char*>(_), _, _))
        .WillOnce(Invoke(
            [](int fd, const char*, uint64_t, int length) { return length; }));

    EXPECT_CALL(*mockfs_, Fsync(_))
        .WillOnce(Return(-1));

    EXPECT_NE(0, confFile_->Save(kEpochFilePath, poolId_, copysetId_, epoch_));
}

TEST_F(ConfEpochFileTest, SaveTest_Success) {
    EXPECT_CALL(*mockfs_, Open(_, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*mockfs_, Close(_))
        .Times(1);

    EXPECT_CALL(*mockfs_, Write(_, Matcher<const char*>(_), _, _))
        .WillOnce(Invoke(
            [](int fd, const char*, uint64_t, int length) { return length; }));

    EXPECT_CALL(*mockfs_, Fsync(_))
        .WillOnce(Return(0));

    EXPECT_EQ(0, confFile_->Save(kEpochFilePath, poolId_, copysetId_, epoch_));
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
