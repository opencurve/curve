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
 * Created Date: Friday August 30th 2019
 * Author: yangyaokai
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <memory>

#include "src/chunkserver/datastore/datastore_file_helper.h"
#include "test/fs/mock_local_filesystem.h"

using curve::fs::LocalFileSystem;
using curve::fs::MockLocalFileSystem;

using ::testing::_;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::Return;
using ::testing::NotNull;
using ::testing::Mock;
using ::testing::Truly;
using ::testing::DoAll;
using ::testing::ReturnArg;
using ::testing::ElementsAre;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;

namespace curve {
namespace chunkserver {

class FileHelper_MockTest : public testing::Test {
 public:
    void SetUp() {
        fs_ = std::make_shared<MockLocalFileSystem>();
        fileHelper_ = std::make_shared<DatastoreFileHelper>(fs_);
    }
    void TearDown() {}
 protected:
    std::shared_ptr<MockLocalFileSystem> fs_;
    std::shared_ptr<DatastoreFileHelper> fileHelper_;
};

TEST_F(FileHelper_MockTest, ListFilesTest) {
    string baseDir = "/copyset/data";
    vector<string> chunkFiles;
    vector<string> snapFiles;

    // Case1: List failed, returned -1
    EXPECT_CALL(*fs_, List(_, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileHelper_->ListFiles(baseDir, &chunkFiles, &snapFiles));
    // If an ENOENT error is returned, success is returned directly
    EXPECT_CALL(*fs_, List(_, _))
        .WillOnce(Return(-ENOENT));
    ASSERT_EQ(0, fileHelper_->ListFiles(baseDir, &chunkFiles, &snapFiles));

    vector<string> files;
    string chunk1 = "chunk_1";
    string chunk2 = "chunk_2";
    string snap1 = "chunk_1_snap_1";
    string other = "chunk_1_S";  // Illegal file name
    files.emplace_back(chunk1);
    files.emplace_back(chunk2);
    files.emplace_back(snap1);
    files.emplace_back(other);
    EXPECT_CALL(*fs_, List(_, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(files),
                        Return(0)));

    // Case2: List successful, returning chunk file and snapshot file
    ASSERT_EQ(0, fileHelper_->ListFiles(baseDir, &chunkFiles, &snapFiles));
    ASSERT_EQ(2, chunkFiles.size());
    ASSERT_STREQ(chunk1.c_str(), chunkFiles[0].c_str());
    ASSERT_STREQ(chunk2.c_str(), chunkFiles[1].c_str());
    ASSERT_EQ(1, snapFiles.size());
    ASSERT_STREQ(snap1.c_str(), snapFiles[0].c_str());

    // Case3: Allow vector to be a null pointer
    ASSERT_EQ(0, fileHelper_->ListFiles(baseDir, nullptr, nullptr));
}

}  // namespace chunkserver
}  // namespace curve
