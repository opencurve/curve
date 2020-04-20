/*
 * Project: curve
 * Created Date: Friday August 30th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
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

    // case1:List失败，返回-1
    EXPECT_CALL(*fs_, List(_, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileHelper_->ListFiles(baseDir, &chunkFiles, &snapFiles));
    // 如果返回ENOENT错误,直接返回成功
    EXPECT_CALL(*fs_, List(_, _))
        .WillOnce(Return(-ENOENT));
    ASSERT_EQ(0, fileHelper_->ListFiles(baseDir, &chunkFiles, &snapFiles));

    vector<string> files;
    string chunk1 = "chunk_1";
    string chunk2 = "chunk_2";
    string snap1 = "chunk_1_snap_1";
    string other = "chunk_1_S";  // 非法文件名
    files.emplace_back(chunk1);
    files.emplace_back(chunk2);
    files.emplace_back(snap1);
    files.emplace_back(other);
    EXPECT_CALL(*fs_, List(_, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(files),
                        Return(0)));

    // case2:List成功，返回chunk文件和snapshot文件
    ASSERT_EQ(0, fileHelper_->ListFiles(baseDir, &chunkFiles, &snapFiles));
    ASSERT_EQ(2, chunkFiles.size());
    ASSERT_STREQ(chunk1.c_str(), chunkFiles[0].c_str());
    ASSERT_STREQ(chunk2.c_str(), chunkFiles[1].c_str());
    ASSERT_EQ(1, snapFiles.size());
    ASSERT_STREQ(snap1.c_str(), snapFiles[0].c_str());

    // case3:允许vector为空指针
    ASSERT_EQ(0, fileHelper_->ListFiles(baseDir, nullptr, nullptr));
}

}  // namespace chunkserver
}  // namespace curve
