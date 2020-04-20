/*
 * Project: curve
 * Created Date: Friday October 11th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <memory>
#include <string>

#include "src/chunkserver/raftsnapshot_attachment.h"
#include "test/fs/mock_local_filesystem.h"

namespace curve {
namespace chunkserver {

using curve::fs::MockLocalFileSystem;

using ::testing::_;
using ::testing::Return;
using ::testing::Mock;
using ::testing::DoAll;
using ::testing::ReturnArg;
using ::testing::ElementsAre;
using ::testing::SetArgPointee;
using ::testing::UnorderedElementsAre;

const char COPYSET_DIR[] = ("./attachcp");
const char DATA_DIR[] = ("./attachcp/data");

class RaftSnapshotAttachmentMockTest : public testing::Test {
 public:
    void SetUp() {
        fs_ = std::make_shared<MockLocalFileSystem>();
        attachment_ = scoped_refptr<RaftSnapshotAttachment>(
            new RaftSnapshotAttachment(fs_));
    }
    void TearDown() {}
 protected:
    std::shared_ptr<MockLocalFileSystem> fs_;
    scoped_refptr<RaftSnapshotAttachment> attachment_;
};

TEST_F(RaftSnapshotAttachmentMockTest, ListTest) {
    // 返回成功
    vector<std::string> fileNames;
    fileNames.emplace_back("chunk_1");
    fileNames.emplace_back("chunk_1_snap_1");
    fileNames.emplace_back("chunk_2_snap_1");
    EXPECT_CALL(*fs_, List(DATA_DIR, _))
        .WillOnce(DoAll(SetArgPointee<1>(fileNames), Return(0)));
    vector<std::string> snapFiles;
    attachment_->list_attach_files(&snapFiles, COPYSET_DIR);

    std::string snapPath1 =
        "./attachcp/data/chunk_1_snap_1:data/chunk_1_snap_1";
    std::string snapPath2 =
        "./attachcp/data/chunk_2_snap_1:data/chunk_2_snap_1";
    EXPECT_THAT(snapFiles, UnorderedElementsAre(snapPath1.c_str(),
                                                snapPath2.c_str()));

    // 路径结尾添加反斜杠
    EXPECT_CALL(*fs_, List(DATA_DIR, _))
        .WillOnce(DoAll(SetArgPointee<1>(fileNames), Return(0)));
    attachment_->list_attach_files(&snapFiles, "./attachcp/");
    EXPECT_THAT(snapFiles, UnorderedElementsAre(snapPath1.c_str(),
                                                snapPath2.c_str()));
    // 返回失败
    EXPECT_CALL(*fs_, List(DATA_DIR, _))
        .WillRepeatedly(Return(-1));
    ASSERT_DEATH(attachment_->list_attach_files(&snapFiles, COPYSET_DIR), "");
}

}   // namespace chunkserver
}   // namespace curve
