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
 * Created Date: Friday October 11th 2019
 * Author: yangyaokai
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <memory>
#include <string>

#include "src/chunkserver/raftsnapshot/curve_snapshot_attachment.h"
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

const char kRaftSnapDir[] = ("./attachcp/raft_snapshot/snapshot_805455");
const char kDataDir[] = ("./attachcp/data");

class CurveSnapshotAttachmentMockTest : public testing::Test {
 public:
    void SetUp() {
        fs_ = std::make_shared<MockLocalFileSystem>();
        attachment_ = scoped_refptr<CurveSnapshotAttachment>(
            new CurveSnapshotAttachment(fs_));
    }
    void TearDown() {}
 protected:
    std::shared_ptr<MockLocalFileSystem> fs_;
    scoped_refptr<CurveSnapshotAttachment> attachment_;
};

TEST_F(CurveSnapshotAttachmentMockTest, ListTest) {
    // 返回成功
    vector<std::string> fileNames;
    fileNames.emplace_back("chunk_1");
    fileNames.emplace_back("chunk_1_snap_1");
    fileNames.emplace_back("chunk_2_snap_1");
    EXPECT_CALL(*fs_, List(kDataDir, _))
        .WillOnce(DoAll(SetArgPointee<1>(fileNames), Return(0)));
    vector<std::string> snapFiles;
    attachment_->list_attach_files(&snapFiles, kRaftSnapDir);

    std::string snapPath1 =
        "../../data/chunk_1_snap_1";
    std::string snapPath2 =
        "../../data/chunk_2_snap_1";
    EXPECT_THAT(snapFiles, UnorderedElementsAre(snapPath1.c_str(),
                                                snapPath2.c_str()));

    // 路径结尾添加反斜杠
    EXPECT_CALL(*fs_, List(kDataDir, _))
        .WillOnce(DoAll(SetArgPointee<1>(fileNames), Return(0)));
    attachment_->list_attach_files(&snapFiles, std::string(kRaftSnapDir) + "/");
    EXPECT_THAT(snapFiles, UnorderedElementsAre(snapPath1.c_str(),
                                                snapPath2.c_str()));
    // 返回失败
    EXPECT_CALL(*fs_, List(kDataDir, _))
        .WillRepeatedly(Return(-1));
    ASSERT_DEATH(attachment_->list_attach_files(&snapFiles, kRaftSnapDir), "");
}

}   // namespace chunkserver
}   // namespace curve
