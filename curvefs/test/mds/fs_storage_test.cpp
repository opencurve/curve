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
 * @Date: 2021-06-10 10:04:21
 * @Author: chenwei
 */
#include "curvefs/src/mds/fs_storage.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <google/protobuf/util/message_differencer.h>

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SaveArg;
using ::google::protobuf::util::MessageDifferencer;

namespace curvefs {
namespace mds {

bool operator==(const FsInfoWrapper& lhs, const FsInfoWrapper& rhs) {
    return MessageDifferencer::Equals(lhs.ProtoFsInfo(), rhs.ProtoFsInfo());
}

class FSStorageTest : public ::testing::Test {
 protected:
    void SetUp() override { return; }

    void TearDown() override { return; }
};

TEST_F(FSStorageTest, test1) {
    MemoryFsStorage storage;
    common::Volume volume;
    uint32_t fsId = 1;
    uint64_t rootInodeId = 1;
    uint64_t capacity = 46900;
    uint64_t blockSize = 4096;

    FsDetail detail;
    detail.set_allocated_volume(new common::Volume(volume));
    FsInfoWrapper fs1 = GenerateFsInfoWrapper(
        "name1", fsId, blockSize, INSERT_ROOT_INODE_ERROR, detail);
    // test insert
    ASSERT_EQ(FSStatusCode::OK, storage.Insert(fs1));
    ASSERT_EQ(FSStatusCode::FS_EXIST, storage.Insert(fs1));

    // test get
    FsInfoWrapper fs2;
    ASSERT_EQ(FSStatusCode::OK, storage.Get(fs1.FsId(), &fs2));
    ASSERT_EQ(fs1.FsName(), fs2.FsName());
    ASSERT_TRUE(fs1 == fs2);
    ASSERT_EQ(FSStatusCode::NOT_FOUND,
              storage.Get(fs1.FsId() + 1, &fs2));
    ASSERT_EQ(FSStatusCode::OK, storage.Get(fs1.FsName(), &fs2));
    ASSERT_EQ(fs2.FsName(), fs1.FsName());
    ASSERT_EQ(FSStatusCode::NOT_FOUND,
              storage.Get(fs1.FsName() + "1", &fs2));
    ASSERT_TRUE(storage.Exist(fs1.FsId()));
    ASSERT_FALSE(storage.Exist(fs1.FsId() + 1));
    ASSERT_TRUE(storage.Exist(fs1.FsName()));
    ASSERT_FALSE(storage.Exist(fs1.FsName() + "1"));

    // test update
    fs1.SetStatus(FsStatus::INITED);
    ASSERT_EQ(FSStatusCode::OK, storage.Update(fs1));
    ASSERT_EQ(FSStatusCode::OK, storage.Get(fs1.FsId(), &fs2));
    ASSERT_EQ(fs2.Status(), FsStatus::INITED);
    FsInfoWrapper fs3 =
        GenerateFsInfoWrapper("name3", fsId, blockSize, rootInodeId, detail);

    LOG(INFO) << "NAME " << fs3.FsName();
    ASSERT_EQ(FSStatusCode::NOT_FOUND, storage.Update(fs3));

    FsInfoWrapper fs4 = GenerateFsInfoWrapper("name1", fsId + 1, blockSize,
                                              rootInodeId, detail);
    ASSERT_EQ(FSStatusCode::FS_ID_MISMATCH, storage.Update(fs4));

    // test delete
    ASSERT_EQ(FSStatusCode::OK, storage.Delete(fs1.FsName()));
    ASSERT_EQ(FSStatusCode::NOT_FOUND, storage.Delete(fs1.FsName()));
}
}  // namespace mds
}  // namespace curvefs
