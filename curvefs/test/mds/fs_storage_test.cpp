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

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SaveArg;

namespace curvefs {
namespace mds {
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
    std::shared_ptr<MdsFsInfo> fs1 = std::make_shared<MdsVolumeFsInfo>(
        fsId, "name1", FsStatus::NEW, rootInodeId, capacity, blockSize, volume);
    // MdsVolumeFsInfo fs1(fsId, "name1", FsStatus::NEW, rootInodeId, capacity,
    //                     blockSize, volume);
    // test insert
    ASSERT_EQ(FSStatusCode::OK, storage.Insert(fs1));
    ASSERT_EQ(FSStatusCode::FS_EXIST, storage.Insert(fs1));

    // test get
    // MdsFsInfo fs2;
    std::shared_ptr<MdsFsInfo> fs2;
    ASSERT_EQ(FSStatusCode::OK, storage.Get(fs1->GetFsId(), &fs2));
    ASSERT_EQ(fs2->GetFsName(), fs1->GetFsName());
    ASSERT_EQ(FSStatusCode::NOT_FOUND, storage.Get(fs1->GetFsId() + 1, &fs2));
    ASSERT_EQ(FSStatusCode::OK, storage.Get(fs1->GetFsName(), &fs2));
    ASSERT_EQ(fs2->GetFsName(), fs1->GetFsName());
    ASSERT_EQ(FSStatusCode::NOT_FOUND,
              storage.Get(fs1->GetFsName() + "1", &fs2));
    ASSERT_TRUE(storage.Exist(fs1->GetFsId()));
    ASSERT_FALSE(storage.Exist(fs1->GetFsId() + 1));
    ASSERT_TRUE(storage.Exist(fs1->GetFsName()));
    ASSERT_FALSE(storage.Exist(fs1->GetFsName() + "1"));

    // test update
    fs1->SetStatus(FsStatus::INITED);
    ASSERT_EQ(FSStatusCode::OK, storage.Update(fs1));
    ASSERT_EQ(FSStatusCode::OK, storage.Get(fs1->GetFsId(), &fs2));
    ASSERT_EQ(fs2->GetStatus(), FsStatus::INITED);
    // MdsFsInfo fs3(fsId, "name3", FsStatus::NEW, rootInodeId, capacity,
    //               blockSize, volume);
    std::shared_ptr<MdsFsInfo> fs3 = std::make_shared<MdsVolumeFsInfo>(
        fsId, "name3", FsStatus::NEW, rootInodeId, capacity, blockSize, volume);
    LOG(INFO) << "NAME " << fs3->GetFsName();
    ASSERT_EQ(FSStatusCode::NOT_FOUND, storage.Update(fs3));

    // MdsFsInfo fs4(fsId + 1, "name1", FsStatus::NEW, rootInodeId, capacity,
    //               blockSize, volume);
    std::shared_ptr<MdsFsInfo> fs4 = std::make_shared<MdsVolumeFsInfo>(
        fsId + 1, "name1", FsStatus::NEW, rootInodeId, capacity, blockSize,
        volume);
    ASSERT_EQ(FSStatusCode::FS_ID_MISMATCH, storage.Update(fs4));

    // test delete
    ASSERT_EQ(FSStatusCode::OK, storage.Delete(fs1->GetFsName()));
    ASSERT_EQ(FSStatusCode::NOT_FOUND, storage.Delete(fs1->GetFsName()));
}
}  // namespace mds
}  // namespace curvefs
