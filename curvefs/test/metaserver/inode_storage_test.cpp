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
 * @Date: 2021-06-10 10:04:47
 * @Author: chenwei
 */
#include "curvefs/src/metaserver/inode_storage.h"
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
namespace metaserver {
class InodeStorageTest : public ::testing::Test {
 protected:
    void SetUp() override { return; }

    void TearDown() override { return; }

    bool CompareInode(const Inode &first, const Inode &second) {
        return first.fsid() == second.fsid() &&
               first.atime() == second.atime() &&
               first.inodeid() == second.inodeid();
    }
};

TEST_F(InodeStorageTest, test1) {
    MemoryInodeStorage storage;
    Inode inode1;
    Inode inode2;
    Inode inode3;
    inode1.set_inodeid(1);
    inode1.set_fsid(1);
    inode1.set_atime(100);
    inode2.set_inodeid(1);
    inode2.set_fsid(2);
    inode2.set_atime(200);
    inode3.set_inodeid(2);
    inode3.set_fsid(1);
    inode3.set_atime(300);

    // insert
    ASSERT_EQ(storage.Insert(inode1), MetaStatusCode::OK);
    ASSERT_EQ(storage.Insert(inode2), MetaStatusCode::OK);
    ASSERT_EQ(storage.Insert(inode3), MetaStatusCode::OK);
    ASSERT_EQ(storage.Insert(inode1), MetaStatusCode::INODE_EXIST);
    ASSERT_EQ(storage.Insert(inode2), MetaStatusCode::INODE_EXIST);
    ASSERT_EQ(storage.Insert(inode3), MetaStatusCode::INODE_EXIST);
    ASSERT_EQ(storage.Count(), 3);

    // get
    Inode temp;
    ASSERT_EQ(storage.Get(InodeKey(inode1), &temp), MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(inode1, temp));
    ASSERT_EQ(storage.Get(InodeKey(inode2), &temp), MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(inode2, temp));
    ASSERT_EQ(storage.Get(InodeKey(inode3), &temp), MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(inode3, temp));

    // delete
    ASSERT_EQ(storage.Delete(InodeKey(inode1)), MetaStatusCode::OK);
    ASSERT_EQ(storage.Count(), 2);
    ASSERT_EQ(storage.Get(InodeKey(inode1), &temp), MetaStatusCode::NOT_FOUND);
    ASSERT_EQ(storage.Delete(InodeKey(inode1)), MetaStatusCode::NOT_FOUND);

    // update
    ASSERT_EQ(storage.Update(inode1), MetaStatusCode::NOT_FOUND);
    Inode oldInode;
    ASSERT_EQ(storage.Get(InodeKey(inode2), &oldInode), MetaStatusCode::OK);
    inode2.set_atime(400);
    ASSERT_EQ(storage.Update(inode2), MetaStatusCode::OK);
    Inode newInode;
    ASSERT_EQ(storage.Get(InodeKey(inode2), &newInode), MetaStatusCode::OK);
    ASSERT_FALSE(CompareInode(oldInode, newInode));
    ASSERT_FALSE(CompareInode(oldInode, inode2));
    ASSERT_TRUE(CompareInode(newInode, inode2));

    // GetInodeContainer
    auto mapPtr = storage.GetContainer();
    ASSERT_TRUE(CompareInode((*mapPtr)[InodeKey(inode2)], inode2));
    ASSERT_TRUE(CompareInode((*mapPtr)[InodeKey(inode3)], inode3));

    // GetInodeContainerData
    auto mapData = storage.GetContainerData();
    ASSERT_TRUE(CompareInode(mapData[InodeKey(inode2)], inode2));
    ASSERT_TRUE(CompareInode(mapData[InodeKey(inode3)], inode3));

    // GetInodeIdList
    std::list<uint64_t> inodeIdList;
    storage.GetInodeIdList(&inodeIdList);
    ASSERT_EQ(inodeIdList.size(), 2);
}
}  // namespace metaserver
}  // namespace curvefs
