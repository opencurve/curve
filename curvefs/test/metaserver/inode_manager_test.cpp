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
 * @Date: 2021-06-10 10:04:57
 * @Author: chenwei
 */
#include "curvefs/src/metaserver/inode_manager.h"
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
class InodeManagerTest : public ::testing::Test {
 protected:
    void SetUp() override { return; }

    void TearDown() override { return; }

    bool CompareInode(const Inode &first, const Inode &second) {
        return first.fsid() == second.fsid() &&
               first.atime() == second.atime() &&
               first.inodeid() == second.inodeid() &&
               first.length() == second.length() &&
               first.uid() == second.uid() && first.gid() == second.gid() &&
               first.mode() == second.mode() && first.type() == second.type() &&
               first.mtime() == second.mtime() &&
               first.ctime() == second.ctime() &&
               first.symlink() == second.symlink() &&
               first.nlink() == second.nlink();
    }
};

TEST_F(InodeManagerTest, test1) {
    std::shared_ptr<InodeStorage> inodeStorage =
        std::make_shared<MemoryInodeStorage>();
    InodeManager manager(inodeStorage);

    // CREATE
    uint32_t fsId = 1;
    uint64_t length = 100;
    uint32_t uid = 200;
    uint32_t gid = 300;
    uint32_t mode = 400;
    FsFileType type = FsFileType::TYPE_FILE;
    std::string symlink = "";
    Inode inode1;
    ASSERT_EQ(manager.CreateInode(fsId, length, uid, gid, mode, type, symlink,
                                  &inode1),
              MetaStatusCode::OK);
    ASSERT_EQ(inode1.inodeid(), 2);

    Inode inode2;
    ASSERT_EQ(manager.CreateInode(fsId, length, uid, gid, mode, type, symlink,
                                  &inode2),
              MetaStatusCode::OK);
    ASSERT_EQ(inode2.inodeid(), 3);

    Inode inode3;
    ASSERT_EQ(manager.CreateInode(fsId, length, uid, gid, mode,
                                  FsFileType::TYPE_SYM_LINK, symlink, &inode3),
              MetaStatusCode::SYM_LINK_EMPTY);

    ASSERT_EQ(
        manager.CreateInode(fsId, length, uid, gid, mode,
                            FsFileType::TYPE_SYM_LINK, "SYMLINK", &inode3),
        MetaStatusCode::OK);
    ASSERT_EQ(inode3.inodeid(), 4);

    Inode inode4;
    ASSERT_EQ(manager.CreateInode(fsId, length, uid, gid, mode,
                                  FsFileType::TYPE_S3, symlink, &inode4),
              MetaStatusCode::OK);
    ASSERT_EQ(inode4.inodeid(), 5);
    ASSERT_EQ(inode4.type(), FsFileType::TYPE_S3);

    // GET
    Inode temp1;
    ASSERT_EQ(manager.GetInode(fsId, inode1.inodeid(), &temp1),
              MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(inode1, temp1));

    Inode temp2;
    ASSERT_EQ(manager.GetInode(fsId, inode2.inodeid(), &temp2),
              MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(inode2, temp2));

    Inode temp3;
    ASSERT_EQ(manager.GetInode(fsId, inode3.inodeid(), &temp3),
              MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(inode3, temp3));

    Inode temp4;
    ASSERT_EQ(manager.GetInode(fsId, inode4.inodeid(), &temp4),
              MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(inode4, temp4));

    // DELETE
    ASSERT_EQ(manager.DeleteInode(fsId, inode1.inodeid()), MetaStatusCode::OK);
    ASSERT_EQ(manager.DeleteInode(fsId, inode1.inodeid()),
              MetaStatusCode::NOT_FOUND);
    ASSERT_EQ(manager.GetInode(fsId, inode1.inodeid(), &temp1),
              MetaStatusCode::NOT_FOUND);

    // UPDATE
    ASSERT_EQ(manager.UpdateInode(inode1), MetaStatusCode::NOT_FOUND);
    temp2.set_atime(100);
    ASSERT_EQ(manager.UpdateInode(temp2), MetaStatusCode::OK);
    Inode temp5;
    ASSERT_EQ(manager.GetInode(fsId, inode2.inodeid(), &temp5),
              MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(temp5, temp2));
    ASSERT_FALSE(CompareInode(inode2, temp2));

    // TODO(huyao): delete version
    /*
    // UPDATE VERSION
    uint64_t version = 0;
    ASSERT_EQ(manager.UpdateInodeVersion(fsId, inode4.inodeid(), &version),
              MetaStatusCode::OK);
    ASSERT_EQ(version, 1);
    ASSERT_EQ(manager.UpdateInodeVersion(fsId, inode4.inodeid(), &version),
              MetaStatusCode::OK);
    ASSERT_EQ(version, 2);

    ASSERT_EQ(manager.UpdateInodeVersion(fsId, inode2.inodeid(), &version),
              MetaStatusCode::PARAM_ERROR);
    ASSERT_EQ(manager.UpdateInodeVersion(fsId, inode1.inodeid(), &version),
              MetaStatusCode::NOT_FOUND);
    */
    // INSERT
    ASSERT_EQ(manager.InsertInode(inode2), MetaStatusCode::INODE_EXIST);
    Inode inode5 = inode2;
    inode5.set_inodeid(100);
    ASSERT_EQ(manager.InsertInode(inode5), MetaStatusCode::OK);
}
}  // namespace metaserver
}  // namespace curvefs
