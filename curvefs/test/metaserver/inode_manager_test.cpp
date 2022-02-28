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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <google/protobuf/util/message_differencer.h>

#include "curvefs/test/metaserver/test_helper.h"
#include "curvefs/src/metaserver/storage/converter.h"
#include "curvefs/src/metaserver/inode_manager.h"
#include "curvefs/src/common/define.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/memory_storage.h"

using ::google::protobuf::util::MessageDifferencer;
using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::StrEq;

using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::StorageOptions;
using ::curvefs::metaserver::storage::MemoryStorage;

namespace curvefs {
namespace metaserver {
class InodeManagerTest : public ::testing::Test {
 protected:
    void SetUp() override {
        StorageOptions options;
        auto tablename = "partition:1";
        auto kvStorage = std::make_shared<MemoryStorage>(options);
        auto inodeStorage = std::make_shared<InodeStorage>(
            kvStorage, tablename);
        auto trash = std::make_shared<TrashImpl>(inodeStorage);
        manager = std::make_shared<InodeManager>(inodeStorage, trash);

        param_.fsId = 1;
        param_.length = 100;
        param_.uid = 200;
        param_.gid = 300;
        param_.mode = 400;
        param_.type = FsFileType::TYPE_FILE;
        param_.symlink = "";
        param_.rdev = 0;

        conv_ = std::make_shared<Converter>();
    }

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

    bool EqualS3ChunkInfo(const S3ChunkInfo& lhs, const S3ChunkInfo& rhs) {
        return lhs.chunkid() == rhs.chunkid() &&
            lhs.compaction() == rhs.compaction() &&
            lhs.offset() == rhs.offset() &&
            lhs.len() == rhs.len() &&
            lhs.size() == rhs.size() &&
            lhs.zero() == rhs.zero();
    }

    bool EqualS3ChunkInfoList(const S3ChunkInfoList& lhs,
                              const S3ChunkInfoList& rhs) {
        size_t size = lhs.s3chunks_size();
        if (size != rhs.s3chunks_size()) {
            return false;
        }

        for (size_t i = 0; i < size; i++) {
            if (!EqualS3ChunkInfo(lhs.s3chunks(i), rhs.s3chunks(i))) {
                return false;
            }
        }
        return true;
    }

    S3ChunkInfoList GenS3ChunkInfoList(uint64_t firstChunkId,
                                       uint64_t lastChunkId) {
        S3ChunkInfoList list;
        for (uint64_t id = firstChunkId; id <= lastChunkId; id++) {
            S3ChunkInfo* info = list.add_s3chunks();
            info->set_chunkid(id);
            info->set_compaction(0);
            info->set_offset(0);
            info->set_len(0);
            info->set_size(0);
            info->set_zero(false);
        }
        return list;
    }

 protected:
    std::shared_ptr<InodeManager> manager;
    InodeParam param_;
    std::shared_ptr<Converter> conv_;
};

TEST_F(InodeManagerTest, test1) {
    // CREATE
    uint32_t fsId = 1;

    Inode inode1;
    ASSERT_EQ(manager->CreateInode(2, param_, &inode1),
              MetaStatusCode::OK);
    ASSERT_EQ(inode1.inodeid(), 2);

    Inode inode2;
    ASSERT_EQ(manager->CreateInode(3, param_, &inode2),
              MetaStatusCode::OK);
    ASSERT_EQ(inode2.inodeid(), 3);

    Inode inode3;
    param_.type = FsFileType::TYPE_SYM_LINK;
    ASSERT_EQ(manager->CreateInode(4, param_, &inode3),
              MetaStatusCode::SYM_LINK_EMPTY);

    param_.symlink = "SYMLINK";
    ASSERT_EQ(manager->CreateInode(4, param_, &inode3),
              MetaStatusCode::OK);
    ASSERT_EQ(inode3.inodeid(), 4);

    Inode inode4;
    param_.type = FsFileType::TYPE_S3;
    ASSERT_EQ(manager->CreateInode(5, param_, &inode4),
              MetaStatusCode::OK);
    ASSERT_EQ(inode4.inodeid(), 5);
    ASSERT_EQ(inode4.type(), FsFileType::TYPE_S3);

    // GET
    Inode temp1;
    ASSERT_EQ(manager->GetInode(fsId, inode1.inodeid(), &temp1),
              MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(inode1, temp1));

    Inode temp2;
    ASSERT_EQ(manager->GetInode(fsId, inode2.inodeid(), &temp2),
              MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(inode2, temp2));

    Inode temp3;
    ASSERT_EQ(manager->GetInode(fsId, inode3.inodeid(), &temp3),
              MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(inode3, temp3));

    Inode temp4;
    ASSERT_EQ(manager->GetInode(fsId, inode4.inodeid(), &temp4),
              MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(inode4, temp4));

    // DELETE
    ASSERT_EQ(manager->DeleteInode(fsId, inode1.inodeid()), MetaStatusCode::OK);
    ASSERT_EQ(manager->DeleteInode(fsId, inode1.inodeid()),
              MetaStatusCode::NOT_FOUND);
    ASSERT_EQ(manager->GetInode(fsId, inode1.inodeid(), &temp1),
              MetaStatusCode::NOT_FOUND);

    // UPDATE
    UpdateInodeRequest request = MakeUpdateInodeRequestFromInode(inode1);
    ASSERT_EQ(manager->UpdateInode(request), MetaStatusCode::NOT_FOUND);
    temp2.set_atime(100);
    UpdateInodeRequest request2 = MakeUpdateInodeRequestFromInode(temp2);
    ASSERT_EQ(manager->UpdateInode(request2), MetaStatusCode::OK);
    Inode temp5;
    ASSERT_EQ(manager->GetInode(fsId, inode2.inodeid(), &temp5),
              MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(temp5, temp2));
    ASSERT_FALSE(CompareInode(inode2, temp2));
}

TEST_F(InodeManagerTest, GetOrModifyS3ChunkInfo) {
    google::protobuf::Map<uint64_t, S3ChunkInfoList> map4add;
    uint32_t fsId = 1;
    uint32_t inodeId = 1;
    S3ChunkInfoList list4add = GenS3ChunkInfoList(1, 100);
    for (int i = 0; i < 10; i++) {
        map4add[i] = list4add;
    }

    // CASE 1: GetOrModifyS3ChunkInfo() success
    {
        std::shared_ptr<Iterator> iterator;
        MetaStatusCode rc = manager->GetOrModifyS3ChunkInfo(
            fsId, inodeId, map4add, &iterator, true, false);
        ASSERT_EQ(rc, MetaStatusCode::OK);

        size_t size = 0;
        Key4S3ChunkInfoList key;
        S3ChunkInfoList list4get;
        ASSERT_EQ(iterator->Status(), 0);
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
            ASSERT_TRUE(conv_->ParseFromString(iterator->Key(), &key));
            ASSERT_TRUE(conv_->ParseFromString(iterator->Value(), &list4get));
            ASSERT_EQ(key.chunkIndex, size);
            ASSERT_TRUE(EqualS3ChunkInfoList(list4add, list4get));
            size++;
        }
        ASSERT_EQ(size, 10);
    }

    // CASE 2: idempotent request
    {
        std::shared_ptr<Iterator> iterator;
        MetaStatusCode rc = manager->GetOrModifyS3ChunkInfo(
            fsId, inodeId, map4add, &iterator, true, false);
        ASSERT_EQ(rc, MetaStatusCode::OK);

        size_t size = 0;
        Key4S3ChunkInfoList key;
        S3ChunkInfoList list4get;
        ASSERT_EQ(iterator->Status(), 0);
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
            ASSERT_TRUE(conv_->ParseFromString(iterator->Key(), &key));
            ASSERT_TRUE(conv_->ParseFromString(iterator->Value(), &list4get));
            ASSERT_EQ(key.chunkIndex, size);
            ASSERT_TRUE(EqualS3ChunkInfoList(list4add, list4get));
            size++;
        }
        ASSERT_EQ(size, 10);
    }

    // CASE 3: compaction
    {
        map4add.clear();
        map4add[0] = GenS3ChunkInfoList(100, 100);
        map4add[1] = GenS3ChunkInfoList(100, 100);
        map4add[2] = GenS3ChunkInfoList(100, 100);
        map4add[7] = GenS3ChunkInfoList(100, 100);
        map4add[8] = GenS3ChunkInfoList(100, 100);
        map4add[9] = GenS3ChunkInfoList(100, 100);

        std::shared_ptr<Iterator> iterator;
        MetaStatusCode rc = manager->GetOrModifyS3ChunkInfo(
            fsId, inodeId, map4add, &iterator, true, true);
        ASSERT_EQ(rc, MetaStatusCode::OK);
        ASSERT_EQ(iterator->Status(), 0);

        size_t size = 0;
        Key4S3ChunkInfoList key;
        S3ChunkInfoList list4get;
        std::vector<S3ChunkInfoList> lists{
            GenS3ChunkInfoList(100, 100),
            GenS3ChunkInfoList(100, 100),
            GenS3ChunkInfoList(100, 100),
            GenS3ChunkInfoList(1, 100),
            GenS3ChunkInfoList(1, 100),
            GenS3ChunkInfoList(1, 100),
            GenS3ChunkInfoList(1, 100),
            GenS3ChunkInfoList(100, 100),
            GenS3ChunkInfoList(100, 100),
            GenS3ChunkInfoList(100, 100),
        };
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
            LOG(INFO) << "check chunkIndex(" << size << ")"
                      << ", key=" << iterator->Key();
            ASSERT_TRUE(conv_->ParseFromString(iterator->Key(), &key));
            ASSERT_TRUE(conv_->ParseFromString(iterator->Value(), &list4get));
            ASSERT_EQ(key.chunkIndex, size);
            ASSERT_TRUE(EqualS3ChunkInfoList(lists[size], list4get));
            size++;
        }
        ASSERT_EQ(size, 10);
    }
}

TEST_F(InodeManagerTest, UpdateInode) {
    // create inode
    uint32_t fsId = 1;
    uint64_t ino = 2;

    Inode inode;
    ASSERT_EQ(MetaStatusCode::OK,
              manager->CreateInode(ino, param_, &inode));

    // 1. test add openmpcount
    UpdateInodeRequest request = MakeUpdateInodeRequestFromInode(inode);
    request.set_inodeopenstatuschange(InodeOpenStatusChange::OPEN);
    ASSERT_EQ(MetaStatusCode::OK, manager->UpdateInode(request));
    Inode updateOne;
    ASSERT_EQ(MetaStatusCode::OK, manager->GetInode(fsId, ino, &updateOne));
    ASSERT_EQ(1, updateOne.openmpcount());

    // 2. test openmpcount nochange
    request.set_inodeopenstatuschange(InodeOpenStatusChange::NOCHANGE);
    ASSERT_EQ(MetaStatusCode::OK, manager->UpdateInode(request));
    ASSERT_EQ(MetaStatusCode::OK, manager->GetInode(fsId, ino, &updateOne));
    ASSERT_EQ(1, updateOne.openmpcount());

    // 3. test sub openmpcount
    request.set_inodeopenstatuschange(InodeOpenStatusChange::CLOSE);
    ASSERT_EQ(MetaStatusCode::OK, manager->UpdateInode(request));
    ASSERT_EQ(MetaStatusCode::OK, manager->GetInode(fsId, ino, &updateOne));
    ASSERT_EQ(0, updateOne.openmpcount());

    // 4. test update fail
    request.set_inodeopenstatuschange(InodeOpenStatusChange::CLOSE);
    ASSERT_EQ(MetaStatusCode::OK, manager->UpdateInode(request));
    ASSERT_EQ(MetaStatusCode::OK, manager->GetInode(fsId, ino, &updateOne));
    ASSERT_EQ(0, updateOne.openmpcount());
}


TEST_F(InodeManagerTest, testGetAttr) {
    // CREATE
    uint32_t fsId = 1;
    Inode inode1;
    ASSERT_EQ(manager.CreateInode(2, param_, &inode1),
        MetaStatusCode::OK);
    ASSERT_EQ(inode1.inodeid(), 2);

    InodeAttr attr;
    ASSERT_EQ(manager->GetInodeAttr(fsId, inode1.inodeid(), &attr),
              MetaStatusCode::OK);
    ASSERT_EQ(attr.fsid(), 1);
    ASSERT_EQ(attr.inodeid(), 2);
    ASSERT_EQ(attr.length(), 100);
    ASSERT_EQ(attr.uid(), 200);
    ASSERT_EQ(attr.gid(), 300);
    ASSERT_EQ(attr.mode(), 400);
    ASSERT_EQ(attr.type(), FsFileType::TYPE_FILE);
    ASSERT_EQ(attr.symlink(), "");
    ASSERT_EQ(attr.rdev(), 0);
}

TEST_F(InodeManagerTest, testGetXAttr) {
    // CREATE
    uint32_t fsId = 1;
    Inode inode1;
    ASSERT_EQ(manager.CreateInode(2, param_, &inode1),
        MetaStatusCode::OK);
    ASSERT_EQ(inode1.inodeid(), 2);
    ASSERT_TRUE(inode1.xattr().empty());

    Inode inode2;
    param_.type = FsFileType::TYPE_DIRECTORY;
    ASSERT_EQ(
        manager.CreateInode(3, param_, &inode2),
        MetaStatusCode::OK);
    ASSERT_FALSE(inode2.xattr().empty());
    ASSERT_EQ(inode2.xattr().find(XATTRFILES)->second, "0");
    ASSERT_EQ(inode2.xattr().find(XATTRSUBDIRS)->second, "0");
    ASSERT_EQ(inode2.xattr().find(XATTRENTRIES)->second, "0");
    ASSERT_EQ(inode2.xattr().find(XATTRFBYTES)->second, "0");

    // GET
    XAttr xattr;
    ASSERT_EQ(manager->GetXAttr(fsId, inode2.inodeid(), &xattr),
              MetaStatusCode::OK);
    ASSERT_EQ(xattr.fsid(), fsId);
    ASSERT_EQ(xattr.inodeid(), inode2.inodeid());
    ASSERT_EQ(xattr.xattrinfos_size(), 4);
    ASSERT_EQ(xattr.xattrinfos().find(XATTRFILES)->second, "0");
    ASSERT_EQ(xattr.xattrinfos().find(XATTRSUBDIRS)->second, "0");
    ASSERT_EQ(xattr.xattrinfos().find(XATTRENTRIES)->second, "0");
    ASSERT_EQ(xattr.xattrinfos().find(XATTRFBYTES)->second, "0");

    // UPDATE
    inode2.mutable_xattr()->find(XATTRFILES)->second = "1";
    inode2.mutable_xattr()->find(XATTRSUBDIRS)->second = "1";
    inode2.mutable_xattr()->find(XATTRENTRIES)->second = "2";
    inode2.mutable_xattr()->find(XATTRFBYTES)->second = "100";
    UpdateInodeRequest request = MakeUpdateInodeRequestFromInode(inode2);
    ASSERT_EQ(manager->UpdateInode(request), MetaStatusCode::OK);

    // GET
    XAttr xattr1;
    ASSERT_EQ(manager->GetXAttr(fsId, inode2.inodeid(), &xattr1),
              MetaStatusCode::OK);
    ASSERT_EQ(xattr1.xattrinfos_size(), 4);
    ASSERT_EQ(xattr1.xattrinfos().find(XATTRFILES)->second, "1");
    ASSERT_EQ(xattr1.xattrinfos().find(XATTRSUBDIRS)->second, "1");
    ASSERT_EQ(xattr1.xattrinfos().find(XATTRENTRIES)->second, "2");
    ASSERT_EQ(xattr1.xattrinfos().find(XATTRFBYTES)->second, "100");
}

}  // namespace metaserver
}  // namespace curvefs
