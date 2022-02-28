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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "curvefs/src/metaserver/inode_storage.h"
#include "curvefs/src/common/define.h"

#include "curvefs/src/metaserver/storage_common.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/memory_storage.h"

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SaveArg;

using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::StorageOptions;
using ::curvefs::metaserver::storage::MemoryStorage;

namespace curvefs {
namespace metaserver {
class InodeStorageTest : public ::testing::Test {
 protected:
    void SetUp() override {
        tablename_ = "partition:1";
        kvStorage_ = std::make_shared<MemoryStorage>(options_);
        conv_ = std::make_shared<Converter>();
    }

    void TearDown() override { return; }

    bool CompareInode(const Inode &first, const Inode &second) {
        return first.fsid() == second.fsid() &&
               first.atime() == second.atime() &&
               first.inodeid() == second.inodeid();
    }

    Inode GenInode(uint32_t fsId, uint64_t inodeId) {
        Inode inode;
        inode.set_fsid(fsId);
        inode.set_inodeid(inodeId);
        inode.set_length(4096);
        inode.set_ctime(0);
        inode.set_ctime_ns(0);
        inode.set_mtime(0);
        inode.set_mtime_ns(0);
        inode.set_atime(0);
        inode.set_atime_ns(0);
        inode.set_uid(0);
        inode.set_gid(0);
        inode.set_mode(0);
        inode.set_nlink(0);
        inode.set_type(FsFileType::TYPE_FILE);
        return inode;
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

    void CHECK_INODE_S3CHUNKINFOLIST(InodeStorage* storage,
                                     uint32_t fsId, uint64_t inodeId,
                                     const std::vector<uint64_t> chunkIndexs,
                                     const std::vector<S3ChunkInfoList> lists) {
        ASSERT_EQ(chunkIndexs.size(), lists.size());
        auto iterator = storage->GetInodeS3ChunkInfoList(fsId, inodeId);
        ASSERT_EQ(iterator->Status(), 0);

        size_t size = 0;
        Key4S3ChunkInfoList key;
        S3ChunkInfoList list4get;
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
            LOG(INFO) << "checking chunkIndex(" << size << "), key="
                      << iterator->Key();
            ASSERT_TRUE(conv_->ParseFromString(iterator->Key(), &key));
            ASSERT_TRUE(conv_->ParseFromString(iterator->Value(), &list4get));
            ASSERT_EQ(key.chunkIndex, chunkIndexs[size]);
            ASSERT_TRUE(EqualS3ChunkInfoList(list4get, lists[size]));
            size++;
        }
        ASSERT_EQ(size, chunkIndexs.size());
    }

 protected:
    std::string tablename_;
    StorageOptions options_;
    std::shared_ptr<KVStorage> kvStorage_;
    std::shared_ptr<Converter> conv_;
};

TEST_F(InodeStorageTest, test1) {
    InodeStorage storage(kvStorage_, tablename_);
    Inode inode1 = GenInode(1, 1);
    Inode inode2 = GenInode(2, 2);
    Inode inode3 = GenInode(3, 3);

    // insert
    ASSERT_EQ(storage.Insert(inode1), MetaStatusCode::OK);
    ASSERT_EQ(storage.Insert(inode2), MetaStatusCode::OK);
    ASSERT_EQ(storage.Insert(inode3), MetaStatusCode::OK);
    ASSERT_EQ(storage.Insert(inode1), MetaStatusCode::INODE_EXIST);
    ASSERT_EQ(storage.Insert(inode2), MetaStatusCode::INODE_EXIST);
    ASSERT_EQ(storage.Insert(inode3), MetaStatusCode::INODE_EXIST);
    ASSERT_EQ(storage.Size(), 3);

    // get
    Inode temp;
    ASSERT_EQ(storage.Get(Key4Inode(inode1), &temp), MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(inode1, temp));
    ASSERT_EQ(storage.Get(Key4Inode(inode2), &temp), MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(inode2, temp));
    ASSERT_EQ(storage.Get(Key4Inode(inode3), &temp), MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(inode3, temp));

    // delete
    ASSERT_EQ(storage.Delete(Key4Inode(inode1)), MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 2);
    ASSERT_EQ(storage.Get(Key4Inode(inode1), &temp),
        MetaStatusCode::NOT_FOUND);
    ASSERT_EQ(storage.Delete(Key4Inode(inode1)), MetaStatusCode::NOT_FOUND);

    // update
    ASSERT_EQ(storage.Update(inode1), MetaStatusCode::NOT_FOUND);
    Inode oldInode;
    ASSERT_EQ(storage.Get(Key4Inode(inode2), &oldInode), MetaStatusCode::OK);
    inode2.set_atime(400);
    ASSERT_EQ(storage.Update(inode2), MetaStatusCode::OK);
    Inode newInode;
    ASSERT_EQ(storage.Get(Key4Inode(inode2), &newInode), MetaStatusCode::OK);
    ASSERT_FALSE(CompareInode(oldInode, newInode));
    ASSERT_FALSE(CompareInode(oldInode, inode2));
    ASSERT_TRUE(CompareInode(newInode, inode2));

    // GetInodeIdList
    std::list<uint64_t> inodeIdList;
    ASSERT_TRUE(storage.GetInodeIdList(&inodeIdList));
    ASSERT_EQ(inodeIdList.size(), 2);
}

TEST_F(InodeStorageTest, testGetAttrNotFound) {
    InodeStorage storage(kvStorage_, tablename_);
    Inode inode;
    inode.set_fsid(1);
    inode.set_inodeid(1);
    inode.set_length(1);
    inode.set_ctime(100);
    inode.set_ctime_ns(100);
    inode.set_mtime(100);
    inode.set_mtime_ns(100);
    inode.set_atime(100);
    inode.set_atime_ns(100);
    inode.set_uid(0);
    inode.set_gid(0);
    inode.set_mode(777);
    inode.set_nlink(2);
    inode.set_type(FsFileType::TYPE_DIRECTORY);

    ASSERT_EQ(storage.Insert(inode), MetaStatusCode::OK);
    InodeAttr attr;
    ASSERT_EQ(storage.GetAttr(Key4Inode(1, 2), &attr),
        MetaStatusCode::NOT_FOUND);
}

TEST_F(InodeStorageTest, testGetAttr) {
    InodeStorage storage(kvStorage_, tablename_);
    Inode inode;
    inode.set_fsid(1);
    inode.set_inodeid(1);
    inode.set_length(1);
    inode.set_ctime(100);
    inode.set_ctime_ns(100);
    inode.set_mtime(100);
    inode.set_mtime_ns(100);
    inode.set_atime(100);
    inode.set_atime_ns(100);
    inode.set_uid(0);
    inode.set_gid(0);
    inode.set_mode(777);
    inode.set_nlink(2);
    inode.set_type(FsFileType::TYPE_DIRECTORY);

    ASSERT_EQ(storage.Insert(inode), MetaStatusCode::OK);
    InodeAttr attr;
    ASSERT_EQ(storage.GetAttr(Key4Inode(1, 1), &attr), MetaStatusCode::OK);
    ASSERT_EQ(attr.inodeid(), 1);
    ASSERT_EQ(attr.ctime(), 100);
    ASSERT_EQ(attr.uid(), 0);
    ASSERT_EQ(attr.mode(), 777);
}

TEST_F(InodeStorageTest, testGetXAttr) {
    InodeStorage storage(kvStorage_, tablename_);
    Inode inode;
    inode.set_fsid(1);
    inode.set_inodeid(1);
    inode.set_length(1);
    inode.set_ctime(100);
    inode.set_ctime_ns(100);
    inode.set_mtime(100);
    inode.set_mtime_ns(100);
    inode.set_atime(100);
    inode.set_atime_ns(100);
    inode.set_uid(0);
    inode.set_gid(0);
    inode.set_mode(777);
    inode.set_nlink(2);
    inode.set_type(FsFileType::TYPE_DIRECTORY);
    inode.mutable_xattr()->insert({XATTRFILES, "1"});
    inode.mutable_xattr()->insert({XATTRSUBDIRS, "1"});
    inode.mutable_xattr()->insert({XATTRENTRIES, "2"});
    inode.mutable_xattr()->insert({XATTRFBYTES, "100"});

    inode.mutable_xattr()->insert({XATTRRFILES, "100"});
    inode.mutable_xattr()->insert({XATTRRSUBDIRS, "100"});
    inode.mutable_xattr()->insert({XATTRRENTRIES, "200"});
    inode.mutable_xattr()->insert({XATTRRFBYTES, "1000"});

    ASSERT_EQ(storage.Insert(inode), MetaStatusCode::OK);
    XAttr xattr;
    ASSERT_EQ(storage.GetXAttr(Key4Inode(1, 1), &xattr), MetaStatusCode::OK);
    ASSERT_FALSE(xattr.xattrinfos().empty());

    ASSERT_EQ(xattr.xattrinfos().find(XATTRFILES)->second, "1");
    ASSERT_EQ(xattr.xattrinfos().find(XATTRSUBDIRS)->second, "1");
    ASSERT_EQ(xattr.xattrinfos().find(XATTRENTRIES)->second, "2");
    ASSERT_EQ(xattr.xattrinfos().find(XATTRFBYTES)->second, "100");

    ASSERT_EQ(xattr.xattrinfos().find(XATTRRFILES)->second, "100");
    ASSERT_EQ(xattr.xattrinfos().find(XATTRRSUBDIRS)->second, "100");
    ASSERT_EQ(xattr.xattrinfos().find(XATTRRENTRIES)->second, "200");
    ASSERT_EQ(xattr.xattrinfos().find(XATTRRFBYTES)->second, "1000");
}

TEST_F(InodeStorageTest, GetOrModifyS3ChunkInfo) {
    uint32_t fsId = 1;
    uint64_t inodeId = 1;
    InodeStorage storage(kvStorage_, tablename_);

    // CASE 1: empty s3chunkinfo
    {
        LOG(INFO) << "CASE 1:";
        Inode inode = GenInode(fsId, inodeId);
        ASSERT_EQ(storage.Insert(inode), MetaStatusCode::OK);

        size_t size = 0;
        auto iterator = storage.GetInodeS3ChunkInfoList(fsId, inodeId);
        ASSERT_EQ(iterator->Status(), 0);
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
            size++;
        }
        ASSERT_EQ(size, 0);
    }

    // CASE 2: append one s3chunkinfo
    {
        LOG(INFO) << "CASE 2:";
        ASSERT_EQ(storage.Clear(), MetaStatusCode::OK);
        std::vector<uint64_t> chunkIndexs{ 1 };
        std::vector<S3ChunkInfoList> lists{ GenS3ChunkInfoList(1, 1) };

        for (size_t size = 0; size < chunkIndexs.size(); size++) {
            MetaStatusCode rc = storage.AppendS3ChunkInfoList(
                fsId, inodeId, chunkIndexs[size], lists[size], false);
            ASSERT_EQ(rc, MetaStatusCode::OK);
        }

        CHECK_INODE_S3CHUNKINFOLIST(
            &storage, fsId, inodeId, chunkIndexs, lists);
    }

    // CASE 3: append multi s3chunkinfos
    {
        LOG(INFO) << "CASE 3:";
        ASSERT_EQ(storage.Clear(), MetaStatusCode::OK);
        std::vector<uint64_t> chunkIndexs{ 1, 2, 3 };
        std::vector<S3ChunkInfoList> lists{
            GenS3ChunkInfoList(1, 1),
            GenS3ChunkInfoList(2, 2),
            GenS3ChunkInfoList(3, 3),
        };

        for (size_t size = 0; size < chunkIndexs.size(); size++) {
            MetaStatusCode rc = storage.AppendS3ChunkInfoList(
                fsId, inodeId, chunkIndexs[size], lists[size], false);
            ASSERT_EQ(rc, MetaStatusCode::OK);
        }

        CHECK_INODE_S3CHUNKINFOLIST(
            &storage, fsId, inodeId, chunkIndexs, lists);
    }

    // CASE 4: check order for s3chunkinfo's chunk index
    {
        LOG(INFO) << "CASE 4:";
        ASSERT_EQ(storage.Clear(), MetaStatusCode::OK);
        std::vector<uint64_t> chunkIndexs{ 2, 1, 3 };
        std::vector<S3ChunkInfoList> lists{
            GenS3ChunkInfoList(2, 2),
            GenS3ChunkInfoList(1, 1),
            GenS3ChunkInfoList(3, 3),
        };

        for (size_t size = 0; size < chunkIndexs.size(); size++) {
            MetaStatusCode rc = storage.AppendS3ChunkInfoList(
                fsId, inodeId, chunkIndexs[size], lists[size], false);
            ASSERT_EQ(rc, MetaStatusCode::OK);
        }

        CHECK_INODE_S3CHUNKINFOLIST(&storage, fsId, inodeId,
            std::vector<uint64_t>{ 1, 2, 3 },
            std::vector<S3ChunkInfoList>{
                GenS3ChunkInfoList(1, 1),
                GenS3ChunkInfoList(2, 2),
                GenS3ChunkInfoList(3, 3),
            });
    }

    // CASE 5: check order for s3chunkinfo's chunk id
    {
        LOG(INFO) << "CASE 5:";
        ASSERT_EQ(storage.Clear(), MetaStatusCode::OK);
        std::vector<uint64_t> chunkIndexs{ 2, 1, 3, 1, 2 };
        std::vector<S3ChunkInfoList> lists{
            GenS3ChunkInfoList(200, 210),
            GenS3ChunkInfoList(120, 130),
            GenS3ChunkInfoList(300, 310),
            GenS3ChunkInfoList(100, 110),
            GenS3ChunkInfoList(220, 230),
        };

        for (size_t size = 0; size < chunkIndexs.size(); size++) {
            MetaStatusCode rc = storage.AppendS3ChunkInfoList(
                fsId, inodeId, chunkIndexs[size], lists[size], false);
            ASSERT_EQ(rc, MetaStatusCode::OK);
        }

        CHECK_INODE_S3CHUNKINFOLIST(&storage, fsId, inodeId,
            std::vector<uint64_t>{ 1, 1, 2, 2, 3 },
            std::vector<S3ChunkInfoList>{
                GenS3ChunkInfoList(100, 110),
                GenS3ChunkInfoList(120, 130),
                GenS3ChunkInfoList(200, 210),
                GenS3ChunkInfoList(220, 230),
                GenS3ChunkInfoList(300, 310),
            });
    }
}

TEST_F(InodeStorageTest, PaddingInodeS3ChunkInfo) {
    uint32_t fsId = 1;
    uint64_t inodeId = 1;
    InodeStorage storage(kvStorage_, tablename_);

    // step1: insert inode
    Inode inode = GenInode(fsId, inodeId);
    ASSERT_EQ(storage.Insert(inode), MetaStatusCode::OK);

    // step2: append s3chunkinfo
    std::vector<uint64_t> chunkIndexs{ 1, 3, 2, 1, 2 };
    std::vector<S3ChunkInfoList> lists{
        GenS3ChunkInfoList(100, 109),
        GenS3ChunkInfoList(300, 310),
        GenS3ChunkInfoList(200, 209),
        GenS3ChunkInfoList(110, 120),
        GenS3ChunkInfoList(210, 220),
    };

    for (size_t size = 0; size < chunkIndexs.size(); size++) {
        MetaStatusCode rc = storage.AppendS3ChunkInfoList(
            fsId, inodeId, chunkIndexs[size], lists[size], false);
        ASSERT_EQ(rc, MetaStatusCode::OK);
    }

    // step3: padding inode s3chunkinfo
    ASSERT_EQ(inode.mutable_s3chunkinfomap()->size(), 0);

    MetaStatusCode rc = storage.PaddingInodeS3ChunkInfo(fsId, inodeId, &inode);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    auto m = inode.s3chunkinfomap();
    ASSERT_EQ(m.size(), 3);
    ASSERT_TRUE(EqualS3ChunkInfoList(m[1], GenS3ChunkInfoList(100, 120)));
    ASSERT_TRUE(EqualS3ChunkInfoList(m[2], GenS3ChunkInfoList(200, 220)));
    ASSERT_TRUE(EqualS3ChunkInfoList(m[3], GenS3ChunkInfoList(300, 310)));
}

TEST_F(InodeStorageTest, GetAllS3ChunkInfoList) {
    InodeStorage storage(kvStorage_, tablename_);
    uint64_t chunkIndex = 1;
    S3ChunkInfoList list4add = GenS3ChunkInfoList(1, 10);

    // step1: prepare inode and its s3chunkinfo
    auto prepareInode = [&](uint32_t fsId, uint64_t inodeId) {
        Inode inode = GenInode(fsId, inodeId);
        ASSERT_EQ(storage.Insert(inode), MetaStatusCode::OK);

        MetaStatusCode rc = storage.AppendS3ChunkInfoList(
            fsId, inodeId, chunkIndex, list4add, false);
        ASSERT_EQ(rc, MetaStatusCode::OK);
    };

    prepareInode(1, 1);
    prepareInode(2, 2);

    // step2: check all s3chunkinfo
    size_t size = 0;
    Key4S3ChunkInfoList key;
    S3ChunkInfoList list4get;
    std::vector<uint32_t> fsIds{ 1, 2 };
    std::vector<uint64_t> inodeIds{ 1, 2 };
    auto iterator = storage.GetAllS3ChunkInfoList();
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        ASSERT_TRUE(conv_->ParseFromString(iterator->Key(), &key));
        ASSERT_TRUE(conv_->ParseFromString(iterator->Value(), &list4get));
        ASSERT_EQ(key.chunkIndex, chunkIndex);
        ASSERT_EQ(key.fsId, fsIds[size]);
        ASSERT_EQ(key.inodeId, inodeIds[size]);
        ASSERT_TRUE(EqualS3ChunkInfoList(list4add, list4get));
        size++;
    }
    ASSERT_EQ(size, 2);
}

}  // namespace metaserver
}  // namespace curvefs
