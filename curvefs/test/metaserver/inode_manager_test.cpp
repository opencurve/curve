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

#include <time.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <google/protobuf/util/message_differencer.h>
#include <memory>

#include "curvefs/test/metaserver/test_helper.h"
#include "curvefs/src/metaserver/inode_manager.h"
#include "curvefs/src/common/define.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/converter.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"
#include "curvefs/test/metaserver/storage/utils.h"
#include "src/fs/ext4_filesystem_impl.h"
#include "absl/types/optional.h"

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
using ::curvefs::metaserver::storage::RocksDBStorage;
using ::curvefs::metaserver::storage::NameGenerator;
using ::curvefs::metaserver::storage::Key4ChunkInfoList;
using ::curvefs::metaserver::storage::RandomStoragePath;

namespace curvefs {
namespace metaserver {

namespace {
auto localfs = curve::fs::Ext4FileSystemImpl::getInstance();
}

class InodeManagerTest : public ::testing::Test {
 protected:
    void SetUp() override {
        dataDir_ = RandomStoragePath();
        StorageOptions options;
        options.dataDir = dataDir_;
        options.localFileSystem = localfs.get();
        kvStorage_ = std::make_shared<RocksDBStorage>(options);
        ASSERT_TRUE(kvStorage_->Open());

        auto nameGenerator = std::make_shared<NameGenerator>(1);
        auto inodeStorage = std::make_shared<InodeStorage>(
            kvStorage_, nameGenerator, 0);
        auto trash = std::make_shared<TrashImpl>(inodeStorage);
        filetype2InodeNum_ = std::make_shared<FileType2InodeNumMap>();
        manager = std::make_shared<InodeManager>(inodeStorage, trash,
                                                 filetype2InodeNum_.get());

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

    void TearDown() override {
        ASSERT_TRUE(kvStorage_->Close());
        auto output = execShell("rm -rf " + dataDir_);
        ASSERT_EQ(output.size(), 0);
    }

    std::string execShell(const std::string& cmd) {
        std::array<char, 128> buffer;
        std::string result;
        std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"),
                                                      pclose);
        if (!pipe) {
            throw std::runtime_error("popen() failed!");
        }
        while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
            result += buffer.data();
        }
        return result;
    }

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

    bool EqualChunkInfo(const ChunkInfo& lhs, const ChunkInfo& rhs) {
        return lhs.chunkid() == rhs.chunkid() &&
            lhs.compaction() == rhs.compaction() &&
            lhs.offset() == rhs.offset() &&
            lhs.len() == rhs.len() &&
            lhs.size() == rhs.size() &&
            lhs.zero() == rhs.zero();
    }

    bool EqualChunkInfoList(const ChunkInfoList& lhs,
                              const ChunkInfoList& rhs) {
        size_t size = lhs.s3chunks_size();
        if (size != rhs.s3chunks_size()) {
            return false;
        }

        for (size_t i = 0; i < size; i++) {
            if (!EqualChunkInfo(lhs.s3chunks(i), rhs.s3chunks(i))) {
                return false;
            }
        }
        return true;
    }

    ChunkInfoList GenChunkInfoList(uint64_t firstChunkId,
                                       uint64_t lastChunkId) {
        ChunkInfoList list;
        for (uint64_t id = firstChunkId; id <= lastChunkId; id++) {
            ChunkInfo* info = list.add_s3chunks();
            info->set_chunkid(id);
            info->set_compaction(0);
            info->set_offset(0);
            info->set_len(0);
            info->set_size(0);
            info->set_zero(false);
        }
        return list;
    }

    void CHECK_ITERATOR_ChunkInfoLIST(
        std::shared_ptr<Iterator> iterator,
        const std::vector<uint64_t> chunkIndexs,
        const std::vector<ChunkInfoList> lists) {
        size_t size = 0;
        Key4ChunkInfoList key;
        ChunkInfoList list4get;
        ASSERT_EQ(iterator->Status(), 0);
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
            ASSERT_TRUE(conv_->ParseFromString(iterator->Key(), &key));
            ASSERT_TRUE(conv_->ParseFromString(iterator->Value(), &list4get));
            ASSERT_EQ(key.chunkIndex, chunkIndexs[size]);
            ASSERT_TRUE(EqualChunkInfoList(list4get, lists[size]));
            size++;
        }
        ASSERT_EQ(size, chunkIndexs.size());
    }

 protected:
    std::shared_ptr<InodeManager> manager;
    InodeParam param_;
    std::shared_ptr<Converter> conv_;
    std::string dataDir_;
    std::shared_ptr<KVStorage> kvStorage_;
    std::shared_ptr<FileType2InodeNumMap> filetype2InodeNum_;
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

    // test struct timespec
    Inode inode5;
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    param_.timestamp = absl::make_optional<struct timespec>(now);
    ASSERT_EQ(manager->CreateInode(6, param_, &inode5),
              MetaStatusCode::OK);

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
              MetaStatusCode::OK);
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

    Inode temp6;
    ASSERT_EQ(manager->GetInode(fsId, inode5.inodeid(), &temp6),
              MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(inode5, temp6));
}

TEST_F(InodeManagerTest, GetOrModifyChunkInfo) {
    uint32_t fsId = 1;
    uint32_t inodeId = 1;

    // CASE 1: GetOrModifyChunkInfo() success
    {
        LOG(INFO) << "CASE 1: GetOrModifyChunkInfo() success";
        google::protobuf::Map<uint64_t, ChunkInfoList> map2add;
        google::protobuf::Map<uint64_t, ChunkInfoList> map2del;

        map2add[1] = GenChunkInfoList(1, 1);
        map2add[2] = GenChunkInfoList(2, 2);
        map2add[3] = GenChunkInfoList(3, 3);

        std::shared_ptr<Iterator> iterator;
        MetaStatusCode rc = manager->GetOrModifyChunkInfo(
            fsId, inodeId, map2add, map2del, true, &iterator);
        ASSERT_EQ(rc, MetaStatusCode::OK);

        CHECK_ITERATOR_ChunkInfoLIST(iterator,
            std::vector<uint64_t>{ 1, 2, 3 },
            std::vector<ChunkInfoList>{
                GenChunkInfoList(1, 1),
                GenChunkInfoList(2, 2),
                GenChunkInfoList(3, 3),
            });

        LOG(INFO) << "CASE 1.1: check idempotent for GetOrModifyChunkInfo()";
        rc = manager->GetOrModifyChunkInfo(
            fsId, inodeId, map2add, map2del, true, &iterator);
        ASSERT_EQ(rc, MetaStatusCode::OK);

        CHECK_ITERATOR_ChunkInfoLIST(iterator,
            std::vector<uint64_t>{ 1, 2, 3 },
            std::vector<ChunkInfoList>{
                GenChunkInfoList(1, 1),
                GenChunkInfoList(2, 2),
                GenChunkInfoList(3, 3),
            });
    }

    // CASE 2: GetOrModifyChunkInfo() with delete
    {
        LOG(INFO) << "CASE 2: GetOrModifyChunkInfo() with delete";
        google::protobuf::Map<uint64_t, ChunkInfoList> map2add;
        google::protobuf::Map<uint64_t, ChunkInfoList> map2del;

        map2del[1] = GenChunkInfoList(1, 1);
        map2del[2] = GenChunkInfoList(2, 2);
        map2del[3] = GenChunkInfoList(3, 3);

        std::shared_ptr<Iterator> iterator;
        MetaStatusCode rc = manager->GetOrModifyChunkInfo(
            fsId, inodeId, map2add, map2del, true, &iterator);
        ASSERT_EQ(rc, MetaStatusCode::OK);

        CHECK_ITERATOR_ChunkInfoLIST(iterator,
            std::vector<uint64_t>{},
            std::vector<ChunkInfoList>{});
    }

    // CASE 3: GetOrModifyChunkInfo() with add and delete
    {
        LOG(INFO) << "CASE 3: GetOrModifyChunkInfo() with add and delete";
        google::protobuf::Map<uint64_t, ChunkInfoList> map2add;
        google::protobuf::Map<uint64_t, ChunkInfoList> map2del;

        // step1: append ChunkInfo
        map2add[0] = GenChunkInfoList(1, 100);
        map2add[1] = GenChunkInfoList(1, 100);
        map2add[2] = GenChunkInfoList(1, 100);
        map2add[7] = GenChunkInfoList(1, 100);
        map2add[8] = GenChunkInfoList(1, 100);
        map2add[9] = GenChunkInfoList(1, 100);

        std::shared_ptr<Iterator> iterator;
        MetaStatusCode rc = manager->GetOrModifyChunkInfo(
            fsId, inodeId, map2add, map2del, true, &iterator);
        ASSERT_EQ(rc, MetaStatusCode::OK);
        ASSERT_EQ(iterator->Status(), 0);

        // step2: delete ChunkInfo
        map2add.clear();
        map2del.clear();

        map2add[0] = GenChunkInfoList(100, 100);
        map2add[7] = GenChunkInfoList(100, 100);
        map2add[8] = GenChunkInfoList(100, 100);
        map2add[9] = GenChunkInfoList(100, 100);

        map2del[0] = GenChunkInfoList(1, 100);
        map2del[7] = GenChunkInfoList(1, 100);
        map2del[8] = GenChunkInfoList(1, 100);
        map2del[9] = GenChunkInfoList(1, 100);

        rc = manager->GetOrModifyChunkInfo(
            fsId, inodeId, map2add, map2del, true, &iterator);
        ASSERT_EQ(rc, MetaStatusCode::OK);
        ASSERT_EQ(iterator->Status(), 0);

        CHECK_ITERATOR_ChunkInfoLIST(iterator,
            std::vector<uint64_t>{ 0, 1, 2, 7, 8, 9 },
            std::vector<ChunkInfoList>{
                GenChunkInfoList(100, 100),
                GenChunkInfoList(1, 100),
                GenChunkInfoList(1, 100),
                GenChunkInfoList(100, 100),
                GenChunkInfoList(100, 100),
                GenChunkInfoList(100, 100),
            });

        // step3: delete all ChunkInfo
        map2add.clear();
        map2del.clear();
        map2add[1] = GenChunkInfoList(100, 100);
        map2add[2] = GenChunkInfoList(100, 100);

        map2del[1] = GenChunkInfoList(1, 100);
        map2del[2] = GenChunkInfoList(1, 100);

        rc = manager->GetOrModifyChunkInfo(
            fsId, inodeId, map2add, map2del, true, &iterator);
        ASSERT_EQ(rc, MetaStatusCode::OK);
        ASSERT_EQ(iterator->Status(), 0);

        CHECK_ITERATOR_ChunkInfoLIST(iterator,
            std::vector<uint64_t>{ 0, 1, 2, 7, 8, 9 },
            std::vector<ChunkInfoList>{
                GenChunkInfoList(100, 100),
                GenChunkInfoList(100, 100),
                GenChunkInfoList(100, 100),
                GenChunkInfoList(100, 100),
                GenChunkInfoList(100, 100),
                GenChunkInfoList(100, 100),
            });
    }
}

TEST_F(InodeManagerTest, UpdateInode) {
    // create inode
    uint64_t ino = 2;

    Inode inode;
    ASSERT_EQ(MetaStatusCode::OK, manager->CreateInode(ino, param_, &inode));

    // test update ok
    UpdateInodeRequest request = MakeUpdateInodeRequestFromInode(inode);
    ASSERT_EQ(MetaStatusCode::OK,
              manager->UpdateInode(request));

    // test update fail
    ASSERT_EQ(MetaStatusCode::OK,
              manager->UpdateInode(request));
}


TEST_F(InodeManagerTest, testGetAttr) {
    // CREATE
    uint32_t fsId = 1;
    Inode inode1;
    ASSERT_EQ(manager->CreateInode(2, param_, &inode1),
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
    ASSERT_EQ(manager->CreateInode(2, param_, &inode1),
        MetaStatusCode::OK);
    ASSERT_EQ(inode1.inodeid(), 2);
    ASSERT_TRUE(inode1.xattr().empty());

    Inode inode2;
    param_.type = FsFileType::TYPE_DIRECTORY;
    ASSERT_EQ(manager->CreateInode(3, param_, &inode2),
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

TEST_F(InodeManagerTest, testCreateManageInode) {
    param_.type = FsFileType::TYPE_DIRECTORY;
    param_.parent = ROOTINODEID;
    ManageInodeType type = ManageInodeType::TYPE_RECYCLE;
    Inode inode;
    ASSERT_EQ(MetaStatusCode::OK,
        manager->CreateManageInode(param_, type, &inode));
    ASSERT_EQ(inode.inodeid(), RECYCLEINODEID);
    ASSERT_EQ(inode.parent()[0], ROOTINODEID);

    Inode temp1;
    ASSERT_EQ(manager->GetInode(1, inode.inodeid(), &temp1),
              MetaStatusCode::OK);
    ASSERT_TRUE(CompareInode(inode, temp1));
}

}  // namespace metaserver
}  // namespace curvefs
