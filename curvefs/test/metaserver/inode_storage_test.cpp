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

#include <gmock/gmock-generated-matchers.h>
#include <gmock/gmock-spec-builders.h>
#include <gmock/gmock.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <memory>
#include <ostream>
#include <random>
#include <string>
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/proto/common.pb.h"
#include "curvefs/src/metaserver/inode_storage.h"
#include "curvefs/src/common/define.h"

#include "curvefs/src/metaserver/storage/config.h"
#include "curvefs/src/metaserver/storage/memory_storage.h"
#include "curvefs/src/metaserver/storage/status.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/converter.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"
#include "curvefs/test/metaserver/storage/utils.h"
#include "curvefs/test/metaserver/mock/mock_kv_storage.h"
#include "src/fs/ext4_filesystem_impl.h"

using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::StrEq;

using ::curvefs::common::EmptyMsg;
using ::curvefs::metaserver::storage::Key4DeallocatableBlockGroup;
using ::curvefs::metaserver::storage::Key4ChunkInfoList;
using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::RandomStoragePath;
using ::curvefs::metaserver::storage::RocksDBStorage;
using ::curvefs::metaserver::storage::StorageOptions;

namespace curvefs {
namespace metaserver {

namespace {
auto localfs = curve::fs::Ext4FileSystemImpl::getInstance();
}

class InodeStorageTest : public ::testing::Test {
 protected:
    void SetUp() override {
        nameGenerator_ = std::make_shared<NameGenerator>(1);
        dataDir_ = RandomStoragePath();
        StorageOptions options;
        options.dataDir = dataDir_;
        options.localFileSystem = localfs.get();
        kvStorage_ = std::make_shared<RocksDBStorage>(options);
        ASSERT_TRUE(kvStorage_->Open());
        conv_ = std::make_shared<Converter>();
    }

    void TearDown() override {
        ASSERT_TRUE(kvStorage_->Close());
        auto output = execShell("rm -rf " + dataDir_);
        ASSERT_EQ(output.size(), 0);
    }

    std::string execShell(const std::string &cmd) {
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

    ChunkInfoList GenChunkInfoList(uint64_t firstChunkId,
                                       uint64_t lastChunkId) {
        ChunkInfoList list;
        for (uint64_t id = firstChunkId; id <= lastChunkId; id++) {
            ChunkInfo *info = list.add_s3chunks();
            info->set_chunkid(id);
            info->set_compaction(0);
            info->set_offset(0);
            info->set_len(0);
            info->set_size(0);
            info->set_zero(false);
        }
        return list;
    }

    bool EqualChunkInfo(const ChunkInfo &lhs, const ChunkInfo &rhs) {
        return lhs.chunkid() == rhs.chunkid() &&
               lhs.compaction() == rhs.compaction() &&
               lhs.offset() == rhs.offset() && lhs.len() == rhs.len() &&
               lhs.size() == rhs.size() && lhs.zero() == rhs.zero();
    }

    bool EqualChunkInfoList(const ChunkInfoList &lhs,
                              const ChunkInfoList &rhs) {
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

    void CHECK_INODE_ChunkInfoLIST(InodeStorage *storage, uint32_t fsId,
                                     uint64_t inodeId,
                                     const std::vector<uint64_t> chunkIndexs,
                                     const std::vector<ChunkInfoList> lists) {
        ASSERT_EQ(chunkIndexs.size(), lists.size());
        auto iterator = storage->GetInodeChunkInfoList(fsId, inodeId);
        ASSERT_EQ(iterator->Status(), 0);

        size_t size = 0;
        Key4ChunkInfoList key;
        ChunkInfoList list4get;
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
            ASSERT_TRUE(conv_->ParseFromString(iterator->Key(), &key));
            LOG(INFO) << "key" << size << "=" << iterator->Key();
            ASSERT_TRUE(iterator->ParseFromValue(&list4get));
            ASSERT_EQ(key.chunkIndex, chunkIndexs[size]);
            ASSERT_TRUE(EqualChunkInfoList(list4get, lists[size]));
            size++;
        }
        ASSERT_EQ(size, chunkIndexs.size());
    }

 protected:
    std::string dataDir_;
    std::shared_ptr<NameGenerator> nameGenerator_;
    std::shared_ptr<KVStorage> kvStorage_;
    std::shared_ptr<Converter> conv_;
};

TEST_F(InodeStorageTest, test1) {
    InodeStorage storage(kvStorage_, nameGenerator_, 0);
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
    ASSERT_EQ(storage.Get(Key4Inode(inode1), &temp), MetaStatusCode::NOT_FOUND);
    ASSERT_EQ(storage.Delete(Key4Inode(inode1)), MetaStatusCode::OK);

    // update
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
    ASSERT_TRUE(storage.GetAllInodeId(&inodeIdList));
    ASSERT_EQ(inodeIdList.size(), 2);
}

TEST_F(InodeStorageTest, testGetAttrNotFound) {
    InodeStorage storage(kvStorage_, nameGenerator_, 0);
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
    InodeStorage storage(kvStorage_, nameGenerator_, 0);
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
    InodeStorage storage(kvStorage_, nameGenerator_, 0);
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

TEST_F(InodeStorageTest, ModifyInodeChunkInfoList) {
    uint32_t fsId = 1;
    uint64_t inodeId = 1;
    InodeStorage storage(kvStorage_, nameGenerator_, 0);

    // CASE 1: get empty ChunkInfo
    {
        LOG(INFO) << "CASE 1: get empty s3chukninfo";
        Inode inode = GenInode(fsId, inodeId);
        ASSERT_EQ(storage.Insert(inode), MetaStatusCode::OK);
        ChunkInfoList list2del;

        size_t size = 0;
        auto iterator = storage.GetInodeChunkInfoList(fsId, inodeId);
        ASSERT_EQ(iterator->Status(), 0);
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
            size++;
        }
        ASSERT_EQ(size, 0);
    }

    // CASE 2: append one ChunkInfo
    {
        LOG(INFO) << "CASE 2: append one ChunkInfo";
        ASSERT_EQ(storage.Clear(), MetaStatusCode::OK);
        std::vector<uint64_t> chunkIndexs{1};
        std::vector<ChunkInfoList> lists2add{GenChunkInfoList(1, 1)};

        for (size_t i = 0; i < chunkIndexs.size(); i++) {
            MetaStatusCode rc = storage.ModifyInodeChunkInfoList(
                fsId, inodeId, chunkIndexs[i], &lists2add[i], nullptr);
            ASSERT_EQ(rc, MetaStatusCode::OK);
        }

        CHECK_INODE_ChunkInfoLIST(&storage, fsId, inodeId, chunkIndexs,
                                    lists2add);
    }

    // CASE 3: append multi ChunkInfos
    {
        LOG(INFO) << "CASE 3: append multi ChunkInfos";
        ASSERT_EQ(storage.Clear(), MetaStatusCode::OK);
        std::vector<uint64_t> chunkIndexs{1, 2, 3};
        std::vector<ChunkInfoList> lists2add{
            GenChunkInfoList(1, 1),
            GenChunkInfoList(2, 2),
            GenChunkInfoList(3, 3),
        };

        for (size_t i = 0; i < chunkIndexs.size(); i++) {
            MetaStatusCode rc = storage.ModifyInodeChunkInfoList(
                fsId, inodeId, chunkIndexs[i], &lists2add[i], nullptr);
            ASSERT_EQ(rc, MetaStatusCode::OK);
        }

        CHECK_INODE_ChunkInfoLIST(&storage, fsId, inodeId, chunkIndexs,
                                    lists2add);
    }

    // CASE 4: check order for ChunkInfo's chunk index
    {
        LOG(INFO) << "CASE 4: check order for ChunkInfo's chunk index";
        ASSERT_EQ(storage.Clear(), MetaStatusCode::OK);
        std::vector<uint64_t> chunkIndexs{2, 1, 3};
        std::vector<ChunkInfoList> lists2add{
            GenChunkInfoList(2, 2),
            GenChunkInfoList(1, 1),
            GenChunkInfoList(3, 3),
        };

        for (size_t i = 0; i < chunkIndexs.size(); i++) {
            MetaStatusCode rc = storage.ModifyInodeChunkInfoList(
                fsId, inodeId, chunkIndexs[i], &lists2add[i], nullptr);
            ASSERT_EQ(rc, MetaStatusCode::OK);
        }

        CHECK_INODE_ChunkInfoLIST(&storage, fsId, inodeId,
                                    std::vector<uint64_t>{1, 2, 3},
                                    std::vector<ChunkInfoList>{
                                        GenChunkInfoList(1, 1),
                                        GenChunkInfoList(2, 2),
                                        GenChunkInfoList(3, 3),
                                    });
    }

    // CASE 5: check order for ChunkInfo's chunk id
    {
        LOG(INFO) << "CASE 5: check order for ChunkInfo's chunk id";
        ASSERT_EQ(storage.Clear(), MetaStatusCode::OK);
        std::vector<uint64_t> chunkIndexs{2, 1, 3, 1, 2};
        std::vector<ChunkInfoList> lists2add{
            GenChunkInfoList(200, 210), GenChunkInfoList(120, 130),
            GenChunkInfoList(300, 310), GenChunkInfoList(100, 110),
            GenChunkInfoList(220, 230),
        };

        for (size_t i = 0; i < chunkIndexs.size(); i++) {
            MetaStatusCode rc = storage.ModifyInodeChunkInfoList(
                fsId, inodeId, chunkIndexs[i], &lists2add[i], nullptr);
            ASSERT_EQ(rc, MetaStatusCode::OK);
        }

        CHECK_INODE_ChunkInfoLIST(&storage, fsId, inodeId,
                                    std::vector<uint64_t>{1, 1, 2, 2, 3},
                                    std::vector<ChunkInfoList>{
                                        GenChunkInfoList(100, 110),
                                        GenChunkInfoList(120, 130),
                                        GenChunkInfoList(200, 210),
                                        GenChunkInfoList(220, 230),
                                        GenChunkInfoList(300, 310),
                                    });
    }


    // CASE 6: delete ChunkInfo list with wrong range
    {
        LOG(INFO) << "CASE 6: delete s3chukninfo list with wrong range";
        ASSERT_EQ(storage.Clear(), MetaStatusCode::OK);

        // step1: add ChunkInfo
        {
            std::vector<uint64_t> chunkIndexs{1, 2, 3};
            std::vector<ChunkInfoList> lists2add{
                GenChunkInfoList(100, 199),
                GenChunkInfoList(200, 299),
                GenChunkInfoList(300, 399),
            };

            for (size_t i = 0; i < chunkIndexs.size(); i++) {
                MetaStatusCode rc = storage.ModifyInodeChunkInfoList(
                    fsId, inodeId, chunkIndexs[i], &lists2add[i], nullptr);
                ASSERT_EQ(rc, MetaStatusCode::OK);
            }
        }

        // step2: compaction
        {
            std::vector<uint64_t> chunkIndexs{1, 2, 3};
            std::vector<ChunkInfoList> lists2add{
                GenChunkInfoList(199, 199),
                GenChunkInfoList(299, 299),
                GenChunkInfoList(399, 399),
            };

            std::vector<ChunkInfoList> lists2del{
                GenChunkInfoList(200, 201),
                GenChunkInfoList(250, 299),
                GenChunkInfoList(250, 301),
            };

            for (size_t i = 0; i < chunkIndexs.size(); i++) {
                MetaStatusCode rc = storage.ModifyInodeChunkInfoList(
                    fsId, inodeId, chunkIndexs[i], &lists2add[i],
                    &lists2del[i]);
                ASSERT_EQ(rc, MetaStatusCode::STORAGE_INTERNAL_ERROR);
            }
        }
    }

    // CASE 7: delete all ChunkInfo list
    {
        LOG(INFO) << "CASE 7: delete all s3chukninfo list";
        ASSERT_EQ(storage.Clear(), MetaStatusCode::OK);

        // step1: add ChunkInfo
        {
            std::vector<uint64_t> chunkIndexs{1, 2, 3};
            std::vector<ChunkInfoList> lists2add{
                GenChunkInfoList(100, 199),
                GenChunkInfoList(200, 299),
                GenChunkInfoList(300, 399),
            };

            for (size_t i = 0; i < chunkIndexs.size(); i++) {
                MetaStatusCode rc = storage.ModifyInodeChunkInfoList(
                    fsId, inodeId, chunkIndexs[i], &lists2add[i], nullptr);
                ASSERT_EQ(rc, MetaStatusCode::OK);
            }
        }

        // step2: compaction
        {
            std::vector<uint64_t> chunkIndexs{1, 2, 3};
            std::vector<ChunkInfoList> lists2add{
                GenChunkInfoList(199, 199),
                GenChunkInfoList(299, 299),
                GenChunkInfoList(399, 399),
            };

            std::vector<ChunkInfoList> lists2del{
                GenChunkInfoList(100, 199),
                GenChunkInfoList(200, 299),
                GenChunkInfoList(300, 399),
            };

            for (size_t i = 0; i < chunkIndexs.size(); i++) {
                MetaStatusCode rc = storage.ModifyInodeChunkInfoList(
                    fsId, inodeId, chunkIndexs[i], &lists2add[i],
                    &lists2del[i]);
                ASSERT_EQ(rc, MetaStatusCode::OK);
            }
        }

        // step3: check result
        CHECK_INODE_ChunkInfoLIST(&storage, fsId, inodeId,
                                    std::vector<uint64_t>{1, 2, 3},
                                    std::vector<ChunkInfoList>{
                                        GenChunkInfoList(199, 199),
                                        GenChunkInfoList(299, 299),
                                        GenChunkInfoList(399, 399),
                                    });
    }


    // CASE 8: delete ChunkInfo list with prefix range
    {
        LOG(INFO) << "CASE 2: delete ChunkInfo list with prefix range";
        ASSERT_EQ(storage.Clear(), MetaStatusCode::OK);

        // step1: add ChunkInfo
        {
            std::vector<uint64_t> chunkIndexs{1, 2, 3};
            std::vector<ChunkInfoList> lists2add{
                GenChunkInfoList(100, 199),
                GenChunkInfoList(1000, 1999),
                GenChunkInfoList(10000, 19999),
            };

            for (size_t i = 0; i < chunkIndexs.size(); i++) {
                MetaStatusCode rc = storage.ModifyInodeChunkInfoList(
                    fsId, inodeId, chunkIndexs[i], &lists2add[i], nullptr);
                ASSERT_EQ(rc, MetaStatusCode::OK);
            }
        }

        // step2: add ChunkInfo again
        {
            std::vector<uint64_t> chunkIndexs{1, 2, 3};
            std::vector<ChunkInfoList> lists2add{
                GenChunkInfoList(200, 299),
                GenChunkInfoList(2000, 2999),
                GenChunkInfoList(20000, 29999),
            };

            for (size_t i = 0; i < chunkIndexs.size(); i++) {
                MetaStatusCode rc = storage.ModifyInodeChunkInfoList(
                    fsId, inodeId, chunkIndexs[i], &lists2add[i], nullptr);
                ASSERT_EQ(rc, MetaStatusCode::OK);
            }
        }

        // step2: compaction
        {
            std::vector<uint64_t> chunkIndexs{1, 2, 3};
            std::vector<ChunkInfoList> lists2add{
                GenChunkInfoList(199, 199),
                GenChunkInfoList(2999, 2999),
                GenChunkInfoList(19999, 19999),
            };

            std::vector<ChunkInfoList> lists2del{
                GenChunkInfoList(100, 199),
                GenChunkInfoList(1000, 2999),
                GenChunkInfoList(10000, 19999),
            };

            for (size_t i = 0; i < chunkIndexs.size(); i++) {
                MetaStatusCode rc = storage.ModifyInodeChunkInfoList(
                    fsId, inodeId, chunkIndexs[i], &lists2add[i],
                    &lists2del[i]);
                ASSERT_EQ(rc, MetaStatusCode::OK);
            }
        }

        // step3: check result
        CHECK_INODE_ChunkInfoLIST(&storage, fsId, inodeId,
                                    std::vector<uint64_t>{1, 1, 2, 3, 3},
                                    std::vector<ChunkInfoList>{
                                        GenChunkInfoList(199, 199),
                                        GenChunkInfoList(200, 299),
                                        GenChunkInfoList(2999, 2999),
                                        GenChunkInfoList(19999, 19999),
                                        GenChunkInfoList(20000, 29999),
                                    });
    }
}

TEST_F(InodeStorageTest, PaddingInodeChunkInfo) {
    uint32_t fsId = 1;
    uint64_t inodeId = 1;
    InodeStorage storage(kvStorage_, nameGenerator_, 0);
    ChunkInfoList list2del;

    // step1: insert inode
    Inode inode = GenInode(fsId, inodeId);
    ASSERT_EQ(storage.Insert(inode), MetaStatusCode::OK);

    // step2: append ChunkInfo
    std::vector<uint64_t> chunkIndexs{1, 3, 2, 1, 2};
    std::vector<ChunkInfoList> lists2add{
        GenChunkInfoList(100, 109), GenChunkInfoList(300, 310),
        GenChunkInfoList(200, 209), GenChunkInfoList(110, 120),
        GenChunkInfoList(210, 220),
    };

    for (size_t i = 0; i < chunkIndexs.size(); i++) {
        MetaStatusCode rc = storage.ModifyInodeChunkInfoList(
            fsId, inodeId, chunkIndexs[i], &lists2add[i], nullptr);
        ASSERT_EQ(rc, MetaStatusCode::OK);
    }
    ASSERT_EQ(inode.mutable_ChunkInfomap()->size(), 0);

    // CASE 1: padding inode ChunkInfo success
    {
        LOG(INFO) << "CASE 1: padding inode ChunkInfo success";
        Inode out;
        MetaStatusCode rc = storage.PaddingInodeChunkInfo(
            fsId, inodeId, out.mutable_ChunkInfomap());
        ASSERT_EQ(rc, MetaStatusCode::OK);

        auto m = out.ChunkInfomap();
        ASSERT_EQ(m.size(), 3);
        ASSERT_TRUE(EqualChunkInfoList(m[1], GenChunkInfoList(100, 120)));
        ASSERT_TRUE(EqualChunkInfoList(m[2], GenChunkInfoList(200, 220)));
        ASSERT_TRUE(EqualChunkInfoList(m[3], GenChunkInfoList(300, 310)));
    }

    // CASE 2: padding inode ChunkInfo within limit
    {
        LOG(INFO) << "CASE 2: padding inode ChunkInfo within limit";
        Inode out;
        MetaStatusCode rc = storage.PaddingInodeChunkInfo(
            fsId, inodeId, out.mutable_ChunkInfomap(), 53);
        ASSERT_EQ(rc, MetaStatusCode::OK);

        auto m = out.ChunkInfomap();
        ASSERT_EQ(m.size(), 3);
        ASSERT_TRUE(EqualChunkInfoList(m[1], GenChunkInfoList(100, 120)));
        ASSERT_TRUE(EqualChunkInfoList(m[2], GenChunkInfoList(200, 220)));
        ASSERT_TRUE(EqualChunkInfoList(m[3], GenChunkInfoList(300, 310)));
    }

    // CASE 3: padding inode ChunkInfo exceed limit
    {
        LOG(INFO) << "CASE 3: padding inode ChunkInfo exceed limit";
        Inode out;
        MetaStatusCode rc = storage.PaddingInodeChunkInfo(
            fsId, inodeId, out.mutable_ChunkInfomap(), 52);
        ASSERT_EQ(rc, MetaStatusCode::INODE_S3_META_TOO_LARGE);

        auto m = out.ChunkInfomap();
        ASSERT_EQ(m.size(), 0);
    }
}

TEST_F(InodeStorageTest, GetAllChunkInfoList) {
    InodeStorage storage(kvStorage_, nameGenerator_, 0);
    uint64_t chunkIndex = 1;
    ChunkInfoList list2add = GenChunkInfoList(1, 10);

    // step1: prepare inode and its ChunkInfo
    auto prepareInode = [&](uint32_t fsId, uint64_t inodeId) {
        Inode inode = GenInode(fsId, inodeId);
        ASSERT_EQ(storage.Insert(inode), MetaStatusCode::OK);

        MetaStatusCode rc = storage.ModifyInodeChunkInfoList(
            fsId, inodeId, chunkIndex, &list2add, nullptr);
        ASSERT_EQ(rc, MetaStatusCode::OK);
    };

    prepareInode(1, 1);
    prepareInode(2, 2);

    // step2: check all ChunkInfo
    size_t size = 0;
    Key4ChunkInfoList key;
    ChunkInfoList list4get;
    std::vector<uint32_t> fsIds{1, 2};
    std::vector<uint64_t> inodeIds{1, 2};
    auto iterator = storage.GetAllChunkInfoList();
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        ASSERT_TRUE(conv_->ParseFromString(iterator->Key(), &key));
        ASSERT_TRUE(iterator->ParseFromValue(&list4get));
        ASSERT_EQ(key.chunkIndex, chunkIndex);
        ASSERT_EQ(key.fsId, fsIds[size]);
        ASSERT_EQ(key.inodeId, inodeIds[size]);
        ASSERT_TRUE(EqualChunkInfoList(list2add, list4get));
        size++;
    }
    ASSERT_EQ(size, 2);
}

TEST_F(InodeStorageTest, TestUpdateVolumeExtentSlice) {
    using storage::Status;

    const std::vector<std::pair<MetaStatusCode, Status>> cases{
        {MetaStatusCode::OK, Status::OK()},
        {MetaStatusCode::STORAGE_INTERNAL_ERROR, Status::InternalError()}};

    for (const auto &test : cases) {
        auto kvStorage = std::make_shared<storage::MockKVStorage>();
        InodeStorage storage(kvStorage, nameGenerator_, 0);

        uint32_t fsId = 1;
        uint64_t inodeId = 1;
        VolumeExtentSlice slice;
        slice.set_offset(1ULL * 1024 * 1024 * 1024);

        const std::string expectTableName =
            nameGenerator_->GetVolumeExtentTableName();
        const std::string expectKey = "4:1:1:" + std::to_string(slice.offset());
        EXPECT_CALL(*kvStorage, SSet(expectTableName, expectKey, _))
            .WillOnce(Return(test.second));

        ASSERT_EQ(test.first,
                  storage.UpdateVolumeExtentSlice(fsId, inodeId, slice));
    }
}

static void RandomSetExtent(VolumeExtent *ext) {
    std::random_device rd;

    ext->set_fsoffset(rd());
    ext->set_volumeoffset(rd());
    ext->set_length(rd());
    ext->set_isused(rd() & 1);
}

static bool PrepareGetAllVolumeExtentTest(InodeStorage *storage, uint32_t fsId,
                                          uint64_t inodeId,
                                          std::vector<VolumeExtentSlice> *out) {
    VolumeExtentSlice slice1;
    slice1.set_offset(0);

    RandomSetExtent(slice1.add_extents());
    RandomSetExtent(slice1.add_extents());

    auto st = storage->UpdateVolumeExtentSlice(fsId, inodeId, slice1);
    if (st != MetaStatusCode::OK) {
        return false;
    }

    VolumeExtentSlice slice2;
    slice2.set_offset(16ULL * 1024 * 1024);

    RandomSetExtent(slice2.add_extents());
    RandomSetExtent(slice2.add_extents());

    st = storage->UpdateVolumeExtentSlice(fsId, inodeId, slice2);
    if (st != MetaStatusCode::OK) {
        return false;
    }

    VolumeExtentSlice slice3;
    slice3.set_offset(0);

    RandomSetExtent(slice3.add_extents());
    RandomSetExtent(slice3.add_extents());

    st = storage->UpdateVolumeExtentSlice(fsId, inodeId * 10, slice3);

    out->push_back(slice1);
    out->push_back(slice2);
    return true;
}

static bool operator==(const VolumeExtentSliceList &list,
                       const std::vector<VolumeExtentSlice> &slices) {
    std::vector<VolumeExtentSlice> clist(list.slices().begin(),
                                         list.slices().end());

    auto copy = slices;
    std::sort(copy.begin(), copy.end(),
              [](const VolumeExtentSlice &s1, const VolumeExtentSlice &s2) {
                  return s1.offset() < s2.offset();
              });

    return true;
}

static bool operator==(const VolumeExtentSlice &s1,
                       const VolumeExtentSlice &s2) {
    return google::protobuf::util::MessageDifferencer::Equals(s1, s2);
}

TEST_F(InodeStorageTest, TestGetAllVolumeExtent) {
    StorageOptions opts;
    opts.compression = false;

    std::shared_ptr<KVStorage> memStore =
        std::make_shared<storage::MemoryStorage>(opts);
    std::shared_ptr<KVStorage> kvStore = kvStorage_;

    for (auto &store : {memStore, kvStore}) {
        InodeStorage storage(store, nameGenerator_, 0);
        const uint32_t fsId = 1;
        const uint64_t inodeId = 2;

        std::vector<VolumeExtentSlice> slices;
        ASSERT_TRUE(
            PrepareGetAllVolumeExtentTest(&storage, fsId, inodeId, &slices));

        VolumeExtentSliceList list;
        ASSERT_EQ(MetaStatusCode::OK,
                  storage.GetAllVolumeExtent(fsId, inodeId, &list));
        ASSERT_EQ(2, list.slices().size());
        ASSERT_EQ(list, slices);

        auto iter = storage.GetAllVolumeExtent(fsId, inodeId);
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        iter->Next();
        ASSERT_TRUE(iter->Valid());
        iter->Next();
        ASSERT_FALSE(iter->Valid());
    }
}

static std::string ToString(storage::STORAGE_TYPE type) {
    switch (type) {
    case storage::STORAGE_TYPE::MEMORY_STORAGE:
        return "memory";
    case storage::STORAGE_TYPE::ROCKSDB_STORAGE:
        return "rocksdb";
    }

    return "unknown";
}

TEST_F(InodeStorageTest, TestGetVolumeExtentByOffset) {
    StorageOptions opts;
    opts.compression = false;

    std::shared_ptr<KVStorage> memStore =
        std::make_shared<storage::MemoryStorage>(opts);
    std::shared_ptr<KVStorage> kvStore = kvStorage_;

    for (auto &store : {kvStorage_, memStore}) {
        InodeStorage storage(store, nameGenerator_, 0);
        const uint32_t fsId = 1;
        const uint64_t inodeId = 2;

        std::vector<VolumeExtentSlice> slices;
        ASSERT_TRUE(
            PrepareGetAllVolumeExtentTest(&storage, fsId, inodeId, &slices))
            << ToString(store->Type());

        VolumeExtentSlice slice;
        auto st = storage.GetVolumeExtentByOffset(fsId, inodeId,
                                                  slices[0].offset(), &slice);
        ASSERT_EQ(st, MetaStatusCode::OK);
        ASSERT_EQ(slice, slices[0]);

        st = storage.GetVolumeExtentByOffset(fsId, inodeId, slices[1].offset(),
                                             &slice);
        ASSERT_EQ(st, MetaStatusCode::OK);
        ASSERT_EQ(slice, slices[1]);

        st = storage.GetVolumeExtentByOffset(
            fsId, inodeId, slices[1].offset() + slices[1].offset(), &slice);
        ASSERT_EQ(st, MetaStatusCode::NOT_FOUND);
    }
}

TEST_F(InodeStorageTest, Test_UpdateInodeWithDeallocate) {
    auto inode = GenInode(1, 1);
    InodeStorage storage(kvStorage_, nameGenerator_, 0);
    ASSERT_EQ(storage.Insert(inode), MetaStatusCode::OK);

    ASSERT_EQ(MetaStatusCode::OK, storage.Update(inode, true));

    auto tableName = nameGenerator_->GetDeallocatableInodeTableName();
    Key4Inode key(1, 1);
    auto skey = key.SerializeToString();
    EmptyMsg value;
    auto s = kvStorage_->HGet(tableName, skey, &value);
    ASSERT_TRUE(s.ok());
}

TEST_F(InodeStorageTest, Test_UpdateDeallocatableBlockGroup) {
    uint64_t blockGroupOffset = 0;
    uint32_t fsId = 1;
    auto tableName = nameGenerator_->GetDeallocatableBlockGroupTableName();
    InodeStorage storage(kvStorage_, nameGenerator_, 0);
    uint64_t increaseSize = 100 * 1024;
    uint64_t decreaseSize = 4 * 1024;
    Key4DeallocatableBlockGroup key(fsId, blockGroupOffset);
    std::vector<DeallocatableBlockGroup> deallocatableBlockGroupVec;
    ASSERT_EQ(MetaStatusCode::NOT_FOUND,
              storage.GeAllBlockGroup(&deallocatableBlockGroupVec));

    // test increase
    {
        DeallocatableBlockGroupVec inputVec;

        DeallocatableBlockGroup blockGroup;
        auto increase = blockGroup.mutable_increase();
        increase->set_increasedeallocatablesize(increaseSize);
        increase->add_inodeidlistadd(1);
        inputVec.Add()->CopyFrom(blockGroup);

        // increase first time
        ASSERT_EQ(MetaStatusCode::OK,
                  storage.UpdateDeallocatableBlockGroup(fsId, inputVec));
        DeallocatableBlockGroup out;
        auto st = kvStorage_->HGet(tableName, key.SerializeToString(), &out);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(increaseSize, out.deallocatablesize());
        ASSERT_EQ(1, out.inodeidlist_size());
        ASSERT_EQ(blockGroupOffset, out.blockgroupoffset());
        ASSERT_EQ(0, out.inodeidunderdeallocate_size());

        // increase second time
        ASSERT_EQ(MetaStatusCode::OK,
                  storage.UpdateDeallocatableBlockGroup(fsId, inputVec));
        st = kvStorage_->HGet(tableName, key.SerializeToString(), &out);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(increaseSize * 2, out.deallocatablesize());
        ASSERT_EQ(1, out.inodeidlist_size());
        ASSERT_EQ(blockGroupOffset, out.blockgroupoffset());
        ASSERT_EQ(0, out.inodeidunderdeallocate_size());
    }

    // test mark
    {
        DeallocatableBlockGroupVec inputVec;

        DeallocatableBlockGroup blockGroup;
        auto mark = blockGroup.mutable_mark();
        mark->add_inodeidunderdeallocate(1);
        inputVec.Add()->CopyFrom(blockGroup);

        ASSERT_EQ(MetaStatusCode::OK,
                  storage.UpdateDeallocatableBlockGroup(fsId, inputVec));
        DeallocatableBlockGroup out;
        auto st = kvStorage_->HGet(tableName, key.SerializeToString(), &out);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(increaseSize * 2, out.deallocatablesize());
        ASSERT_EQ(0, out.inodeidlist_size());
        ASSERT_EQ(1, out.inodeidunderdeallocate_size());
    }

    // test decrease
    {
        DeallocatableBlockGroupVec inputVec;

        DeallocatableBlockGroup blockGroup;
        auto decrease = blockGroup.mutable_decrease();
        decrease->set_decreasedeallocatablesize(decreaseSize);
        inputVec.Add()->CopyFrom(blockGroup);
        ASSERT_EQ(MetaStatusCode::OK,
                  storage.UpdateDeallocatableBlockGroup(fsId, inputVec));
        DeallocatableBlockGroup out;
        auto st = kvStorage_->HGet(tableName, key.SerializeToString(), &out);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(increaseSize * 2 - decreaseSize, out.deallocatablesize());
    }

    ASSERT_EQ(MetaStatusCode::OK,
              storage.GeAllBlockGroup(&deallocatableBlockGroupVec));
    ASSERT_EQ(1, deallocatableBlockGroupVec.size());
}

}  // namespace metaserver
}  // namespace curvefs
