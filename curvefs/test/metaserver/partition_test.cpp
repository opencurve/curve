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
 * @Date: 2021-09-01 19:38:55
 * @Author: chenwei
 */

#include "curvefs/src/metaserver/partition.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "curvefs/test/metaserver/test_helper.h"

#include "curvefs/src/metaserver/dentry_manager.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"
#include "curvefs/test/metaserver/storage/utils.h"
#include "src/fs/ext4_filesystem_impl.h"

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
using ::curvefs::metaserver::storage::RocksDBStorage;
using ::curvefs::metaserver::storage::RandomStoragePath;

namespace curvefs {
namespace metaserver {

namespace {
auto localfs = curve::fs::Ext4FileSystemImpl::getInstance();
}

class PartitionTest : public ::testing::Test {
 protected:
    void SetUp() override {
        param_.fsId = 1;
        param_.length = 0;
        param_.uid = 0;
        param_.gid = 0;
        param_.mode = 0;
        param_.type = FsFileType::TYPE_FILE;
        param_.symlink = "";
        param_.rdev = 0;

        dataDir_ = RandomStoragePath();;
        StorageOptions options;
        options.dataDir = dataDir_;
        options.localFileSystem = localfs.get();
        kvStorage_ = std::make_shared<RocksDBStorage>(options);
        ASSERT_TRUE(kvStorage_->Open());
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

    InodeParam param_;

 protected:
    std::string dataDir_;
    StorageOptions options_;
    std::shared_ptr<KVStorage> kvStorage_;
};

TEST_F(PartitionTest, testInodeIdGen1) {
    PartitionInfo partitionInfo1;
    partitionInfo1.set_fsid(1);
    partitionInfo1.set_poolid(2);
    partitionInfo1.set_copysetid(3);
    partitionInfo1.set_partitionid(4);
    partitionInfo1.set_start(100);
    partitionInfo1.set_end(199);

    Partition partition1(partitionInfo1, kvStorage_);

    ASSERT_TRUE(partition1.IsDeletable());
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(partition1.GetNewInodeId(), partitionInfo1.start() + i);
    }
    ASSERT_EQ(partition1.GetNewInodeId(), UINT64_MAX);
    ASSERT_EQ(partition1.GetNewInodeId(), UINT64_MAX);
}

TEST_F(PartitionTest, testInodeIdGen2) {
    PartitionInfo partitionInfo1;
    partitionInfo1.set_fsid(1);
    partitionInfo1.set_poolid(2);
    partitionInfo1.set_copysetid(3);
    partitionInfo1.set_partitionid(4);
    partitionInfo1.set_start(100);
    partitionInfo1.set_end(199);
    partitionInfo1.set_nextid(150);

    Partition partition1(partitionInfo1, kvStorage_);

    ASSERT_TRUE(partition1.IsDeletable());
    for (int i = 0; i < 50; i++) {
        ASSERT_EQ(partition1.GetNewInodeId(), partitionInfo1.nextid() + i);
    }
    ASSERT_EQ(partition1.GetNewInodeId(), UINT64_MAX);
    ASSERT_EQ(partition1.GetNewInodeId(), UINT64_MAX);
}

TEST_F(PartitionTest, testInodeIdGen3) {
    PartitionInfo partitionInfo1;
    partitionInfo1.set_fsid(1);
    partitionInfo1.set_poolid(2);
    partitionInfo1.set_copysetid(3);
    partitionInfo1.set_partitionid(4);
    partitionInfo1.set_start(100);
    partitionInfo1.set_end(199);
    partitionInfo1.set_nextid(200);

    Partition partition1(partitionInfo1, kvStorage_);

    ASSERT_EQ(partition1.GetNewInodeId(), UINT64_MAX);
    ASSERT_EQ(partition1.GetNewInodeId(), UINT64_MAX);
}

TEST_F(PartitionTest, testInodeIdGen4_NextId) {
    std::vector<std::pair<uint64_t, uint64_t>> testsets = {
        {0, 3}, {1, 3}, {2, 3}, {3, 3}, {4, 4}};

    for (auto& t : testsets) {
        PartitionInfo partitionInfo1;
        partitionInfo1.set_fsid(1);
        partitionInfo1.set_poolid(2);
        partitionInfo1.set_copysetid(3);
        partitionInfo1.set_partitionid(4);
        partitionInfo1.set_start(t.first);
        partitionInfo1.set_end(199);

        Partition p(partitionInfo1, kvStorage_);
        EXPECT_EQ(t.second, p.GetNewInodeId());
    }
}

TEST_F(PartitionTest, testInodeIdGen5_paritionstatus) {
    PartitionInfo partitionInfo1;
    partitionInfo1.set_fsid(1);
    partitionInfo1.set_poolid(2);
    partitionInfo1.set_copysetid(3);
    partitionInfo1.set_partitionid(4);
    partitionInfo1.set_start(100);
    partitionInfo1.set_end(199);
    partitionInfo1.set_nextid(198);
    partitionInfo1.set_status(PartitionStatus::READWRITE);

    Partition partition1(partitionInfo1, kvStorage_);

    ASSERT_EQ(partition1.GetNewInodeId(), 198);
    ASSERT_EQ(partition1.GetPartitionInfo().status(),
              PartitionStatus::READWRITE);
    ASSERT_EQ(partition1.GetNewInodeId(), 199);
    ASSERT_EQ(partition1.GetPartitionInfo().status(),
              PartitionStatus::READWRITE);
    ASSERT_EQ(partition1.GetNewInodeId(), UINT64_MAX);
    ASSERT_EQ(partition1.GetPartitionInfo().status(),
              PartitionStatus::READONLY);
}

TEST_F(PartitionTest, test1) {
    PartitionInfo partitionInfo1;
    partitionInfo1.set_fsid(1);
    partitionInfo1.set_poolid(2);
    partitionInfo1.set_copysetid(3);
    partitionInfo1.set_partitionid(4);
    partitionInfo1.set_start(100);
    partitionInfo1.set_end(199);

    Partition partition1(partitionInfo1, kvStorage_);

    ASSERT_TRUE(partition1.IsDeletable());
    ASSERT_TRUE(partition1.IsInodeBelongs(1, 100));
    ASSERT_TRUE(partition1.IsInodeBelongs(1, 199));
    ASSERT_FALSE(partition1.IsInodeBelongs(2, 100));
    ASSERT_FALSE(partition1.IsInodeBelongs(2, 199));
    ASSERT_TRUE(partition1.IsInodeBelongs(1));
    ASSERT_FALSE(partition1.IsInodeBelongs(2));
    ASSERT_EQ(partition1.GetPartitionId(), 4);
    ASSERT_EQ(partition1.GetPartitionInfo().partitionid(), 4);
}

TEST_F(PartitionTest, inodenum) {
    PartitionInfo partitionInfo1;
    partitionInfo1.set_fsid(1);
    partitionInfo1.set_poolid(2);
    partitionInfo1.set_copysetid(3);
    partitionInfo1.set_partitionid(4);
    partitionInfo1.set_start(100);
    partitionInfo1.set_end(199);

    Partition partition1(partitionInfo1, kvStorage_);

    ASSERT_EQ(partition1.GetInodeNum(), 0);
    Inode inode;

    ASSERT_EQ(partition1.CreateInode(param_, &inode),
              MetaStatusCode::OK);
    ASSERT_EQ(partition1.GetInodeNum(), 1);

    ASSERT_EQ(partition1.DeleteInode(1, 100), MetaStatusCode::OK);
    ASSERT_EQ(partition1.GetInodeNum(), 0);
}

TEST_F(PartitionTest, dentrynum) {
    PartitionInfo partitionInfo1;
    partitionInfo1.set_fsid(1);
    partitionInfo1.set_poolid(2);
    partitionInfo1.set_copysetid(3);
    partitionInfo1.set_partitionid(4);
    partitionInfo1.set_start(100);
    partitionInfo1.set_end(199);

    Partition partition1(partitionInfo1, kvStorage_);
    ASSERT_EQ(partition1.GetDentryNum(), 0);


    // create parent inode
    Inode inode;
    inode.set_inodeid(100);
    ASSERT_EQ(partition1.CreateInode(param_, &inode),
        MetaStatusCode::OK);

    Dentry dentry;
    dentry.set_fsid(1);
    dentry.set_inodeid(101);
    dentry.set_parentinodeid(100);
    dentry.set_name("name");
    dentry.set_txid(0);
    dentry.set_type(FsFileType::TYPE_DIRECTORY);
    ASSERT_EQ(partition1.CreateDentry(dentry), MetaStatusCode::OK);
    ASSERT_EQ(partition1.GetDentryNum(), 1);

    ASSERT_EQ(partition1.DeleteDentry(dentry), MetaStatusCode::OK);
    ASSERT_EQ(partition1.GetDentryNum(), 0);
}

TEST_F(PartitionTest, PARTITION_ID_MISSMATCH_ERROR) {
    PartitionInfo partitionInfo1;
    partitionInfo1.set_fsid(1);
    partitionInfo1.set_poolid(2);
    partitionInfo1.set_copysetid(3);
    partitionInfo1.set_partitionid(4);
    partitionInfo1.set_start(100);
    partitionInfo1.set_end(199);

    Partition partition1(partitionInfo1, kvStorage_);

    Dentry dentry1;
    dentry1.set_fsid(2);
    dentry1.set_parentinodeid(100);

    Dentry dentry2;
    dentry2.set_fsid(1);
    dentry2.set_parentinodeid(200);

    // test CreateDentry
    ASSERT_EQ(partition1.CreateDentry(dentry1),
              MetaStatusCode::PARTITION_ID_MISSMATCH);
    ASSERT_EQ(partition1.CreateDentry(dentry2),
              MetaStatusCode::PARTITION_ID_MISSMATCH);

    // test DeleteDentry
    ASSERT_EQ(partition1.DeleteDentry(dentry1),
              MetaStatusCode::PARTITION_ID_MISSMATCH);
    ASSERT_EQ(partition1.DeleteDentry(dentry2),
              MetaStatusCode::PARTITION_ID_MISSMATCH);

    // test GetDentry
    ASSERT_EQ(partition1.GetDentry(&dentry1),
              MetaStatusCode::PARTITION_ID_MISSMATCH);
    ASSERT_EQ(partition1.GetDentry(&dentry2),
              MetaStatusCode::PARTITION_ID_MISSMATCH);

    // test ListDentry
    std::vector<Dentry> dentrys;
    uint32_t limit = 1;
    ASSERT_EQ(partition1.ListDentry(dentry1, &dentrys, limit),
              MetaStatusCode::PARTITION_ID_MISSMATCH);
    ASSERT_EQ(partition1.ListDentry(dentry2, &dentrys, limit),
              MetaStatusCode::PARTITION_ID_MISSMATCH);

    // test IsDirEmpty
    bool empty;
    ASSERT_EQ(partition1.IsDirEmpty(dentry1, &empty),
              MetaStatusCode::PARTITION_ID_MISSMATCH);
    ASSERT_EQ(partition1.IsDirEmpty(dentry2, &empty),
              MetaStatusCode::PARTITION_ID_MISSMATCH);

    // test HandleRenameTx
    std::vector<Dentry> dentrys1 = {dentry1};
    std::vector<Dentry> dentrys2 = {dentry2};
    ASSERT_EQ(partition1.HandleRenameTx(dentrys1),
              MetaStatusCode::PARTITION_ID_MISSMATCH);
    ASSERT_EQ(partition1.HandleRenameTx(dentrys2),
              MetaStatusCode::PARTITION_ID_MISSMATCH);

    // test InsertPendingTx
    PrepareRenameTxRequest pendingTx;
    pendingTx.add_dentrys()->CopyFrom(dentry1);
    ASSERT_FALSE(partition1.InsertPendingTx(pendingTx));

    // test CreateInode
    uint32_t fsId = 1;
    param_.type = FsFileType::TYPE_DIRECTORY;
    param_.fsId = fsId + 1;
    Inode inode1;
    ASSERT_EQ(partition1.CreateInode(param_, &inode1),
              MetaStatusCode::PARTITION_ID_MISSMATCH);

    // test CreateRootInode
    ASSERT_EQ(partition1.CreateRootInode(param_),
              MetaStatusCode::PARTITION_ID_MISSMATCH);

    // test GetInode
    uint64_t rightInodeId = 100;
    uint64_t wrongInodeId = 200;
    ASSERT_EQ(partition1.GetInode(fsId + 1, rightInodeId, &inode1),
              MetaStatusCode::PARTITION_ID_MISSMATCH);
    ASSERT_EQ(partition1.GetInode(fsId, wrongInodeId, &inode1),
              MetaStatusCode::PARTITION_ID_MISSMATCH);

    // test DeleteInode
    ASSERT_EQ(partition1.DeleteInode(fsId + 1, rightInodeId),
              MetaStatusCode::PARTITION_ID_MISSMATCH);
    ASSERT_EQ(partition1.DeleteInode(fsId, wrongInodeId),
              MetaStatusCode::PARTITION_ID_MISSMATCH);

    Inode inode2;
    inode2.set_fsid(fsId + 1);
    inode2.set_inodeid(rightInodeId);

    Inode inode3;
    inode3.set_fsid(fsId);
    inode3.set_inodeid(wrongInodeId);

    // test UpdateInode
    UpdateInodeRequest inode2Request = MakeUpdateInodeRequestFromInode(inode2);
    ASSERT_EQ(partition1.UpdateInode(inode2Request),
              MetaStatusCode::PARTITION_ID_MISSMATCH);
    UpdateInodeRequest inode3Request = MakeUpdateInodeRequestFromInode(inode3);
    ASSERT_EQ(partition1.UpdateInode(inode3Request),
              MetaStatusCode::PARTITION_ID_MISSMATCH);

    // test InsertInode
    ASSERT_EQ(partition1.InsertInode(inode2),
              MetaStatusCode::PARTITION_ID_MISSMATCH);
    ASSERT_EQ(partition1.InsertInode(inode3),
              MetaStatusCode::PARTITION_ID_MISSMATCH);
}

TEST_F(PartitionTest, testGetInodeAttr) {
    PartitionInfo partitionInfo1;
    partitionInfo1.set_fsid(1);
    partitionInfo1.set_poolid(2);
    partitionInfo1.set_copysetid(3);
    partitionInfo1.set_partitionid(4);
    partitionInfo1.set_start(100);
    partitionInfo1.set_end(199);

    Partition partition1(partitionInfo1, kvStorage_);

    // create parent inode
    Inode inode;
    inode.set_inodeid(100);
    param_.type = FsFileType::TYPE_FILE;
    param_.fsId = 1;
    ASSERT_EQ(partition1.CreateInode(param_, &inode), MetaStatusCode::OK);
    InodeAttr attr;
    ASSERT_EQ(partition1.GetInodeAttr(1, 100, &attr), MetaStatusCode::OK);
    ASSERT_EQ(attr.inodeid(), 100);
    ASSERT_EQ(attr.fsid(), 1);
    ASSERT_EQ(attr.length(), 0);
    ASSERT_EQ(attr.uid(), 0);
    ASSERT_EQ(attr.gid(), 0);
    ASSERT_EQ(attr.mode(), 0);
    ASSERT_EQ(attr.type(), FsFileType::TYPE_FILE);
}

TEST_F(PartitionTest, testGetXAttr) {
    PartitionInfo partitionInfo1;
    partitionInfo1.set_fsid(1);
    partitionInfo1.set_poolid(2);
    partitionInfo1.set_copysetid(3);
    partitionInfo1.set_partitionid(4);
    partitionInfo1.set_start(100);
    partitionInfo1.set_end(199);

    Partition partition1(partitionInfo1, kvStorage_);

    // create parent inode
    Inode inode;
    inode.set_inodeid(100);
    param_.type = FsFileType::TYPE_DIRECTORY;
    ASSERT_EQ(partition1.CreateInode(param_, &inode), MetaStatusCode::OK);
    XAttr xattr;
    ASSERT_EQ(partition1.GetXAttr(1, 100, &xattr), MetaStatusCode::OK);
    ASSERT_EQ(xattr.inodeid(), 100);
    ASSERT_EQ(xattr.fsid(), 1);
    ASSERT_EQ(xattr.xattrinfos().find(XATTRFILES)->second, "0");
    ASSERT_EQ(xattr.xattrinfos().find(XATTRSUBDIRS)->second, "0");
    ASSERT_EQ(xattr.xattrinfos().find(XATTRENTRIES)->second, "0");
    ASSERT_EQ(xattr.xattrinfos().find(XATTRFBYTES)->second, "0");
}

}  // namespace metaserver
}  // namespace curvefs
