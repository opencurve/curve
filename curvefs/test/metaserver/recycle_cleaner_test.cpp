/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * @Date: 2022-09-14 17:01:42
 * @Author: chenwei
 */
#include "curvefs/src/metaserver/recycle_cleaner.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "curvefs/src/metaserver/storage/rocksdb_storage.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/test/client/mock_metaserver_client.h"
#include "curvefs/test/client/rpcclient/mock_mds_client.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_node.h"
#include "curvefs/test/metaserver/storage/utils.h"
#include "src/fs/ext4_filesystem_impl.h"

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;
using ::testing::WithArg;

using ::curvefs::client::rpcclient::MockMdsClient;
using ::curvefs::client::rpcclient::MockMetaServerClient;
using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::RandomStoragePath;
using ::curvefs::metaserver::storage::RocksDBStorage;
using ::curvefs::metaserver::storage::StorageOptions;

namespace curvefs {
namespace metaserver {
namespace {
auto localfs = curve::fs::Ext4FileSystemImpl::getInstance();
}

class RecycleCleanerTest : public testing::Test {
 protected:
    void SetUp() override {
        dataDir_ = RandomStoragePath();
        StorageOptions options;
        options.dataDir = dataDir_;
        options.localFileSystem = localfs.get();
        kvStorage_ = std::make_shared<RocksDBStorage>(options);
        ASSERT_TRUE(kvStorage_->Open());

        uint32_t partitionId1 = 1;
        uint32_t fsId = 2;
        PartitionInfo partitionInfo;
        partitionInfo.set_partitionid(partitionId1);
        partitionInfo.set_fsid(fsId);
        partitionInfo.set_start(0);
        partitionInfo.set_end(2000);
        partition_ = std::make_shared<Partition>(partitionInfo, kvStorage_);
        cleaner_ = std::make_shared<RecycleCleaner>(partition_);
        mdsclient_ = std::make_shared<MockMdsClient>();
        metaClient_ = std::make_shared<MockMetaServerClient>();

        cleaner_->SetCopysetNode(&copysetNode_);
        cleaner_->SetMdsClient(mdsclient_);
        cleaner_->SetMetaClient(metaClient_);
        cleaner_->SetScanLimit(5);

        // create recycle dir and root dir
        InodeParam rootPram;
        rootPram.fsId = fsId;
        rootPram.parent = 0;
        rootPram.type = FsFileType::TYPE_DIRECTORY;
        ASSERT_EQ(partition_->CreateRootInode(rootPram), MetaStatusCode::OK);
        InodeParam managePram;
        managePram.fsId = fsId;
        managePram.parent = ROOTINODEID;
        managePram.type = FsFileType::TYPE_DIRECTORY;
        Inode inode;
        ASSERT_EQ(partition_->CreateManageInode(
                      managePram, ManageInodeType::TYPE_RECYCLE, &inode),
                  MetaStatusCode::OK);
        Dentry dentry;
        dentry.set_fsid(fsId);
        dentry.set_inodeid(RECYCLEINODEID);
        dentry.set_name(RECYCLENAME);
        dentry.set_parentinodeid(ROOTINODEID);
        dentry.set_type(FsFileType::TYPE_DIRECTORY);
        dentry.set_txid(0);
        ASSERT_EQ(partition_->CreateDentry(dentry), MetaStatusCode::OK);
    }

    void TearDown() override {
        ASSERT_TRUE(kvStorage_->Close());
        auto output = execShell("rm -rf " + dataDir_);
        ASSERT_EQ(output.size(), 0);
    }

    std::string execShell(const string& cmd) {
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

    std::string GetRecycleTimeDirName() {
        time_t timeStamp;
        time(&timeStamp);
        struct tm p = *localtime_r(&timeStamp, &p);
        char now[64];
        strftime(now, 64, "%Y-%m-%d-%H", &p);
        LOG(INFO) << "now " << timeStamp << ", " << now;
        return now;
    }

 protected:
    std::string dataDir_;
    std::shared_ptr<KVStorage> kvStorage_;
    std::shared_ptr<RecycleCleaner> cleaner_;
    std::shared_ptr<MockMdsClient> mdsclient_;
    std::shared_ptr<MockMetaServerClient> metaClient_;
    std::shared_ptr<Partition> partition_;
    copyset::MockCopysetNode copysetNode_;
};

TEST_F(RecycleCleanerTest, time_func_test) {
    time_t timeStamp;
    time(&timeStamp);
    struct tm p = *localtime_r(&timeStamp, &p);
    char now[64];
    strftime(now, 64, "%Y-%m-%d-%H", &p);
    LOG(INFO) << "now " << timeStamp << ", " << now;

    struct tm tmDir;
    memset(&tmDir, 0, sizeof(tmDir));
    char* c = strptime(now, "%Y-%m-%d-%H", &tmDir);

    time_t dirTime = mktime(&tmDir);
    LOG(INFO) << "befor, time = " << timeStamp;
    LOG(INFO) << "after, time = " << dirTime;
    ASSERT_TRUE(timeStamp > dirTime);
    ASSERT_TRUE(timeStamp < dirTime + 3600);
}

TEST_F(RecycleCleanerTest, dir_time_out_test) {
    uint32_t recycleTime = 10;
    FsInfo fsInfo;
    fsInfo.set_recycletimehour(recycleTime);
    EXPECT_CALL(*mdsclient_, GetFsInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(fsInfo),
                        Return(FSStatusCode::UNKNOWN_ERROR)))
        .WillOnce(DoAll(SetArgPointee<1>(fsInfo), Return(FSStatusCode::OK)));
    ASSERT_FALSE(cleaner_->UpdateFsInfo());
    ASSERT_TRUE(cleaner_->UpdateFsInfo());

    ASSERT_EQ(cleaner_->GetRecycleTime(), recycleTime);

    ASSERT_FALSE(cleaner_->IsDirTimeOut("3000-1-1-1"));
    ASSERT_TRUE(cleaner_->IsDirTimeOut("2000-1-1-1"));
    sleep(1);
    ASSERT_FALSE(cleaner_->IsDirTimeOut(GetRecycleTimeDirName()));
    ASSERT_TRUE(cleaner_->IsDirTimeOut("2022-1-1-1"));
}

TEST_F(RecycleCleanerTest, delete_node_test) {
    uint32_t recycleTime = 10;
    FsInfo fsInfo;
    fsInfo.set_recycletimehour(recycleTime);
    EXPECT_CALL(*mdsclient_, GetFsInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(fsInfo), Return(FSStatusCode::OK)));

    ASSERT_TRUE(cleaner_->UpdateFsInfo());

    ASSERT_EQ(cleaner_->GetRecycleTime(), recycleTime);

    Dentry dentry;
    dentry.set_parentinodeid(1);
    dentry.set_inodeid(2);
    dentry.set_name("name");

    // delete dentry fail
    {
        EXPECT_CALL(*metaClient_, DeleteDentry(_, _, _, _))
            .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));

        ASSERT_FALSE(cleaner_->DeleteNode(dentry));
    }

    // get parent inode fail
    {
        EXPECT_CALL(*metaClient_, DeleteDentry(_, _, _, _))
            .WillOnce(Return(MetaStatusCode::OK));
        EXPECT_CALL(*metaClient_, GetInode(_, _, _, _))
            .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));

        ASSERT_FALSE(cleaner_->DeleteNode(dentry));
    }

    // update parent inode fail
    {
        EXPECT_CALL(*metaClient_, DeleteDentry(_, _, _, _))
            .WillOnce(Return(MetaStatusCode::OK));
        EXPECT_CALL(*metaClient_, GetInode(_, _, _, _))
            .WillOnce(Return(MetaStatusCode::OK));
        EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
            .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));

        ASSERT_FALSE(cleaner_->DeleteNode(dentry));
    }

    // get inode fail
    {
        EXPECT_CALL(*metaClient_, DeleteDentry(_, _, _, _))
            .WillOnce(Return(MetaStatusCode::OK));
        EXPECT_CALL(*metaClient_, GetInode(_, _, _, _))
            .WillOnce(Return(MetaStatusCode::OK))
            .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));
        EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
            .WillOnce(Return(MetaStatusCode::OK));

        ASSERT_FALSE(cleaner_->DeleteNode(dentry));
    }

    // nlink is zero before update
    {
        Inode inode;
        inode.set_nlink(0);
        EXPECT_CALL(*metaClient_, DeleteDentry(_, _, _, _))
            .WillOnce(Return(MetaStatusCode::OK));
        EXPECT_CALL(*metaClient_, GetInode(_, _, _, _))
            .WillOnce(Return(MetaStatusCode::OK))
            .WillOnce(
                 DoAll(SetArgPointee<2>(inode), Return(MetaStatusCode::OK)));
        EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
            .WillOnce(Return(MetaStatusCode::OK));

        ASSERT_FALSE(cleaner_->DeleteNode(dentry));
    }

    // update inode fail
    {
        Inode inode;
        inode.set_nlink(1);
        EXPECT_CALL(*metaClient_, DeleteDentry(_, _, _, _))
            .WillOnce(Return(MetaStatusCode::OK));
        EXPECT_CALL(*metaClient_, GetInode(_, _, _, _))
            .WillOnce(Return(MetaStatusCode::OK))
            .WillOnce(
                DoAll(SetArgPointee<2>(inode), Return(MetaStatusCode::OK)));
        EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
            .WillOnce(Return(MetaStatusCode::OK));
        EXPECT_CALL(*metaClient_, UpdateInodeAttr(_, _, _))
            .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));

        ASSERT_FALSE(cleaner_->DeleteNode(dentry));
    }

    // success
    {
        Inode inode;
        inode.set_nlink(1);
        EXPECT_CALL(*metaClient_, DeleteDentry(_, _, _, _))
            .WillOnce(Return(MetaStatusCode::OK));
        EXPECT_CALL(*metaClient_, GetInode(_, _, _, _))
            .WillOnce(Return(MetaStatusCode::OK))
            .WillOnce(
                DoAll(SetArgPointee<2>(inode), Return(MetaStatusCode::OK)));
        EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
            .WillOnce(Return(MetaStatusCode::OK));
        EXPECT_CALL(*metaClient_, UpdateInodeAttr(_, _, _))
            .WillOnce(Return(MetaStatusCode::OK));

        ASSERT_TRUE(cleaner_->DeleteNode(dentry));
    }
}

TEST_F(RecycleCleanerTest, scan_recycle_test1) {
    // cleaner is stop
    cleaner_->Stop();
    ASSERT_TRUE(cleaner_->ScanRecycle());
}

TEST_F(RecycleCleanerTest, scan_recycle_test2) {
    // cleaner is not leader copyset
    EXPECT_CALL(copysetNode_, IsLeaderTerm()).WillOnce(Return(false));
    ASSERT_FALSE(cleaner_->ScanRecycle());
}

TEST_F(RecycleCleanerTest, scan_recycle_test3) {
    uint32_t recycleTime = 10;
    FsInfo fsInfo;
    fsInfo.set_recycletimehour(recycleTime);
    EXPECT_CALL(copysetNode_, IsLeaderTerm()).WillOnce(Return(true));
    EXPECT_CALL(*mdsclient_, GetFsInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(fsInfo),
                        Return(FSStatusCode::UNKNOWN_ERROR)));
    ASSERT_FALSE(cleaner_->ScanRecycle());
}

// recycle is empty
TEST_F(RecycleCleanerTest, scan_recycle_test4) {
    uint32_t recycleTime = 10;
    FsInfo fsInfo;
    fsInfo.set_recycletimehour(recycleTime);
    EXPECT_CALL(copysetNode_, IsLeaderTerm()).WillOnce(Return(true));
    EXPECT_CALL(*mdsclient_, GetFsInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(fsInfo), Return(FSStatusCode::OK)));
    ASSERT_FALSE(cleaner_->ScanRecycle());
}

// recycle dir not timeout
TEST_F(RecycleCleanerTest, scan_recycle_test5) {
    uint32_t recycleTime = 10;
    FsInfo fsInfo;
    fsInfo.set_recycletimehour(recycleTime);
    EXPECT_CALL(copysetNode_, IsLeaderTerm()).WillRepeatedly(Return(true));
    EXPECT_CALL(*mdsclient_, GetFsInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(fsInfo), Return(FSStatusCode::OK)));
    Dentry dentry1;
    dentry1.set_name("3000-1-1-1");
    dentry1.set_fsid(2);
    dentry1.set_parentinodeid(RECYCLEINODEID);
    dentry1.set_inodeid(2000);
    dentry1.set_txid(0);
    dentry1.set_type(FsFileType::TYPE_DIRECTORY);
    Dentry dentry2;
    dentry2.set_name(GetRecycleTimeDirName());
    dentry2.set_fsid(2);
    dentry2.set_parentinodeid(RECYCLEINODEID);
    dentry2.set_inodeid(2001);
    dentry2.set_txid(0);
    dentry2.set_type(FsFileType::TYPE_DIRECTORY);
    partition_->CreateDentry(dentry1);
    partition_->CreateDentry(dentry2);
    LOG(INFO) << "create dentry1 " << dentry1.ShortDebugString();
    LOG(INFO) << "create dentry2 " << dentry2.ShortDebugString();

    ASSERT_FALSE(cleaner_->ScanRecycle());
}

// recycle dir has 1 dir timeout, 1 dir not timeout
TEST_F(RecycleCleanerTest, scan_recycle_test6) {
    uint32_t recycleTime = 10;
    FsInfo fsInfo;
    fsInfo.set_recycletimehour(recycleTime);
    EXPECT_CALL(copysetNode_, IsLeaderTerm()).WillRepeatedly(Return(true));
    EXPECT_CALL(*mdsclient_, GetFsInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(fsInfo), Return(FSStatusCode::OK)));
    Dentry dentry1;
    dentry1.set_name("2000-1-1-1");
    dentry1.set_fsid(2);
    dentry1.set_parentinodeid(RECYCLEINODEID);
    dentry1.set_inodeid(2000);
    dentry1.set_txid(0);
    dentry1.set_type(FsFileType::TYPE_DIRECTORY);
    Dentry dentry2;
    dentry2.set_name(GetRecycleTimeDirName());
    dentry2.set_fsid(2);
    dentry2.set_parentinodeid(RECYCLEINODEID);
    dentry2.set_inodeid(2001);
    dentry2.set_txid(0);
    dentry2.set_type(FsFileType::TYPE_DIRECTORY);
    ASSERT_EQ(partition_->CreateDentry(dentry1), MetaStatusCode::OK);
    ASSERT_EQ(partition_->CreateDentry(dentry2), MetaStatusCode::OK);
    LOG(INFO) << "create dentry1 " << dentry1.ShortDebugString();
    LOG(INFO) << "create dentry2 " << dentry2.ShortDebugString();

    EXPECT_CALL(*metaClient_, ListDentry(_, _, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::OK));

    ASSERT_FALSE(cleaner_->ScanRecycle());
}
}  // namespace metaserver
}  // namespace curvefs
