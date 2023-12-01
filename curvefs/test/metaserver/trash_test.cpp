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
 * @Date: 2021-09-01
 * @Author: xuchaojie
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "curvefs/src/metaserver/trash_manager.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"
#include "curvefs/test/metaserver/storage/utils.h"
#include "src/fs/ext4_filesystem_impl.h"
#include "curvefs/test/client/rpcclient/mock_mds_client.h"
#include "curvefs/test/metaserver/mock_metaserver_s3_adaptor.h"
#include "src/common/timeutility.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_node.h"

using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::StrEq;
using ::testing::Invoke;

namespace curvefs {
namespace metaserver {

DECLARE_uint32(trash_scanPeriodSec);
DECLARE_uint32(trash_expiredAfterSec);

namespace {
auto localfs = curve::fs::Ext4FileSystemImpl::getInstance();
}

DECLARE_uint32(trash_expiredAfterSec);
DECLARE_uint32(trash_scanPeriodSec);

using ::curvefs::client::rpcclient::MockMdsClient;
using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::RandomStoragePath;
using ::curvefs::metaserver::storage::RocksDBStorage;
using ::curvefs::metaserver::storage::StorageOptions;
using ::curvefs::metaserver::copyset::MockCopysetNode;

DECLARE_uint32(trash_expiredAfterSec);
DECLARE_uint32(trash_scanPeriodSec);

class TestTrash : public ::testing::Test {
 protected:
    void SetUp() override {
        dataDir_ = RandomStoragePath();
        StorageOptions options;
        options.dataDir = dataDir_;
        options.localFileSystem = localfs.get();
        kvStorage_ = std::make_shared<RocksDBStorage>(options);
        ASSERT_TRUE(kvStorage_->Open());

        auto nameGenerator = std::make_shared<NameGenerator>(1);
        inodeStorage_ =
            std::make_shared<InodeStorage>(kvStorage_, nameGenerator, 0);
        trashManager_ = std::make_shared<TrashManager>();
        logIndex_ = 0;
        copysetNode_ = std::make_shared<MockCopysetNode>();
        mdsClient_ = std::make_shared<MockMdsClient>();
        s3Adaptor_ = std::make_shared<MockS3ClientAdaptor>();
    }

    void TearDown() override {
        trashManager_->Fini();
        inodeStorage_ = nullptr;
        trashManager_ = nullptr;
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
        inode.set_type(FsFileType::TYPE_S3);
        return inode;
    }

    Inode GenInodeHasChunks(uint32_t fsId, uint64_t inodeId) {
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
        inode.set_type(FsFileType::TYPE_S3);

        S3ChunkInfoList s3ChunkInfoList;
        inode.mutable_s3chunkinfomap()->insert({0, s3ChunkInfoList});
        return inode;
    }

 protected:
    std::string dataDir_;
    std::shared_ptr<KVStorage> kvStorage_;
    std::shared_ptr<InodeStorage> inodeStorage_;
    std::shared_ptr<TrashManager> trashManager_;
    std::shared_ptr<MockCopysetNode> copysetNode_;
    std::shared_ptr<MockMdsClient> mdsClient_;
    std::shared_ptr<MockS3ClientAdaptor> s3Adaptor_;
    int64_t logIndex_;
};

TEST_F(TestTrash, testAdd3ItemAndDelete) {
    TrashOption option;
    option.mdsClient = mdsClient_;
    option.s3Adaptor = s3Adaptor_;
    FLAGS_trash_scanPeriodSec = 1;
    FLAGS_trash_expiredAfterSec = 4;
    trashManager_->Init(option);
    trashManager_->Run();
    auto trash1 = std::make_shared<TrashImpl>(inodeStorage_, 1, 1, 1, 1);
    auto trash2 = std::make_shared<TrashImpl>(inodeStorage_, 2, 1, 2, 2);
    trashManager_->Add(1, trash1);
    trashManager_->Add(2, trash2);
    trash1->SetCopysetNode(copysetNode_);
    trash2->SetCopysetNode(copysetNode_);

    inodeStorage_->Insert(GenInodeHasChunks(1, 1), logIndex_++);
    inodeStorage_->Insert(GenInodeHasChunks(1, 2), logIndex_++);
    inodeStorage_->Insert(GenInodeHasChunks(2, 1), logIndex_++);
    ASSERT_EQ(inodeStorage_->Size(), 3);

    EXPECT_CALL(*copysetNode_, IsLeaderTerm())
        .WillRepeatedly(Return(true));
    FsInfo fsInfo;
    fsInfo.set_fsid(1);
    fsInfo.set_recycletimehour(0);
    EXPECT_CALL(*mdsClient_, GetFsInfo(1, _))
        .WillOnce(DoAll(SetArgPointee<1>(fsInfo), Return(FSStatusCode::OK)));
    fsInfo.set_fsid(2);
    EXPECT_CALL(*mdsClient_, GetFsInfo(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(fsInfo), Return(FSStatusCode::OK)));
    EXPECT_CALL(*s3Adaptor_, GetS3ClientAdaptorOption(_))
        .Times(3);
    EXPECT_CALL(*s3Adaptor_, Reinit(_, _, _, _, _))
        .Times(3);
    EXPECT_CALL(*s3Adaptor_, Delete(_))
        .Times(3)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*copysetNode_, Propose(_))
        .WillOnce(Invoke([&](const braft::Task& task) {
            ASSERT_EQ(inodeStorage_->Delete(Key4Inode(1, 1), logIndex_++),
                      MetaStatusCode::OK);
            LOG(INFO) << "trash deleteInode 1:1";
            task.done->Run();
        }))
        .WillOnce(Invoke([&](const braft::Task& task) {
            ASSERT_EQ(inodeStorage_->Delete(Key4Inode(1, 2), logIndex_++),
                      MetaStatusCode::OK);
            LOG(INFO) << "trash deleteInode 1:2";
            task.done->Run();
        }))
        .WillOnce(Invoke([&](const braft::Task& task) {
            ASSERT_EQ(inodeStorage_->Delete(Key4Inode(2, 1), logIndex_++),
                      MetaStatusCode::OK);
            LOG(INFO) << "trash deleteInode 2:1";
            task.done->Run();
        }));

    uint64_t dtime = curve::common::TimeUtility::GetTimeofDaySec();
    trash1->Add(1, dtime - 6, false);
    trash1->Add(2, dtime - 2, false);
    trash2->Add(1, dtime, false);

    std::this_thread::sleep_for(std::chrono::seconds(5));

    ASSERT_EQ(0, trashManager_->Size());
    ASSERT_EQ(inodeStorage_->Size(), 0);
}

}  // namespace metaserver
}  // namespace curvefs
