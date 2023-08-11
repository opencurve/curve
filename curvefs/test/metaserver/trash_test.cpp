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

using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::StrEq;

namespace curvefs {
namespace metaserver {

namespace {
auto localfs = curve::fs::Ext4FileSystemImpl::getInstance();
}

using ::curvefs::client::rpcclient::MockMdsClient;
using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::RandomStoragePath;
using ::curvefs::metaserver::storage::RocksDBStorage;
using ::curvefs::metaserver::storage::StorageOptions;

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
    }

    void TearDown() override {
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
    int64_t logIndex_;
};

TEST_F(TestTrash, testAdd3ItemAndDelete) {
    TrashOption option;
    option.scanPeriodSec = 1;
    option.expiredAfterSec = 1;
    option.mdsClient = std::make_shared<MockMdsClient>();
    option.s3Adaptor = std::make_shared<MockS3ClientAdaptor>();
    trashManager_->Init(option);
    trashManager_->Run();
    auto trash1 = std::make_shared<TrashImpl>(inodeStorage_);
    auto trash2 = std::make_shared<TrashImpl>(inodeStorage_);
    trashManager_->Add(1, trash1);
    trashManager_->Add(2, trash2);

    inodeStorage_->Insert(GenInodeHasChunks(1, 1), logIndex_++);
    inodeStorage_->Insert(GenInodeHasChunks(1, 2), logIndex_++);
    inodeStorage_->Insert(GenInodeHasChunks(2, 1), logIndex_++);

    ASSERT_EQ(inodeStorage_->Size(), 3);

    trash1->Add(1, 1, 0);
    trash1->Add(1, 2, 0);
    trash2->Add(2, 1, 0);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::list<TrashItem> list;

    trashManager_->ListItems(&list);

    ASSERT_EQ(0, list.size());
    ASSERT_EQ(inodeStorage_->Size(), 0);

    trashManager_->Fini();
}

TEST_F(TestTrash, testAdd3ItemAndNoDelete) {
    TrashOption option;
    option.scanPeriodSec = 1;
    option.expiredAfterSec = 1;
    option.mdsClient = std::make_shared<MockMdsClient>();
    option.s3Adaptor = std::make_shared<MockS3ClientAdaptor>();
    trashManager_->Init(option);
    trashManager_->Run();

    auto trash1 = std::make_shared<TrashImpl>(inodeStorage_);
    trashManager_->Add(1, trash1);

    inodeStorage_->Insert(GenInode(1, 1), logIndex_++);
    inodeStorage_->Insert(GenInode(1, 2), logIndex_++);
    inodeStorage_->Insert(GenInode(2, 1), logIndex_++);
    ASSERT_EQ(inodeStorage_->Size(), 3);
    trash1->Add(1, 1, 0);
    trash1->Add(1, 2, 0);
    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::list<TrashItem> list;

    trashManager_->ListItems(&list);
    ASSERT_EQ(0, list.size());
    ASSERT_EQ(inodeStorage_->Size(), 3);
    trashManager_->Fini();
}

}  // namespace metaserver
}  // namespace curvefs
