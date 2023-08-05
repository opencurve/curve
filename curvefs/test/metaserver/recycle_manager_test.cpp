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
 * @Date: 2022-09-14 10:44:29
 * @Author: chenwei
 */
#include "curvefs/src/metaserver/recycle_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "curvefs/src/metaserver/storage/rocksdb_storage.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/mds/fsinfo_manager.h"
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

class RecycleManangeTest : public testing::Test {
 protected:
    void SetUp() override {
        dataDir_ = RandomStoragePath();
        StorageOptions options;
        options.dataDir = dataDir_;
        options.localFileSystem = localfs.get();
        kvStorage_ = std::make_shared<RocksDBStorage>(options);
        ASSERT_TRUE(kvStorage_->Open());

        mdsclient_ = std::make_shared<MockMdsClient>();
        FsInfoManager::GetInstance().SetMdsClient(mdsclient_);
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
    std::shared_ptr<MockMdsClient> mdsclient_;
};

TEST_F(RecycleManangeTest, test_empty_recycle) {
    RecycleManager* manager = &RecycleManager::GetInstance();
    RecycleManagerOption opt;
    opt.mdsClient = mdsclient_;
    opt.metaClient = std::make_shared<MockMetaServerClient>();
    opt.scanPeriodSec = 1;
    opt.scanLimit = 2;

    manager->Init(opt);

    // test run and stop
    LOG(INFO) << "test run";
    manager->Run();
    LOG(INFO) << "test run";
    manager->Run();
    LOG(INFO) << "test stop";
    manager->Stop();
    LOG(INFO) << "test stop";
    manager->Stop();
    LOG(INFO) << "test run";
    manager->Run();

    // test add recycle cleaner
    uint32_t partitionId1 = 1;
    uint32_t fsId = 2;
    PartitionInfo partitionInfo;
    partitionInfo.set_partitionid(partitionId1);
    partitionInfo.set_fsid(fsId);
    partitionInfo.set_start(0);
    partitionInfo.set_end(2000);
    std::shared_ptr<Partition> partition =
        std::make_shared<Partition>(partitionInfo, kvStorage_);
    std::shared_ptr<RecycleCleaner> cleaner =
        std::make_shared<RecycleCleaner>(partition);
    copyset::MockCopysetNode copysetNode;
    EXPECT_CALL(copysetNode, IsLeaderTerm())
        .WillOnce(Return(false))
        .WillRepeatedly(Return(true));

    FsInfo fsInfo;
    fsInfo.set_recycletimehour(10);
    EXPECT_CALL(*mdsclient_, GetFsInfo(fsId, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(fsInfo), Return(FSStatusCode::OK)));

    manager->Add(partitionId1, cleaner, &copysetNode);

    // create recycle dir and root dir
    InodeParam rootPram;
    rootPram.fsId = fsId;
    rootPram.parent = 0;
    rootPram.type = FsFileType::TYPE_DIRECTORY;
    ASSERT_EQ(partition->CreateRootInode(rootPram), MetaStatusCode::OK);
    InodeParam managePram;
    managePram.fsId = fsId;
    managePram.parent = ROOTINODEID;
    managePram.type = FsFileType::TYPE_DIRECTORY;
    Inode manageInode;
    ASSERT_EQ(partition->CreateManageInode(
                  managePram, ManageInodeType::TYPE_RECYCLE, &manageInode),
              MetaStatusCode::OK);
    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_inodeid(RECYCLEINODEID);
    dentry.set_name(RECYCLENAME);
    dentry.set_parentinodeid(ROOTINODEID);
    dentry.set_type(FsFileType::TYPE_DIRECTORY);
    dentry.set_txid(0);
    ASSERT_EQ(partition->CreateDentry(dentry), MetaStatusCode::OK);

    // create recycle time dir
    InodeParam param;
    Inode inode;
    param.fsId = fsId;
    param.parent = RECYCLEINODEID;
    param.type = FsFileType::TYPE_DIRECTORY;
    ASSERT_EQ(partition->CreateInode(param, &inode), MetaStatusCode::OK);

    Dentry dentry1;
    dentry1.set_name(GetRecycleTimeDirName());
    dentry1.set_fsid(fsId);
    dentry1.set_parentinodeid(RECYCLEINODEID);
    dentry1.set_inodeid(2001);
    dentry1.set_txid(0);
    dentry1.set_type(FsFileType::TYPE_DIRECTORY);
    ASSERT_EQ(partition->CreateDentry(dentry1), MetaStatusCode::OK);

    // wait clean recycle
    sleep(3);

    // test remove recycle cleaner
    manager->Remove(partitionId1);

    // test stop
    LOG(INFO) << "test stop";
    manager->Stop();
}

}  // namespace metaserver
}  // namespace curvefs
