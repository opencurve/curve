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
 * Project: curve
 * Created Date: Tue Aug 31 19:41:52 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/copyset_reloader.h"

#include <brpc/server.h>
#include <gtest/gtest.h>

#include "curvefs/src/metaserver/copyset/copyset_node_manager.h"
#include "src/fs/ext4_filesystem_impl.h"
#include "src/fs/local_filesystem.h"
#include "test/fs/mock_local_filesystem.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

const char* kCopysetDataUri = "local://./runlog/copyset_reloader_test";

using ::curve::fs::Ext4FileSystemImpl;
using ::curve::fs::FileSystemType;
using ::curve::fs::LocalFileSystem;
using ::curve::fs::LocalFsFactory;
using ::curve::fs::MockLocalFileSystem;

using ::testing::_;
using ::testing::Return;

const int kTestPort = 29950;

class CopysetReloaderTest : public testing::Test {
 protected:
    void SetUp() override {
        fs_ = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
        ASSERT_NE(nullptr, fs_);

        copysetNodeOptions_.dataUri = kCopysetDataUri;
        copysetNodeOptions_.raftNodeOptions.log_uri = kCopysetDataUri;
        copysetNodeOptions_.raftNodeOptions.raft_meta_uri = kCopysetDataUri;
        copysetNodeOptions_.raftNodeOptions.snapshot_uri = kCopysetDataUri;

        copysetNodeOptions_.ip = "127.0.0.1";
        copysetNodeOptions_.port = 29950;

        copysetNodeOptions_.loadConcurrency = 10;
        copysetNodeOptions_.checkRetryTimes = 3;
        copysetNodeOptions_.finishLoadMargin = 1000;
        copysetNodeOptions_.localFileSystem = fs_.get();

        // disable raft snapshot
        copysetNodeOptions_.raftNodeOptions.snapshot_interval_s = -1;
        copysetNodeOptions_.raftNodeOptions.catchup_margin = 50;
    }

    void TearDown() override {
        ::system("rm -rf ./runlog/copyset_reloader_test");
    }

 protected:
    CopysetNodeOptions copysetNodeOptions_;
    CopysetNodeManager copysetNodeManager_;
    std::shared_ptr<LocalFileSystem> fs_;
};

TEST_F(CopysetReloaderTest, InitTest_LoadConcurrencyIsZero) {
    copysetNodeOptions_.loadConcurrency = 0;

    CopysetReloader reloader(&copysetNodeManager_);
    EXPECT_FALSE(reloader.Init(copysetNodeOptions_));
}

TEST_F(CopysetReloaderTest, InitTest_Success) {
    copysetNodeOptions_.loadConcurrency = 1;

    CopysetReloader reloader(&copysetNodeManager_);
    EXPECT_TRUE(reloader.Init(copysetNodeOptions_));
}

TEST_F(CopysetReloaderTest, ReloadCopysetsTest_DirNotExists) {
    std::unique_ptr<MockLocalFileSystem> mocklfs(new MockLocalFileSystem());
    copysetNodeOptions_.localFileSystem = mocklfs.get();

    CopysetReloader copysetReloader(&copysetNodeManager_);

    EXPECT_CALL(*mocklfs, DirExists(_))
        .WillOnce(Return(false));

    EXPECT_TRUE(copysetReloader.Init(copysetNodeOptions_));
    EXPECT_TRUE(copysetReloader.ReloadCopysets());
}

TEST_F(CopysetReloaderTest, ReloadCopysetsTest_ListDirFailed) {
    std::unique_ptr<MockLocalFileSystem> mocklfs(new MockLocalFileSystem());
    copysetNodeOptions_.localFileSystem = mocklfs.get();

    CopysetReloader copysetReloader(&copysetNodeManager_);

    EXPECT_CALL(*mocklfs, DirExists(_))
        .WillOnce(Return(true));

    EXPECT_CALL(*mocklfs, List(_, _))
        .WillOnce(Return(-1));

    EXPECT_TRUE(copysetReloader.Init(copysetNodeOptions_));
    EXPECT_FALSE(copysetReloader.ReloadCopysets());
}

TEST_F(CopysetReloaderTest, ReloadCopysetTest) {
    PoolId poolId = 1;
    CopysetId copysetId = 12345;
    braft::Configuration conf;

    butil::EndPoint listenAddr;
    ASSERT_EQ(
        0, butil::str2endpoint(("127.0.0.1:" + std::to_string(29950)).c_str(),
                               &listenAddr));
    brpc::Server server;
    copysetNodeManager_.AddService(&server, listenAddr);

    EXPECT_TRUE(copysetNodeManager_.Init(copysetNodeOptions_));
    EXPECT_TRUE(copysetNodeManager_.Start());

    for (int i = 0; i < 5; ++i) {
        EXPECT_TRUE(
            copysetNodeManager_.CreateCopysetNode(poolId, copysetId + i, conf));
    }

    std::vector<CopysetNode*> nodes;
    copysetNodeManager_.GetAllCopysets(&nodes);
    EXPECT_EQ(5, nodes.size());
    EXPECT_TRUE(copysetNodeManager_.Stop());

    nodes.clear();
    copysetNodeManager_.GetAllCopysets(&nodes);
    EXPECT_EQ(0, nodes.size());

    // re-start copyset node manger, this will trigger copysets reload
    LOG(INFO) << "re-start copyset node manager";
    EXPECT_TRUE(copysetNodeManager_.Start());
    nodes.clear();
    copysetNodeManager_.GetAllCopysets(&nodes);
    EXPECT_EQ(5, nodes.size());

    EXPECT_TRUE(copysetNodeManager_.Stop());
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
