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
 * Date: Wed Sep  1 20:36:15 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/copyset_node_manager.h"

#include <brpc/server.h>
#include <gtest/gtest.h>

#include "curvefs/src/metaserver/common/types.h"
#include "src/fs/local_filesystem.h"
#include "test/fs/mock_local_filesystem.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::curve::fs::FileSystemType;
using ::curve::fs::LocalFileSystem;
using ::curve::fs::LocalFsFactory;
using ::curve::fs::MockLocalFileSystem;

using ::testing::_;
using ::testing::Return;

const char* kCopysetUri = "local://./runlog/fs/copyset_node_manager";
const int kPort = 29920;
const char* kInitConf = "127.0.0.1:29920:0,127.0.0.1:29921:0,127.0.0.1:29922:0";
const PoolId kPoolId = 12345;
const CopysetId kCopysetId = 12345;

class CopysetNodeManagerTest : public testing::Test {
 protected:
    void SetUp() override {
        nodeManager_ = &CopysetNodeManager::GetInstance();
        fs_ = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");

        options_.ip = "127.0.0.1";
        options_.port = kPort;
        options_.raftNodeOptions.election_timeout_ms = 1000;
        options_.raftNodeOptions.snapshot_interval_s = -1;
        options_.dataUri = kCopysetUri;
        options_.raftNodeOptions.log_uri = kCopysetUri;
        options_.raftNodeOptions.raft_meta_uri = kCopysetUri;
        options_.raftNodeOptions.snapshot_uri = kCopysetUri;
        options_.loadConcurrency = 5;

        options_.localFileSystem = fs_.get();
    }

    void TearDown() override {
        nodeManager_->Stop();
        system("rm -rf ./runlog/fs/copyset_node_manager");
    }

 protected:
    std::shared_ptr<LocalFileSystem> fs_;
    CopysetNodeManager* nodeManager_;
    CopysetNodeOptions options_;
};

TEST_F(CopysetNodeManagerTest, CommonTest) {
    EXPECT_FALSE(nodeManager_->IsLoadFinished());

    std::vector<CopysetNode*> nodes;
    nodeManager_->GetAllCopysets(&nodes);
    EXPECT_TRUE(nodes.empty());

    braft::Configuration conf;
    EXPECT_FALSE(nodeManager_->CreateCopysetNode(1, 1, conf));

    EXPECT_TRUE(nodeManager_->Init(options_));
}

TEST_F(CopysetNodeManagerTest, StartTest_DataUriError) {
    auto errorOptions = options_;
    errorOptions.dataUri = "hello, world";

    EXPECT_TRUE(nodeManager_->Init(errorOptions));

    // invalid data path it is considered the directory does not exist
    EXPECT_TRUE(nodeManager_->Start());
    EXPECT_TRUE(nodeManager_->Stop());
}

TEST_F(CopysetNodeManagerTest, StartTest_ReloaderInitFailed) {
    auto errorOptions = options_;
    errorOptions.loadConcurrency = 0;

    EXPECT_TRUE(nodeManager_->Init(errorOptions));

    EXPECT_FALSE(nodeManager_->Start());

    // double start always return false
    EXPECT_FALSE(nodeManager_->Start());

    // stop return false if not started
    EXPECT_FALSE(nodeManager_->Stop());
}

TEST_F(CopysetNodeManagerTest, InitTest_InitMoreThanOnce) {
    EXPECT_TRUE(nodeManager_->Init(options_));
    EXPECT_TRUE(nodeManager_->Init(options_));
    EXPECT_TRUE(nodeManager_->Init(options_));
}

TEST_F(CopysetNodeManagerTest, CreateCopysetTest_DoesNotAddService) {
    EXPECT_TRUE(nodeManager_->Init(options_));
    EXPECT_TRUE(nodeManager_->Start());

    braft::Configuration conf;
    EXPECT_EQ(0, conf.parse_from(kInitConf));

    EXPECT_FALSE(nodeManager_->CreateCopysetNode(kPoolId, kCopysetId, conf));
    EXPECT_EQ(nullptr, nodeManager_->GetCopysetNode(kPoolId, kCopysetId));
}

TEST_F(CopysetNodeManagerTest, CreateCopysetTest_Common) {
    EXPECT_TRUE(nodeManager_->Init(options_));
    EXPECT_TRUE(nodeManager_->Start());

    braft::Configuration conf;
    EXPECT_EQ(0, conf.parse_from(kInitConf));

    brpc::Server server;
    butil::ip_t ip;
    EXPECT_EQ(0, butil::str2ip("127.0.0.1", &ip));
    EXPECT_NO_FATAL_FAILURE(
        nodeManager_->AddService(&server, butil::EndPoint(ip, kPort)));

    EXPECT_TRUE(nodeManager_->CreateCopysetNode(kPoolId, kCopysetId, conf));
    EXPECT_TRUE(nodeManager_->IsCopysetNodeExist(kPoolId, kCopysetId));

    // create same copyset will failed
    EXPECT_FALSE(nodeManager_->CreateCopysetNode(kPoolId, kCopysetId, conf));

    std::vector<CopysetNode*> nodes;
    nodeManager_->GetAllCopysets(&nodes);
    EXPECT_EQ(1, nodes.size());
    EXPECT_EQ(kPoolId, nodes[0]->GetPoolId());
    EXPECT_EQ(kCopysetId, nodes[0]->GetCopysetId());
}

TEST_F(CopysetNodeManagerTest, DeleteCopysetNodeTest_CopysetNodeNotExists) {
    EXPECT_TRUE(nodeManager_->Init(options_));
    EXPECT_FALSE(nodeManager_->DeleteCopysetNode(kPoolId, kCopysetId));
}

TEST_F(CopysetNodeManagerTest, DeleteCopysetNodeTest_Success) {
    EXPECT_TRUE(nodeManager_->Init(options_));
    EXPECT_TRUE(nodeManager_->Start());

    braft::Configuration conf;
    EXPECT_EQ(0, conf.parse_from(kInitConf));

    brpc::Server server;
    butil::ip_t ip;
    EXPECT_EQ(0, butil::str2ip("127.0.0.1", &ip));
    EXPECT_NO_FATAL_FAILURE(
        nodeManager_->AddService(&server, butil::EndPoint(ip, kPort)));

    EXPECT_TRUE(nodeManager_->CreateCopysetNode(kPoolId, kCopysetId, conf));
    EXPECT_TRUE(nodeManager_->IsCopysetNodeExist(kPoolId, kCopysetId));

    // create same copyset will failed
    EXPECT_FALSE(nodeManager_->CreateCopysetNode(kPoolId, kCopysetId, conf));

    std::vector<CopysetNode*> nodes;
    nodeManager_->GetAllCopysets(&nodes);
    EXPECT_EQ(1, nodes.size());
    EXPECT_EQ(kPoolId, nodes[0]->GetPoolId());
    EXPECT_EQ(kCopysetId, nodes[0]->GetCopysetId());

    EXPECT_TRUE(nodeManager_->DeleteCopysetNode(kPoolId, kCopysetId));
    nodeManager_->GetAllCopysets(&nodes);
    EXPECT_EQ(0, nodes.size());
}

TEST_F(CopysetNodeManagerTest,
       PurgeCopysetNodeTest_SuccessButRemoveDataFailed) {
    MockLocalFileSystem mockfs;
    CopysetTrashOptions trashoptions;
    trashoptions.localFileSystem = &mockfs;
    trashoptions.trashUri = "local:///mnt/data";
    CopysetTrash trash;

    EXPECT_TRUE(trash.Init(trashoptions));

    options_.trash = &trash;

    EXPECT_TRUE(nodeManager_->Init(options_));
    EXPECT_TRUE(nodeManager_->Start());

    braft::Configuration conf;
    EXPECT_EQ(0, conf.parse_from(kInitConf));

    brpc::Server server;
    butil::ip_t ip;
    EXPECT_EQ(0, butil::str2ip("127.0.0.1", &ip));
    EXPECT_NO_FATAL_FAILURE(
        nodeManager_->AddService(&server, butil::EndPoint(ip, kPort)));

    EXPECT_TRUE(nodeManager_->CreateCopysetNode(kPoolId, kCopysetId, conf));
    EXPECT_TRUE(nodeManager_->IsCopysetNodeExist(kPoolId, kCopysetId));

    // create same copyset will failed
    EXPECT_FALSE(nodeManager_->CreateCopysetNode(kPoolId, kCopysetId, conf));

    std::vector<CopysetNode*> nodes;
    nodeManager_->GetAllCopysets(&nodes);
    EXPECT_EQ(1, nodes.size());
    EXPECT_EQ(kPoolId, nodes[0]->GetPoolId());
    EXPECT_EQ(kCopysetId, nodes[0]->GetCopysetId());

    EXPECT_CALL(mockfs, DirExists(_))
        .WillOnce(Return(false));
    EXPECT_CALL(mockfs, Mkdir(_))
        .WillOnce(Return(-1));

    EXPECT_FALSE(nodeManager_->PurgeCopysetNode(kPoolId, kCopysetId));
    nodeManager_->GetAllCopysets(&nodes);
    EXPECT_EQ(0, nodes.size());
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
