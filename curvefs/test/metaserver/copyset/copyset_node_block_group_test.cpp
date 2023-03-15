/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Date: Mon Apr 24 14:30:20 CST 2023
 * Author: lixiaocui
 */

#include <gtest/gtest.h>

#include "curvefs/src/metaserver/copyset/copyset_node.h"
#include "curvefs/src/metaserver/copyset/copyset_node_manager.h"
#include "curvefs/test/metaserver/mock/mock_partition.h"
#include "curvefs/test/metaserver/mock/mock_metastore.h"
#include "src/common/uuid.h"
#include "test/fs/mock_local_filesystem.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::curve::common::UUIDGenerator;
using ::curve::fs::MockLocalFileSystem;
using ::curvefs::metaserver::mock::MockPartition;
using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;

class CopysetNodeBlockGroupTest : public testing::Test {
 protected:
    void SetUp() override {
        dataPath_ = "./runlog/" + UUIDGenerator{}.GenerateUUID();
        trashPath_ = dataPath_ + "_trash";
        mockfs_ = absl::make_unique<MockLocalFileSystem>();
        nodeManager_ = &CopysetNodeManager::GetInstance();
        mockMetaStore_ = absl::make_unique<mock::MockMetaStore>();

        poolId_ = time(nullptr) % 12345;
        copysetId_ = time(nullptr) % reinterpret_cast<uint64_t>(this);

        options_.ip = "127.0.0.1";
        options_.port = 6666;

        options_.dataUri = "local://" + dataPath_;
        options_.raftNodeOptions.log_uri  = "local://" + dataPath_;
        options_.raftNodeOptions.raft_meta_uri = "local://" + dataPath_;
        options_.raftNodeOptions.snapshot_uri = "local://" + dataPath_;

        // disable raft snapshot
        options_.raftNodeOptions.snapshot_interval_s = -1;

        options_.localFileSystem = mockfs_.get();

        options_.trashOptions.trashUri = "local://" + trashPath_;
        options_.storageOptions.type = "memory";

        butil::ip_t ip;
        ASSERT_EQ(0, butil::str2ip(options_.ip.c_str(), &ip));
        butil::EndPoint listenAddr(ip, 6666);

        ASSERT_TRUE(nodeManager_->Init(options_));
    }

    void TearDown() {
        system(std::string("rm -rf " + dataPath_).c_str());
    }

 protected:
    PoolId poolId_;
    CopysetId copysetId_;
    std::string dataPath_;
    std::string trashPath_;

    CopysetNodeOptions options_;
    CopysetNodeManager *nodeManager_;

    std::unique_ptr<MockLocalFileSystem> mockfs_;
    std::unique_ptr<mock::MockMetaStore> mockMetaStore_;
};

TEST_F(CopysetNodeBlockGroupTest, Test_AggregateBlockStatInfo) {
    auto partition = std::make_shared<MockPartition>();
    std::map<uint32_t, BlockGroupStatInfo> blockStatInfoMap;
    uint32_t blockGroupNum = 0;
    uint32_t fsId = 1;
    uint32_t partitionId = 1;

    auto copyset = nodeManager_->GetCopysetNode(poolId_, copysetId_);

    // get fail
    {
        EXPECT_CALL(*partition, GetFsId()).WillOnce(Return(fsId));
        EXPECT_CALL(*partition, GeAllBlockGroup(_))
            .WillOnce(Return(MetaStatusCode::PARAM_ERROR));
        EXPECT_CALL(*partition, GetPartitionId()).WillOnce(Return(partitionId));
        ASSERT_FALSE(copyset->AggregateBlockStatInfo(
            partition, &blockStatInfoMap, &blockGroupNum));
        ASSERT_EQ(0, blockGroupNum);
    }

    // get success
    {
        std::vector<DeallocatableBlockGroup> in;
        DeallocatableBlockGroup bg;
        bg.set_blockgroupoffset(0);
        bg.set_deallocatablesize(1024);
        bg.add_inodeidlist(1);
        in.emplace_back(bg);

        EXPECT_CALL(*partition, GetFsId()).WillOnce(Return(fsId));
        EXPECT_CALL(*partition, GeAllBlockGroup(_))
            .WillOnce(DoAll(SetArgPointee<0>(in), Return(MetaStatusCode::OK)));
        ASSERT_TRUE(copyset->AggregateBlockStatInfo(
            partition, &blockStatInfoMap, &blockGroupNum));
        ASSERT_EQ(1, blockGroupNum);
        ASSERT_EQ(1, blockStatInfoMap.size());
        auto &out = blockStatInfoMap[fsId];
        ASSERT_EQ(fsId, out.fsid());
        ASSERT_EQ(1, out.deallocatableblockgroups_size());
    }
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
