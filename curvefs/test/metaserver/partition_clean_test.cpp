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
 * @Date: 2021-12-23 16:16:52
 * @Author: chenwei
 */
#include <gtest/gtest.h>
#include "curvefs/src/metaserver/partition_clean_manager.h"
#include "curvefs/test/metaserver/mock_metaserver_s3_adaptor.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_node.h"

using ::testing::Return;
using ::testing::_;
using ::testing::Invoke;

namespace curvefs {
namespace metaserver {
class PartitionCleanManagerTest : public testing::Test {
 protected:
    void SetUp() override {}
};
TEST_F(PartitionCleanManagerTest, test1) {
    ASSERT_TRUE(true);
    PartitionCleanManager* manager = &PartitionCleanManager::GetInstance();

    PartitionCleanOption option;
    option.scanPeriodSec = 1;
    option.inodeDeletePeriodMs = 500;
    std::shared_ptr<MockS3ClientAdaptor> s3Adaptor =
                            std::make_shared<MockS3ClientAdaptor>();
    option.s3Adaptor = s3Adaptor;
    manager->Init(option);
    manager->Run();
    uint32_t partitionId = 1;
    uint32_t fsId = 2;
    PartitionInfo partitionInfo;
    partitionInfo.set_partitionid(partitionId);
    partitionInfo.set_fsid(fsId);
    partitionInfo.set_start(0);
    partitionInfo.set_end(100);
    std::shared_ptr<Partition> partition =
                std::make_shared<Partition>(partitionInfo);
    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_parentinodeid(0);
    ASSERT_EQ(partition->CreateDentry(dentry, true), MetaStatusCode::OK);

    ASSERT_EQ(partition->CreateRootInode(fsId, 0, 0, 0), MetaStatusCode::OK);
    Inode inode1;
    ASSERT_EQ(partition->CreateInode(fsId, 0, 0, 0, 0, FsFileType::TYPE_S3,
                                "", 0, &inode1), MetaStatusCode::OK);
    ASSERT_EQ(partition->GetInodeNum(), 2);
    ASSERT_EQ(partition->GetDentryNum(), 1);
    Inode inode2;
    ASSERT_EQ(partition->GetInode(fsId, ROOTINODEID, &inode2),
                MetaStatusCode::OK);

    std::shared_ptr<PartitionCleaner> partitionCleaner =
                std::make_shared<PartitionCleaner>(partition);

    copyset::MockCopysetNode copysetNode;

    EXPECT_CALL(copysetNode, IsLeaderTerm())
        .WillOnce(Return(false))
        .WillRepeatedly(Return(true));

    EXPECT_CALL(copysetNode, Propose(_))
        .WillOnce(Invoke([partition, fsId](const braft::Task& task) {
            ASSERT_EQ(partition->DeleteInode(fsId, ROOTINODEID + 1),
                      MetaStatusCode::OK);
            LOG(INFO) << "Partition DeleteInode, fsId = " << fsId
                      << ", inodeId = " << ROOTINODEID + 1;
            task.done->Run();
        }))
        .WillOnce(Invoke([partition, fsId](const braft::Task& task) {
            ASSERT_EQ(partition->DeleteInode(fsId, ROOTINODEID),
                      MetaStatusCode::OK);
            LOG(INFO) << "Partition DeleteInode, fsId = " << fsId
                      << ", inodeId = " << ROOTINODEID;
            task.done->Run();
        }))
        .WillOnce(Invoke([partition](const braft::Task& task) {
            LOG(INFO) << "Partition deletePartition";
            task.done->Run();
        }));

    EXPECT_CALL(*s3Adaptor, Delete(_))
        .WillOnce(Return(0));

    manager->Add(partitionId, partitionCleaner, &copysetNode);

    sleep(4);
    manager->Fini();
    ASSERT_EQ(manager->GetCleanerCount(), 0);
}
}  // namespace metaserver
}  // namespace curvefs
