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

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SaveArg;

namespace curvefs {
namespace metaserver {
class PartitionTest : public ::testing::Test {
 protected:
    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(PartitionTest, testInodeIdGen1) {
    PartitionInfo partitionInfo1;
    partitionInfo1.set_fsid(1);
    partitionInfo1.set_poolid(2);
    partitionInfo1.set_copysetid(3);
    partitionInfo1.set_partitionid(4);
    partitionInfo1.set_start(100);
    partitionInfo1.set_end(199);

    Partition partition1(partitionInfo1);

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

    Partition partition1(partitionInfo1);

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

    Partition partition1(partitionInfo1);

    ASSERT_EQ(partition1.GetNewInodeId(), UINT64_MAX);
    ASSERT_EQ(partition1.GetNewInodeId(), UINT64_MAX);
}

TEST_F(PartitionTest, testInodeIdGen4_NextId) {
    std::vector<std::pair<uint64_t, uint64_t>> testsets = {{0, 2},
                                                           {1, 2},
                                                           {2, 2},
                                                           {3, 3}};

    for (auto& t : testsets) {
        PartitionInfo partitionInfo1;
        partitionInfo1.set_fsid(1);
        partitionInfo1.set_poolid(2);
        partitionInfo1.set_copysetid(3);
        partitionInfo1.set_partitionid(4);
        partitionInfo1.set_start(t.first);
        partitionInfo1.set_end(199);

        Partition p(partitionInfo1);
        EXPECT_EQ(t.second, p.GetNewInodeId());
    }
}

TEST_F(PartitionTest, test1) {
    PartitionInfo partitionInfo1;
    partitionInfo1.set_fsid(1);
    partitionInfo1.set_poolid(2);
    partitionInfo1.set_copysetid(3);
    partitionInfo1.set_partitionid(4);
    partitionInfo1.set_start(100);
    partitionInfo1.set_end(199);

    Partition partition1(partitionInfo1);

    ASSERT_TRUE(partition1.IsDeletable());
    ASSERT_TRUE(partition1.IsInodeBelongs(1, 100));
    ASSERT_TRUE(partition1.IsInodeBelongs(1, 199));
    ASSERT_FALSE(partition1.IsInodeBelongs(2, 100));
    ASSERT_FALSE(partition1.IsInodeBelongs(2, 199));
    ASSERT_EQ(partition1.GetPartitionId(), 4);
    ASSERT_EQ(partition1.GetPartitionInfo().partitionid(), 4);
}
}  // namespace metaserver
}  // namespace curvefs
