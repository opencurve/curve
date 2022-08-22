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
 * @Date: 2021-12-22 14:15:22
 * @Author: chenwei
 */
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/time.h>

#include "curvefs/src/mds/heartbeat/topo_updater.h"
#include "curvefs/test/mds/mock/mock_topology.h"
// #include "src/common/timeutility.h"

using ::curvefs::mds::topology::MockIdGenerator;
using ::curvefs::mds::topology::MockStorage;
using ::curvefs::mds::topology::MockTokenGenerator;
using ::curvefs::mds::topology::MockTopology;
using ::curvefs::mds::topology::TopoStatusCode;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArgPointee;

namespace curvefs {
namespace mds {
namespace heartbeat {
class TestTopoUpdater : public ::testing::Test {
 protected:
    TestTopoUpdater() {}
    ~TestTopoUpdater() {}

    void SetUp() override {
        topology_ = std::make_shared<MockTopology>(idGenerator_,
                                                   tokenGenerator_, storage_);
        updater_ = std::make_shared<TopoUpdater>(topology_);
    }

    void TearDown() override {}

 protected:
    std::shared_ptr<MockIdGenerator> idGenerator_;
    std::shared_ptr<MockTokenGenerator> tokenGenerator_;
    std::shared_ptr<MockStorage> storage_;
    std::shared_ptr<MockTopology> topology_;
    std::shared_ptr<TopoUpdater> updater_;
};

// partition not in topology, not in heartbeat
TEST_F(TestTopoUpdater, test_UpdatePartitionTopo_case1) {
    CopySetIdType copysetId = 1;

    std::list<::curvefs::mds::topology::Partition> topoPartitionList;
    EXPECT_CALL(*topology_, GetPartitionInfosInCopyset(_))
        .WillOnce(Return(topoPartitionList));

    std::list<::curvefs::mds::topology::Partition> partitionList;
    updater_->UpdatePartitionTopo(copysetId, partitionList);
}

// partition both in topology and in heartbeat
TEST_F(TestTopoUpdater, test_UpdatePartitionTopo_case2) {
    CopySetIdType copysetId = 1;

    ::curvefs::mds::topology::Partition partition1;
    partition1.SetStatus(PartitionStatus::DELETING);
    partition1.SetInodeNum(10);
    std::list<::curvefs::mds::topology::Partition> topoPartitionList;
    topoPartitionList.push_back(partition1);

    ::curvefs::mds::topology::Partition partition2;
    partition2.SetStatus(PartitionStatus::DELETING);
    partition2.SetInodeNum(5);
    std::list<::curvefs::mds::topology::Partition> partitionList;
    partitionList.push_back(partition2);

    {
        // update partition statistic
        EXPECT_CALL(*topology_, GetPartitionInfosInCopyset(_))
            .WillOnce(Return(topoPartitionList));

        EXPECT_CALL(*topology_, GetPartition(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(partition1), Return(true)));

        EXPECT_CALL(*topology_, UpdatePartitionStatistic(_, _))
            .WillOnce(Return(TopoStatusCode::TOPO_OK));

        updater_->UpdatePartitionTopo(copysetId, partitionList);
    }

    {
        // get from topo fail
        EXPECT_CALL(*topology_, GetPartitionInfosInCopyset(_))
            .WillOnce(Return(topoPartitionList));

        EXPECT_CALL(*topology_, GetPartition(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(partition1), Return(false)));

        updater_->UpdatePartitionTopo(copysetId, partitionList);
    }

    {
        // status is not available
        partition2.SetStatus(PartitionStatus::READWRITE);
        partitionList.clear();
        partitionList.push_back(partition2);
        EXPECT_CALL(*topology_, GetPartitionInfosInCopyset(_))
            .WillOnce(Return(topoPartitionList));

        EXPECT_CALL(*topology_, GetPartition(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(partition1), Return(true)));

        updater_->UpdatePartitionTopo(copysetId, partitionList);
    }

    {
        // update partition statistic, only next id not same
        partition1.SetStatus(PartitionStatus::READWRITE);
        partition1.SetInodeNum(10);
        partition1.SetIdNext(10);
        topoPartitionList.clear();
        topoPartitionList.push_back(partition1);

        partition2.SetStatus(PartitionStatus::READWRITE);
        partition2.SetInodeNum(10);
        partition2.SetIdNext(11);
        partitionList.clear();
        partitionList.push_back(partition2);

        EXPECT_CALL(*topology_, GetPartitionInfosInCopyset(_))
            .WillOnce(Return(topoPartitionList));

        EXPECT_CALL(*topology_, GetPartition(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(partition1), Return(true)));

        EXPECT_CALL(*topology_, UpdatePartitionStatistic(_, _))
            .WillOnce(Return(TopoStatusCode::TOPO_OK));

        updater_->UpdatePartitionTopo(copysetId, partitionList);
    }
}

// partition in topology, not in heartbeat
TEST_F(TestTopoUpdater, test_UpdatePartitionTopo_case3) {
    CopySetIdType copysetId = 1;

    ::curvefs::mds::topology::Partition partition;
    partition.SetStatus(PartitionStatus::DELETING);
    std::list<::curvefs::mds::topology::Partition> topoPartitionList;
    topoPartitionList.push_back(partition);
    std::list<::curvefs::mds::topology::Partition> partitionList;

    // delete partition in topo success
    EXPECT_CALL(*topology_, GetPartitionInfosInCopyset(_))
        .WillOnce(Return(topoPartitionList));

    EXPECT_CALL(*topology_, RemovePartition(_))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));

    updater_->UpdatePartitionTopo(copysetId, partitionList);

    // delete partition in topo fail
    EXPECT_CALL(*topology_, GetPartitionInfosInCopyset(_))
        .WillOnce(Return(topoPartitionList));

    EXPECT_CALL(*topology_, RemovePartition(_))
        .WillOnce(Return(TopoStatusCode::TOPO_INTERNAL_ERROR));

    updater_->UpdatePartitionTopo(copysetId, partitionList);
}

// partition not in topology, but in heartbeat
TEST_F(TestTopoUpdater, test_UpdatePartitionTopo_case4) {
    CopySetIdType copysetId = 1;

    ::curvefs::mds::topology::Partition partition;
    partition.SetStatus(PartitionStatus::DELETING);

    std::list<::curvefs::mds::topology::Partition> topoPartitionList;
    EXPECT_CALL(*topology_, GetPartitionInfosInCopyset(_))
        .WillOnce(Return(topoPartitionList));

    EXPECT_CALL(*topology_, GetPartition(_, _)).WillOnce(Return(false));

    std::list<::curvefs::mds::topology::Partition> partitionList;
    partitionList.push_back(partition);

    updater_->UpdatePartitionTopo(copysetId, partitionList);
}

TEST_F(TestTopoUpdater, test_PartitionStatusAvailable) {
    ASSERT_TRUE(updater_->CanPartitionStatusChange(PartitionStatus::READWRITE,
                                                   PartitionStatus::READWRITE));
    ASSERT_TRUE(updater_->CanPartitionStatusChange(PartitionStatus::READWRITE,
                                                    PartitionStatus::READONLY));
    ASSERT_TRUE(updater_->CanPartitionStatusChange(PartitionStatus::READWRITE,
                                                   PartitionStatus::DELETING));
    ASSERT_FALSE(updater_->CanPartitionStatusChange(
        PartitionStatus::READONLY, PartitionStatus::READWRITE));
    ASSERT_TRUE(updater_->CanPartitionStatusChange(PartitionStatus::READONLY,
                                                   PartitionStatus::READONLY));
    ASSERT_TRUE(updater_->CanPartitionStatusChange(PartitionStatus::READONLY,
                                                    PartitionStatus::DELETING));
    ASSERT_FALSE(updater_->CanPartitionStatusChange(
        PartitionStatus::DELETING, PartitionStatus::READWRITE));
    ASSERT_FALSE(updater_->CanPartitionStatusChange(PartitionStatus::DELETING,
                                                    PartitionStatus::READONLY));
    ASSERT_TRUE(updater_->CanPartitionStatusChange(PartitionStatus::DELETING,
                                                   PartitionStatus::DELETING));
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curvefs
