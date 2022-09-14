/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: Fri Jun 28 2019
 * Author: xuchaojie
 */

#include <bvar/bvar.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "src/mds/topology/topology_metric.h"
#include "test/mds/topology/mock_topology.h"
#include "test/mds/mock/mock_alloc_statistic.h"

namespace curve {
namespace mds {
namespace topology {

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;

class TestTopologyMetric : public ::testing::Test {
 public:
    TestTopologyMetric() {}

    void SetUp() {
        idGenerator_ = std::make_shared<MockIdGenerator>();
        tokenGenerator_ = std::make_shared<MockTokenGenerator>();
        storage_ = std::make_shared<MockStorage>();
        // 使用真实的topology
        topology_ = std::make_shared<TopologyImpl>(idGenerator_,
                                               tokenGenerator_,
                                               storage_);

        topologyStat_ = std::make_shared<MockTopologyStat>();
        allocStatistic_ = std::make_shared<MockAllocStatistic>();
        testObj_ = std::make_shared<TopologyMetricService>(
            topology_, topologyStat_, allocStatistic_);
    }

    void TearDown() {
        idGenerator_ = nullptr;
        tokenGenerator_ = nullptr;
        storage_ = nullptr;

        topology_ = nullptr;
        topologyStat_ = nullptr;
        allocStatistic_ = nullptr;

        testObj_ = nullptr;
    }

    void PrepareAddPoolset(PoolsetIdType pid = 0x61,
                           const std::string& name = "ssdPoolset1",
                           const std::string& type = "SSD",
                           const std::string& desc = "descPoolset") {
        Poolset poolset(pid, name, type, desc);
        EXPECT_CALL(*storage_, StoragePoolset(_))
            .WillOnce(Return(true));

        int ret = topology_->AddPoolset(poolset);
        ASSERT_EQ(kTopoErrCodeSuccess, ret);
    }

    void PrepareAddLogicalPool(PoolIdType id = 0x01,
            const std::string &name = "testLogicalPool",
            PoolIdType phyPoolId = 0x11,
            LogicalPoolType  type = PAGEFILE,
            const LogicalPool::RedundanceAndPlaceMentPolicy &rap =
                LogicalPool::RedundanceAndPlaceMentPolicy(),
            const LogicalPool::UserPolicy &policy = LogicalPool::UserPolicy(),
            uint64_t createTime = 0x888
            ) {
        LogicalPool pool(id,
                name,
                phyPoolId,
                type,
                rap,
                policy,
                createTime,
                true);

        EXPECT_CALL(*storage_, StorageLogicalPool(_))
            .WillOnce(Return(true));

        int ret = topology_->AddLogicalPool(pool);
        ASSERT_EQ(kTopoErrCodeSuccess, ret)
            << "should have PrepareAddPhysicalPool()";
    }


    void PrepareAddPhysicalPool(PoolIdType id = 0x11,
                 const std::string &name = "testPhysicalPool",
                 PoolsetIdType pid = 0x61,
                 const std::string &desc = "descPhysicalPool") {
        PhysicalPool pool(id,
                name,
                pid,
                desc);
        EXPECT_CALL(*storage_, StoragePhysicalPool(_))
            .WillOnce(Return(true));

        int ret = topology_->AddPhysicalPool(pool);
        ASSERT_EQ(kTopoErrCodeSuccess, ret);
    }

    void PrepareAddZone(ZoneIdType id = 0x21,
            const std::string &name = "testZone",
            PoolIdType physicalPoolId = 0x11,
            const std::string &desc = "descZone") {
        Zone zone(id, name, physicalPoolId, desc);
        EXPECT_CALL(*storage_, StorageZone(_))
            .WillOnce(Return(true));
        int ret = topology_->AddZone(zone);
        ASSERT_EQ(kTopoErrCodeSuccess, ret) <<
            "should have PrepareAddPhysicalPool()";
    }

    void PrepareAddServer(ServerIdType id = 0x31,
           const std::string &hostName = "testServer",
           const std::string &internalHostIp = "testInternalIp",
           const std::string &externalHostIp = "testExternalIp",
           ZoneIdType zoneId = 0x21,
           PoolIdType physicalPoolId = 0x11,
           const std::string &desc = "descServer") {
        Server server(id,
                hostName,
                internalHostIp,
                0,
                externalHostIp,
                0,
                zoneId,
                physicalPoolId,
                desc);
        EXPECT_CALL(*storage_, StorageServer(_))
            .WillOnce(Return(true));
        int ret = topology_->AddServer(server);
        ASSERT_EQ(kTopoErrCodeSuccess, ret) << "should have PrepareAddZone()";
    }

    void PrepareAddChunkServer(ChunkServerIdType id = 0x41,
                const std::string &token = "testToken",
                const std::string &diskType = "nvme",
                ServerIdType serverId = 0x31,
                const std::string &hostIp = "testInternalIp",
                uint32_t port = 0,
                const std::string &diskPath = "/") {
            ChunkServer cs(id,
                    token,
                    diskType,
                    serverId,
                    hostIp,
                    port,
                    diskPath);
            ChunkServerState st;
            st.SetDiskCapacity(100 * 1024);
            st.SetDiskUsed(10 * 1024);
            cs.SetChunkServerState(st);
            EXPECT_CALL(*storage_, StorageChunkServer(_))
                .WillOnce(Return(true));
        int ret = topology_->AddChunkServer(cs);
        ASSERT_EQ(kTopoErrCodeSuccess, ret) << "should have PrepareAddServer()";
    }

    void PrepareAddCopySet(CopySetIdType copysetId,
        PoolIdType logicalPoolId,
        const std::set<ChunkServerIdType> &members) {
        CopySetInfo cs(logicalPoolId,
            copysetId);
        cs.SetCopySetMembers(members);
        EXPECT_CALL(*storage_, StorageCopySet(_))
            .WillOnce(Return(true));
        int ret = topology_->AddCopySet(cs);
        ASSERT_EQ(kTopoErrCodeSuccess, ret)
            << "should have PrepareAddLogicalPool()";
    }

 protected:
    std::shared_ptr<MockIdGenerator> idGenerator_;
    std::shared_ptr<MockTokenGenerator> tokenGenerator_;
    std::shared_ptr<MockStorage> storage_;
    std::shared_ptr<MockAllocStatistic> allocStatistic_;
    std::shared_ptr<Topology> topology_;
    std::shared_ptr<MockTopologyStat> topologyStat_;
    std::shared_ptr<TopologyMetricService> testObj_;
};

TEST_F(TestTopologyMetric,  TestUpdateTopologyMetricsOneLogicalPool) {
    PoolsetIdType poolsetId = 0x61;
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPoolset(poolsetId);
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", "127.0.0.1", 0x21, 0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", "127.0.0.1", 0x22, 0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", "127.0.0.1", 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8888);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8888);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8888);

    LogicalPool::RedundanceAndPlaceMentPolicy rap;
    rap.pageFileRAP.replicaNum = 3;

    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId,
        PAGEFILE, rap);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, logicalPoolId, replicas);


    ChunkServerStat stat1;
    CopysetStat cstat1;
    stat1.leaderCount = 1;
    stat1.copysetCount = 1;
    stat1.readRate = 1;
    stat1.writeRate = 1;
    stat1.readIOPS = 1;
    stat1.writeIOPS = 1;
    stat1.chunkSizeUsedBytes = 1024;
    stat1.chunkSizeLeftBytes = 1024;
    stat1.chunkSizeTrashedBytes = 1024;

    cstat1.logicalPoolId = logicalPoolId;
    cstat1.copysetId = copysetId;
    cstat1.readRate = 1;
    cstat1.writeRate = 1;
    cstat1.readIOPS = 1;
    cstat1.writeIOPS = 1;
    stat1.copysetStats.push_back(cstat1);

    EXPECT_CALL(*topologyStat_, GetChunkServerStat(_, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(stat1),
            Return(true)));

    EXPECT_CALL(*allocStatistic_, GetAllocByLogicalPool(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(20 * 1024),
                Return(true)));

    testObj_->UpdateTopologyMetrics();

    ASSERT_EQ(3, gChunkServerMetrics.size());

    ASSERT_EQ(2, gChunkServerMetrics[0x41]->scatterWidth.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x41]->copysetNum.get_value());
    ASSERT_EQ(0, gChunkServerMetrics[0x41]->leaderNum.get_value());
    ASSERT_EQ(100 * 1024, gChunkServerMetrics[0x41]->diskCapacity.get_value());
    ASSERT_EQ(10 * 1024, gChunkServerMetrics[0x41]->diskUsed.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x41]->leaderCount.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x41]->copysetCount.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x41]->readRate.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x41]->writeRate.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x41]->readIOPS.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x41]->writeIOPS.get_value());
    ASSERT_EQ(1024, gChunkServerMetrics[0x41]->chunkSizeUsedBytes.get_value());
    ASSERT_EQ(1024, gChunkServerMetrics[0x41]->chunkSizeLeftBytes.get_value());
    ASSERT_EQ(1024,
        gChunkServerMetrics[0x41]->chunkSizeTrashedBytes.get_value());
    ASSERT_EQ(1024 * 3,
        gChunkServerMetrics[0x41]->chunkSizeTotalBytes.get_value());

    ASSERT_EQ(2, gChunkServerMetrics[0x42]->scatterWidth.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x42]->copysetNum.get_value());
    ASSERT_EQ(0, gChunkServerMetrics[0x42]->leaderNum.get_value());
    ASSERT_EQ(100 * 1024, gChunkServerMetrics[0x42]->diskCapacity.get_value());
    ASSERT_EQ(10 * 1024, gChunkServerMetrics[0x42]->diskUsed.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x42]->leaderCount.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x42]->copysetCount.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x42]->readRate.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x42]->writeRate.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x42]->readIOPS.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x42]->writeIOPS.get_value());
    ASSERT_EQ(1024, gChunkServerMetrics[0x42]->chunkSizeUsedBytes.get_value());
    ASSERT_EQ(1024, gChunkServerMetrics[0x42]->chunkSizeLeftBytes.get_value());
    ASSERT_EQ(1024,
        gChunkServerMetrics[0x42]->chunkSizeTrashedBytes.get_value());
    ASSERT_EQ(1024 * 3,
        gChunkServerMetrics[0x42]->chunkSizeTotalBytes.get_value());

    ASSERT_EQ(2, gChunkServerMetrics[0x43]->scatterWidth.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x43]->copysetNum.get_value());
    ASSERT_EQ(0, gChunkServerMetrics[0x43]->leaderNum.get_value());
    ASSERT_EQ(100 * 1024, gChunkServerMetrics[0x43]->diskCapacity.get_value());
    ASSERT_EQ(10 * 1024, gChunkServerMetrics[0x43]->diskUsed.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x43]->leaderCount.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x43]->copysetCount.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x43]->readRate.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x43]->writeRate.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x43]->readIOPS.get_value());
    ASSERT_EQ(1, gChunkServerMetrics[0x43]->writeIOPS.get_value());
    ASSERT_EQ(1024, gChunkServerMetrics[0x43]->chunkSizeUsedBytes.get_value());
    ASSERT_EQ(1024, gChunkServerMetrics[0x43]->chunkSizeLeftBytes.get_value());
    ASSERT_EQ(1024,
        gChunkServerMetrics[0x43]->chunkSizeTrashedBytes.get_value());
    ASSERT_EQ(1024 * 3,
        gChunkServerMetrics[0x43]->chunkSizeTotalBytes.get_value());

    ASSERT_EQ(1, gLogicalPoolMetrics.size());
    ASSERT_EQ(3, gLogicalPoolMetrics[logicalPoolId]->serverNum.get_value()); //NOLINT
    ASSERT_EQ(3, gLogicalPoolMetrics[logicalPoolId]->chunkServerNum.get_value()); //NOLINT
    ASSERT_EQ(1, gLogicalPoolMetrics[logicalPoolId]->copysetNum.get_value()); //NOLINT
    ASSERT_EQ(2, gLogicalPoolMetrics[logicalPoolId]->scatterWidthAvg.get_value()); //NOLINT
    ASSERT_EQ(0, gLogicalPoolMetrics[logicalPoolId]->scatterWidthVariance.get_value()); //NOLINT
    ASSERT_EQ(0, gLogicalPoolMetrics[logicalPoolId]->scatterWidthStandardDeviation.get_value()); //NOLINT
    ASSERT_EQ(0, gLogicalPoolMetrics[logicalPoolId]->scatterWidthRange.get_value()); //NOLINT
    ASSERT_EQ(2, gLogicalPoolMetrics[logicalPoolId]->scatterWidthMin.get_value()); //NOLINT
    ASSERT_EQ(2, gLogicalPoolMetrics[logicalPoolId]->scatterWidthMax.get_value()); //NOLINT
    ASSERT_EQ(1, gLogicalPoolMetrics[logicalPoolId]->copysetNumAvg.get_value()); //NOLINT
    ASSERT_EQ(0, gLogicalPoolMetrics[logicalPoolId]->copysetNumVariance.get_value()); //NOLINT
    ASSERT_EQ(0, gLogicalPoolMetrics[logicalPoolId]->copysetNumStandardDeviation.get_value()); //NOLINT
    ASSERT_EQ(0, gLogicalPoolMetrics[logicalPoolId]->copysetNumRange.get_value()); //NOLINT
    ASSERT_EQ(1, gLogicalPoolMetrics[logicalPoolId]->copysetNumMin.get_value()); //NOLINT
    ASSERT_EQ(1, gLogicalPoolMetrics[logicalPoolId]->copysetNumMax.get_value()); //NOLINT
    ASSERT_EQ(0, gLogicalPoolMetrics[logicalPoolId]->leaderNumAvg.get_value()); //NOLINT
    ASSERT_EQ(0, gLogicalPoolMetrics[logicalPoolId]->leaderNumVariance.get_value()); //NOLINT
    ASSERT_EQ(0, gLogicalPoolMetrics[logicalPoolId]->leaderNumStandardDeviation.get_value()); //NOLINT
    ASSERT_EQ(0, gLogicalPoolMetrics[logicalPoolId]->leaderNumRange.get_value()); //NOLINT
    ASSERT_EQ(0, gLogicalPoolMetrics[logicalPoolId]->leaderNumMin.get_value()); //NOLINT
    ASSERT_EQ(0, gLogicalPoolMetrics[logicalPoolId]->leaderNumMax.get_value()); //NOLINT
    ASSERT_EQ(100 * 1024 * 3, gLogicalPoolMetrics[logicalPoolId]->diskCapacity.get_value()); //NOLINT
    ASSERT_EQ(20 * 1024 * 3, gLogicalPoolMetrics[logicalPoolId]->diskAlloc.get_value()); //NOLINT
    ASSERT_EQ(10 * 1024 * 3, gLogicalPoolMetrics[logicalPoolId]->diskUsed.get_value()); //NOLINT

    ASSERT_EQ(1024 * 3,
        gLogicalPoolMetrics[logicalPoolId]->chunkSizeUsedBytes.get_value());
    ASSERT_EQ(1024 * 3,
        gLogicalPoolMetrics[logicalPoolId]->chunkSizeLeftBytes.get_value());
    ASSERT_EQ(1024 * 3,
        gLogicalPoolMetrics[logicalPoolId]->chunkSizeTrashedBytes.get_value());
    ASSERT_EQ(1024 * 9,
        gLogicalPoolMetrics[logicalPoolId]->chunkSizeTotalBytes.get_value());

    ASSERT_EQ(3, gLogicalPoolMetrics[logicalPoolId]->readIOPS.get_value());
    ASSERT_EQ(3, gLogicalPoolMetrics[logicalPoolId]->writeIOPS.get_value());
    ASSERT_EQ(3, gLogicalPoolMetrics[logicalPoolId]->readRate.get_value());
    ASSERT_EQ(3, gLogicalPoolMetrics[logicalPoolId]->writeRate.get_value());
    ASSERT_EQ(3, gClusterMetrics->readIOPS.get_value());
    ASSERT_EQ(3, gClusterMetrics->writeIOPS.get_value());
    ASSERT_EQ(3, gClusterMetrics->readRate.get_value());
    ASSERT_EQ(3, gClusterMetrics->writeRate.get_value());
    ASSERT_EQ(1, gClusterMetrics->logicalPoolNum.get_value());
    ASSERT_EQ(3, gClusterMetrics->serverNum.get_value());
    ASSERT_EQ(3, gClusterMetrics->chunkServerNum.get_value());
    ASSERT_EQ(1, gClusterMetrics->copysetNum.get_value());
}

TEST_F(TestTopologyMetric,  TestUpdateTopologyMetricsCleanRetired) {
    PrepareAddPoolset();
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", "127.0.0.1", 0x21, 0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", "127.0.0.1", 0x22, 0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", "127.0.0.1", 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8888);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8888);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8888);
    PrepareAddChunkServer(0x44, "token4", "nvme", 0x33, "127.0.0.1", 8888);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, logicalPoolId, replicas);


    ChunkServerStat stat1;
    CopysetStat cstat1;
    stat1.leaderCount = 1;
    stat1.copysetCount = 1;
    stat1.readRate = 1;
    stat1.writeRate = 1;
    stat1.readIOPS = 1;
    stat1.writeIOPS = 1;
    cstat1.logicalPoolId = logicalPoolId;
    cstat1.copysetId = copysetId;
    cstat1.readRate = 1;
    cstat1.writeRate = 1;
    cstat1.readIOPS = 1;
    cstat1.writeIOPS = 1;
    stat1.copysetStats.push_back(cstat1);

    EXPECT_CALL(*topologyStat_, GetChunkServerStat(_, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(stat1),
            Return(true)));

    testObj_->UpdateTopologyMetrics();

    ASSERT_EQ(4, gChunkServerMetrics.size());
    ASSERT_EQ(1, gLogicalPoolMetrics.size());

    topology_->UpdateChunkServerRwState(ChunkServerStatus::RETIRED, 0x41);

    std::set<ChunkServerIdType> replicas2{0x41, 0x42, 0x44};
    CopySetInfo cs(logicalPoolId, copysetId);
    cs.SetCopySetMembers(replicas2);
    topology_->UpdateCopySetTopo(cs);

    testObj_->UpdateTopologyMetrics();

    ASSERT_EQ(3, gChunkServerMetrics.size());
    ASSERT_EQ(1, gLogicalPoolMetrics.size());
}


}  // namespace topology
}  // namespace mds
}  // namespace curve
