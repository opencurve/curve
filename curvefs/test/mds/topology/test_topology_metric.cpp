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
 * @Project: curvefs
 * @Date: 2021-11-04 15:03:28
 * @Author: chenwei
 */

#include <bvar/bvar.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "curvefs/src/mds/topology/topology_metric.h"
#include "curvefs/test/mds/mock/mock_topology.h"

namespace curvefs {
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
        topology_ = std::make_shared<TopologyImpl>(idGenerator_,
                                                   tokenGenerator_, storage_);
        testObj_ = std::make_shared<TopologyMetricService>(topology_);
    }

    void TearDown() {
        idGenerator_ = nullptr;
        tokenGenerator_ = nullptr;
        storage_ = nullptr;
        topology_ = nullptr;
        testObj_ = nullptr;
    }

    void PrepareAddPool(PoolIdType id = 0x11,
                        const std::string &name = "testPool",
                        const Pool::RedundanceAndPlaceMentPolicy &rap =
                            Pool::RedundanceAndPlaceMentPolicy(),
                        uint64_t createTime = 0x888) {
        Pool pool(id, name, rap, createTime);

        EXPECT_CALL(*storage_, StoragePool(_)).WillOnce(Return(true));

        TopoStatusCode ret = topology_->AddPool(pool);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret)
            << "should have PrepareAddPhysicalPool()";
    }

    void PrepareAddZone(ZoneIdType id = 0x21,
                        const std::string &name = "testZone",
                        PoolIdType poolId = 0x11) {
        Zone zone(id, name, poolId);
        EXPECT_CALL(*storage_, StorageZone(_)).WillOnce(Return(true));
        TopoStatusCode ret = topology_->AddZone(zone);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret)
            << "should have PrepareAddPhysicalPool()";
    }

    void PrepareAddServer(ServerIdType id = 0x31,
                          const std::string &hostName = "testServer",
                          const std::string &internalHostIp = "testInternalIp",
                          const std::string &externalHostIp = "testExternalIp",
                          ZoneIdType zoneId = 0x21, PoolIdType poolId = 0x11) {
        Server server(id, hostName, internalHostIp, 0, externalHostIp, 0,
                      zoneId, poolId);
        EXPECT_CALL(*storage_, StorageServer(_)).WillOnce(Return(true));
        TopoStatusCode ret = topology_->AddServer(server);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret)
            << "should have PrepareAddZone()";
    }

    void PrepareAddMetaServer(MetaServerIdType id = 0x41,
                              const std::string &hostname = "hostname",
                              const std::string &token = "testToken",
                              ServerIdType serverId = 0x31,
                              const std::string &hostIp = "testInternalIp",
                              uint32_t port = 0) {
        MetaServer cs(id, hostname, token, serverId, hostIp, port, "ip", 0);
        MetaServerSpace st;
        st.SetDiskThreshold(100 * 1024);
        st.SetDiskUsed(10 * 1024);
        st.SetMemoryUsed(20 * 1024);
        cs.SetMetaServerSpace(st);
        EXPECT_CALL(*storage_, StorageMetaServer(_)).WillOnce(Return(true));
        TopoStatusCode ret = topology_->AddMetaServer(cs);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret)
            << "should have PrepareAddServer()";
    }

    void PrepareAddCopySet(CopySetIdType copysetId, PoolIdType poolId,
                           const std::set<MetaServerIdType> &members) {
        CopySetInfo cs(poolId, copysetId);
        cs.SetCopySetMembers(members);
        EXPECT_CALL(*storage_, StorageCopySet(_)).WillOnce(Return(true));
        TopoStatusCode ret = topology_->AddCopySet(cs);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret)
            << "should have PrepareAddPool()";
    }

    void PrepareAddPartition(PartitionIdType partitionId,
                             CopySetIdType copysetId, PoolIdType poolId) {
        Partition partition;
        partition.SetPoolId(poolId);
        partition.SetCopySetId(copysetId);
        partition.SetPartitionId(partitionId);
        partition.SetInodeNum(10);
        partition.SetDentryNum(100);

        EXPECT_CALL(*storage_, StoragePartition(_)).WillOnce(Return(true));
        TopoStatusCode ret = topology_->AddPartition(partition);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret)
            << "should have PrepareAddPartition()";
    }

 protected:
    std::shared_ptr<MockIdGenerator> idGenerator_;
    std::shared_ptr<MockTokenGenerator> tokenGenerator_;
    std::shared_ptr<MockStorage> storage_;
    std::shared_ptr<Topology> topology_;
    std::shared_ptr<TopologyMetricService> testObj_;
};

TEST_F(TestTopologyMetric, TestUpdateTopologyMetricsOnePool) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", "127.0.0.1", 0x21, 0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", "127.0.0.1", 0x22, 0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", "127.0.0.1", 0x23, 0x11);
    PrepareAddMetaServer(0x41, "host1", "token1", 0x31, "127.0.0.1", 8888);
    PrepareAddMetaServer(0x42, "host2", "token2", 0x32, "127.0.0.1", 8889);
    PrepareAddMetaServer(0x43, "host3", "token3", 0x33, "127.0.0.1", 8890);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    PrepareAddPartition(0x61, copysetId, poolId);
    PrepareAddPartition(0x62, copysetId, poolId);
    PrepareAddPartition(0x63, copysetId, poolId);

    testObj_->UpdateTopologyMetrics();

    ASSERT_EQ(3, gMetaServerMetrics.size());

    ASSERT_EQ(2, gMetaServerMetrics[0x41]->scatterWidth.get_value());
    ASSERT_EQ(1, gMetaServerMetrics[0x41]->copysetNum.get_value());
    ASSERT_EQ(0, gMetaServerMetrics[0x41]->leaderNum.get_value());
    ASSERT_EQ(100 * 1024, gMetaServerMetrics[0x41]->diskCapacity.get_value());
    ASSERT_EQ(10 * 1024, gMetaServerMetrics[0x41]->diskUsed.get_value());
    ASSERT_EQ(20 * 1024, gMetaServerMetrics[0x41]->memoryUsed.get_value());
    ASSERT_EQ(3, gMetaServerMetrics[0x41]->partitionNum.get_value());

    ASSERT_EQ(2, gMetaServerMetrics[0x42]->scatterWidth.get_value());
    ASSERT_EQ(1, gMetaServerMetrics[0x42]->copysetNum.get_value());
    ASSERT_EQ(0, gMetaServerMetrics[0x42]->leaderNum.get_value());
    ASSERT_EQ(100 * 1024, gMetaServerMetrics[0x42]->diskCapacity.get_value());
    ASSERT_EQ(10 * 1024, gMetaServerMetrics[0x42]->diskUsed.get_value());
    ASSERT_EQ(20 * 1024, gMetaServerMetrics[0x42]->memoryUsed.get_value());
    ASSERT_EQ(3, gMetaServerMetrics[0x42]->partitionNum.get_value());

    ASSERT_EQ(2, gMetaServerMetrics[0x43]->scatterWidth.get_value());
    ASSERT_EQ(1, gMetaServerMetrics[0x43]->copysetNum.get_value());
    ASSERT_EQ(0, gMetaServerMetrics[0x43]->leaderNum.get_value());
    ASSERT_EQ(100 * 1024, gMetaServerMetrics[0x43]->diskCapacity.get_value());
    ASSERT_EQ(10 * 1024, gMetaServerMetrics[0x43]->diskUsed.get_value());
    ASSERT_EQ(20 * 1024, gMetaServerMetrics[0x43]->memoryUsed.get_value());
    ASSERT_EQ(3, gMetaServerMetrics[0x43]->partitionNum.get_value());

    ASSERT_EQ(1, gPoolMetrics.size());
    ASSERT_EQ(3, gPoolMetrics[poolId]->metaServerNum.get_value());
    ASSERT_EQ(1, gPoolMetrics[poolId]->copysetNum.get_value());
    ASSERT_EQ(100 * 1024 * 3, gPoolMetrics[poolId]->diskCapacity.get_value());
    ASSERT_EQ(10 * 1024 * 3, gPoolMetrics[poolId]->diskUsed.get_value());
    ASSERT_EQ(30, gPoolMetrics[poolId]->inodeNum.get_value());
    ASSERT_EQ(300, gPoolMetrics[poolId]->dentryNum.get_value());
    ASSERT_EQ(3, gPoolMetrics[poolId]->partitionNum.get_value());
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
