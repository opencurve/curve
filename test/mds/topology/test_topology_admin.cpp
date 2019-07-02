/*
 * Project: curve
 * Created Date: Wed Nov 14 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <memory>


#include "src/mds/topology/topology_admin.h"
#include "src/mds/common/mds_define.h"
#include "test/mds/topology/mock_topology.h"
#include "proto/nameserver2.pb.h"
#include "src/common/timeutility.h"

namespace curve {
namespace mds {
namespace topology {

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;


class TestTopoloyAdmin : public ::testing::Test {
 protected:
    TestTopoloyAdmin() {}
    virtual ~TestTopoloyAdmin() {}
    virtual void SetUp() {
        idGenerator_ = std::make_shared<MockIdGenerator>();
        tokenGenerator_ = std::make_shared<MockTokenGenerator>();
        storage_ = std::make_shared<MockStorage>();
        topology_ = std::make_shared<TopologyImpl>(idGenerator_,
                                               tokenGenerator_,
                                               storage_);
        testObj_ = std::make_shared<TopologyAdminImpl>(topology_);
    }

    virtual void TearDown() {
        idGenerator_ = nullptr;
        tokenGenerator_ = nullptr;
        storage_ = nullptr;
        topology_ = nullptr;
        testObj_ = nullptr;
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
                 const std::string &desc = "descPhysicalPool") {
        PhysicalPool pool(id,
                name,
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
    std::shared_ptr<Topology> topology_;
    std::shared_ptr<TopologyAdminImpl> testObj_;
};


TEST_F(TestTopoloyAdmin, Test_AllocateChunkRandomInSingleLogicalPool_success) {
    std::vector<CopysetIdInfo> infos;

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
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId,
        PAGEFILE);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, logicalPoolId, replicas);


    bool ret =
        testObj_->AllocateChunkRandomInSingleLogicalPool(INODE_PAGEFILE,
            1,
            1024,
            &infos);

    ASSERT_TRUE(ret);

    ASSERT_EQ(1, infos.size());
    ASSERT_EQ(logicalPoolId, infos[0].logicalPoolId);
    ASSERT_EQ(copysetId, infos[0].copySetId);
}

TEST_F(TestTopoloyAdmin,
    Test_AllocateChunkRandomInSingleLogicalPool_logicalPoolNotFound) {
    std::vector<CopysetIdInfo> infos;
    bool ret =
        testObj_->AllocateChunkRandomInSingleLogicalPool(INODE_PAGEFILE,
            1,
            1024,
            &infos);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopoloyAdmin,
    Test_AllocateChunkRoundRobinInSingleLogicalPool_success) {
    std::vector<CopysetIdInfo> infos;

    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", "127.0.0.1", 0x21, 0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", "127.0.0.1", 0x22, 0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", "127.0.0.1", 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId,
        PAGEFILE);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(0x51, logicalPoolId, replicas);
    PrepareAddCopySet(0x52, logicalPoolId, replicas);
    PrepareAddCopySet(0x53, logicalPoolId, replicas);
    PrepareAddCopySet(0x54, logicalPoolId, replicas);
    PrepareAddCopySet(0x55, logicalPoolId, replicas);


    bool ret =
        testObj_->AllocateChunkRoundRobinInSingleLogicalPool(INODE_PAGEFILE,
            3,
            1024,
            &infos);

    ASSERT_TRUE(ret);

    ASSERT_EQ(3, infos.size());
    ASSERT_EQ(logicalPoolId, infos[0].logicalPoolId);
    ASSERT_EQ(logicalPoolId, infos[1].logicalPoolId);
    ASSERT_EQ(logicalPoolId, infos[2].logicalPoolId);

    // second time
    std::vector<CopysetIdInfo> infos2;
    ret =
        testObj_->AllocateChunkRoundRobinInSingleLogicalPool(INODE_PAGEFILE,
            3,
            1024,
            &infos2);

    ASSERT_TRUE(ret);

    ASSERT_EQ(3, infos2.size());
    ASSERT_EQ(logicalPoolId, infos2[0].logicalPoolId);
    ASSERT_EQ(logicalPoolId, infos2[1].logicalPoolId);
    ASSERT_EQ(logicalPoolId, infos2[2].logicalPoolId);

    if (0x51 == infos[0].copySetId) {
        ASSERT_EQ(0x52, infos[1].copySetId);
        ASSERT_EQ(0x53, infos[2].copySetId);
        ASSERT_EQ(0x54, infos2[0].copySetId);
        ASSERT_EQ(0x55, infos2[1].copySetId);
        ASSERT_EQ(0x51, infos2[2].copySetId);
    } else if (0x52 == infos[0].copySetId) {
        ASSERT_EQ(0x53, infos[1].copySetId);
        ASSERT_EQ(0x54, infos[2].copySetId);
        ASSERT_EQ(0x55, infos2[0].copySetId);
        ASSERT_EQ(0x51, infos2[1].copySetId);
        ASSERT_EQ(0x52, infos2[2].copySetId);
    } else if (0x53 == infos[0].copySetId) {
        ASSERT_EQ(0x54, infos[1].copySetId);
        ASSERT_EQ(0x55, infos[2].copySetId);
        ASSERT_EQ(0x51, infos2[0].copySetId);
        ASSERT_EQ(0x52, infos2[1].copySetId);
        ASSERT_EQ(0x53, infos2[2].copySetId);
    } else if (0x54 == infos[0].copySetId) {
        ASSERT_EQ(0x55, infos[1].copySetId);
        ASSERT_EQ(0x51, infos[2].copySetId);
        ASSERT_EQ(0x52, infos2[0].copySetId);
        ASSERT_EQ(0x53, infos2[1].copySetId);
        ASSERT_EQ(0x54, infos2[2].copySetId);
    } else if (0x55 == infos[0].copySetId) {
        ASSERT_EQ(0x51, infos[1].copySetId);
        ASSERT_EQ(0x52, infos[2].copySetId);
        ASSERT_EQ(0x53, infos2[0].copySetId);
        ASSERT_EQ(0x54, infos2[1].copySetId);
        ASSERT_EQ(0x55, infos2[2].copySetId);
    } else {
        FAIL();
    }
}

TEST_F(TestTopoloyAdmin,
    Test_AllocateChunkRoundRobinInSingleLogicalPool_logicalPoolNotFound) {
    std::vector<CopysetIdInfo> infos;
    bool ret =
        testObj_->AllocateChunkRoundRobinInSingleLogicalPool(INODE_PAGEFILE,
            1,
            1024,
            &infos);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopoloyAdmin,
    Test_AllocateChunkRoundRobinInSingleLogicalPool_copysetEmpty) {
    std::vector<CopysetIdInfo> infos;
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddLogicalPool(logicalPoolId);
    bool ret =
        testObj_->AllocateChunkRoundRobinInSingleLogicalPool(INODE_PAGEFILE,
            1,
            1024,
            &infos);

    ASSERT_FALSE(ret);
}

TEST(TestAllocateChunkPolicy, TestAllocateChunkRandomInSingleLogicalPoolPoc) {
    // 2000个copyset分配100000次，每次分配64个chunk
    std::vector<CopySetIdType> copySetIds;
    std::map<CopySetIdType, int> copySetMap;
    for (int i = 0; i < 2000; i++) {
        copySetIds.push_back(i);
        copySetMap.emplace(i, 0);
    }

    for (int i = 0; i < 100000; i++) {
        int chunkNumber = 64;
        std::vector<CopysetIdInfo> infos;
        bool ret =
            AllocateChunkPolicy::AllocateChunkRandomInSingleLogicalPool(
            copySetIds,
            1,
            chunkNumber,
            &infos);
        ASSERT_TRUE(ret);
        ASSERT_EQ(chunkNumber, infos.size());
        for (int j = 0; j < chunkNumber; j++) {
            copySetMap[infos[j].copySetId]++;
        }
    }
    int minCount = copySetMap[0];
    int maxCount = copySetMap[0];
    for (auto &pair : copySetMap) {
        if (pair.second > maxCount) {
            maxCount = pair.second;
        }
        if (pair.second < minCount) {
            minCount = pair.second;
        }
    }
    int avg = 100000 * 64 / 2000;
    double minPercent = static_cast<double>(avg - minCount) / avg;
    double maxPercent = static_cast<double>(maxCount - avg) / avg;
    LOG(INFO) << "AllocateChunkRandomInSingleLogicalPool poc"
              <<", minCount = " << minCount
              <<", maxCount = " << maxCount
              << ", avg = " << avg
              << ", minPercent = " << minPercent
              << ", maxPercent = " << maxPercent;

    ASSERT_TRUE(minPercent < 0.1);
    ASSERT_TRUE(maxPercent < 0.1);
}

TEST(TestAllocateChunkPolicy, TestAllocateChunkRandomInSingleLogicalPoolTps) {
    // 2000个copyset分配100000次，每次分配64个chunk
    std::vector<CopySetIdType> copySetIds;
    for (int i = 0; i < 2000; i++) {
        copySetIds.push_back(i);
    }


    uint64_t startime = curve::common::TimeUtility::GetTimeofDayUs();
    for (int i = 0; i < 100000; i++) {
        int chunkNumber = 64;
        std::vector<CopysetIdInfo> infos;
        AllocateChunkPolicy::AllocateChunkRandomInSingleLogicalPool(
        copySetIds,
        1,
        chunkNumber,
        &infos);
    }
    uint64_t stoptime = curve::common::TimeUtility::GetTimeofDayUs();

    double usetime = stoptime - startime;
    double tps = 1000000.0 * 100000.0/usetime;

    std::cout << "TestAllocateChunkRandomInSingleLogicalPool, TPS = "
              << tps
              << " * 64 chunk per second.";
}

TEST(TestAllocateChunkPolicy,
    TestAllocateChunkRoundRobinInSingleLogicalPoolSuccess) {
    std::vector<CopySetIdType> copySetIds;
    std::map<CopySetIdType, int> copySetMap;
    for (int i = 0; i < 20; i++) {
        copySetIds.push_back(i);
    }
    uint32_t nextIndex = 15;
    int chunkNumber = 10;
    std::vector<CopysetIdInfo> infos;
    bool ret =
        AllocateChunkPolicy::AllocateChunkRoundRobinInSingleLogicalPool(
        copySetIds,
        1,
        &nextIndex,
        chunkNumber,
        &infos);
    ASSERT_TRUE(ret);
    ASSERT_EQ(5, nextIndex);
    ASSERT_EQ(chunkNumber, infos.size());
    ASSERT_EQ(15, infos[0].copySetId);
    ASSERT_EQ(16, infos[1].copySetId);
    ASSERT_EQ(17, infos[2].copySetId);
    ASSERT_EQ(18, infos[3].copySetId);
    ASSERT_EQ(19, infos[4].copySetId);
    ASSERT_EQ(0, infos[5].copySetId);
    ASSERT_EQ(1, infos[6].copySetId);
    ASSERT_EQ(2, infos[7].copySetId);
    ASSERT_EQ(3, infos[8].copySetId);
    ASSERT_EQ(4, infos[9].copySetId);
}

TEST(TestAllocateChunkPolicy,
    TestAllocateChunkRoundRobinInSingleLogicalPoolEmpty) {
    std::vector<CopySetIdType> copySetIds;
    std::map<CopySetIdType, int> copySetMap;
    uint32_t nextIndex = 15;
    int chunkNumber = 10;
    std::vector<CopysetIdInfo> infos;
    bool ret =
        AllocateChunkPolicy::AllocateChunkRoundRobinInSingleLogicalPool(
        copySetIds,
        1,
        &nextIndex,
        chunkNumber,
        &infos);
    ASSERT_FALSE(ret);
    ASSERT_EQ(15, nextIndex);
    ASSERT_EQ(0, infos.size());
}

TEST(TestAllocateChunkPolicy,
    TestChooseSingleLogicalPoolByWeightPoc) {
    std::map<PoolIdType, double> poolWeightMap;
    std::map<PoolIdType, int> poolMap;
    for (int i = 0; i < 5; i++) {
        poolWeightMap.emplace(i, i);
        poolMap.emplace(i, 0);
    }

    for (int i = 0; i < 100000; i++) {
        PoolIdType pid;
        AllocateChunkPolicy::ChooseSingleLogicalPoolByWeight(
            poolWeightMap, &pid);
        poolMap[pid]++;
    }

    ASSERT_EQ(0, poolMap[0]);
    ASSERT_TRUE(poolMap[0] < poolMap[1]);
    ASSERT_TRUE(poolMap[1] < poolMap[2]);
    ASSERT_TRUE(poolMap[2] < poolMap[3]);
    ASSERT_TRUE(poolMap[3] < poolMap[4]);
    // 5个池大概分布因该是0, 10000，20000，30000，40000
    LOG(INFO) << "pool0 : " << poolMap[0] << std::endl
              << "pool1 : " << poolMap[1] << std::endl
              << "pool2 : " << poolMap[2] << std::endl
              << "pool3 : " << poolMap[3] << std::endl
              << "pool4 : " << poolMap[4] << std::endl;
}

TEST(TestAllocateChunkPolicy,
    TestChooseSingleLogicalPoolByWeightPoc2) {
    std::map<PoolIdType, double> poolMap;
    poolMap[0] = 100000;
    poolMap[1] = 90000;
    poolMap[2] = 80000;
    poolMap[3] = 70000;
    poolMap[4] = 60000;

    for (int i = 0; i < 100000; i++) {
        PoolIdType pid;
        AllocateChunkPolicy::ChooseSingleLogicalPoolByWeight(
            poolMap, &pid);
        poolMap[pid] -= 1;
    }

    // 测试是否能逐渐拉平pool之间差距
    LOG(INFO) << "pool0 : " << poolMap[0] << std::endl
              << "pool1 : " << poolMap[1] << std::endl
              << "pool2 : " << poolMap[2] << std::endl
              << "pool3 : " << poolMap[3] << std::endl
              << "pool4 : " << poolMap[4] << std::endl;
}


}  // namespace topology
}  // namespace mds
}  // namespace curve
