/*
 * Project: curve
 * File Created: 2019-11-26
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */
#include <gtest/gtest.h>
#include "src/tools/status_tool.h"
#include "test/tools/mock_namespace_tool_core.h"
#include "test/tools/mock_copyset_check_core.h"
#include "test/tools/mock_mds_client.h"
#include "test/tools/mock_etcd_client.h"

using ::testing::_;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::An;
using curve::mds::topology::LogicalPoolType;
using curve::mds::topology::AllocateStatus;

DEFINE_uint64(rpcTimeout, 3000, "millisecond for rpc timeout");
DEFINE_uint64(rpcRetryTimes, 5, "rpc retry times");
DECLARE_bool(offline);
DECLARE_bool(unhealthy);

class StatusToolTest : public ::testing::Test {
 protected:
    StatusToolTest() {
        statistics1 = curve::tool::CopysetStatistics(2, 0);
        statistics2 = curve::tool::CopysetStatistics(10, 5);
    }
    void SetUp() {
        mdsClient_ = std::make_shared<curve::tool::MockMDSClient>();
        nameSpaceTool_ =
                std::make_shared<curve::tool::MockNameSpaceToolCore>();
        copysetCheck_ = std::make_shared<curve::tool::MockCopysetCheckCore>();
        etcdClient_ = std::make_shared<curve::tool::MockEtcdClient>();
    }

    void TearDown() {
        mdsClient_ = nullptr;
        nameSpaceTool_ = nullptr;
        copysetCheck_ = nullptr;
        etcdClient_ = nullptr;
    }

    void GetPhysicalPoolInfoForTest(PoolIdType id, PhysicalPoolInfo* pool) {
        pool->set_physicalpoolid(id);
        pool->set_physicalpoolname("testPool");
        pool->set_desc("physical pool for test");
    }

    void GetLogicalPoolForTest(PoolIdType id, LogicalPoolInfo *lpInfo) {
        lpInfo->set_logicalpoolid(id);
        lpInfo->set_logicalpoolname("defaultLogicalPool");
        lpInfo->set_physicalpoolid(1);
        lpInfo->set_type(LogicalPoolType::PAGEFILE);
        lpInfo->set_createtime(1574218021);
        lpInfo->set_redundanceandplacementpolicy(
            "{\"zoneNum\": 3, \"copysetNum\": 4000, \"replicaNum\": 3}");
        lpInfo->set_userpolicy("{\"policy\": 1}");
        lpInfo->set_allocatestatus(AllocateStatus::ALLOW);
    }

    void GetCsInfoForTest(curve::mds::topology::ChunkServerInfo *csInfo,
                            uint64_t csId, bool offline = false,
                            bool retired = false) {
        csInfo->set_chunkserverid(csId);
        csInfo->set_disktype("ssd");
        csInfo->set_hostip("127.0.0.1");
        csInfo->set_port(9190 + csId);
        if (retired) {
            csInfo->set_onlinestate(OnlineState::OFFLINE);
            csInfo->set_status(ChunkServerStatus::RETIRED);
        } else {
            if (offline) {
                csInfo->set_onlinestate(OnlineState::OFFLINE);
            } else {
                csInfo->set_onlinestate(OnlineState::ONLINE);
            }
            csInfo->set_status(ChunkServerStatus::READWRITE);
        }
        csInfo->set_diskstatus(DiskState::DISKNORMAL);
        csInfo->set_mountpoint("/test");
        csInfo->set_diskcapacity(1024);
        csInfo->set_diskused(512);
    }
    curve::tool::CopysetStatistics statistics1;
    curve::tool::CopysetStatistics statistics2;

    std::shared_ptr<curve::tool::MockMDSClient> mdsClient_;
    std::shared_ptr<curve::tool::MockNameSpaceToolCore> nameSpaceTool_;
    std::shared_ptr<curve::tool::MockCopysetCheckCore> copysetCheck_;
    std::shared_ptr<curve::tool::MockEtcdClient> etcdClient_;
};

TEST_F(StatusToolTest, SpaceCmd) {
    curve::tool::StatusTool statusTool(mdsClient_, etcdClient_,
                            nameSpaceTool_, copysetCheck_);
    LogicalPoolInfo lgPool;
    PhysicalPoolInfo phyPool;
    GetLogicalPoolForTest(1, &lgPool);
    GetPhysicalPoolInfoForTest(1, &phyPool);
    std::vector<LogicalPoolInfo> lgPools;
    lgPools.emplace_back(lgPool);
    std::vector<PhysicalPoolInfo> phyPools;
    phyPools.emplace_back(phyPool);

    // 1、正常情况
    EXPECT_CALL(*mdsClient_, ListPhysicalPoolsInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(phyPools),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, ListLogicalPoolsInPhysicalPool(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(lgPools),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, GetMetric(_, _))
        .Times(3)
        .WillOnce(DoAll(SetArgPointee<1>(340050401280000),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(58584427659264),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(29292213829632),
                        Return(0)));
    EXPECT_CALL(*nameSpaceTool_, GetAllocatedSize(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(14646106914816),
                        Return(0)));
    ASSERT_EQ(0, statusTool.RunCommand("space"));

    // 2、ListPhysicalPoolsInCluster失败的情况
    EXPECT_CALL(*mdsClient_, ListPhysicalPoolsInCluster(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool.RunCommand("space"));

    // 3、ListLogicalPoolsInPhysicalPool失败的情况
    EXPECT_CALL(*mdsClient_, ListPhysicalPoolsInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(phyPools),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, ListLogicalPoolsInPhysicalPool(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool.RunCommand("space"));

    // 4、获取metric失败的情况
    EXPECT_CALL(*mdsClient_, ListPhysicalPoolsInCluster(_))
        .Times(3)
        .WillRepeatedly(DoAll(SetArgPointee<0>(phyPools),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, ListLogicalPoolsInPhysicalPool(_, _))
        .Times(3)
        .WillRepeatedly(DoAll(SetArgPointee<1>(lgPools),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, GetMetric(_, _))
        .Times(6)
        .WillOnce(Return(-1))
        .WillOnce(DoAll(SetArgPointee<1>(340050401280000),
                        Return(0)))
        .WillOnce(Return(-1))
        .WillOnce(DoAll(SetArgPointee<1>(340050401280000),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(58584427659264),
                        Return(0)))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool.RunCommand("space"));
    ASSERT_EQ(-1, statusTool.RunCommand("space"));
    ASSERT_EQ(-1, statusTool.RunCommand("space"));

    // 5、获取RecyleBin大小失败的情况
    EXPECT_CALL(*mdsClient_, ListPhysicalPoolsInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(phyPools),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, ListLogicalPoolsInPhysicalPool(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(lgPools),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, GetMetric(_, _))
        .Times(3)
        .WillOnce(DoAll(SetArgPointee<1>(340050401280000),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(58584427659264),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(29292213829632),
                        Return(0)));
    EXPECT_CALL(*nameSpaceTool_, GetAllocatedSize(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool.RunCommand("space"));
}

TEST_F(StatusToolTest, ChunkServerCmd) {
    curve::tool::StatusTool statusTool(mdsClient_, etcdClient_,
                            nameSpaceTool_, copysetCheck_);
    std::vector<ChunkServerInfo> chunkservers;
    // 加入5个chunkserver，其中1个retired，1个offline
    ChunkServerInfo csInfo;
    GetCsInfoForTest(&csInfo, 1, false, true);
    chunkservers.emplace_back(csInfo);
    GetCsInfoForTest(&csInfo, 2, true);
    chunkservers.emplace_back(csInfo);
    for (uint64_t i = 3; i <= 5; ++i) {
        GetCsInfoForTest(&csInfo, i);
        chunkservers.emplace_back(csInfo);
    }

    // 正常情况，有一个chunkserver的UnhealthyRatio大于0
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(chunkservers),
                        Return(0)));
    EXPECT_CALL(*copysetCheck_, CheckCopysetsOnChunkServer(
                An<const ChunkServerIdType&>()))
        .Times(3)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*copysetCheck_, GetCopysetStatistics())
        .Times(3)
        .WillOnce(Return(statistics2))
        .WillRepeatedly(Return(statistics1));
    ASSERT_EQ(0, statusTool.RunCommand("chunkserver-list"));

    // 只显示offline的
    FLAGS_offline = true;
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(chunkservers),
                        Return(0)));
    ASSERT_EQ(0, statusTool.RunCommand("chunkserver-list"));

    // 只显示unhealthy ratio大于0的
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(chunkservers),
                        Return(0)));
    EXPECT_CALL(*copysetCheck_, CheckCopysetsOnChunkServer(
                An<const ChunkServerIdType&>()))
        .Times(3)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*copysetCheck_, GetCopysetStatistics())
        .Times(3)
        .WillOnce(Return(statistics2))
        .WillRepeatedly(Return(statistics1));
    FLAGS_offline = false;
    FLAGS_unhealthy = true;
    ASSERT_EQ(0, statusTool.RunCommand("chunkserver-list"));

    // list chunkserver失败
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool.RunCommand("chunkserver-list"));
}

TEST_F(StatusToolTest, StatusCmdCommon) {
    curve::tool::StatusTool statusTool(mdsClient_, etcdClient_,
                            nameSpaceTool_, copysetCheck_);
    LogicalPoolInfo lgPool;
    PhysicalPoolInfo phyPool;
    GetLogicalPoolForTest(1, &lgPool);
    GetPhysicalPoolInfoForTest(1, &phyPool);
    std::vector<LogicalPoolInfo> lgPools;
    lgPools.emplace_back(lgPool);
    std::vector<PhysicalPoolInfo> phyPools;
    phyPools.emplace_back(phyPool);
    std::string mdsAddr = "127.0.0.1:6666";
    std::vector<std::string> mdsAddrVec;
    mdsAddrVec.emplace_back(mdsAddr);
    mdsAddrVec.emplace_back("127.0.0.1:6667");
    mdsAddrVec.emplace_back("127.0.0.1:6668");
    // 加入5个chunkserver，其中1个retired，1个offline
    ChunkServerInfo csInfo;
    std::vector<ChunkServerInfo> chunkservers;
    for (uint64_t i = 1; i <= 3; ++i) {
        GetCsInfoForTest(&csInfo, i);
        chunkservers.emplace_back(csInfo);
    }

    // 正常情况
    // 1、设置cluster的输出
    EXPECT_CALL(*copysetCheck_, CheckCopysetsInCluster())
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*copysetCheck_, GetCopysetStatistics())
        .Times(1)
        .WillOnce(Return(statistics1));
    EXPECT_CALL(*mdsClient_, ListPhysicalPoolsInCluster(_))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<0>(phyPools),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, ListLogicalPoolsInPhysicalPool(_, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(lgPools),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, GetMetric(_, _))
        .Times(6)
        .WillOnce(DoAll(SetArgPointee<1>(340050401280000),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(58584427659264),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(58584427659264),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(340050401280000),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(58584427659264),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(58584427659264),
                        Return(0)));
    EXPECT_CALL(*nameSpaceTool_, GetAllocatedSize(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(14646106914816),
                        Return(0)));

    // 2、设置MDS status的输出
    EXPECT_CALL(*mdsClient_, GetCurrentMds())
        .Times(1)
        .WillOnce(Return(mdsAddr));
    EXPECT_CALL(*mdsClient_, GetMdsAddrVec())
        .Times(1)
        .WillOnce(ReturnRef(mdsAddrVec));

    // 3、设置etcd status的输出
    std::string leaderAddr = "127.0.0.1:2379";
    std::map<std::string, bool> onlineState = {{"127.0.0.1:2379", true},
                                               {"127.0.0.1:2381", false},
                                               {"127.0.0.1:2383", true}};
    EXPECT_CALL(*etcdClient_, GetEtcdClusterStatus(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(leaderAddr),
                        SetArgPointee<1>(onlineState),
                        Return(0)));

    // 4、设置chunkserver status的输出
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(chunkservers),
                        Return(0)));
    ASSERT_EQ(0, statusTool.RunCommand("status"));

    // 5、设置chunkserver status的输出
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(chunkservers),
                        Return(0)));
    ASSERT_EQ(0, statusTool.RunCommand("chunkserver-status"));
}

TEST_F(StatusToolTest, StatusCmdError) {
    curve::tool::StatusTool statusTool(mdsClient_, etcdClient_,
                            nameSpaceTool_, copysetCheck_);
    // 1、cluster unhealthy
    EXPECT_CALL(*copysetCheck_, CheckCopysetsInCluster())
        .Times(1)
        .WillOnce(Return(-1));
    EXPECT_CALL(*copysetCheck_, GetCopysetStatistics())
        .Times(1)
        .WillOnce(Return(statistics2));
    // 列出物理池失败
    EXPECT_CALL(*mdsClient_, ListPhysicalPoolsInCluster(_))
        .Times(1)
        .WillOnce(Return(-1));

    // 2、当前无mds可用
    std::vector<std::string> mdsAddrVec;
    EXPECT_CALL(*mdsClient_, GetCurrentMds())
        .Times(1)
        .WillOnce(Return(""));
    EXPECT_CALL(*mdsClient_, GetMdsAddrVec())
        .Times(1)
        .WillOnce(ReturnRef(mdsAddrVec));

    // 3、GetEtcdClusterStatus失败
    EXPECT_CALL(*etcdClient_, GetEtcdClusterStatus(_, _))
        .Times(1)
        .WillOnce(Return(-1));

    // 4、ListChunkServersInCluster失败
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool.RunCommand("status"));
}
