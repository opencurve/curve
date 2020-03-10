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
#include "test/tools/mock_version_tool.h"
#include "test/tools/mock_metric_client.h"

using ::testing::_;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::An;
using curve::mds::topology::LogicalPoolType;
using curve::mds::topology::AllocateStatus;

DECLARE_bool(offline);
DECLARE_bool(unhealthy);
DECLARE_bool(checkHealth);
DECLARE_bool(checkCSAlive);

namespace curve {
namespace tool {

class StatusToolTest : public ::testing::Test {
 protected:
    StatusToolTest() {
        statistics1 = CopysetStatistics(2, 0);
        statistics2 = CopysetStatistics(10, 5);
    }
    void SetUp() {
        mdsClient_ = std::make_shared<MockMDSClient>();
        nameSpaceTool_ =
                std::make_shared<MockNameSpaceToolCore>();
        copysetCheck_ = std::make_shared<MockCopysetCheckCore>();
        etcdClient_ = std::make_shared<MockEtcdClient>();
        versionTool_ = std::make_shared<MockVersionTool>();
        metricClient_ = std::make_shared<MockMetricClient>();
    }

    void TearDown() {
        mdsClient_ = nullptr;
        nameSpaceTool_ = nullptr;
        copysetCheck_ = nullptr;
        etcdClient_ = nullptr;
        versionTool_ = nullptr;
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
    CopysetStatistics statistics1;
    CopysetStatistics statistics2;

    std::shared_ptr<MockMDSClient> mdsClient_;
    std::shared_ptr<MockNameSpaceToolCore> nameSpaceTool_;
    std::shared_ptr<MockCopysetCheckCore> copysetCheck_;
    std::shared_ptr<MockEtcdClient> etcdClient_;
    std::shared_ptr<MockVersionTool> versionTool_;
    std::shared_ptr<MockMetricClient> metricClient_;
};

TEST_F(StatusToolTest, InitAndSupportCommand) {
    StatusTool statusTool(mdsClient_, etcdClient_,
                          nameSpaceTool_, copysetCheck_,
                          versionTool_, metricClient_);
    ASSERT_TRUE(statusTool.SupportCommand("status"));
    ASSERT_TRUE(statusTool.SupportCommand("space"));
    ASSERT_TRUE(statusTool.SupportCommand("mds-status"));
    ASSERT_TRUE(statusTool.SupportCommand("chunkserver-status"));
    ASSERT_TRUE(statusTool.SupportCommand("chunkserver-list"));
    ASSERT_TRUE(statusTool.SupportCommand("etcd-status"));
    ASSERT_FALSE(statusTool.SupportCommand("none"));
}

TEST_F(StatusToolTest, InitFail) {
    StatusTool statusTool1(mdsClient_, etcdClient_,
                           nameSpaceTool_, copysetCheck_,
                           versionTool_, metricClient_);
    // 1、status命令需要所有的init
    EXPECT_CALL(*mdsClient_, Init(_, _))
        .Times(5)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*nameSpaceTool_, Init(_))
        .Times(4)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*copysetCheck_, Init(_))
        .Times(3)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*versionTool_, Init(_))
        .Times(2)
        .WillOnce(Return(-1))
        .WillOnce(Return(0));
    EXPECT_CALL(*etcdClient_, Init(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool1.RunCommand("status"));
    ASSERT_EQ(-1, statusTool1.RunCommand("status"));
    ASSERT_EQ(-1, statusTool1.RunCommand("status"));
    ASSERT_EQ(-1, statusTool1.RunCommand("status"));
    ASSERT_EQ(-1, statusTool1.RunCommand("status"));

    // 2、etcd-status命令只需要初始化etcdClinet
    StatusTool statusTool2(mdsClient_, etcdClient_,
                           nameSpaceTool_, copysetCheck_,
                           versionTool_, metricClient_);
    EXPECT_CALL(*etcdClient_, Init(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool2.RunCommand("etcd-status"));

    // 3、space和其他命令不需要初始化etcdClient
    StatusTool statusTool3(mdsClient_, etcdClient_,
                           nameSpaceTool_, copysetCheck_,
                           versionTool_, metricClient_);
    EXPECT_CALL(*mdsClient_, Init(_, _))
        .Times(4)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*nameSpaceTool_, Init(_))
        .Times(3)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*copysetCheck_, Init(_))
        .Times(2)
        .WillOnce(Return(-1))
        .WillOnce(Return(0));
    EXPECT_CALL(*versionTool_, Init(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool3.RunCommand("space"));
    ASSERT_EQ(-1, statusTool3.RunCommand("chunkserver-list"));
    ASSERT_EQ(-1, statusTool3.RunCommand("chunkserver-status"));
    ASSERT_EQ(-1, statusTool3.RunCommand("client-status"));
}

TEST_F(StatusToolTest, SpaceCmd) {
    StatusTool statusTool(mdsClient_, etcdClient_,
                          nameSpaceTool_, copysetCheck_,
                          versionTool_, metricClient_);
    statusTool.PrintHelp("space");
    statusTool.PrintHelp("123");
    LogicalPoolInfo lgPool;
    PhysicalPoolInfo phyPool;
    GetLogicalPoolForTest(1, &lgPool);
    GetPhysicalPoolInfoForTest(1, &phyPool);
    std::vector<LogicalPoolInfo> lgPools;
    lgPools.emplace_back(lgPool);
    std::vector<PhysicalPoolInfo> phyPools;
    phyPools.emplace_back(phyPool);

    // 设置Init的期望
    EXPECT_CALL(*mdsClient_, Init(_, _))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*nameSpaceTool_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*copysetCheck_, Init(_))
        .Times(1)
        .WillOnce(Return(0));

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
    ASSERT_EQ(-1, statusTool.RunCommand("123"));

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
    StatusTool statusTool(mdsClient_, etcdClient_,
                          nameSpaceTool_, copysetCheck_,
                          versionTool_, metricClient_);
    statusTool.PrintHelp("chunkserver-list");
    std::vector<ChunkServerInfo> chunkservers;
    // 加入5个chunkserver，2个offline
    ChunkServerInfo csInfo;
    for (uint64_t i = 1; i <= 5; ++i) {
        GetCsInfoForTest(&csInfo, i, i <= 2);
        chunkservers.emplace_back(csInfo);
    }
    // 设置Init的期望
    EXPECT_CALL(*mdsClient_, Init(_, _))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*nameSpaceTool_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*copysetCheck_, Init(_))
        .Times(1)
        .WillOnce(Return(0));

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

    // FLAGS_checkCSAlive为true的时候，会发送rpc检查chunkserver在线状态
    FLAGS_checkHealth = false;
    FLAGS_checkCSAlive = true;
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(chunkservers),
                        Return(0)));
    EXPECT_CALL(*copysetCheck_, CheckChunkServerOnline(_))
        .Times(5)
        .WillOnce(Return(false))
        .WillRepeatedly(Return(true));
    ASSERT_EQ(0, statusTool.RunCommand("chunkserver-list"));
}

TEST_F(StatusToolTest, StatusCmdCommon) {
    StatusTool statusTool(mdsClient_, etcdClient_,
                          nameSpaceTool_, copysetCheck_,
                          versionTool_, metricClient_);
    statusTool.PrintHelp("status");
    statusTool.PrintHelp("chunkserver-status");
    statusTool.PrintHelp("mds-status");
    statusTool.PrintHelp("etcd-status");
    LogicalPoolInfo lgPool;
    PhysicalPoolInfo phyPool;
    GetLogicalPoolForTest(1, &lgPool);
    GetPhysicalPoolInfoForTest(1, &phyPool);
    std::vector<LogicalPoolInfo> lgPools;
    lgPools.emplace_back(lgPool);
    std::vector<PhysicalPoolInfo> phyPools;
    phyPools.emplace_back(phyPool);
    std::string mdsAddr = "127.0.0.1:6666";
    std::map<std::string, bool> mdsOnlineStatus = {{"127.0.0.1:6666", true},
                                                   {"127.0.0.1:6667", true},
                                                   {"127.0.0.1:6668", true}};
    VersionMapType versionMap = {{"0.0.1", {"127.0.0.1:8001"}},
                                 {"0.0.2", {"127.0.0.1:8002"}},
                                 {"0.0.3", {"127.0.0.1:8003"}}};
    std::vector<std::string> offlineList = {"127.0.0.1:8004",
                                            "127.0.0.1:8005"};
    std::string leaderAddr = "127.0.0.1:2379";
    std::map<std::string, bool> onlineState = {{"127.0.0.1:2379", true},
                                               {"127.0.0.1:2381", true},
                                               {"127.0.0.1:2383", true}};

    ChunkServerInfo csInfo;
    std::vector<ChunkServerInfo> chunkservers;
    for (uint64_t i = 1; i <= 3; ++i) {
        GetCsInfoForTest(&csInfo, i, i == 1);
        chunkservers.emplace_back(csInfo);
    }

    // 设置Init的期望
    EXPECT_CALL(*mdsClient_, Init(_, _))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*nameSpaceTool_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*copysetCheck_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*versionTool_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*etcdClient_, Init(_))
        .Times(1)
        .WillOnce(Return(0));

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
        .Times(3)
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

    // 设置client status的输出
    EXPECT_CALL(*versionTool_, GetClientVersion(_, _))
        .WillOnce(DoAll(SetArgPointee<0>(versionMap),
                        SetArgPointee<1>(offlineList),
                        Return(0)));

    // 2、设置MDS status的输出
    EXPECT_CALL(*mdsClient_, GetCurrentMds())
        .Times(2)
        .WillRepeatedly(Return(mdsAddr));
    EXPECT_CALL(*mdsClient_, GetMdsOnlineStatus(_))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<0>(mdsOnlineStatus),
                        Return(0)));
    EXPECT_CALL(*versionTool_, GetAndCheckMdsVersion(_, _))
        .WillOnce(DoAll(SetArgPointee<0>("0.0.1"),
                        Return(0)));

    // 3、设置etcd status的输出
    EXPECT_CALL(*etcdClient_, GetEtcdClusterStatus(_, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<0>(leaderAddr),
                        SetArgPointee<1>(onlineState),
                        Return(0)));

    // 4、设置chunkserver status的输出
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(chunkservers),
                        Return(0)));
    EXPECT_CALL(*versionTool_, GetAndCheckChunkServerVersion(_, _))
        .WillOnce(DoAll(SetArgPointee<0>("0.0.1"),
                        Return(0)));
    EXPECT_CALL(*metricClient_, GetMetricUint(_, _, _))
        .Times(3)
        .WillRepeatedly(DoAll(SetArgPointee<2>(1000),
                        Return(0)));
    EXPECT_CALL(*copysetCheck_, CheckChunkServerOnline(_))
        .Times(3)
        .WillRepeatedly(Return(true));
    ASSERT_EQ(0, statusTool.RunCommand("status"));

    // 5、设置chunkserver status的输出
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(chunkservers),
                        Return(0)));
    EXPECT_CALL(*versionTool_, GetAndCheckChunkServerVersion(_, _))
        .WillOnce(DoAll(SetArgPointee<0>("0.0.1"),
                  Return(0)));
    EXPECT_CALL(*copysetCheck_, CheckChunkServerOnline(_))
        .Times(3)
        .WillRepeatedly(Return(true));
    ASSERT_EQ(0, statusTool.RunCommand("chunkserver-status"));

    // 6、设置mds status的输出
    EXPECT_CALL(*mdsClient_, GetCurrentMds())
        .Times(1)
        .WillOnce(Return(mdsAddr));
    EXPECT_CALL(*mdsClient_, GetMdsOnlineStatus(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(mdsOnlineStatus),
                        Return(0)));
    EXPECT_CALL(*versionTool_, GetAndCheckMdsVersion(_, _))
        .WillOnce(DoAll(SetArgPointee<0>("0.0.1"),
                  Return(0)));
    ASSERT_EQ(0, statusTool.RunCommand("mds-status"));

    // 7、设置etcd status的输出
    EXPECT_CALL(*etcdClient_, GetEtcdClusterStatus(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(leaderAddr),
                        SetArgPointee<1>(onlineState),
                        Return(0)));
    ASSERT_EQ(0, statusTool.RunCommand("etcd-status"));
}

TEST_F(StatusToolTest, StatusCmdError) {
    StatusTool statusTool(mdsClient_, etcdClient_,
                          nameSpaceTool_, copysetCheck_,
                          versionTool_, metricClient_);

    // 设置Init的期望
    EXPECT_CALL(*mdsClient_, Init(_, _))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*nameSpaceTool_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*copysetCheck_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*versionTool_, Init(_))
        .Times(1)
        .WillOnce(Return(0));

    // 1、cluster unhealthy
    EXPECT_CALL(*copysetCheck_, CheckCopysetsInCluster())
        .Times(1)
        .WillOnce(Return(-1));
    EXPECT_CALL(*copysetCheck_, GetCopysetStatistics())
        .Times(1)
        .WillOnce(Return(statistics2));
    // 列出物理池失败
    EXPECT_CALL(*mdsClient_, ListPhysicalPoolsInCluster(_))
        .Times(2)
        .WillRepeatedly(Return(-1));

    // 获取client version失败
    EXPECT_CALL(*versionTool_, GetClientVersion(_, _))
        .WillOnce(Return(-1));

    // 2、当前无mds可用
    std::vector<std::string> failedList = {"127.0.0.1:6666", "127.0.0.1:6667"};
    EXPECT_CALL(*versionTool_, GetAndCheckMdsVersion(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(failedList),
                  Return(0)));
    std::map<std::string, bool> mdsOnlineStatus = {{"127.0.0.1:6666", false},
                                                   {"127.0.0.1:6667", false}};
    EXPECT_CALL(*mdsClient_, GetCurrentMds())
        .Times(1)
        .WillOnce(Return(""));
    EXPECT_CALL(*mdsClient_, GetMdsOnlineStatus(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(mdsOnlineStatus),
                        Return(0)));

    // 3、GetEtcdClusterStatus失败
    EXPECT_CALL(*etcdClient_, GetEtcdClusterStatus(_, _))
        .Times(1)
        .WillOnce(Return(-1));

    // 4、获取chunkserver version失败并ListChunkServersInCluster失败
    EXPECT_CALL(*versionTool_, GetAndCheckChunkServerVersion(_, _))
        .WillOnce(Return(-1));
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool.RunCommand("status"));

    // 获取mds在线状态失败
    EXPECT_CALL(*mdsClient_, GetCurrentMds())
        .Times(1)
        .WillOnce(Return(""));
    EXPECT_CALL(*mdsClient_, GetMdsOnlineStatus(_))
        .Times(1)
        .WillOnce(Return(-1));
    // 获取mdsversion失败
    EXPECT_CALL(*versionTool_, GetAndCheckMdsVersion(_, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool.RunCommand("mds-status"));

    // 个别chunkserver获取version失败
    EXPECT_CALL(*versionTool_, GetAndCheckChunkServerVersion(_, _))
        .WillOnce(DoAll(SetArgPointee<0>("0.0.1"),
                  SetArgPointee<1>(failedList),
                  Return(0)));
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool.RunCommand("chunkserver-status"));
}

TEST_F(StatusToolTest, IsClusterHeatlhy) {
    StatusTool statusTool(mdsClient_, etcdClient_,
                          nameSpaceTool_, copysetCheck_,
                          versionTool_, metricClient_);
    std::string leader = "127.0.0.1:8001";
    std::map<std::string, bool> onlineStatus = {{"127.0.0.1:8001", true},
                                                {"127.0.0.1:8002", true},
                                                {"127.0.0.1:8003", true}};
    std::map<std::string, bool> onlineStatus2 = {{"127.0.0.1:8001", true},
                                                 {"127.0.0.1:8002", false},
                                                 {"127.0.0.1:8003", true}};
    // 1、copysets不健康
    EXPECT_CALL(*copysetCheck_, CheckCopysetsInCluster())
        .Times(6)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(0));
    ASSERT_FALSE(statusTool.IsClusterHeatlhy());

    // 2、没有mds可用
    EXPECT_CALL(*mdsClient_, GetCurrentMds())
        .Times(5)
        .WillOnce(Return(""))
        .WillRepeatedly(Return(leader));
    ASSERT_FALSE(statusTool.IsClusterHeatlhy());

    // 3、获取mds在线状态失败
    EXPECT_CALL(*mdsClient_, GetMdsOnlineStatus(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_FALSE(statusTool.IsClusterHeatlhy());

    // 4、有mds不在线
    EXPECT_CALL(*mdsClient_, GetMdsOnlineStatus(_))
        .Times(3)
        .WillOnce(DoAll(SetArgPointee<0>(onlineStatus2),
                        Return(0)))
        .WillRepeatedly(DoAll(SetArgPointee<0>(onlineStatus),
                        Return(0)));
    ASSERT_FALSE(statusTool.IsClusterHeatlhy());

    // 5、etcd没有leader
    EXPECT_CALL(*etcdClient_, GetEtcdClusterStatus(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(""),
                        SetArgPointee<1>(onlineStatus),
                        Return(0)));
    ASSERT_FALSE(statusTool.IsClusterHeatlhy());

    // 6、有etcd不在线
    EXPECT_CALL(*etcdClient_, GetEtcdClusterStatus(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(leader),
                        SetArgPointee<1>(onlineStatus2),
                        Return(0)));
    ASSERT_FALSE(statusTool.IsClusterHeatlhy());
}

}  // namespace tool
}  // namespace curve

