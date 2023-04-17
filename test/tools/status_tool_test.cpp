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
 * File Created: 2019-11-26
 * Author: charisu
 */
#include <gtest/gtest.h>
#include "src/tools/status_tool.h"
#include "test/tools/mock/mock_namespace_tool_core.h"
#include "test/tools/mock/mock_copyset_check_core.h"
#include "test/tools//mock/mock_mds_client.h"
#include "test/tools/mock/mock_etcd_client.h"
#include "test/tools/mock/mock_version_tool.h"
#include "test/tools/mock/mock_metric_client.h"
#include "test/tools/mock/mock_snapshot_clone_client.h"

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
        copysetCheck_ = std::make_shared<MockCopysetCheckCore>();
        etcdClient_ = std::make_shared<MockEtcdClient>();
        versionTool_ = std::make_shared<MockVersionTool>();
        metricClient_ = std::make_shared<MockMetricClient>();
        snapshotClient_ = std::make_shared<MockSnapshotCloneClient>();
    }

    void TearDown() {
        mdsClient_ = nullptr;
        copysetCheck_ = nullptr;
        etcdClient_ = nullptr;
        versionTool_ = nullptr;
        snapshotClient_ = nullptr;
    }

    void GetPhysicalPoolInfoForTest(PoolIdType id, PhysicalPoolInfo* pool) {
        pool->set_physicalpoolid(id);
        pool->set_physicalpoolname("testPool");
        pool->set_desc("physical pool for test");
    }

    void GetLogicalPoolForTest(PoolIdType id, LogicalPoolInfo *lpInfo,
                               bool getSpace = true) {
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

    void GetServerInfoForTest(curve::mds::topology::ServerInfo *server,
                              uint64_t id) {
        server->set_serverid(id);
        server->set_hostname("localhost");
        server->set_internalip("internal_ip");
        server->set_internalport(8200);
        server->set_externalip("external_ip");
        server->set_externalport(8200);
        server->set_zoneid(1);
        server->set_physicalpoolid(1);
    }
    CopysetStatistics statistics1;
    CopysetStatistics statistics2;

    std::shared_ptr<MockMDSClient> mdsClient_;
    std::shared_ptr<MockCopysetCheckCore> copysetCheck_;
    std::shared_ptr<MockEtcdClient> etcdClient_;
    std::shared_ptr<MockVersionTool> versionTool_;
    std::shared_ptr<MockMetricClient> metricClient_;
    std::shared_ptr<MockSnapshotCloneClient> snapshotClient_;
    const uint64_t DefaultSegmentSize = 1024 * 1024 * 1024;
};

TEST_F(StatusToolTest, InitAndSupportCommand) {
    StatusTool statusTool(mdsClient_, etcdClient_,
                          copysetCheck_, versionTool_,
                          metricClient_, snapshotClient_);
    ASSERT_TRUE(statusTool.SupportCommand("status"));
    ASSERT_TRUE(statusTool.SupportCommand("space"));
    ASSERT_TRUE(statusTool.SupportCommand("mds-status"));
    ASSERT_TRUE(statusTool.SupportCommand("chunkserver-status"));
    ASSERT_TRUE(statusTool.SupportCommand("chunkserver-list"));
    ASSERT_TRUE(statusTool.SupportCommand("etcd-status"));
    ASSERT_TRUE(statusTool.SupportCommand("client-status"));
    ASSERT_TRUE(statusTool.SupportCommand("snapshot-clone-status"));
    ASSERT_TRUE(statusTool.SupportCommand("cluster-status"));
    ASSERT_FALSE(statusTool.SupportCommand("none"));
}

TEST_F(StatusToolTest, InitFail) {
    StatusTool statusTool1(mdsClient_, etcdClient_,
                          copysetCheck_, versionTool_,
                          metricClient_, snapshotClient_);
    // 1、status命令需要所有的init
    EXPECT_CALL(*mdsClient_, Init(_, _))
        .Times(3)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*copysetCheck_, Init(_))
        .Times(2)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*etcdClient_, Init(_))
        .Times(2)
        .WillOnce(Return(-1))
        .WillOnce(Return(0));
    EXPECT_CALL(*snapshotClient_, Init(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool1.RunCommand("status"));
    ASSERT_EQ(-1, statusTool1.RunCommand("status"));
    ASSERT_EQ(-1, statusTool1.RunCommand("status"));
    ASSERT_EQ(-1, statusTool1.RunCommand("status"));

    // 2、etcd-status命令只需要初始化etcdClinet
    StatusTool statusTool2(mdsClient_, etcdClient_,
                          copysetCheck_, versionTool_,
                          metricClient_, snapshotClient_);
    EXPECT_CALL(*etcdClient_, Init(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool2.RunCommand("etcd-status"));

    // 3、space和其他命令不需要初始化etcdClient
    StatusTool statusTool3(mdsClient_, etcdClient_,
                          copysetCheck_, versionTool_,
                          metricClient_, snapshotClient_);
    EXPECT_CALL(*mdsClient_, Init(_, _))
        .Times(2)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*copysetCheck_, Init(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool3.RunCommand("space"));
    ASSERT_EQ(-1, statusTool3.RunCommand("chunkserver-list"));

    // 4、snapshot-clone-status只需要snapshot clone
    StatusTool statusTool4(mdsClient_, etcdClient_,
                          copysetCheck_, versionTool_,
                          metricClient_, snapshotClient_);
    EXPECT_CALL(*snapshotClient_, Init(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool4.RunCommand("snapshot-clone-status"));
}

TEST_F(StatusToolTest, SpaceCmd) {
    StatusTool statusTool(mdsClient_, etcdClient_,
                          copysetCheck_, versionTool_,
                          metricClient_, snapshotClient_);
    statusTool.PrintHelp("space");
    statusTool.PrintHelp("123");
    LogicalPoolInfo lgPool;
    GetLogicalPoolForTest(1, &lgPool);
    std::vector<LogicalPoolInfo> lgPools;
    lgPools.emplace_back(lgPool);

    // 设置Init的期望
    EXPECT_CALL(*mdsClient_, Init(_, _))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*copysetCheck_, Init(_))
        .Times(1)
        .WillOnce(Return(0));

    // 1、正常情况
    EXPECT_CALL(*mdsClient_, ListLogicalPoolsInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(lgPools),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, GetFileSize(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(150 * DefaultSegmentSize),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, GetMetric(_, _))
        .Times(4)
        .WillOnce(DoAll(SetArgPointee<1>(300 * DefaultSegmentSize),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(20 * DefaultSegmentSize),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(100 * DefaultSegmentSize),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(10 * DefaultSegmentSize),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, GetAllocatedSize(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(10 * DefaultSegmentSize),
                        Return(0)));
    ASSERT_EQ(0, statusTool.RunCommand("space"));
    ASSERT_EQ(-1, statusTool.RunCommand("123"));

    // 2、ListLogicalPoolsInPhysicalPool失败的情况
    EXPECT_CALL(*mdsClient_, ListLogicalPoolsInCluster(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool.RunCommand("space"));

    // 3、获取filesize失败
    EXPECT_CALL(*mdsClient_, ListLogicalPoolsInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(lgPools),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, GetFileSize(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool.RunCommand("space"));

    // 4、获取metric失败的情况
    EXPECT_CALL(*mdsClient_, ListLogicalPoolsInCluster(_))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<0>(lgPools),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, GetFileSize(_, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(150 * DefaultSegmentSize),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, GetMetric(_, _))
        .WillOnce(Return(-1))
        .WillOnce(DoAll(SetArgPointee<1>(300 * DefaultSegmentSize),
                        Return(0)))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool.RunCommand("space"));
    ASSERT_EQ(-1, statusTool.RunCommand("space"));

    // 5、获取RecyleBin大小失败的情况
    EXPECT_CALL(*mdsClient_, ListLogicalPoolsInCluster(_))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<0>(lgPools),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, GetFileSize(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(150 * DefaultSegmentSize),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, GetMetric(_, _))
        .Times(4)
        .WillOnce(DoAll(SetArgPointee<1>(300 * DefaultSegmentSize),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(20 * DefaultSegmentSize),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(100 * DefaultSegmentSize),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(10 * DefaultSegmentSize),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, GetAllocatedSize(_, _, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool.RunCommand("space"));
}

TEST_F(StatusToolTest, ChunkServerCmd) {
    StatusTool statusTool(mdsClient_, etcdClient_,
                          copysetCheck_, versionTool_,
                          metricClient_, snapshotClient_);
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
                          copysetCheck_, versionTool_,
                          metricClient_, snapshotClient_);
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
    std::vector<std::string> mdsAddr = {"127.0.0.1:6666"};
    std::map<std::string, bool> mdsOnlineStatus = {{"127.0.0.1:6666", true},
                                                   {"127.0.0.1:6667", true},
                                                   {"127.0.0.1:6668", true}};
    VersionMapType versionMap = {{"0.0.1", {"127.0.0.1:8001"}},
                                 {"0.0.2", {"127.0.0.1:8002"}},
                                 {"0.0.3", {"127.0.0.1:8003"}}};
    ClientVersionMapType clientVersionMap = {{"nebd-server", versionMap},
                                              {"python", versionMap},
                                              {"qemu", versionMap}};
    std::vector<std::string> offlineList = {"127.0.0.1:8004",
                                            "127.0.0.1:8005"};
    std::vector<std::string> leaderAddr = {"127.0.0.1:2379"};
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
    EXPECT_CALL(*copysetCheck_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*etcdClient_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*snapshotClient_, Init(_, _))
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
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<0>(phyPools),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, ListLogicalPoolsInPhysicalPool(_, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<1>(lgPools),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, ListLogicalPoolsInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(lgPools),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, GetFileSize(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(150 * DefaultSegmentSize),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, GetMetric(_, _))
        .Times(4)
        .WillOnce(DoAll(SetArgPointee<1>(300 * DefaultSegmentSize),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(20 * DefaultSegmentSize),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(100 * DefaultSegmentSize),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(10 * DefaultSegmentSize),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, GetAllocatedSize(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(10 * DefaultSegmentSize),
                        Return(0)));

    // 设置client status的输出
    EXPECT_CALL(*versionTool_, GetClientVersion(_))
        .WillOnce(DoAll(SetArgPointee<0>(clientVersionMap),
                        Return(0)));

    // 2、设置MDS status的输出
    EXPECT_CALL(*mdsClient_, GetCurrentMds())
        .Times(2)
        .WillRepeatedly(Return(mdsAddr));
    EXPECT_CALL(*mdsClient_, GetMdsOnlineStatus(_))
        .Times(2)
        .WillRepeatedly(SetArgPointee<0>(mdsOnlineStatus));
    EXPECT_CALL(*versionTool_, GetAndCheckMdsVersion(_, _))
        .WillOnce(DoAll(SetArgPointee<0>("0.0.1"),
                        Return(0)));

    // 3、设置etcd status的输出
    EXPECT_CALL(*etcdClient_, GetAndCheckEtcdVersion(_, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<0>("3.4.1"),
                        Return(0)));
    EXPECT_CALL(*etcdClient_, GetEtcdClusterStatus(_, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<0>(leaderAddr),
                        SetArgPointee<1>(onlineState),
                        Return(0)));

    // 设置snapshot clone的输出
    std::vector<std::string> activeAddr = {"127.0.0.1:5555"};
    EXPECT_CALL(*versionTool_, GetAndCheckSnapshotCloneVersion(_, _))
        .WillOnce(DoAll(SetArgPointee<0>("0.0.1"),
                        Return(0)));
    EXPECT_CALL(*snapshotClient_, GetActiveAddrs())
        .Times(2)
        .WillRepeatedly(Return(activeAddr));
    EXPECT_CALL(*snapshotClient_, GetOnlineStatus(_))
        .Times(2)
        .WillRepeatedly(SetArgPointee<0>(onlineState));

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
                        Return(MetricRet::kOK)));
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
        .WillOnce(SetArgPointee<0>(mdsOnlineStatus));
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
                          copysetCheck_, versionTool_,
                          metricClient_, snapshotClient_);

    // 设置Init的期望
    EXPECT_CALL(*mdsClient_, Init(_, _))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*copysetCheck_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*etcdClient_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*snapshotClient_, Init(_, _))
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
        .Times(1)
        .WillRepeatedly(Return(-1));
    // 列出逻辑池失败
    EXPECT_CALL(*mdsClient_, ListLogicalPoolsInCluster(_))
        .Times(1)
        .WillRepeatedly(Return(-1));

    // 获取client version失败
    EXPECT_CALL(*versionTool_, GetClientVersion(_))
        .WillOnce(Return(-1));

    // 2、当前无mds可用
    std::vector<std::string> failedList = {"127.0.0.1:6666", "127.0.0.1:6667"};
    EXPECT_CALL(*versionTool_, GetAndCheckMdsVersion(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(failedList),
                  Return(0)));
    std::map<std::string, bool> mdsOnlineStatus = {{"127.0.0.1:6666", false},
                                                   {"127.0.0.1:6667", false}};
    EXPECT_CALL(*mdsClient_, GetCurrentMds())
        .Times(2)
        .WillRepeatedly(Return(std::vector<std::string>()));
    EXPECT_CALL(*mdsClient_, GetMdsOnlineStatus(_))
        .Times(2)
        .WillRepeatedly(SetArgPointee<0>(mdsOnlineStatus));

    // 3、GetEtcdClusterStatus失败
    EXPECT_CALL(*etcdClient_, GetAndCheckEtcdVersion(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    EXPECT_CALL(*etcdClient_, GetEtcdClusterStatus(_, _))
        .Times(2)
        .WillRepeatedly(Return(-1));

    // 当前无snapshot clone server可用
    EXPECT_CALL(*versionTool_, GetAndCheckSnapshotCloneVersion(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(failedList),
                  Return(0)));
    std::map<std::string, bool> onlineStatus = {{"127.0.0.1:5555", false},
                                                {"127.0.0.1:5556", false}};
    EXPECT_CALL(*snapshotClient_, GetActiveAddrs())
        .Times(2)
        .WillRepeatedly(Return(std::vector<std::string>()));
    EXPECT_CALL(*snapshotClient_, GetOnlineStatus(_))
        .Times(2)
        .WillRepeatedly(SetArgPointee<0>(onlineStatus));

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
        .WillOnce(Return(std::vector<std::string>()));
    EXPECT_CALL(*mdsClient_, GetMdsOnlineStatus(_))
        .Times(1)
        .WillOnce(SetArgPointee<0>(mdsOnlineStatus));
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
                          copysetCheck_, versionTool_,
                          metricClient_, snapshotClient_);
    std::map<std::string, bool> onlineStatus = {{"127.0.0.1:8001", true},
                                                {"127.0.0.1:8002", true},
                                                {"127.0.0.1:8003", true}};
    std::map<std::string, bool> onlineStatus2 = {{"127.0.0.1:8001", true},
                                                 {"127.0.0.1:8002", false},
                                                 {"127.0.0.1:8003", true}};
    // 1、copysets不健康
    EXPECT_CALL(*copysetCheck_, CheckCopysetsInCluster())
        .Times(1)
        .WillOnce(Return(-1));
    // 2、没有mds可用
    EXPECT_CALL(*mdsClient_, GetCurrentMds())
        .Times(1)
        .WillOnce(Return(std::vector<std::string>()));
    // 3、有mds不在线
    EXPECT_CALL(*mdsClient_, GetMdsOnlineStatus(_))
        .Times(1)
        .WillOnce(SetArgPointee<0>(onlineStatus2));
    // 4、获取etcd集群状态失败
    EXPECT_CALL(*etcdClient_, GetEtcdClusterStatus(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    // 5、没有snapshot-clone-server可用
    EXPECT_CALL(*snapshotClient_, GetActiveAddrs())
        .Times(1)
        .WillOnce(Return(std::vector<std::string>()));
    // 6、有snapshot-clone-server不在线
    EXPECT_CALL(*snapshotClient_, GetOnlineStatus(_))
        .Times(1)
        .WillOnce(SetArgPointee<0>(onlineStatus2));
    ASSERT_FALSE(statusTool.IsClusterHeatlhy());

    // 1、copyset健康
    EXPECT_CALL(*copysetCheck_, CheckCopysetsInCluster())
        .Times(1)
        .WillOnce(Return(0));
    // 2、超过一个mds在服务
    EXPECT_CALL(*mdsClient_, GetCurrentMds())
        .Times(1)
        .WillOnce(Return(std::vector<std::string>(2)));
    // 3、mds都在线
    EXPECT_CALL(*mdsClient_, GetMdsOnlineStatus(_))
        .Times(1)
        .WillOnce(SetArgPointee<0>(onlineStatus));
    // 4、etcd没有leader且有etcd不在线
    EXPECT_CALL(*etcdClient_, GetEtcdClusterStatus(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(std::vector<std::string>()),
                        SetArgPointee<1>(onlineStatus2),
                        Return(0)));
    // 5、有多个snapshot-clone-server可用
    EXPECT_CALL(*snapshotClient_, GetActiveAddrs())
        .Times(1)
        .WillOnce(Return(std::vector<std::string>(2)));
    // 9、snapshot-clone-server都在线
    EXPECT_CALL(*snapshotClient_, GetOnlineStatus(_))
        .Times(1)
        .WillOnce(SetArgPointee<0>(onlineStatus));
    ASSERT_FALSE(statusTool.IsClusterHeatlhy());
}

TEST_F(StatusToolTest, ListClientCmd) {
    StatusTool statusTool(mdsClient_, etcdClient_,
                          copysetCheck_, versionTool_,
                          metricClient_, snapshotClient_);
    EXPECT_CALL(*mdsClient_, Init(_, _))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*copysetCheck_, Init(_))
        .Times(1)
        .WillOnce(Return(0));

    std::vector<std::string> clientAddrs;
    for (int i = 0; i < 10; ++i) {
        clientAddrs.emplace_back("127.0.0.1:900" + std::to_string(i));
    }
    // 成功
    EXPECT_CALL(*mdsClient_, ListClient(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(clientAddrs),
                        Return(0)));
    ASSERT_EQ(0, statusTool.RunCommand("client-list"));
    // 失败
    EXPECT_CALL(*mdsClient_, ListClient(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool.RunCommand("client-list"));
}

TEST_F(StatusToolTest, ServerList) {
    StatusTool statusTool(mdsClient_, etcdClient_,
                          copysetCheck_, versionTool_,
                          metricClient_, snapshotClient_);
    EXPECT_CALL(*mdsClient_, Init(_, _))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*copysetCheck_, Init(_))
        .Times(1)
        .WillOnce(Return(0));

    std::vector<ServerInfo> servers;
    for (int i = 0; i < 3; ++i) {
        ServerInfo server;
        GetServerInfoForTest(&server, i);
        servers.emplace_back(server);
    }
    // 成功
    EXPECT_CALL(*mdsClient_, ListServersInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(servers),
                        Return(0)));
    ASSERT_EQ(0, statusTool.RunCommand("server-list"));
    // 失败
    EXPECT_CALL(*mdsClient_, ListServersInCluster(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool.RunCommand("server-list"));
}

TEST_F(StatusToolTest, LogicalPoolList) {
    StatusTool statusTool(mdsClient_, etcdClient_,
                          copysetCheck_, versionTool_,
                          metricClient_, snapshotClient_);
    EXPECT_CALL(*mdsClient_, Init(_, _))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*copysetCheck_, Init(_))
        .Times(1)
        .WillOnce(Return(0));

    std::vector<LogicalPoolInfo> lgPools;
    for (int i = 1; i <= 3; ++i) {
        LogicalPoolInfo lgPool;
        GetLogicalPoolForTest(i, &lgPool);
        lgPools.emplace_back(lgPool);
    }
    // 成功
    EXPECT_CALL(*mdsClient_, ListLogicalPoolsInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(lgPools),
                        Return(0)));
    AllocMap allocMap = {{1, DefaultSegmentSize}, {2, DefaultSegmentSize * 20}};
    EXPECT_CALL(*mdsClient_, GetAllocatedSize(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(allocMap),
                        Return(0)));
    ASSERT_EQ(0, statusTool.RunCommand("logical-pool-list"));
    // 失败
    EXPECT_CALL(*mdsClient_, ListLogicalPoolsInCluster(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool.RunCommand("logical-pool-list"));
    EXPECT_CALL(*mdsClient_, ListLogicalPoolsInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(lgPools),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, GetAllocatedSize(_, _, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, statusTool.RunCommand("logical-pool-list"));
}

}  // namespace tool
}  // namespace curve

