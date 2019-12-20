/*
 * Project: curve
 * File Created: 2019-11-28
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include "src/tools/copyset_check_core.h"
#include "test/tools/mock_mds_client.h"
#include "test/tools/mock_chunkserver_client.h"

using ::testing::_;
using ::testing::Return;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using curve::mds::topology::ChunkServerStatus;
using curve::mds::topology::DiskState;
using curve::mds::topology::OnlineState;
using curve::tool::kTotal;
using curve::tool::kInstallingSnapshot;
using curve::tool::kNoLeader;
using curve::tool::kLogIndexGapTooBig;
using curve::tool::kPeersNoSufficient;
using curve::tool::kPeerNotOnline;

DEFINE_uint64(rpcTimeout, 3000, "millisecond for rpc timeout");
DEFINE_uint64(rpcRetryTimes, 5, "rpc retry times");
DECLARE_uint64(operatorMaxPeriod);
DECLARE_bool(checkOperator);

class CopysetCheckCoreTest : public ::testing::Test {
 protected:
    void SetUp() {
        mdsClient_ = std::make_shared<curve::tool::MockMDSClient>();
        csClient_ = std::make_shared<curve::tool::MockChunkServerClient>();
        FLAGS_operatorMaxPeriod = 3;
        FLAGS_checkOperator = true;
    }
    void TearDown() {
        mdsClient_ = nullptr;
        csClient_ = nullptr;
    }

    void GetCsLocForTest(ChunkServerLocation* csLoc, uint64_t csId) {
        csLoc->set_chunkserverid(csId);
        csLoc->set_hostip("127.0.0.1");
        csLoc->set_port(9190 + csId);
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

    void GetServerInfoForTest(curve::mds::topology::ServerInfo *serverInfo) {
        serverInfo->set_serverid(1);
        serverInfo->set_hostname("localhost");
        serverInfo->set_internalip("127.0.0.1");
        serverInfo->set_internalport(8080);
        serverInfo->set_externalip("127.0.0.1");
        serverInfo->set_externalport(8081);
        serverInfo->set_zoneid(1);
        serverInfo->set_zonename("testZone");
        serverInfo->set_physicalpoolid(1);
        serverInfo->set_physicalpoolname("testPool");
        serverInfo->set_desc("123");
    }

    void GetIoBufForTest(butil::IOBuf* buf, const std::string& gId,
                                        const std::string& state = "FOLLOWER",
                                        bool noLeader = false,
                                        bool installingSnapshot = false,
                                        bool peersLess = false,
                                        bool gapBig = false,
                                        bool parseErr = false,
                                        bool peerOffline = false) {
        butil::IOBufBuilder os;
        os << "[" << gId <<  "]\r\n";
        if (peersLess) {
            os << "peers: \r\n";
        } else if (peerOffline) {
            os << "peers: 127.0.0.1:9191:0 127.0.0.1:9192:0 127.0.0.1:9194:0\r\n";  // NOLINT
        } else {
            os << "peers: 127.0.0.1:9191:0 127.0.0.1:9192:0 127.0.0.1:9193:0\r\n";  // NOLINT
        }
        os << "storage: [2581, 2580]\n";
        if (parseErr) {
            os << "\n";
        } else {
            os << "last_log_id: (index=2580,term=4)\n";
        }
        os << "state_machine: Idle\r\n";
        if (state == "LEADER") {
            os << "state: " << "LEADER" << "\r\n";
            os << "replicator_123: next_index=";
            if (gapBig) {
                os << "1000";
            } else {
                os << "2581";
            }
            os << " flying_append_entries_size=0 ";
            if (installingSnapshot) {
                os << "installing snapshot {1234, 3} ";
            } else {
                os << "idle ";
            }
            os << "hc=4211759 ac=1089 ic=0\r\n";
        } else if (state == "FOLLOWER") {
            os << "state: " << "FOLLOWER" << "\r\n";
            if (noLeader) {
                os << "leader: " << "0.0.0.0:0:0\r\n";
            } else {
                os << "leader: " << "127.0.0.1:9192:0\r\n";
            }
        } else {
            os << "state: " << state << "\r\n";
        }
        os.move_to(*buf);
    }

    std::shared_ptr<curve::tool::MockMDSClient> mdsClient_;
    std::shared_ptr<curve::tool::MockChunkServerClient> csClient_;
};

// CheckOneCopyset正常情况
TEST_F(CopysetCheckCoreTest, CheckOneCopysetNormal) {
    std::vector<ChunkServerLocation> csLocs;
    butil::IOBuf followerBuf;
    butil::IOBuf leaderBuf;
    GetIoBufForTest(&leaderBuf, "4294967396", "LEADER");
    GetIoBufForTest(&followerBuf, "4294967396");
    for (uint64_t i = 1; i <= 3; ++i) {
        ChunkServerLocation csLoc;
        GetCsLocForTest(&csLoc, i);
        csLocs.emplace_back(csLoc);
    }

    EXPECT_CALL(*mdsClient_, GetChunkServerListInCopySets(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(csLocs),
                        Return(0)));
    EXPECT_CALL(*csClient_, Init(_))
        .Times(5)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_))
        .Times(3)
        .WillOnce(DoAll(SetArgPointee<0>(leaderBuf),
                        Return(0)))
        .WillRepeatedly(DoAll(SetArgPointee<0>(followerBuf),
                        Return(0)));
    EXPECT_CALL(*csClient_, CheckChunkServerOnline())
        .Times(2)
        .WillRepeatedly(Return(true));
    curve::tool::CopysetCheckCore copysetCheck(mdsClient_, csClient_);
    ASSERT_EQ(0, copysetCheck.CheckOneCopyset(1, 100));
    butil::IOBuf iobuf;
    iobuf.append("\r\n");
    iobuf.append(leaderBuf);
    iobuf.append("\r\n");
    iobuf.append(followerBuf);
    iobuf.append("\r\n");
    iobuf.append(followerBuf);
    std::cout << copysetCheck.GetCopysetDetail();
    ASSERT_EQ(iobuf.to_string(), copysetCheck.GetCopysetDetail());
}

// CheckOneCopyset异常情况
TEST_F(CopysetCheckCoreTest, CheckOneCopysetError) {
    std::vector<ChunkServerLocation> csLocs;
    butil::IOBuf followerBuf;
    butil::IOBuf leaderBuf;
    GetIoBufForTest(&leaderBuf, "4294967396", "LEADER");
    GetIoBufForTest(&followerBuf, "4294967396");
    for (uint64_t i = 1; i <= 3; ++i) {
        ChunkServerLocation csLoc;
        GetCsLocForTest(&csLoc, i);
        csLocs.emplace_back(csLoc);
    }
    CopysetInfo copyset;
    copyset.set_logicalpoolid(1);
    copyset.set_copysetid(100);

    // 1、GetChunkServerListInCopySets失败
    EXPECT_CALL(*mdsClient_, GetChunkServerListInCopySets(_, _, _))
        .Times(1)
        .WillOnce(Return(-1));
    curve::tool::CopysetCheckCore copysetCheck1(mdsClient_, csClient_);
    ASSERT_EQ(-1, copysetCheck1.CheckOneCopyset(1, 100));

    // 2、copyset不健康
    GetIoBufForTest(&followerBuf, "4294967396", "FOLLOWER", true);
    EXPECT_CALL(*mdsClient_, GetChunkServerListInCopySets(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(csLocs),
                        Return(0)));
    EXPECT_CALL(*csClient_, Init(_))
        .Times(3)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_))
        .Times(3)
        .WillRepeatedly(DoAll(SetArgPointee<0>(followerBuf),
                        Return(0)));
    curve::tool::CopysetCheckCore copysetCheck2(mdsClient_, csClient_);
    ASSERT_EQ(-1, copysetCheck2.CheckOneCopyset(1, 100));

    // 3、第一个peer不在线
    EXPECT_CALL(*mdsClient_, GetChunkServerListInCopySets(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(csLocs),
                        Return(0)));
    EXPECT_CALL(*csClient_, Init(_))
        .Times(3)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_))
    .Times(2)
    .WillOnce(DoAll(SetArgPointee<0>(leaderBuf),
                        Return(0)))
    .WillOnce(DoAll(SetArgPointee<0>(followerBuf),
                        Return(0)));
    curve::tool::CopysetCheckCore copysetCheck3(mdsClient_, csClient_);
    ASSERT_EQ(-1, copysetCheck3.CheckOneCopyset(1, 100));
}


// CheckCopysetsOnChunkserver正常情况
TEST_F(CopysetCheckCoreTest, CheckCopysetsOnChunkServerHealthy) {
    ChunkServerIdType csId = 1;
    std::string csAddr = "127.0.0.1:9191";
    ChunkServerInfo csInfo;
    GetCsInfoForTest(&csInfo, csId);
    std::map<std::string, std::set<std::string>> expectedRes;
    std::string gId = "4294967396";
    butil::IOBuf followerBuf;
    GetIoBufForTest(&followerBuf, gId);
    butil::IOBuf leaderBuf;
    GetIoBufForTest(&leaderBuf, gId, "LEADER");

    // mds返回Chunkserver retired的情况,直接返回0
    GetCsInfoForTest(&csInfo, csId, false, "LEADER");
    EXPECT_CALL(*mdsClient_, GetChunkServerInfo(csId, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(csInfo),
                        Return(0)));
    curve::tool::CopysetCheckCore copysetCheck1(mdsClient_, csClient_);
    ASSERT_EQ(0, copysetCheck1.CheckCopysetsOnChunkServer(csId));
    ASSERT_DOUBLE_EQ(0, copysetCheck1.GetCopysetStatistics().unhealthyRatio);
    ASSERT_EQ(expectedRes, copysetCheck1.GetCopysetsRes());

    expectedRes[kTotal].insert(gId);
    // 通过id查询
    GetCsInfoForTest(&csInfo, csId);
    EXPECT_CALL(*mdsClient_, GetChunkServerInfo(csId, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(csInfo),
                        Return(0)));
    EXPECT_CALL(*csClient_, Init(_))
        .Times(3)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<0>(followerBuf),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<0>(leaderBuf),
                        Return(0)));
    EXPECT_CALL(*csClient_, CheckChunkServerOnline())
        .Times(1)
        .WillRepeatedly(Return(true));
    curve::tool::CopysetCheckCore copysetCheck2(mdsClient_, csClient_);
    ASSERT_EQ(0, copysetCheck2.CheckCopysetsOnChunkServer(csId));
    ASSERT_DOUBLE_EQ(0, copysetCheck2.GetCopysetStatistics().unhealthyRatio);
    ASSERT_EQ(expectedRes, copysetCheck2.GetCopysetsRes());

    // 通过地址查询
    EXPECT_CALL(*mdsClient_, GetChunkServerInfo(csAddr, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(csInfo),
                        Return(0)));
    EXPECT_CALL(*csClient_, Init(_))
        .Times(3)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<0>(followerBuf),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<0>(leaderBuf),
                        Return(0)));
    EXPECT_CALL(*csClient_, CheckChunkServerOnline())
        .Times(1)
        .WillRepeatedly(Return(true));
    curve::tool::CopysetCheckCore copysetCheck3(mdsClient_, csClient_);
    ASSERT_EQ(0, copysetCheck3.CheckCopysetsOnChunkServer(csAddr));
    ASSERT_DOUBLE_EQ(0, copysetCheck3.GetCopysetStatistics().unhealthyRatio);
    ASSERT_EQ(expectedRes, copysetCheck3.GetCopysetsRes());
}

// CheckCopysetsOnChunkserver异常情况
TEST_F(CopysetCheckCoreTest, CheckCopysetsOnChunkServerError) {
    ChunkServerIdType csId = 1;
    std::string csAddr = "127.0.0.1:9191";
    ChunkServerInfo csInfo;
    GetCsInfoForTest(&csInfo, csId);
    std::vector<CopysetInfo> copysets;
    std::set<std::string> gIds;
    for (int i = 1; i <= 5; ++i) {
        CopysetInfo copyset;
        copyset.set_logicalpoolid(1);
        copyset.set_copysetid(100 + i);
        copysets.emplace_back(copyset);
        uint64_t gId = (static_cast<uint64_t>(1) << 32) | (100 + i);
        gIds.emplace(std::to_string(gId));
    }
    std::map<std::string, std::set<std::string>> expectedRes;

    // 1、GetChunkServerInfo失败的情况
    curve::tool::CopysetCheckCore copysetCheck1(mdsClient_, csClient_);
    EXPECT_CALL(*mdsClient_, GetChunkServerInfo(csId, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, copysetCheck1.CheckCopysetsOnChunkServer(csId));
    ASSERT_DOUBLE_EQ(0, copysetCheck1.GetCopysetStatistics().unhealthyRatio);
    ASSERT_EQ(expectedRes, copysetCheck1.GetCopysetsRes());

    expectedRes[kPeerNotOnline] = gIds;
    expectedRes[kTotal] = gIds;
    std::set<std::string> expectedExcepCs = {csAddr};

    // 3、向chunkserver发送RPC失败的情况,一次是Init失败，一次是发送RPC失败
    GetCsInfoForTest(&csInfo, csId);
    EXPECT_CALL(*mdsClient_, GetChunkServerInfo(csId, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo),
                        Return(0)));
    EXPECT_CALL(*csClient_, Init(_))
        .Times(2)
        .WillOnce(Return(-1))
        .WillOnce(Return(0));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_))
        .Times(1)
        .WillOnce(Return(-1));
    EXPECT_CALL(*mdsClient_, GetCopySetsInChunkServer(csAddr, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(copysets),
                        Return(0)));
    // Init失败的情况
    curve::tool::CopysetCheckCore copysetCheck3(mdsClient_, csClient_);
    ASSERT_EQ(-1, copysetCheck3.CheckCopysetsOnChunkServer(csId));
    ASSERT_DOUBLE_EQ(1, copysetCheck3.GetCopysetStatistics().unhealthyRatio);
    ASSERT_EQ(expectedExcepCs, copysetCheck3.GetServiceExceptionChunkServer());
    ASSERT_EQ(expectedRes, copysetCheck3.GetCopysetsRes());
    // 发送RPC失败的情况
    curve::tool::CopysetCheckCore copysetCheck4(mdsClient_, csClient_);
    ASSERT_EQ(-1, copysetCheck4.CheckCopysetsOnChunkServer(csId));
    ASSERT_DOUBLE_EQ(1, copysetCheck4.GetCopysetStatistics().unhealthyRatio);
    ASSERT_EQ(expectedExcepCs, copysetCheck4.GetServiceExceptionChunkServer());
    ASSERT_EQ(expectedRes, copysetCheck4.GetCopysetsRes());
}

// chunkserver上copyset不健康的情况
// 检查单个server和集群都是复用的CheckCopysetsOnChunkserver
// 所以CheckCopysetsOnChunkserver要测每个不健康的情况，其他的只要测健康和不健康还有不在线的情况就好
// 具体什么原因不健康不用关心
TEST_F(CopysetCheckCoreTest, CheckCopysetsOnChunkServerUnhealthy) {
    ChunkServerIdType csId = 1;
    std::string csAddr = "127.0.0.1:9194";
    ChunkServerInfo csInfo;
    GetCsInfoForTest(&csInfo, csId);
    butil::IOBuf iobuf;
    butil::IOBuf temp;
    butil::IOBufBuilder os;
    std::map<std::string, std::set<std::string>> expectedRes;
    uint64_t gId = 4294967396;
    std::string groupId;

    // 1、首先加入8个健康的copyset
    for (int i = 0; i < 8; ++i) {
        groupId = std::to_string(gId++);
        GetIoBufForTest(&temp, groupId, "LEADER", false, false, false,
                                false, false, false);
        expectedRes[kTotal].emplace(groupId);
        os << temp << "\r\n";
    }
    // 2、加入没有leader的copyset
    groupId = std::to_string(gId++);
    GetIoBufForTest(&temp, groupId, "FOLLOWER", true, false, false,
                                false, false, false);
    expectedRes[kTotal].emplace(groupId);
    expectedRes[kNoLeader].emplace(groupId);
    os << temp << "\r\n";
    // 3、加入正在安装快照的copyset
    groupId = std::to_string(gId++);
    GetIoBufForTest(&temp, groupId, "LEADER", false, true, false,
                            false, false, false);
    expectedRes[kTotal].emplace(groupId);
    expectedRes[kInstallingSnapshot].emplace(groupId);
    os << temp << "\r\n";
    // 4、加入peer不足的copyset
    groupId = std::to_string(gId++);
    GetIoBufForTest(&temp, groupId, "LEADER", false, false, true,
                            false, false, false);
    expectedRes[kTotal].emplace(groupId);
    expectedRes[kPeersNoSufficient].emplace(groupId);
    os << temp << "\r\n";
    // 5、加入日志差距大的copset
    groupId = std::to_string(gId++);
    GetIoBufForTest(&temp, groupId, "LEADER", false, false, false,
                            true, false, false);
    expectedRes[kTotal].emplace(groupId);
    expectedRes[kLogIndexGapTooBig].emplace(groupId);
    os << temp << "\r\n";
    // 6、加入无法解析的copyset，这种情况不会发生，发生了表明程序有bug
    // 打印错误信息，但不会加入到unhealthy
    groupId = std::to_string(gId++);
    GetIoBufForTest(&temp, groupId, "LEADER", false, false, false,
                            false, true, false);
    expectedRes[kTotal].emplace(groupId);
    os << temp << "\r\n";

    // 7、加入peer不在线的copyset
    groupId = std::to_string(gId++);
    GetIoBufForTest(&temp, groupId, "LEADER", false, false, false,
                            false, false, true);
    expectedRes[kTotal].emplace(groupId);
    expectedRes[kPeerNotOnline].emplace(groupId);
    os << temp << "\r\n";

    // 8、加入CANDIDATE状态的copyset
    groupId = std::to_string(gId++);
    GetIoBufForTest(&temp, groupId, "CANDIDATE");
    expectedRes[kTotal].emplace(groupId);
    expectedRes[kNoLeader].emplace(groupId);
    os << temp << "\r\n";

    // 9、加入TRANSFERRING状态的copyset
    groupId = std::to_string(gId++);
    GetIoBufForTest(&temp, groupId, "TRANSFERRING");
    expectedRes[kTotal].emplace(groupId);
    expectedRes[kNoLeader].emplace(groupId);
    os << temp << "\r\n";

    // 10、加入ERROR状态的copyset
    groupId = std::to_string(gId++);
    GetIoBufForTest(&temp, groupId, "ERROR");
    expectedRes[kTotal].emplace(groupId);
    expectedRes["state ERROR"].emplace(groupId);
    os << temp << "\r\n";

    // 11、加入SHUTDOWN状态的copyset
    groupId = std::to_string(gId++);
    GetIoBufForTest(&temp, groupId, "SHUTDOWN");
    expectedRes[kTotal].emplace(groupId);
    expectedRes["state SHUTDOWN"].emplace(groupId);
    os << temp;

    // 设置mock对象的返回,8个正常iobuf里面，设置一个的peer不在线，因此unhealthy++
    os.move_to(iobuf);
    EXPECT_CALL(*mdsClient_, GetChunkServerInfo(csId, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(csInfo),
                        Return(0)));
    EXPECT_CALL(*csClient_, Init(_))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*csClient_, Init(csAddr))
        .WillOnce(Return(-1));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(iobuf),
                        Return(0)));
    EXPECT_CALL(*csClient_, CheckChunkServerOnline())
        .Times(2)
        .WillRepeatedly(Return(true));

    // 检查结果
    std::set<std::string> expectedExcepCs = {csAddr};
    curve::tool::CopysetCheckCore copysetCheck(mdsClient_, csClient_);
    ASSERT_EQ(-1, copysetCheck.CheckCopysetsOnChunkServer(csId));
    ASSERT_DOUBLE_EQ(0.5, copysetCheck.GetCopysetStatistics().unhealthyRatio);
    ASSERT_EQ(expectedRes, copysetCheck.GetCopysetsRes());
    ASSERT_EQ(expectedExcepCs, copysetCheck.GetServiceExceptionChunkServer());
}

// CheckCopysetsOnServer正常情况
TEST_F(CopysetCheckCoreTest, CheckCopysetsOnServerNormal) {
    ServerIdType serverId = 1;
    std::string serverIp = "127.0.0.1";
    std::vector<ChunkServerInfo> chunkservers;
    std::vector<std::string> unhealthyCs;
    for (uint64_t i = 1; i <= 2; ++i) {
        ChunkServerInfo csInfo;
        GetCsInfoForTest(&csInfo, i);
        chunkservers.emplace_back(csInfo);
    }
    std::map<std::string, std::set<std::string>> expectedRes;
    uint64_t gId = 4294967396;
    butil::IOBuf iobuf1;
    std::string groupId;
    groupId = std::to_string(gId++);
    expectedRes[kTotal].emplace(groupId);
    GetIoBufForTest(&iobuf1, groupId, "LEADER", false, false, false,
                                                    false, false, false);
    butil::IOBuf iobuf2;
    groupId = std::to_string(gId++);
    expectedRes[kTotal].emplace(groupId);
    GetIoBufForTest(&iobuf2, groupId, "LEADER", false, false, false,
                                                    false, false, false);

    // 通过id查询
    EXPECT_CALL(*mdsClient_, ListChunkServersOnServer(serverId, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(chunkservers),
                        Return(0)));
    EXPECT_CALL(*csClient_, Init(_))
        .Times(4)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<0>(iobuf1),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<0>(iobuf2),
                        Return(0)));
    EXPECT_CALL(*csClient_, CheckChunkServerOnline())
        .Times(2)
        .WillRepeatedly(Return(true));
    curve::tool::CopysetCheckCore copysetCheck1(mdsClient_, csClient_);
    ASSERT_EQ(0, copysetCheck1.CheckCopysetsOnServer(serverId, &unhealthyCs));
    ASSERT_EQ(0, unhealthyCs.size());
    ASSERT_EQ(0, copysetCheck1.GetCopysetStatistics().unhealthyRatio);
    ASSERT_EQ(expectedRes, copysetCheck1.GetCopysetsRes());

    // 通过ip查询
    EXPECT_CALL(*mdsClient_, ListChunkServersOnServer(serverIp, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(chunkservers),
                        Return(0)));
    EXPECT_CALL(*csClient_, Init(_))
        .Times(4)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<0>(iobuf1),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<0>(iobuf2),
                        Return(0)));
    EXPECT_CALL(*csClient_, CheckChunkServerOnline())
        .Times(2)
        .WillRepeatedly(Return(true));
    // 通过ip查询
    curve::tool::CopysetCheckCore copysetCheck2(mdsClient_, csClient_);
    ASSERT_EQ(0, copysetCheck2.CheckCopysetsOnServer(serverIp, &unhealthyCs));
    ASSERT_EQ(0, unhealthyCs.size());
    ASSERT_EQ(0, copysetCheck2.GetCopysetStatistics().unhealthyRatio);
    ASSERT_EQ(expectedRes, copysetCheck2.GetCopysetsRes());
}

// CheckCopysetsOnServer异常情况
TEST_F(CopysetCheckCoreTest, CheckCopysetsOnServerError) {
    ServerIdType serverId = 1;
    butil::IOBuf iobuf;
    std::string groupId = "4294967396";
    std::vector<ChunkServerInfo> chunkservers;
    std::map<std::string, std::set<std::string>> expectedRes;
    for (uint64_t i = 1; i <= 2; ++i) {
        ChunkServerInfo csInfo;
        GetCsInfoForTest(&csInfo, i);
        chunkservers.emplace_back(csInfo);
    }
    std::vector<CopysetInfo> copysets;
    std::set<std::string> gIds;
    for (int i = 1; i <= 5; ++i) {
        CopysetInfo copyset;
        copyset.set_logicalpoolid(1);
        copyset.set_copysetid(200 + i);
        copysets.emplace_back(copyset);
        uint64_t gId = (static_cast<uint64_t>(1) << 32) | (200 + i);
        gIds.emplace(std::to_string(gId));
    }

    // 1、ListChunkServersOnServer失败的情况
    EXPECT_CALL(*mdsClient_, ListChunkServersOnServer(serverId, _))
        .Times(1)
        .WillOnce(Return(-1));
    curve::tool::CopysetCheckCore copysetCheck1(mdsClient_, csClient_);
    ASSERT_EQ(-1, copysetCheck1.CheckCopysetsOnServer(serverId));
    ASSERT_EQ(0, copysetCheck1.GetCopysetStatistics().unhealthyRatio);
    ASSERT_EQ(expectedRes, copysetCheck1.GetCopysetsRes());

    // 3、一个chunkserver访问失败，一个chunkserver不健康的情况
    GetIoBufForTest(&iobuf, groupId, "FOLLOWER", true, false, false,
                                                 false, false, false);
    expectedRes[kTotal] = gIds;
    expectedRes[kTotal].emplace(groupId);
    expectedRes[kNoLeader].emplace(groupId);
    expectedRes[kPeerNotOnline] = gIds;
    EXPECT_CALL(*mdsClient_, ListChunkServersOnServer(serverId, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(chunkservers),
                        Return(0)));
    EXPECT_CALL(*csClient_, Init(_))
        .Times(2)
        .WillOnce(Return(-1))
        .WillOnce(Return(0));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<0>(iobuf),
                        Return(0)));
    EXPECT_CALL(*mdsClient_,
                    GetCopySetsInChunkServer("127.0.0.1:9191", _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(copysets),
                        Return(0)));
    std::vector<std::string> unhealthyCs;
    curve::tool::CopysetCheckCore copysetCheck2(mdsClient_, csClient_);
    ASSERT_EQ(-1, copysetCheck2.CheckCopysetsOnServer(serverId, &unhealthyCs));
    ASSERT_EQ(1, copysetCheck2.GetCopysetStatistics().unhealthyRatio);
    std::vector<std::string> unhealthyCsExpected =
                {"127.0.0.1:9191", "127.0.0.1:9192"};
    ASSERT_EQ(unhealthyCsExpected, unhealthyCs);
    std::set<std::string> expectedExcepCs =
                    {"127.0.0.1:9191"};
    ASSERT_EQ(expectedExcepCs, copysetCheck2.GetServiceExceptionChunkServer());
}

// CheckCopysetsInCluster正常情况
TEST_F(CopysetCheckCoreTest, CheckCopysetsInClusterNormal) {
    butil::IOBuf iobuf;
    GetIoBufForTest(&iobuf, "4294967396", "LEADER");
    std::map<std::string, std::set<std::string>> expectedRes;
    expectedRes[kTotal] = {"4294967396"};
    ServerInfo server;
    GetServerInfoForTest(&server);
    std::vector<ServerInfo> servers = {server};
    ChunkServerInfo chunkserver;
    GetCsInfoForTest(&chunkserver, 1);
    std::vector<ChunkServerInfo> chunkservers = {chunkserver};

    EXPECT_CALL(*mdsClient_, ListServersInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(servers),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, ListChunkServersOnServer(1, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(chunkservers),
                        Return(0)));
    EXPECT_CALL(*csClient_, Init(_))
        .Times(3)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(iobuf),
                        Return(0)));
    EXPECT_CALL(*csClient_, CheckChunkServerOnline())
        .Times(2)
        .WillRepeatedly(Return(true));
    EXPECT_CALL(*mdsClient_, GetMetric(_, _))
        .Times(3)
        .WillRepeatedly(DoAll(SetArgPointee<1>(0),
                        Return(0)));
    curve::tool::CopysetCheckCore copysetCheck1(mdsClient_, csClient_);
    ASSERT_EQ(0, copysetCheck1.CheckCopysetsInCluster());
    ASSERT_EQ(0, copysetCheck1.GetCopysetStatistics().unhealthyRatio);
    ASSERT_EQ(expectedRes, copysetCheck1.GetCopysetsRes());
}

TEST_F(CopysetCheckCoreTest, CheckCopysetsInClusterError) {
    butil::IOBuf iobuf;
    GetIoBufForTest(&iobuf, "4294967396", "LEADER");
    std::map<std::string, std::set<std::string>> expectedRes;
    ServerInfo server;
    GetServerInfoForTest(&server);
    std::vector<ServerInfo> servers = {server};
    ChunkServerInfo chunkserver;
    GetCsInfoForTest(&chunkserver, 1);
    std::vector<ChunkServerInfo> chunkservers = {chunkserver};

    // 1、ListServersInCluster失败
    EXPECT_CALL(*mdsClient_, ListServersInCluster(_))
        .Times(1)
        .WillOnce(Return(-1));
    curve::tool::CopysetCheckCore copysetCheck1(mdsClient_, csClient_);
    ASSERT_EQ(-1, copysetCheck1.CheckCopysetsInCluster());
    ASSERT_EQ(0, copysetCheck1.GetCopysetStatistics().unhealthyRatio);
    ASSERT_EQ(expectedRes, copysetCheck1.GetCopysetsRes());

    // 2、CheckCopysetsOnServer返回不为0
    EXPECT_CALL(*mdsClient_, ListServersInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(servers),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, ListChunkServersOnServer(1, _))
        .Times(1)
        .WillOnce(Return(-1));
    curve::tool::CopysetCheckCore copysetCheck2(mdsClient_, csClient_);
    ASSERT_EQ(-1, copysetCheck2.CheckCopysetsInCluster());
    ASSERT_EQ(0, copysetCheck2.GetCopysetStatistics().unhealthyRatio);
    ASSERT_EQ(expectedRes, copysetCheck2.GetCopysetsRes());

    // 3、GetMetric失败
    expectedRes[kTotal] = {"4294967396"};
    EXPECT_CALL(*mdsClient_, ListServersInCluster(_))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<0>(servers),
                        Return(0)));
    EXPECT_CALL(*mdsClient_, ListChunkServersOnServer(1, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkservers),
                        Return(0)));
    EXPECT_CALL(*csClient_, Init(_))
        .Times(6)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<0>(iobuf),
                        Return(0)));
    EXPECT_CALL(*csClient_, CheckChunkServerOnline())
        .Times(4)
        .WillRepeatedly(Return(true));
    EXPECT_CALL(*mdsClient_, GetMetric(_, _))
        .Times(2)
        .WillOnce(Return(-1))
        .WillRepeatedly(DoAll(SetArgPointee<1>(10),
                        Return(0)));
    // 获取operator失败
    curve::tool::CopysetCheckCore copysetCheck3(mdsClient_, csClient_);
    ASSERT_EQ(-1, copysetCheck3.CheckCopysetsInCluster());
    ASSERT_EQ(0, copysetCheck3.GetCopysetStatistics().unhealthyRatio);
    ASSERT_EQ(expectedRes, copysetCheck3.GetCopysetsRes());
    // operator数量大于0
    curve::tool::CopysetCheckCore copysetCheck4(mdsClient_, csClient_);
    ASSERT_EQ(-1, copysetCheck4.CheckCopysetsInCluster());
    ASSERT_EQ(0, copysetCheck4.GetCopysetStatistics().unhealthyRatio);
    ASSERT_EQ(expectedRes, copysetCheck4.GetCopysetsRes());
}
