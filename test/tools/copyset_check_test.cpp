/*
 * Project: curve
 * File Created: 2019-11-1
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>

#include "src/tools/copyset_check.h"
#include "test/client/fake/fakeMDS.h"

using curve::mds::topology::ChunkServerStatus;
using curve::mds::topology::DiskState;
using curve::mds::topology::OnlineState;
using curve::mds::topology::GetChunkServerListInCopySetsResponse;
using curve::mds::topology::ListChunkServerResponse;
using curve::mds::topology::ListPhysicalPoolResponse;
using curve::mds::topology::ListPoolZoneResponse;
using curve::mds::topology::ListZoneServerResponse;
using curve::mds::topology::ListChunkServerResponse;
using curve::mds::topology::GetChunkServerInfoResponse;

std::string metaserver_addr = "127.0.0.1:9170";  // NOLINT
uint32_t chunk_size = 4*1024*1024;  // NOLINT
uint32_t segment_size = 1*1024*1024*1024;  // NOLINT

DECLARE_string(chunkserver_list);
DECLARE_string(mdsAddr);
DECLARE_uint32(logicalPoolId);
DECLARE_uint32(copysetId);
DECLARE_uint32(chunkserverId);
DECLARE_string(chunkserverAddr);
DECLARE_uint32(serverId);
DECLARE_string(serverIp);
DECLARE_bool(detail);

class CopysetCheckTest : public ::testing::Test {
 protected:
    CopysetCheckTest() : fakemds("test") {}
    void SetUp() {
        FLAGS_mdsAddr = "127.0.0.1:9999,127.0.0.1:9170";
        FLAGS_chunkserver_list =
            "127.0.0.1:9191:0,127.0.0.1:9192:0,127.0.0.1:9193:0";
        FLAGS_detail = true;
        fakemds.Initialize();
        fakemds.StartService();
        fakemds.CreateFakeChunkservers(false);
    }
    void TearDown() {
        FLAGS_logicalPoolId = 0;
        FLAGS_copysetId = 0;
        FLAGS_chunkserverId = 0;
        FLAGS_chunkserverAddr = "";
        FLAGS_serverId = 0;
        FLAGS_serverIp = "";
        fakemds.UnInitialize();
    }

    void GetCopysetInfoForTest(curve::mds::topology::CopySetServerInfo* info,
                                int num) {
        for (int i = 0; i < num; ++i) {
            curve::mds::topology::ChunkServerLocation *csLoc =
                                                info->add_cslocs();
            csLoc->set_chunkserverid(i);
            csLoc->set_hostip("127.0.0.1");
            csLoc->set_port(9191 + i);
        }
        info->set_copysetid(1);
    }

    void GetCsInfoForTest(curve::mds::topology::ChunkServerInfo *csInfo,
                            int port) {
        csInfo->set_chunkserverid(1);
        csInfo->set_disktype("ssd");
        csInfo->set_hostip("127.0.0.1");
        csInfo->set_port(port);
        csInfo->set_status(ChunkServerStatus::READWRITE);
        csInfo->set_diskstatus(DiskState::DISKNORMAL);
        csInfo->set_onlinestate(OnlineState::ONLINE);
        csInfo->set_mountpoint("/test");
        csInfo->set_diskcapacity(1024);
        csInfo->set_diskused(512);
    }

    void GetZoneInfoForTest(curve::mds::topology::ZoneInfo *zoneInfo) {
        zoneInfo->set_zoneid(1);
        zoneInfo->set_zonename("testZone");
        zoneInfo->set_physicalpoolid(1);
        zoneInfo->set_physicalpoolname("testPool");
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

    void GetIoBufForTest(butil::IOBuf& buf, bool isLeader, bool snapshot, // NOLINT
                        bool noLeader, bool peersLess, bool gapBig) {
        butil::IOBufBuilder os;
        os << "[8589934692]\r\n";
        if (peersLess) {
            os << "peers: 127.0.0.1:9191:0 127.0.0.1:9192:0\r\n";
        } else {
            os << "peers: 127.0.0.1:9191:0 127.0.0.1:9192:0 127.0.0.1:9193:0\r\n";  // NOLINT
        }
        os << "storage: [2581, 2580]\n";
        os << "last_log_id: (index=2580,term=4)\n";
        os << "state_machine: Idle\r\n";
        if (isLeader) {
            os << "state: " << "LEADER" << "\r\n";
            os << "replicator_123: next_index=";
            if (gapBig) {
                os << "1000";
            } else {
                os << "2581";
            }
            os << " flying_append_entries_size=0 ";
            if (snapshot) {
                os << "installing snapshot {1234, 3} ";
            } else {
                os << "idle ";
            }
            os << "hc=4211759 ac=1089 ic=0\r\n";
        } else {
            os << "state: " << "FOLLOWER" << "\r\n";
            if (noLeader) {
                os << "leader: " << "0.0.0.0:0:0\r\n";
            } else {
                os << "leader: " << "127.0.0.1:9192:0\r\n";
            }
        }
        os.move_to(buf);
    }
    FakeMDS fakemds;
};

TEST_F(CopysetCheckTest, testCheckOneCopyset) {
    curve::tool::CopysetCheck copysetCheck;
    // 为了测试server是否服务
    FakeMDSTopologyService* topology = fakemds.GetTopologyService();
    std::unique_ptr<ListPhysicalPoolResponse> listPoolResp(
                                    new ListPhysicalPoolResponse());
    listPoolResp->set_statuscode(kTopoErrCodeSuccess);
    auto poolPtr = listPoolResp->add_physicalpoolinfos();
    poolPtr->set_physicalpoolid(1);
    poolPtr->set_physicalpoolname("testPool");
    std::unique_ptr<FakeReturn> fakelistpoolret(
         new FakeReturn(nullptr, static_cast<void*>(listPoolResp.get())));
    topology->fakelistpoolret_ = fakelistpoolret.get();
    ASSERT_EQ(0, copysetCheck.Init());
    // 没有指定逻辑池和copyset的话返回失败
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-copyset"));
    FLAGS_logicalPoolId = 2;
    FLAGS_copysetId = 100;
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-nothins"));
    copysetCheck.PrintHelp("check-nothins");
    copysetCheck.PrintHelp("check-copyset");
    // 设置好IOBuf
    butil::IOBuf leaderBuf;
    GetIoBufForTest(leaderBuf, true, false, false, false, false);
    butil::IOBuf followerBuf;
    GetIoBufForTest(followerBuf, false, false, false, false, false);
    // 1、GetChunkServerListInCopySets失败的情况
    // 设置GetChunkServerListInCopySets的返回
    std::unique_ptr<GetChunkServerListInCopySetsResponse> response(
                            new GetChunkServerListInCopySetsResponse());
    std::unique_ptr<brpc::Controller> cntl(new brpc::Controller());
    cntl->SetFailed("error for test");
    std::unique_ptr<FakeReturn> fakeRet(
            new FakeReturn(cntl.get(), static_cast<void*>(response.get())));
    topology->SetFakeReturn(fakeRet.get());
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-copyset"));
    response->set_statuscode(-1);
    cntl->Reset();
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-copyset"));

    // 2、正常情况
    // 设置GetChunkServerListInCopySets的返回
    curve::mds::topology::CopySetServerInfo csInfo;
    GetCopysetInfoForTest(&csInfo, 3);
    response->set_statuscode(kTopoErrCodeSuccess);
    auto infoPtr = response->add_csinfo();
    infoPtr->CopyFrom(csInfo);
    // 设置chunkserver的返回
    std::vector<FakeRaftStateService *> statServices =
                                    fakemds.GetRaftStateServie();
    statServices[0]->SetBuf(followerBuf);
    statServices[1]->SetBuf(leaderBuf);
    statServices[2]->SetBuf(followerBuf);
    ASSERT_EQ(0, copysetCheck.RunCommand("check-copyset"));

    // 3、获取chunkserver状态失败的情况
    for (auto service : statServices) {
        service->SetFailed(true);
    }
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-copyset"));

    // 4、iobuf没找到copyset的情况
    for (auto service : statServices) {
        butil::IOBuf buf;
        service->SetFailed(false);
        service->SetBuf(buf);
    }
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-copyset"));

    // 5、peer数量不足的情况
    GetIoBufForTest(leaderBuf, true, false, false, true, false);
    statServices[1]->SetBuf(leaderBuf);
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-copyset"));

    // 6、index之间差距太大的情况
    GetIoBufForTest(leaderBuf, true, false, false, false, true);
    statServices[1]->SetBuf(leaderBuf);
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-copyset"));

    // 7、正在安装快照的情况
    GetIoBufForTest(leaderBuf, true, true, false, false, false);
    statServices[1]->SetBuf(leaderBuf);
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-copyset"));

    // 8、解析失败的情况
    leaderBuf.pop_back(20);
    statServices[1]->SetBuf(leaderBuf);
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-copyset"));

    // 9、follower中的leader为空的情况
    GetIoBufForTest(followerBuf, false, false, true, false, false);
    statServices[0]->SetBuf(followerBuf);
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-copyset"));
}

TEST_F(CopysetCheckTest, testCheckChunkserver) {
    // 为了测试server是否服务
    FakeMDSTopologyService* topology = fakemds.GetTopologyService();
    std::unique_ptr<ListPhysicalPoolResponse> listPoolResp(
                                    new ListPhysicalPoolResponse());
    listPoolResp->set_statuscode(kTopoErrCodeSuccess);
    auto poolPtr = listPoolResp->add_physicalpoolinfos();
    poolPtr->set_physicalpoolid(1);
    poolPtr->set_physicalpoolname("testPool");
    std::unique_ptr<FakeReturn> fakelistpoolret(
         new FakeReturn(nullptr, static_cast<void*>(listPoolResp.get())));
    topology->fakelistpoolret_ = fakelistpoolret.get();
    curve::tool::CopysetCheck copysetCheck;
    ASSERT_EQ(0, copysetCheck.Init());
    // 没有指定chunkserver的话报错
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-chunkserver"));
    FLAGS_chunkserverId = 1;
    copysetCheck.PrintHelp("check-chunkserver");
    // 设置好IOBuf
    butil::IOBuf leaderBuf;
    GetIoBufForTest(leaderBuf, true, false, false, false, false);
    butil::IOBuf followerBuf;
    GetIoBufForTest(followerBuf, false, false, false, false, false);
    // 1、GetChunkServerInfo失败的情况
    std::unique_ptr<GetChunkServerInfoResponse> response(
                                new GetChunkServerInfoResponse());
    std::unique_ptr<brpc::Controller> cntl(new brpc::Controller());
    cntl->SetFailed("error for test");
    std::unique_ptr<FakeReturn> fakeRet(
                new FakeReturn(cntl.get(), static_cast<void*>(response.get())));
    topology->SetFakeReturn(fakeRet.get());
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-chunkserver"));
    response->set_statuscode(-1);
    cntl->Reset();
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-chunkserver"));

    // 2、正常情况
    // 设置GetChunkServerInfo的返回
    std::vector<FakeRaftStateService *> statServices =
                                    fakemds.GetRaftStateServie();
    FLAGS_chunkserverId = 1;
    curve::mds::topology::ChunkServerInfo* csInfo =
                    new curve::mds::topology::ChunkServerInfo();
    GetCsInfoForTest(csInfo, 9191);
    response->set_statuscode(kTopoErrCodeSuccess);
    response->set_allocated_chunkserverinfo(csInfo);
    // 设置chunkserver的返回
    statServices[0]->SetBuf(leaderBuf);
    ASSERT_EQ(0, copysetCheck.RunCommand("check-chunkserver"));
    FLAGS_chunkserverId = 0;
    FLAGS_chunkserverAddr = "127.0.0.1:9191";
    ASSERT_EQ(0, copysetCheck.RunCommand("check-chunkserver"));

    // 3、获取chunkserver状态失败的情况
    statServices[0]->SetFailed(true);
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-chunkserver"));

    // 4、iobuf没找到copyset的情况
    butil::IOBuf buf;
    statServices[0]->SetFailed(false);
    statServices[0]->SetBuf(buf);
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-chunkserver"));

    // 5、peer数量不足的情况
    butil::IOBuf leaderBuf2;
    GetIoBufForTest(leaderBuf2, true, false, false, true, false);
    statServices[0]->SetBuf(followerBuf);
    statServices[1]->SetBuf(leaderBuf2);
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-chunkserver"));

    // 6、index之间差距太大的情况
    GetIoBufForTest(leaderBuf2, true, false, false, false, true);
    statServices[0]->SetBuf(leaderBuf);
    statServices[1]->SetBuf(leaderBuf2);
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-chunkserver"));

    // 7、正在安装快照的情况
    GetIoBufForTest(leaderBuf2, true, true, false, false, false);
    statServices[0]->SetBuf(leaderBuf);
    statServices[1]->SetBuf(leaderBuf2);
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-chunkserver"));

    // 8、解析失败的情况
    leaderBuf2.pop_back(20);
    statServices[0]->SetBuf(leaderBuf);
    statServices[1]->SetBuf(leaderBuf2);
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-chunkserver"));

    // 9、follower中的leader为空的情况
    GetIoBufForTest(followerBuf, false, false, true, false, false);
    statServices[0]->SetBuf(followerBuf);
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-chunkserver"));
}

TEST_F(CopysetCheckTest, testCheckServer) {
    // 为了测试server是否服务
    FakeMDSTopologyService* topology = fakemds.GetTopologyService();
    std::unique_ptr<ListPhysicalPoolResponse> listPoolResp(
                                    new ListPhysicalPoolResponse());
    listPoolResp->set_statuscode(kTopoErrCodeSuccess);
    auto poolPtr = listPoolResp->add_physicalpoolinfos();
    poolPtr->set_physicalpoolid(1);
    poolPtr->set_physicalpoolname("testPool");
    std::unique_ptr<FakeReturn> fakelistpoolret(
         new FakeReturn(nullptr, static_cast<void*>(listPoolResp.get())));
    topology->fakelistpoolret_ = fakelistpoolret.get();
    curve::tool::CopysetCheck copysetCheck;
    ASSERT_EQ(0, copysetCheck.Init());
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-chunkserver"));
    FLAGS_serverId = 1;
    FLAGS_serverIp = "127.0.0.1";
    copysetCheck.PrintHelp("check-chunkserver");
    // 设置好IOBuf
    butil::IOBuf leaderBuf;
    GetIoBufForTest(leaderBuf, true, false, false, false, false);
    butil::IOBuf followerBuf;
    GetIoBufForTest(followerBuf, false, false, false, false, false);
    std::unique_ptr<ListChunkServerResponse> response(
                                new ListChunkServerResponse());
    std::unique_ptr<brpc::Controller> cntl(new brpc::Controller());
    std::unique_ptr<FakeReturn> fakeRet(
            new FakeReturn(cntl.get(), static_cast<void*>(response.get())));
    topology->SetFakeReturn(fakeRet.get());

    // 1、发送RPC失败的情况
    cntl->SetFailed("error for test");
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-server"));
    response->set_statuscode(-1);
    cntl->Reset();
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-server"));

    // 2、正常情况
    for (int i = 0; i < 3; ++i) {
        auto csPtr = response->add_chunkserverinfos();
        GetCsInfoForTest(csPtr, 9191 + i);
    }
    cntl->Reset();
    response->set_statuscode(kTopoErrCodeSuccess);
    // 设置chunkserver的返回
    std::vector<FakeRaftStateService *> statServices =
                                    fakemds.GetRaftStateServie();
    statServices[0]->SetBuf(followerBuf);
    statServices[1]->SetBuf(leaderBuf);
    statServices[2]->SetBuf(followerBuf);
    ASSERT_EQ(0, copysetCheck.RunCommand("check-server"));
}

TEST_F(CopysetCheckTest, testCheckCluster) {
    // 为了测试server是否服务
    FakeMDSTopologyService* topology = fakemds.GetTopologyService();
    std::unique_ptr<ListPhysicalPoolResponse> listPoolResp(
                                    new ListPhysicalPoolResponse());
    listPoolResp->set_statuscode(kTopoErrCodeSuccess);
    auto poolPtr = listPoolResp->add_physicalpoolinfos();
    poolPtr->set_physicalpoolid(1);
    poolPtr->set_physicalpoolname("testPool");
    std::unique_ptr<FakeReturn> fakelistpoolret(
         new FakeReturn(nullptr, static_cast<void*>(listPoolResp.get())));
    topology->fakelistpoolret_ = fakelistpoolret.get();
    curve::tool::CopysetCheck copysetCheck;
    ASSERT_EQ(0, copysetCheck.Init());
    copysetCheck.PrintHelp("check-cluster");
    std::unique_ptr<brpc::Controller> cntl(new brpc::Controller());
    // 设置好IOBuf
    butil::IOBuf leaderBuf;
    GetIoBufForTest(leaderBuf, true, false, false, false, false);
    butil::IOBuf followerBuf;
    GetIoBufForTest(followerBuf, false, false, false, false, false);
    // 正常情况
    // 设置ListPoolZone的返回
    std::unique_ptr<ListPoolZoneResponse> listZoneResp(
                        new ListPoolZoneResponse());
    listZoneResp->set_statuscode(kTopoErrCodeSuccess);
    auto zonePtr = listZoneResp->add_zones();
    GetZoneInfoForTest(zonePtr);
    std::unique_ptr<FakeReturn>  fakelistzoneret(
         new FakeReturn(nullptr, static_cast<void*>(listZoneResp.get())));
    topology->fakelistzoneret_ = fakelistzoneret.get();
    // 设置ListZoneServer的返回
    std::unique_ptr<ListZoneServerResponse> listServerResp(
                    new ListZoneServerResponse());
    listServerResp->set_statuscode(kTopoErrCodeSuccess);
    auto serverPtr = listServerResp->add_serverinfo();
    GetServerInfoForTest(serverPtr);
    std::unique_ptr<FakeReturn> fakelistserverret(
         new FakeReturn(nullptr, static_cast<void*>(listServerResp.get())));
    topology->fakelistserverret_ = fakelistserverret.get();
    // 设置ListChunkserver的返回
    std::unique_ptr<ListChunkServerResponse> listCsResp(
                            new ListChunkServerResponse());
    std::unique_ptr<FakeReturn> fakeRet(new FakeReturn(
                nullptr, static_cast<void*>(listCsResp.get())));
    for (int i = 0; i < 3; ++i) {
        auto csPtr = listCsResp->add_chunkserverinfos();
        GetCsInfoForTest(csPtr, 9191 + i);
    }
    cntl->Reset();
    listCsResp->set_statuscode(kTopoErrCodeSuccess);
    topology->SetFakeReturn(fakeRet.get());
    // 设置chunkserver的返回
    std::vector<FakeRaftStateService *> statServices =
                                    fakemds.GetRaftStateServie();
    statServices[0]->SetBuf(followerBuf);
    statServices[1]->SetBuf(leaderBuf);
    statServices[2]->SetBuf(followerBuf);
    ASSERT_EQ(0, copysetCheck.RunCommand("check-cluster"));

    // 发送RPC失败的情况
    // ListZoneServer失败的情况
    listServerResp->set_statuscode(-1);
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-cluster"));
    cntl->SetFailed("Failed for test");
    fakelistserverret->controller_ = cntl.get();
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-cluster"));
    // ListPoolZone失败的情况
    listZoneResp->set_statuscode(-1);
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-cluster"));
    fakelistzoneret->controller_ = cntl.get();
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-cluster"));
    // ListPysicalPool失败的情况
    listPoolResp->set_statuscode(-1);
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-cluster"));
    fakelistpoolret->controller_ = cntl.get();
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-cluster"));
}
