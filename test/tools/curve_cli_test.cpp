/*
 * Project: curve
 * File Created: 2019-12-18
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include <brpc/controller.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <string>
#include "src/tools/curve_cli.h"
#include "test/tools/mock_cli_service.h"

using ::testing::_;
using ::testing::Return;
using ::testing::Invoke;
using ::testing::DoAll;
using ::testing::SetArgPointee;

DECLARE_int32(timeout_ms);
DECLARE_int32(max_retry);
DECLARE_string(conf);
DECLARE_string(peer);
DECLARE_string(new_conf);
DECLARE_uint32(logic_pool_id);
DECLARE_uint32(copyset_id);
DECLARE_bool(affirm);

namespace curve {
namespace tool {

class CurveCliTest : public ::testing::Test {
 protected:
    CurveCliTest() {}
    void SetUp() {
        server = new brpc::Server();
        mockCliService = new MockCliService();
        ASSERT_EQ(0, server->AddService(mockCliService,
                                      brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server->Start("127.0.0.1:9192", nullptr));
        FLAGS_affirm = false;
    }
    void TearDown() {
        server->Stop(0);
        server->Join();
        delete server;
        server = nullptr;
        delete mockCliService;
        mockCliService = nullptr;
    }
    brpc::Server *server;
    MockCliService *mockCliService;
    const std::string conf = "127.0.0.1:9192:0";
    const std::string peer = "127.0.0.1:9192:0";
};

TEST_F(CurveCliTest, RemovePeer) {
    curve::tool::CurveCli curveCli;
    curveCli.PrintHelp("remove-peer");
    curveCli.PrintHelp("test");
    curveCli.RunCommand("test");
    // peer为空
    FLAGS_peer = "";
    ASSERT_EQ(-1, curveCli.RunCommand("remove-peer"));
    // conf为空
    FLAGS_peer = peer;
    FLAGS_conf = "";
    ASSERT_EQ(-1, curveCli.RunCommand("remove-peer"));
    // 解析conf失败
    FLAGS_conf = "1234";
    ASSERT_EQ(-1, curveCli.RunCommand("remove-peer"));
    // 解析peer失败
    FLAGS_conf = conf;
    FLAGS_peer = "1234";
    // 执行变更成功
    FLAGS_peer = peer;
    curve::common::Peer* targetPeer = new curve::common::Peer;
    targetPeer->set_address(peer);
    GetLeaderResponse2 response;
    response.set_allocated_leader(targetPeer);
    EXPECT_CALL(*mockCliService, GetLeader(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const GetLeaderRequest2 *request,
                          GetLeaderResponse2 *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    })));
    EXPECT_CALL(*mockCliService, RemovePeer(_, _, _, _))
        .WillOnce(Invoke([](RpcController *controller,
                          const RemovePeerRequest2 *request,
                          RemovePeerResponse2 *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    }));
    ASSERT_EQ(0, curveCli.RunCommand("remove-peer"));
    // 执行变更失败
    EXPECT_CALL(*mockCliService, GetLeader(_, _, _, _))
        .WillOnce(
                Invoke([](RpcController *controller,
                          const GetLeaderRequest2 *request,
                          GetLeaderResponse2 *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                          brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                          cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, curveCli.RunCommand("remove-peer"));
}

TEST_F(CurveCliTest, TransferLeader) {
    curve::tool::CurveCli curveCli;
    curveCli.PrintHelp("transfer-leader");
    // peer为空
    FLAGS_peer = "";
    ASSERT_EQ(-1, curveCli.RunCommand("transfer-leader"));
    // conf为空
    FLAGS_peer = peer;
    FLAGS_conf = "";
    ASSERT_EQ(-1, curveCli.RunCommand("transfer-leader"));
    // 解析conf失败
    FLAGS_conf = "1234";
    ASSERT_EQ(-1, curveCli.RunCommand("transfer-leader"));
    // 解析peer失败
    FLAGS_conf = conf;
    FLAGS_peer = "1234";
    ASSERT_EQ(-1, curveCli.RunCommand("transfer-leader"));
    // 执行变更成功
    FLAGS_peer = peer;
    curve::common::Peer* targetPeer = new curve::common::Peer;
    targetPeer->set_address(peer);
    GetLeaderResponse2 response;
    response.set_allocated_leader(targetPeer);
    EXPECT_CALL(*mockCliService, GetLeader(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const GetLeaderRequest2 *request,
                          GetLeaderResponse2 *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, curveCli.RunCommand("transfer-leader"));
    // 执行变更失败
    EXPECT_CALL(*mockCliService, GetLeader(_, _, _, _))
        .WillOnce(
                Invoke([](RpcController *controller,
                          const GetLeaderRequest2 *request,
                          GetLeaderResponse2 *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                          brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                          cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, curveCli.RunCommand("transfer-leader"));
}

TEST_F(CurveCliTest, ResetPeer) {
    curve::tool::CurveCli curveCli;
    curveCli.PrintHelp("reset-peer");
    // peer为空
    FLAGS_peer = "";
    ASSERT_EQ(-1, curveCli.RunCommand("reset-peer"));
    // newConf为空
    FLAGS_peer = peer;
    FLAGS_new_conf = "";
    ASSERT_EQ(-1, curveCli.RunCommand("reset-peer"));
    // 解析newConf失败
    FLAGS_new_conf = "1234";
    ASSERT_EQ(-1, curveCli.RunCommand("reset-peer"));
    // 解析peer失败
    FLAGS_new_conf = conf;
    FLAGS_peer = "1234";
    ASSERT_EQ(-1, curveCli.RunCommand("reset-peer"));
    // newConf有三个副本
    FLAGS_peer = peer;
    FLAGS_new_conf = "127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0";
    ASSERT_EQ(-1, curveCli.RunCommand("reset-peer"));
    // newConf不包含peer
    FLAGS_new_conf = "127.0.0.1:8201:0";
    ASSERT_EQ(-1, curveCli.RunCommand("reset-peer"));
    // 执行变更成功
    FLAGS_new_conf = conf;
    EXPECT_CALL(*mockCliService, ResetPeer(_, _, _, _))
        .WillOnce(Invoke([](RpcController *controller,
                          const ResetPeerRequest2 *request,
                          ResetPeerResponse2 *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    }));
    ASSERT_EQ(0, curveCli.RunCommand("reset-peer"));
    // 执行变更失败
     EXPECT_CALL(*mockCliService, ResetPeer(_, _, _, _))
        .WillOnce(Invoke([](RpcController *controller,
                          const ResetPeerRequest2 *request,
                          ResetPeerResponse2 *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                          brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                          cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, curveCli.RunCommand("reset-peer"));
}

}  // namespace tool
}  // namespace curve
