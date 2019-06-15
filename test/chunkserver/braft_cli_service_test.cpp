/*
 * Project: curve
 * Created Date: 18-11-12
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>

#include <iostream>

#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/braft_cli_service.h"
#include "src/chunkserver/cli.h"
#include "proto/copyset.pb.h"
#include "test/chunkserver/chunkserver_test_util.h"

namespace curve {
namespace chunkserver {

class BraftCliServiceTest : public testing::Test {
 protected:
    static void SetUpTestCase() {
        LOG(INFO) << "BraftCliServiceTest " << "SetUpTestCase";
    }
    static void TearDownTestCase() {
        LOG(INFO) << "BraftCliServiceTest " << "TearDownTestCase";
    }
    virtual void SetUp() {
        Exec("mkdir 6");
        Exec("mkdir 7");
        Exec("mkdir 8");
    }
    virtual void TearDown() {
        Exec("rm -fr 6");
        Exec("rm -fr 7");
        Exec("rm -fr 8");
    }

 public:
    pid_t pid1;
    pid_t pid2;
    pid_t pid3;
};

butil::AtExitManager atExitManager;

TEST_F(BraftCliServiceTest, basic) {
    const char *ip = "127.0.0.1";
    int port = 9010;
    const char *confs = "127.0.0.1:9010:0,127.0.0.1:9011:0,127.0.0.1:9012:0";
    int snapshotInterval = 600;
    PeerId peer1("127.0.0.1:9010:0");
    PeerId peer2("127.0.0.1:9011:0");
    PeerId peer3("127.0.0.1:9012:0");

    /* default election timeout */
    int electionTimeoutMs = 3000;

    /**
     * Start three chunk server by fork
     */
    pid1 = fork();
    if (0 > pid1) {
        std::cerr << "fork chunkserver 1 failed" << std::endl;
        ASSERT_TRUE(false);
    } else if (0 == pid1) {
        const char *copysetdir = "local://./6";
        StartChunkserver(ip,
                         port + 0,
                         copysetdir,
                         confs,
                         snapshotInterval,
                         electionTimeoutMs);
        return;
    }

    pid2 = fork();
    if (0 > pid2) {
        std::cerr << "fork chunkserver 2 failed" << std::endl;
        ASSERT_TRUE(false);
    } else if (0 == pid2) {
        const char *copysetdir = "local://./7";
        StartChunkserver(ip,
                         port + 1,
                         copysetdir,
                         confs,
                         snapshotInterval,
                         electionTimeoutMs);
        return;
    }

    pid3 = fork();
    if (0 > pid3) {
        std::cerr << "fork chunkserver 3 failed" << std::endl;
        ASSERT_TRUE(false);
    } else if (0 == pid3) {
        const char *copysetdir = "local://./8";
        StartChunkserver(ip,
                         port + 2,
                         copysetdir,
                         confs,
                         snapshotInterval,
                         electionTimeoutMs);
        return;
    }

    /* 保证进程一定会退出 */
    class WaitpidGuard {
     public:
        WaitpidGuard(pid_t pid1, pid_t pid2, pid_t pid3) {
            pid1_ = pid1;
            pid2_ = pid2;
            pid3_ = pid3;
        }
        virtual ~WaitpidGuard() {
            int waitState;
            kill(pid1_, SIGINT);
            waitpid(pid1_, &waitState, 0);
            kill(pid2_, SIGINT);
            waitpid(pid2_, &waitState, 0);
            kill(pid3_, SIGINT);
            waitpid(pid3_, &waitState, 0);
        }
     private:
        pid_t pid1_;
        pid_t pid2_;
        pid_t pid3_;
    };
    WaitpidGuard waitpidGuard(pid1, pid2, pid3);

    PeerId leader;
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    Configuration conf;
    conf.parse_from(confs);

    ::usleep(1.2 * 1000 * electionTimeoutMs);
    butil::Status status =
        WaitLeader(logicPoolId, copysetId, conf, &leader, electionTimeoutMs);
    ASSERT_TRUE(status.ok());

    braft::cli::CliOptions options;
    options.timeout_ms = 1500;
    options.max_retry = 3;

    /* add peer - 非法 copyset */
    {
        PeerId leaderId = leader;
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
        AddPeerRequest request;
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId);
        request.set_leader_id(leaderId.to_string());
        request.set_peer_id(peer1.to_string());
        AddPeerResponse response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService_Stub stub(&channel);
        stub.add_peer(&cntl, &request, &response, NULL);
        LOG(INFO) << "add peer: " << cntl.ErrorCode() << ", "
                  << cntl.ErrorText();
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }
    /* add peer - 非法 peerid */
    {
        PeerId leaderId = leader;
        butil::Status st = GetLeader(logicPoolId, copysetId, conf, &leaderId);
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
        AddPeerRequest request;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_leader_id(leaderId.to_string());
        request.set_peer_id("129.0.0");
        AddPeerResponse response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService_Stub stub(&channel);
        stub.add_peer(&cntl, &request, &response, NULL);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(EINVAL, cntl.ErrorCode());
        LOG(INFO) << "add peer: " << cntl.ErrorText();
    }
    /* add peer - 发送给不是leader的peer */
    {
        PeerId leaderId;
        LOG(INFO) << "true leader is: " << leader.to_string();
        if (0
            == strcmp(leader.to_string().c_str(), peer1.to_string().c_str())) {
            leaderId = peer2;
        } else {
            leaderId = peer1;
        }
        LOG(INFO) << "false leader is: " << leaderId.to_string();
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
        AddPeerRequest request;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_leader_id(leaderId.to_string());
        request.set_peer_id(peer1.to_string());
        AddPeerResponse response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService_Stub stub(&channel);
        stub.add_peer(&cntl, &request, &response, NULL);
        LOG(INFO) << "add peer: " << cntl.ErrorCode() << ", "
                  << cntl.ErrorText();
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(EPERM, cntl.ErrorCode());
    }
    /* remove peer - 非法 copyset */
    {
        PeerId leaderId = leader;
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
        RemovePeerRequest request;
        /* 非法 copyset */
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId);
        request.set_leader_id(leaderId.to_string());
        request.set_peer_id(peer1.to_string());
        RemovePeerResponse response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService_Stub stub(&channel);
        stub.remove_peer(&cntl, &request, &response, NULL);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }
    /* remove peer - 非法 peer id */
    {
        PeerId leaderId = leader;
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
        RemovePeerRequest request;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_leader_id(leaderId.to_string());
        request.set_peer_id("129.0.0");
        RemovePeerResponse response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService_Stub stub(&channel);
        stub.remove_peer(&cntl, &request, &response, NULL);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(EINVAL, cntl.ErrorCode());
    }
    /* remove peer - 发送给不是 leader 的 peer */
    {
        PeerId leaderId;
        LOG(INFO) << "true leader is: " << leader.to_string();
        if (0
            == strcmp(leader.to_string().c_str(), peer1.to_string().c_str())) {
            leaderId = peer2;
        } else {
            leaderId = peer1;
        }
        LOG(INFO) << "false leader is: " << leaderId.to_string();
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
        RemovePeerRequest request;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_leader_id(leaderId.to_string());
        request.set_peer_id(peer1.to_string());
        RemovePeerResponse response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService_Stub stub(&channel);
        stub.remove_peer(&cntl, &request, &response, NULL);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(EPERM, cntl.ErrorCode());
    }
    /* transfer leader - 非法 copyset */
    {
        PeerId leaderId = leader;
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
        TransferLeaderRequest request;
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId);
        request.set_leader_id(leaderId.to_string());
        request.set_peer_id(peer1.to_string());
        TransferLeaderResponse response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);
        CliService_Stub stub(&channel);
        stub.transfer_leader(&cntl, &request, &response, NULL);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }
    /* transfer leader to leader */
    {
        PeerId leaderId = leader;
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
        TransferLeaderRequest request;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_leader_id(leaderId.to_string());
        request.set_peer_id(leaderId.to_string());
        TransferLeaderResponse response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);
        CliService_Stub stub(&channel);
        stub.transfer_leader(&cntl, &request, &response, NULL);
        ASSERT_FALSE(cntl.Failed());
    }
    /* transfer leader - 非法 peer */
    {
        PeerId leaderId = leader;
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
        TransferLeaderRequest request;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_leader_id(leaderId.to_string());
        request.set_peer_id("129.0.0");
        TransferLeaderResponse response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);
        CliService_Stub stub(&channel);
        stub.transfer_leader(&cntl, &request, &response, NULL);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(EINVAL, cntl.ErrorCode());
    }
    /* get leader - 非法 copyset */
    {
        PeerId leaderId = leaderId;
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
        CliService_Stub stub(&channel);
        GetLeaderRequest request;
        GetLeaderResponse response;
        brpc::Controller cntl;
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId);
        stub.get_leader(&cntl, &request, &response, NULL);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }
}

}  // namespace chunkserver
}  // namespace curve
