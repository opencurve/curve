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
#include "src/chunkserver/braft_cli_service2.h"
#include "src/chunkserver/cli.h"
#include "proto/copyset.pb.h"
#include "test/chunkserver/chunkserver_test_util.h"
#include "src/common/uuid.h"

namespace curve {
namespace chunkserver {

using curve::common::UUIDGenerator;

class BraftCliService2Test : public testing::Test {
 protected:
    static void SetUpTestCase() {
        LOG(INFO) << "BraftCliServiceTest " << "SetUpTestCase";
    }
    static void TearDownTestCase() {
        LOG(INFO) << "BraftCliServiceTest " << "TearDownTestCase";
    }
    virtual void SetUp() {
        UUIDGenerator uuidGenerator;
        dir1 = uuidGenerator.GenerateUUID();
        dir2 = uuidGenerator.GenerateUUID();
        dir3 = uuidGenerator.GenerateUUID();
        Exec(("mkdir " + dir1).c_str());
        Exec(("mkdir " + dir2).c_str());
        Exec(("mkdir " + dir3).c_str());
    }
    virtual void TearDown() {
        Exec(("rm -fr " + dir1).c_str());
        Exec(("rm -fr " + dir2).c_str());
        Exec(("rm -fr " + dir3).c_str());
    }

 public:
    const char *ip    = "127.0.0.1";
    int port          = 9000;
    const char *confs = "127.0.0.1:9000:0,127.0.0.1:9001:0,127.0.0.1:9002:0";
    int snapshotInterval  = 600;
    int electionTimeoutMs = 3000;

    pid_t pid1;
    pid_t pid2;
    pid_t pid3;

    std::string dir1;
    std::string dir2;
    std::string dir3;
};

butil::AtExitManager atExitManager;

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

TEST_F(BraftCliService2Test, basic2) {
    Peer peer1;
    peer1.set_address("127.0.0.1:9000:0");
    Peer peer2;
    peer2.set_address("127.0.0.1:9001:0");
    Peer peer3;
    peer3.set_address("127.0.0.1:9002:0");

    PeerId leaderId;
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    Configuration conf;
    conf.parse_from(confs);

    pid1 = fork();
    if (0 > pid1) {
        std::cerr << "fork chunkserver 1 failed" << std::endl;
        ASSERT_TRUE(false);
    } else if (0 == pid1) {
        std::string copysetdir = "local://./" + dir1;
        StartChunkserver(ip,
                         port + 0,
                         copysetdir.c_str(),
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
        std::string copysetdir = "local://./" + dir2;
        StartChunkserver(ip,
                         port + 1,
                         copysetdir.c_str(),
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
        std::string copysetdir = "local://./" + dir3;
        StartChunkserver(ip,
                         port + 2,
                         copysetdir.c_str(),
                         confs,
                         snapshotInterval,
                         electionTimeoutMs);
        return;
    }

    /* 保证进程一定会退出 */
    WaitpidGuard waitpidGuard(pid1, pid2, pid3);

    ::usleep(1.2 * 1000 * electionTimeoutMs);
    butil::Status status =
        WaitLeader(logicPoolId, copysetId, conf, &leaderId, electionTimeoutMs);
    butil::EndPoint leaderAddr = leaderId.addr;
    Peer gLeader;
    gLeader.set_address(leaderId.to_string());
    ASSERT_TRUE(status.ok());

    braft::cli::CliOptions options;
    options.timeout_ms = 3000;
    options.max_retry = 3;

    /* add peer - 非法copyset */
    {
        Peer *leaderPeer = new Peer();
        Peer *peer = new Peer();
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderAddr, NULL));

        AddPeerRequest2 request;
        // 设置一个不存在的logicPoolId
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId);
        request.set_allocated_leader(leaderPeer);
        *leaderPeer = gLeader;
        request.set_allocated_addpeer(peer);
        *peer = peer1;

        AddPeerResponse2 response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService2_Stub stub(&channel);
        stub.AddPeer(&cntl, &request, &response, NULL);
        LOG(INFO) << "add peer: " << cntl.ErrorCode() << ", "
                  << cntl.ErrorText();
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }
    /* add peer - 非法peerid */
    {
        Peer *leaderPeer = new Peer();
        Peer *peer = new Peer();
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderAddr, NULL));

        AddPeerRequest2 request;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_allocated_leader(leaderPeer);
        *leaderPeer = gLeader;
        request.set_allocated_addpeer(peer);
        // request中的peer id是非法的
        peer->set_address("127.0.0");

        AddPeerResponse2 response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService2_Stub stub(&channel);
        stub.AddPeer(&cntl, &request, &response, NULL);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(EINVAL, cntl.ErrorCode());
        LOG(INFO) << "add peer: " << cntl.ErrorText();
    }
    /* add peer - 发送给不是leader的peer */
    {
        Peer *leaderPeer = new Peer();
        Peer *peer = new Peer();
        PeerId leaderId;
        LOG(INFO) << "true leader is: " << gLeader.address();
        // 找一个不是leader的peer，然后将配置变更请求发送给它处理
        if (0 == strcmp(gLeader.address().c_str(), peer1.address().c_str())) {
            leaderId.parse(peer2.address());
            *leaderPeer = peer2;
        } else {
            leaderId.parse(peer1.address());
            *leaderPeer = peer1;
        }
        LOG(INFO) << "false leader is: " << leaderId.to_string();
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));

        AddPeerRequest2 request;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_allocated_leader(leaderPeer);
        request.set_allocated_addpeer(peer);
        *peer = peer1;

        AddPeerResponse2 response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService2_Stub stub(&channel);
        stub.AddPeer(&cntl, &request, &response, NULL);
        LOG(INFO) << "add peer: " << cntl.ErrorCode() << ", "
                  << cntl.ErrorText();
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(EPERM, cntl.ErrorCode());
    }
    /* remove peer - 非法copyset */
    {
        Peer *leaderPeer = new Peer();
        Peer *peer = new Peer();
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderAddr, NULL));

        RemovePeerRequest2 request;
        // 设置一个不存在的logicPoolId
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId);
        request.set_allocated_leader(leaderPeer);
        *leaderPeer = gLeader;
        request.set_allocated_removepeer(peer);
        *peer = peer1;

        RemovePeerResponse2 response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService2_Stub stub(&channel);
        stub.RemovePeer(&cntl, &request, &response, NULL);
        LOG(INFO) << "remove peer: " << cntl.ErrorCode() << ", "
                  << cntl.ErrorText();
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }
    /* remove peer - 非法peer id */
    {
        Peer *leaderPeer = new Peer();
        Peer *peer = new Peer();
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderAddr, NULL));

        RemovePeerRequest2 request;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_allocated_leader(leaderPeer);
        *leaderPeer = gLeader;
        request.set_allocated_removepeer(peer);
        // request中的peer id是非法的
        peer->set_address("127.0.0");

        RemovePeerResponse2 response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService2_Stub stub(&channel);
        stub.RemovePeer(&cntl, &request, &response, NULL);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(EINVAL, cntl.ErrorCode());
        LOG(INFO) << "remove peer: " << cntl.ErrorText();
    }
    /* remove peer - 发送给不是leader的peer */
    {
        Peer *leaderPeer = new Peer();
        Peer *peer = new Peer();
        PeerId leaderId;
        LOG(INFO) << "true leader is: " << gLeader.address();
        // 找一个不是leader的peer，然后将配置变更请求发送给它处理
        if (0
            == strcmp(gLeader.address().c_str(), peer1.address().c_str())) {
            leaderId.parse(peer2.address());
            *leaderPeer = peer2;
        } else {
            leaderId.parse(peer1.address());
            *leaderPeer = peer1;
        }
        LOG(INFO) << "false leader is: " << leaderId.to_string();
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));

        RemovePeerRequest2 request;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_allocated_leader(leaderPeer);
        request.set_allocated_removepeer(peer);
        *peer = peer1;

        RemovePeerResponse2 response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService2_Stub stub(&channel);
        stub.RemovePeer(&cntl, &request, &response, NULL);
        LOG(INFO) << "add peer: " << cntl.ErrorCode() << ", "
                  << cntl.ErrorText();
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(EPERM, cntl.ErrorCode());
    }
    /* transfer leader - 非法copyset */
    {
        Peer *leaderPeer = new Peer();
        Peer *peer = new Peer();
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderAddr, NULL));

        TransferLeaderRequest2 request;
        // 设置一个不存在的logicPoolId
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId);
        request.set_allocated_leader(leaderPeer);
        *leaderPeer = gLeader;
        request.set_allocated_transferee(peer);
        *peer = peer1;

        TransferLeaderResponse2 response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService2_Stub stub(&channel);
        stub.TransferLeader(&cntl, &request, &response, NULL);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }
    /* transfer leader to leader */
    {
        Peer *leaderPeer = new Peer();
        Peer *peer = new Peer();
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderAddr, NULL));

        TransferLeaderRequest2 request;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_allocated_leader(leaderPeer);
        *leaderPeer = gLeader;
        request.set_allocated_transferee(peer);
        *peer = gLeader;

        TransferLeaderResponse2 response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService2_Stub stub(&channel);
        stub.TransferLeader(&cntl, &request, &response, NULL);
        ASSERT_FALSE(cntl.Failed());
    }
    /* transfer leader - 非法peer */
    {
        Peer *leaderPeer = new Peer();
        Peer *peer = new Peer();
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderAddr, NULL));

        TransferLeaderRequest2 request;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_allocated_leader(leaderPeer);
        *leaderPeer = gLeader;
        request.set_allocated_transferee(peer);
        // request中的peer id是非法的
        peer->set_address("127.0.0");

        TransferLeaderResponse2 response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService2_Stub stub(&channel);
        stub.TransferLeader(&cntl, &request, &response, NULL);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(EINVAL, cntl.ErrorCode());
        LOG(INFO) << "Transfer leader peer: " << cntl.ErrorText();
    }
    /* get leader - 非法copyset */
    {
        PeerId leaderId = leaderId;
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderAddr, NULL));


        GetLeaderRequest2 request;
        GetLeaderResponse2 response;

        brpc::Controller cntl;
        // 设置一个不存在的logicPoolId
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId);

        CliService2_Stub stub(&channel);
        stub.GetLeader(&cntl, &request, &response, NULL);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }
    /* remove peer then add peer */
    {
        // 1 remove peer
        Peer *removePeer = new Peer();
        Peer *leaderPeer1 = new Peer();
        Peer *leaderPeer2 = new Peer();
        Peer *addPeer = new Peer();
        PeerId removePeerId;
        // 找一个不是leader的peer，作为remove peer
        if (0
            == strcmp(gLeader.address().c_str(), peer1.address().c_str())) {
            removePeerId.parse(peer2.address());
            *removePeer = peer2;
        } else {
            removePeerId.parse(peer1.address());
            *removePeer = peer1;
        }
        *addPeer = *removePeer;

        LOG(INFO) << "remove peer is: " << removePeerId.to_string();
        brpc::Channel channel;
        PeerId leaderId;
        ASSERT_EQ(0, leaderId.parse(gLeader.address()));
        ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));

        RemovePeerRequest2 request1;
        request1.set_logicpoolid(logicPoolId);
        request1.set_copysetid(copysetId);
        *leaderPeer1 = gLeader;
        request1.set_allocated_leader(leaderPeer1);
        request1.set_allocated_removepeer(removePeer);

        RemovePeerResponse2 response1;
        brpc::Controller cntl1;
        cntl1.set_timeout_ms(options.timeout_ms);
        cntl1.set_max_retry(options.max_retry);

        CliService2_Stub stub1(&channel);
        stub1.RemovePeer(&cntl1, &request1, &response1, NULL);
        LOG(INFO) << "remove peer: " << cntl1.ErrorCode() << ", "
                  << cntl1.ErrorText();
        ASSERT_FALSE(cntl1.Failed());
        ASSERT_EQ(0, cntl1.ErrorCode());


        // add peer
        AddPeerRequest2 request2;
        request2.set_logicpoolid(logicPoolId);
        request2.set_copysetid(copysetId);
        *leaderPeer2 = gLeader;
        request2.set_allocated_leader(leaderPeer2);
        request2.set_allocated_addpeer(addPeer);

        AddPeerResponse2 response2;
        brpc::Controller cntl2;
        cntl2.set_timeout_ms(options.timeout_ms);
        cntl2.set_max_retry(options.max_retry);

        CliService2_Stub stub2(&channel);
        stub2.AddPeer(&cntl2, &request2, &response2, NULL);
        LOG(INFO) << "add peer: " << cntl2.ErrorCode() << ", "
                  << cntl2.ErrorText();
        ASSERT_FALSE(cntl2.Failed());
        ASSERT_EQ(0, cntl2.ErrorCode());
    }
}

}  // namespace chunkserver
}  // namespace curve
