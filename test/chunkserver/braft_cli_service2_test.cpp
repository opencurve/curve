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
 * Created Date: 18-11-12
 * Author: wudemiao
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
#include "src/common/timeutility.h"
#include "src/fs/local_filesystem.h"

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
        peer1.set_address("127.0.0.1:9310:0");
        peer2.set_address("127.0.0.1:9311:0");
        peer3.set_address("127.0.0.1:9312:0");
        UUIDGenerator uuidGenerator;
        std::string dir = uuidGenerator.GenerateUUID();
        dirMap[peer1.address()] = dir;
        Exec(("mkdir " + dir).c_str());
        dir = uuidGenerator.GenerateUUID();
        dirMap[peer2.address()] = dir;
        Exec(("mkdir " + dir).c_str());
        dir = uuidGenerator.GenerateUUID();
        dirMap[peer3.address()] = dir;
        Exec(("mkdir " + dir).c_str());
    }
    virtual void TearDown() {
        for (const auto& item : dirMap) {
            Exec(("rm -fr " + item.second).c_str());
        }
    }

 public:
    const char *ip    = "127.0.0.1";
    int port          = 9310;
    const char *confs = "127.0.0.1:9310:0,127.0.0.1:9311:0,127.0.0.1:9312:0";
    int snapshotInterval  = 3600;  //Prevent automatic snapshot taking
    int electionTimeoutMs = 3000;

    pid_t pid1;
    pid_t pid2;
    pid_t pid3;
    Peer peer1;
    Peer peer2;
    Peer peer3;
    std::map<std::string, std::string> dirMap;
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
        std::string copysetdir = "local://./" + dirMap[peer1.address()];
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
        std::string copysetdir = "local://./" + dirMap[peer2.address()];
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
        std::string copysetdir = "local://./" + dirMap[peer3.address()];
        StartChunkserver(ip,
                         port + 2,
                         copysetdir.c_str(),
                         confs,
                         snapshotInterval,
                         electionTimeoutMs);
        return;
    }

    /*Ensure that the process will definitely exit*/
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

    /*Add peer - illegal copyset*/
    {
        Peer *leaderPeer = new Peer();
        Peer *peer = new Peer();
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderAddr, NULL));

        AddPeerRequest2 request;
        //Set a non-existent logicPoolId
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
    /*Add peer - illegal peer id*/
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
        //The peer id in the request is illegal
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
    /*Add peer - sent to peers who are not leaders*/
    {
        Peer *leaderPeer = new Peer();
        Peer *peer = new Peer();
        PeerId leaderId;
        LOG(INFO) << "true leader is: " << gLeader.address();
        //Find a peer that is not a leader and send the configuration change request to it for processing
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
    /*Remove peer - illegal copyset*/
    {
        Peer *leaderPeer = new Peer();
        Peer *peer = new Peer();
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderAddr, NULL));

        RemovePeerRequest2 request;
        //Set a non-existent logicPoolId
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
    /*Remove peer - illegal peer id*/
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
        //The peer id in the request is illegal
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
    /*Remove peer - sent to peers who are not leaders*/
    {
        Peer *leaderPeer = new Peer();
        Peer *peer = new Peer();
        PeerId leaderId;
        LOG(INFO) << "true leader is: " << gLeader.address();
        //Find a peer that is not a leader and send the configuration change request to it for processing
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
    /*Transfer leader - illegal copyset*/
    {
        Peer *leaderPeer = new Peer();
        Peer *peer = new Peer();
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderAddr, NULL));

        TransferLeaderRequest2 request;
        //Set a non-existent logicPoolId
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
    /*Transfer leader - illegal peer*/
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
        //The peer id in the request is illegal
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
    /*Get leader - illegal copyset*/
    {
        PeerId leaderId = leaderId;
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderAddr, NULL));


        GetLeaderRequest2 request;
        GetLeaderResponse2 response;

        brpc::Controller cntl;
        //Set a non-existent logicPoolId
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
        //Find a peer that is not a leader as a remove peer
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
    /*Snapshot - illegal copyset*/
    {
        PeerId peer(peer1.address());
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(peer.addr, NULL));

        SnapshotRequest2 request;
        /*Illegal copyset*/
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId);
        Peer *peerPtr = new Peer();
        *peerPtr = peer1;
        request.set_allocated_peer(peerPtr);

        SnapshotResponse2 response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService2_Stub stub(&channel);
        stub.Snapshot(&cntl, &request, &response, NULL);
        LOG(INFO) << "snapshot: " << cntl.ErrorCode() << ", "
                  << cntl.ErrorText();
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }
    /* snapshot - normal */
    {
        //The initial state snapshot is not empty
        std::string copysetDataDir = dirMap[gLeader.address()] + "/" +
                ToGroupId(logicPoolId, copysetId) + "/" + RAFT_LOG_DIR;
        std::shared_ptr<LocalFileSystem> fs(
        LocalFsFactory::CreateFs(curve::fs::FileSystemType::EXT4, ""));
        std::vector<std::string> files;
        fs->List(copysetDataDir.c_str(), &files);
        ASSERT_GE(files.size(), 1);

        brpc::Channel channel;
        PeerId leaderId;
        ASSERT_EQ(0, leaderId.parse(gLeader.address()));
        ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));

        SnapshotRequest2 request;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        Peer *peerPtr = new Peer();
        peerPtr->set_address(leaderId.to_string());
        request.set_allocated_peer(peerPtr);

        SnapshotResponse2 response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        LOG(INFO) << "Start do snapshot";
        CliService2_Stub stub(&channel);
        stub.Snapshot(&cntl, &request, &response, NULL);
        ASSERT_FALSE(cntl.Failed()) << "Do snapshot fail, error: "
                                    << cntl.ErrorText();
        //Two consecutive snapshots are required to delete the log from the first snapshot
        sleep(5);
        cntl.Reset();
        LOG(INFO) << "Start do snapshot";
        stub.Snapshot(&cntl, &request, &response, NULL);
        ASSERT_FALSE(cntl.Failed()) << "Do snapshot fail, error: "
                                    << cntl.ErrorText();
        for (int i = 0; i < 60; ++i) {
            files.clear();
            fs->List(copysetDataDir.c_str(), &files);
            //After taking the snapshot, only meta information should be left
            if (files.size() == 1) {
                break;
            }
            sleep(1);
        }
        ASSERT_EQ(1, files.size());
    }
    /* snapshot all - normal */
    {
        SnapshotAllRequest request;
        SnapshotAllResponse response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);
        brpc::Channel channel;
        PeerId peer(peer1.address());
        ASSERT_EQ(0, channel.Init(peer.addr, NULL));

        CliService2_Stub stub(&channel);
        stub.SnapshotAll(&cntl, &request, &response, NULL);
        ASSERT_FALSE(cntl.Failed()) << "Do snapshot all fail, error: "
                                    << cntl.ErrorText();
    }
    /*Reset peer - illegal copyset*/
    {
        Peer *targetPeer = new Peer();
        *targetPeer = peer1;
        PeerId peer(peer1.address());
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(peer.addr, NULL));
        ResetPeerRequest2 request;
        /*Illegal copyset*/
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId);
        request.set_allocated_requestpeer(targetPeer);
        Peer* newPeer = request.add_newpeers();
        newPeer->CopyFrom(peer1);
        ResetPeerResponse2 response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService2_Stub stub(&channel);
        stub.ResetPeer(&cntl, &request, &response, NULL);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }
    /*Reset peer - new peer is empty*/
    {
        Peer *targetPeer = new Peer();
        *targetPeer = peer1;
        PeerId peer(peer1.address());
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(peer.addr, NULL));
        ResetPeerRequest2 request;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_allocated_requestpeer(targetPeer);
        ResetPeerResponse2 response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService2_Stub stub(&channel);
        stub.ResetPeer(&cntl, &request, &response, NULL);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(EINVAL, cntl.ErrorCode());
    }
    /* reset peer - normal */
    {
        Peer *targetPeer = new Peer();
        *targetPeer = peer1;
        PeerId peer(peer1.address());
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(peer.addr, NULL));
        ResetPeerRequest2 request;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_allocated_requestpeer(targetPeer);
        Peer* newPeer = request.add_newpeers();
        newPeer->CopyFrom(peer1);
        ResetPeerResponse2 response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(options.timeout_ms);
        cntl.set_max_retry(options.max_retry);

        CliService2_Stub stub(&channel);
        stub.ResetPeer(&cntl, &request, &response, NULL);
        ASSERT_FALSE(cntl.Failed());
    }
}

}  // namespace chunkserver
}  // namespace curve
