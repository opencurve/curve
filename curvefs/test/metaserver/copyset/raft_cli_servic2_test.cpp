/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: Wed Sep  1 15:39:58 CST 2021
 * Author: wuhanqing
 */

#include <brpc/server.h>
#include <butil/endpoint.h>
#include <butil/status.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include <utility>

#include "curvefs/src/metaserver/copyset/copyset_node_manager.h"
#include "curvefs/src/metaserver/copyset/raft_cli2.h"
#include "curvefs/src/metaserver/copyset/raft_cli_service2.h"
#include "src/common/uuid.h"
#include "src/fs/local_filesystem.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::curve::common::UUIDGenerator;
using ::curve::fs::FileSystemType;
using ::curve::fs::LocalFileSystem;
using ::curve::fs::LocalFsFactory;
using ::google::protobuf::util::MessageDifferencer;

const char* kInitConf = "127.0.0.1:29910:0,127.0.0.1:29911:0,127.0.0.1:29912:0";
const int kElectionTimeoutMs = 3000;
const PoolId kPoolId = 1234;
const CopysetId kCopysetId = 1234;

class RaftCliService2Test : public testing::Test {
 protected:
    void SetUp() override {}

    void TearDown() override {}

    static void SetUpTestCase() {
        LOG(INFO) << "BraftCliServiceTest SetUpTestCase";

        StartOneCopysetGroup();
        StartOneStandByMetaserver("127.0.0.1", 29913, "127.0.0.1:29913:0");
        StartOneStandByMetaserver("127.0.0.1", 29914, "127.0.0.1:29914:0");
    }

    static void TearDownTestCase() {
        LOG(INFO) << "BraftCliServiceTest TearDownTestCase";

        for (auto& s : servers_) {
            kill(s.first, SIGKILL);
            int wstatus;
            wait(&wstatus);
            system(std::string("rm -rf " + s.second).c_str());
        }
    }

    static void StartOneCopysetGroup() {
        std::string copysetDir = "./runlog/" + uuid_.GenerateUUID();
        pid_t pid = StartOneFakeMetaserver(
            "127.0.0.1", 29910, "local://" + copysetDir, kInitConf);

        CHECK_GE(pid, 0);
        servers_.emplace_back(pid, copysetDir);
        usleep(kElectionTimeoutMs * 1000);

        copysetDir = "./runlog/" + uuid_.GenerateUUID();
        pid = StartOneFakeMetaserver("127.0.0.1", 29911,
                                     "local://" + copysetDir, kInitConf);
        CHECK_GE(pid, 0);
        servers_.emplace_back(pid, copysetDir);
        usleep(kElectionTimeoutMs * 1000);

        copysetDir = "./runlog/" + uuid_.GenerateUUID();
        pid = StartOneFakeMetaserver("127.0.0.1", 29912,
                                     "local://" + copysetDir, kInitConf);
        CHECK_GE(pid, 0);
        servers_.emplace_back(pid, copysetDir);
        usleep(kElectionTimeoutMs * 1000 * 2);

        CHECK_EQ(0, conf_.parse_from(kInitConf));
    }

    static void StartOneStandByMetaserver(const char* ip, int port,
                                          const std::string& initConf) {
        std::string copysetDir = "./runlog/" + uuid_.GenerateUUID();
        pid_t pid =
            StartOneFakeMetaserver(ip, port, "local://" + copysetDir, initConf);
        ASSERT_GE(pid, 0);

        servers_.emplace_back(pid, copysetDir);
        usleep(kElectionTimeoutMs * 1000);
    }

    static pid_t StartOneFakeMetaserver(const char* ip, int port,
                                        const std::string& copysetDir,
                                        const std::string& initConf) {
        LOG(INFO) << "Going to start metaserver on " << ip << ":" << port;

        pid_t pid = fork();
        if (pid < 0) {
            return -1;
        } else if (pid > 0) {
            return pid;
        }

        brpc::Server server;
        butil::EndPoint listenAddr(butil::IP_ANY, port);
        CopysetNodeManager::GetInstance().AddService(&server, listenAddr);

        butil::ip_t listenIp;
        butil::str2ip(ip, &listenIp);
        CHECK_EQ(0, server.Start(butil::EndPoint(listenIp, port), nullptr));

        std::shared_ptr<LocalFileSystem> fs =
            LocalFsFactory::CreateFs(FileSystemType::EXT4, "");

        CopysetNodeOptions options;
        options.ip = ip;
        options.port = port;
        options.raftNodeOptions.election_timeout_ms = kElectionTimeoutMs;

        // disable raft snapshot
        options.raftNodeOptions.snapshot_interval_s = -1;
        options.dataUri = copysetDir;
        options.raftNodeOptions.log_uri = copysetDir;
        options.raftNodeOptions.snapshot_uri = copysetDir;
        options.raftNodeOptions.raft_meta_uri = copysetDir;
        options.localFileSystem = fs.get();

        braft::Configuration conf;
        CHECK_EQ(0, conf.parse_from(initConf));

        CopysetNodeManager::GetInstance().Init(options);
        CopysetNodeManager::GetInstance().Start();

        CHECK_EQ(true, CopysetNodeManager::GetInstance().CreateCopysetNode(
                           kPoolId, kCopysetId, conf));

        server.RunUntilAskedToQuit();
        server.Stop(0);
        server.Join();

        LOG(INFO) << "Metaserver start at " << ip << ":" << port << " stopped";
        CopysetNodeManager::GetInstance().Stop();

        exit(0);
    }

    static bool GetCurrentLeader(braft::PeerId* leaderId) {
        return WaitLeader(1, leaderId);
    }

    static bool WaitLeader(int retryTimes, braft::PeerId* leaderId) {
        for (int i = 0; i < retryTimes; ++i) {
            auto status = GetLeader(kPoolId, kCopysetId, conf_, leaderId);
            if (status.ok()) {
                usleep(kElectionTimeoutMs * 1000);
                return true;
            } else {
                LOG(WARNING) << "Get leader failed, " << status.error_str();
                usleep(kElectionTimeoutMs * 1000);
            }
        }

        return false;
    }

    static Peer RandomSelectOnePeer() {
        std::vector<braft::PeerId> peers;
        conf_.list_peers(&peers);

        int idx = rand() % peers.size();  // NOLINT
        Peer peer;
        peer.set_address(peers[idx].to_string());
        return peer;
    }

    static Peer RandomSelectOneFollower(const braft::PeerId& leader) {
        std::vector<braft::PeerId> peers;
        conf_.list_peers(&peers);

        Peer follower;

        while (true) {
            int idx = rand() % peers.size();  // NOLINT
            if (leader != peers[idx]) {
                follower.set_address(peers[idx].to_string());
                break;
            }
        }

        return follower;
    }

    template <typename Request>
    static void SetRequestPoolAndCopysetId(Request* request,
                                           bool random = false) {
        if (random) {
            request->set_poolid(kPoolId + rand() % kPoolId + 1);  // NOLINT
            request->set_copysetid(kCopysetId + rand() % kCopysetId + 1);  // NOLINT
        } else {
            request->set_poolid(kPoolId);
            request->set_copysetid(kCopysetId);
        }
    }

    static void UpdateConfigurations(
        const google::protobuf::RepeatedPtrField<curvefs::common::Peer>&
            peers) {
        conf_.reset();

        for (auto& peer : peers) {
            braft::PeerId pid;
            CHECK_EQ(0, pid.parse(peer.address()));
            conf_.add_peer(pid);
        }
    }

 protected:
    static std::vector<std::pair<pid_t, std::string>> servers_;
    static UUIDGenerator uuid_;
    static braft::Configuration conf_;
};

std::vector<std::pair<pid_t, std::string>> RaftCliService2Test::servers_;
UUIDGenerator RaftCliService2Test::uuid_;
braft::Configuration RaftCliService2Test::conf_;

TEST_F(RaftCliService2Test, GetLeaderTest) {
    braft::PeerId leaderId;
    ASSERT_TRUE(WaitLeader(10, &leaderId));
    ASSERT_FALSE(leaderId.is_empty());

    // copyset not exist
    {
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, nullptr));

        GetLeaderRequest2 request;
        GetLeaderResponse2 response;
        SetRequestPoolAndCopysetId(&request, true);

        brpc::Controller cntl;
        CliService2_Stub stub(&channel);
        stub.GetLeader(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }

    // succeeded
    {
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, nullptr));

        GetLeaderRequest2 request;
        GetLeaderResponse2 response;
        SetRequestPoolAndCopysetId(&request, false);

        brpc::Controller cntl;
        CliService2_Stub stub(&channel);
        stub.GetLeader(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(leaderId.to_string(), response.leader().address());
    }
}

TEST_F(RaftCliService2Test, TransferLeaderTest) {
    braft::PeerId leaderId;
    ASSERT_TRUE(WaitLeader(10, &leaderId));
    ASSERT_FALSE(leaderId.is_empty());

    // pool or copyset is not exists
    {
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, nullptr));

        TransferLeaderRequest2 request;
        TransferLeaderResponse2 response;
        SetRequestPoolAndCopysetId(&request, true);
        auto* peer = request.mutable_transferee();
        *peer = RandomSelectOneFollower(leaderId);

        brpc::Controller cntl;
        CliService2_Stub stub(&channel);
        stub.TransferLeader(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }

    // send to follower
    {
        TransferLeaderRequest2 request;
        TransferLeaderResponse2 response;
        SetRequestPoolAndCopysetId(&request);
        auto* peer = request.mutable_transferee();
        *peer = RandomSelectOneFollower(leaderId);

        braft::PeerId follower(peer->address());

        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(follower.addr, nullptr));

        brpc::Controller cntl;
        CliService2_Stub stub(&channel);
        stub.TransferLeader(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(EPERM, cntl.ErrorCode());
    }

    // getleader after transfer succeeded
    {
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, nullptr));

        TransferLeaderRequest2 request;
        TransferLeaderResponse2 response;
        SetRequestPoolAndCopysetId(&request);
        auto* peer = request.mutable_transferee();
        *peer = RandomSelectOneFollower(leaderId);

        brpc::Controller cntl;
        CliService2_Stub stub(&channel);
        stub.TransferLeader(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());

        braft::PeerId current;
        braft::PeerId expected(peer->address());

        while (true) {
            bool rc = GetCurrentLeader(&current);
            if (!rc) {
                continue;
            } else if (current == leaderId) {
                continue;
            } else {
                break;
            }
        }

        EXPECT_EQ(current, expected);
    }
}

TEST_F(RaftCliService2Test, AddPeerTest) {
    braft::PeerId leaderId;
    ASSERT_TRUE(WaitLeader(10, &leaderId));
    ASSERT_FALSE(leaderId.is_empty());

    // pool or copyset not exists
    {
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, nullptr));

        AddPeerRequest2 request;
        AddPeerResponse2 response;
        SetRequestPoolAndCopysetId(&request, true);
        auto* peer = request.mutable_addpeer();
        peer->set_address("127.0.0.1:29913:0");

        brpc::Controller cntl;
        CliService2_Stub stub(&channel);
        stub.AddPeer(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }

    // add peer succeeded
    {
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, nullptr));

        AddPeerRequest2 request;
        AddPeerResponse2 response;
        SetRequestPoolAndCopysetId(&request);
        auto* peerToAdd = request.mutable_addpeer();
        peerToAdd->set_address("127.0.0.1:29913:0");

        brpc::Controller cntl;
        cntl.set_timeout_ms(5 * 1000);
        CliService2_Stub stub(&channel);
        stub.AddPeer(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

        // check response
        ASSERT_EQ(3, response.oldpeers_size());
        ASSERT_EQ(4, response.newpeers_size());
        bool found = false;
        for (auto& peer : response.newpeers()) {
            if (MessageDifferencer::Equals(peer, *peerToAdd)) {
                found = true;
            }
        }
        ASSERT_TRUE(found);

        UpdateConfigurations(response.newpeers());
    }
}

TEST_F(RaftCliService2Test, RemovePeerTest) {
    braft::PeerId leaderId;
    ASSERT_TRUE(WaitLeader(10, &leaderId));
    ASSERT_FALSE(leaderId.is_empty());

    // pool or copyset not exists
    {
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, nullptr));

        RemovePeerRequest2 request;
        RemovePeerResponse2 response;
        SetRequestPoolAndCopysetId(&request, true);
        auto* peer = request.mutable_removepeer();
        peer->set_address("127.0.0.1:29912:0");

        brpc::Controller cntl;
        CliService2_Stub stub(&channel);
        stub.RemovePeer(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }

    // remove peer succeeded
    {
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, nullptr));

        RemovePeerRequest2 request;
        RemovePeerResponse2 response;
        SetRequestPoolAndCopysetId(&request);

        auto* peerToRemove = request.mutable_removepeer();
        *peerToRemove = RandomSelectOnePeer();

        LOG(INFO) << "Remove peer: " << peerToRemove->address();

        brpc::Controller cntl;
        CliService2_Stub stub(&channel);
        stub.RemovePeer(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());

        // check response
        EXPECT_EQ(4, response.oldpeers_size());
        EXPECT_EQ(3, response.newpeers_size());
        for (auto& peer : response.newpeers()) {
            EXPECT_FALSE(MessageDifferencer::Equals(peer, *peerToRemove));
        }

        UpdateConfigurations(response.newpeers());
    }
}

TEST_F(RaftCliService2Test, ChangePeerTest) {
    braft::PeerId leaderId;
    ASSERT_TRUE(WaitLeader(10, &leaderId));
    ASSERT_FALSE(leaderId.is_empty());

    // pool or copyset is not exists
    {
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, nullptr));

        ChangePeersRequest2 request;
        ChangePeersResponse2 response;
        SetRequestPoolAndCopysetId(&request, true);

        auto* peer = request.add_newpeers();
        peer->set_address("127.0.0.1:29911:0");
        peer = request.add_newpeers();
        peer->set_address("127.0.0.1:29912:0");
        peer = request.add_newpeers();
        peer->set_address("127.0.0.1:29913:0");

        brpc::Controller cntl;
        CliService2_Stub stub(&channel);
        stub.ChangePeers(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }

    // change peer succeed
    {
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, nullptr));

        ChangePeersRequest2 request;
        ChangePeersResponse2 response;
        SetRequestPoolAndCopysetId(&request);

        // remove one from current conf, and add 29914
        Peer peerToRemove = RandomSelectOnePeer();
        std::vector<braft::PeerId> peers;
        conf_.list_peers(&peers);
        for (auto& peer : peers) {
            if (peer.to_string() != peerToRemove.address()) {
                auto* add = request.add_newpeers();
                add->set_address(peer.to_string());
            }
        }

        Peer peerToAdd;
        peerToAdd.set_address("127.0.0.1:29914:0");
        *request.add_newpeers() = peerToAdd;

        brpc::Controller cntl;
        CliService2_Stub stub(&channel);
        stub.ChangePeers(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());

        // check response
        ASSERT_EQ(3, response.oldpeers_size());
        ASSERT_EQ(3, response.newpeers_size());
    }
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

// XXX: unknown reason why this test will failed when build with other test into
// one cc_test
int main(int argc, char* argv[]) {
    butil::AtExitManager atExit;

    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
