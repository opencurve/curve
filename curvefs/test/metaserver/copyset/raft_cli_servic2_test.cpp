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
#include <glog/logging.h>
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

const char* kInitConf = "127.0.0.1:29910:0,127.0.0.1:29911:0,127.0.0.1:29912:0";

const int kElectionTimeoutMs = 3000;

const PoolId kPoolId = 1234;
const CopysetId kCopysetId = 1234;

class RaftCliService2Test : public testing::Test {
 protected:
    static void SetUpTestCase() {
        LOG(INFO) << "BraftCliServiceTest SetUpTestCase";
    }

    static void TearDownTestCase() {
        LOG(INFO) << "BraftCliServiceTest TearDownTestCase";
    }

    void SetUp() override {}

    void TearDown() override {
        for (auto& s : servers_) {
            kill(s.first, SIGTERM);
            ::system(std::string("rm -rf " + s.second).c_str());
        }
    }

    void StartThreeMetaserver() {
        UUIDGenerator uuid;

        std::string copysetDir = "./runlog/" + uuid.GenerateUUID();
        pid_t pid = StartFakeMetaserver("127.0.0.1", 29910,
                                        "local://" + copysetDir, kInitConf);
        ASSERT_GE(pid, 0);
        servers_.emplace_back(pid, copysetDir);
        usleep(kElectionTimeoutMs * 1000);

        copysetDir = "./runlog/" + uuid.GenerateUUID();
        pid = StartFakeMetaserver("127.0.0.1", 29911, "local://" + copysetDir,
                                  kInitConf);
        ASSERT_GE(pid, 0);
        servers_.emplace_back(pid, copysetDir);
        usleep(kElectionTimeoutMs * 1000);

        copysetDir = "./runlog/" + uuid.GenerateUUID();
        pid = StartFakeMetaserver("127.0.0.1", 29912, "local://" + copysetDir,
                                  kInitConf);
        ASSERT_GE(pid, 0);
        servers_.emplace_back(pid, copysetDir);
        usleep(kElectionTimeoutMs * 1000 * 2);
    }

    static pid_t StartFakeMetaserver(const char* ip, int port,
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

    bool WaitLeader(int retryTimes, braft::PeerId* leaderId) {
        braft::Configuration conf;
        conf.parse_from(kInitConf);

        for (int i = 0; i < retryTimes; ++i) {
            auto status = GetLeader(kPoolId, kCopysetId, conf, leaderId);
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

 protected:
    std::vector<std::pair<pid_t, std::string>> servers_;
};

TEST_F(RaftCliService2Test, GetLeaderTest) {
    StartThreeMetaserver();

    braft::PeerId leaderId;
    ASSERT_TRUE(WaitLeader(10, &leaderId));
    ASSERT_FALSE(leaderId.is_empty());

    // copyset not exist
    {
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, nullptr));

        GetLeaderRequest2 request;
        GetLeaderResponse2 response;
        request.set_poolid(1);
        request.set_copysetid(1);

        brpc::Controller cntl;
        CliService2_Stub stub(&channel);
        stub.GetLeader(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }

    {
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leaderId.addr, nullptr));

        GetLeaderRequest2 request;
        GetLeaderResponse2 response;
        request.set_poolid(kPoolId);
        request.set_copysetid(kCopysetId);

        brpc::Controller cntl;
        CliService2_Stub stub(&channel);
        stub.GetLeader(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(leaderId.to_string(), response.leader().address());
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
