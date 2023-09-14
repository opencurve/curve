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
 * Created Date: 18-9-7
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
#include "src/chunkserver/cli.h"
#include "proto/copyset.pb.h"
#include "test/chunkserver/chunkserver_test_util.h"
#include "src/common/uuid.h"

namespace curve {
namespace chunkserver {

using curve::common::UUIDGenerator;

class CliTest : public testing::Test {
 protected:
    static void SetUpTestCase() {
        LOG(INFO) << "CliTest " << "SetUpTestCase";
    }
    static void TearDownTestCase() {
        LOG(INFO) << "CliTest " << "TearDownTestCase";
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
    pid_t pid1;
    pid_t pid2;
    pid_t pid3;

    std::string dir1;
    std::string dir2;
    std::string dir3;
};

butil::AtExitManager atExitManager;

TEST_F(CliTest, basic) {
    const char *ip = "127.0.0.1";
    int port = 9030;
    const char *confs = "127.0.0.1:9030:0,127.0.0.1:9031:0,127.0.0.1:9032:0";
    int snapshotInterval = 600;

    /**
     * Set a larger default election timeout because the current CI environment is prone to timeout
     */
    int electionTimeoutMs = 3000;

    /**
     * Start three chunk server by fork
     */
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

    /*Ensure that the process will definitely exit*/
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

    /* wait for leader & become leader flush config */
    ::usleep(1.5 * 1000 * electionTimeoutMs);
    butil::Status status =
        WaitLeader(logicPoolId, copysetId, conf, &leader, electionTimeoutMs);
    ASSERT_TRUE(status.ok());

    /* Waiting for transfer leader to succeed*/
    int waitTransferLeader = 3000 * 1000;
    /**
     * The configuration change requires a longer timeout due to the completion of copying a log entry
     * Time to avoid occasional timeout errors in the CI environment
     */
    braft::cli::CliOptions opt;
    opt.timeout_ms = 6000;
    opt.max_retry = 3;

    /* remove peer */
    {
        PeerId peerId("127.0.0.1:9032:0");
        butil::Status st = curve::chunkserver::RemovePeer(logicPoolId,
                                                          copysetId,
                                                          conf,
                                                          peerId,
                                                          opt);
        LOG(INFO) << "remove peer: "
                  << st.error_code() << ", " << st.error_str();
        ASSERT_TRUE(st.ok());
        /* It is possible to remove a leader. If a leader is being removed, it is necessary to wait until a new leader is generated,
         * Otherwise, the add peer test below will fail and wait for a long time to ensure removal
         * After the successful election of the new leader, switch to the flush configuration of the become leader
         * Complete*/
        ::usleep(1.5 * 1000 * electionTimeoutMs);
        butil::Status status = WaitLeader(logicPoolId,
                                          copysetId,
                                          conf,
                                          &leader,
                                          electionTimeoutMs);
        ASSERT_TRUE(status.ok());
    }
    /*  add peer */
    {
        Configuration conf;
        conf.parse_from("127.0.0.1:9030:0,127.0.0.1:9031:0");
        PeerId peerId("127.0.0.1:9032:0");
        butil::Status st = curve::chunkserver::AddPeer(logicPoolId,
                                                       copysetId,
                                                       conf,
                                                       peerId,
                                                       opt);
        LOG(INFO) << "add peer: "
                  << st.error_code() << ", " << st.error_str();
        ASSERT_TRUE(st.ok());
    }
    /*Repeatedly add the same peer*/
    {
        Configuration conf;
        conf.parse_from("127.0.0.1:9030:0,127.0.0.1:9031:0");
        PeerId peerId("127.0.0.1:9032:0");
        butil::Status st = curve::chunkserver::AddPeer(logicPoolId,
                                                       copysetId,
                                                       conf,
                                                       peerId,
                                                       opt);
        LOG(INFO) << "add one peer repeat: "
                  << st.error_code() << ", " << st.error_str();
        ASSERT_TRUE(st.ok());
    }
    /*  transfer leader */
    {
        Configuration conf;
        conf.parse_from("127.0.0.1:9030:0,127.0.0.1:9031:0,127.0.0.1:9032:0");
        PeerId peer1("127.0.0.1:9030:0");
        PeerId peer2("127.0.0.1:9031:0");
        PeerId peer3("127.0.0.1:9032:0");
        {
            LOG(INFO) << "start transfer leader";
            butil::Status st = curve::chunkserver::TransferLeader(logicPoolId,
                                                                  copysetId,
                                                                  conf,
                                                                  peer1,
                                                                  opt);
            LOG(INFO) << "transfer leader: "
                      << st.error_code() << ", " << st.error_str();
            ASSERT_TRUE(st.ok());
            /* The transfer leader only sends rpc to the leader and does not wait for the leader to transfer
             * We only return after success, so we need to wait here. In addition, we cannot immediately check the leader because
             * After the leader transfer, the previous leader may be returned, except for the transfer
             * After the leader is successful, when the benefit leader is in progress, the leader is already visible, but
             * The benefit leader will execute flush current conf to act as the noop. If at this time
             * Immediately proceed to the next transfer leader, which will be organized because there can only be one configuration at the same time
             * Changes in progress*/
            ::usleep(waitTransferLeader);
            butil::Status status = WaitLeader(logicPoolId,
                                              copysetId,
                                              conf,
                                              &leader,
                                              electionTimeoutMs);
            LOG(INFO) << "get leader: "
                      << status.error_code() << ", " << status.error_str();
            ASSERT_TRUE(status.ok());
            ASSERT_STREQ(peer1.to_string().c_str(), leader.to_string().c_str());
        }
        {
            LOG(INFO) << "start transfer leader";
            butil::Status st = curve::chunkserver::TransferLeader(logicPoolId,
                                                                  copysetId,
                                                                  conf,
                                                                  peer2,
                                                                  opt);
            LOG(INFO) << "transfer leader: "
                      << st.error_code() << ", " << st.error_str();
            ASSERT_TRUE(st.ok());
            ::usleep(waitTransferLeader);
            butil::Status status = WaitLeader(logicPoolId,
                                              copysetId,
                                              conf,
                                              &leader,
                                              electionTimeoutMs);
            LOG(INFO) << "get leader: "
                      << status.error_code() << ", " << status.error_str();
            ASSERT_TRUE(status.ok());
            ASSERT_STREQ(peer2.to_string().c_str(), leader.to_string().c_str());
        }
        {
            LOG(INFO) << "start transfer leader";
            butil::Status st = curve::chunkserver::TransferLeader(logicPoolId,
                                                                  copysetId,
                                                                  conf,
                                                                  peer3,
                                                                  opt);
            LOG(INFO) << "transfer leader: " << st.error_str();
            ASSERT_TRUE(st.ok());
            ::usleep(waitTransferLeader);
            butil::Status status = WaitLeader(logicPoolId,
                                              copysetId,
                                              conf,
                                              &leader,
                                              electionTimeoutMs);
            LOG(INFO) << "get leader: "
                      << status.error_code() << ", " << status.error_str();
            ASSERT_TRUE(status.ok());
            ASSERT_STREQ(peer3.to_string().c_str(), leader.to_string().c_str());
        }
        /*Transfer to leader to leader, still returns success*/
        {
            LOG(INFO) << "start transfer leader";
            butil::Status st = curve::chunkserver::TransferLeader(logicPoolId,
                                                                  copysetId,
                                                                  conf,
                                                                  peer3,
                                                                  opt);
            ASSERT_TRUE(st.ok());
            ::usleep(waitTransferLeader);
            butil::Status status = WaitLeader(logicPoolId,
                                              copysetId,
                                              conf,
                                              &leader,
                                              electionTimeoutMs);
            LOG(INFO) << "get leader: "
                      << status.error_code() << ", " << status.error_str();
            ASSERT_TRUE(status.ok());
            ASSERT_STREQ(peer3.to_string().c_str(), leader.to_string().c_str());
        }
    }
    /*Abnormal Branch Test*/
    /* get leader - conf empty */
    {
        Configuration conf;
        butil::Status status = GetLeader(logicPoolId, copysetId, conf, &leader);
        ASSERT_FALSE(status.ok());
        ASSERT_EQ(EINVAL, status.error_code());
    }
    /*Get leader - illegal address*/
    {
        Configuration conf;
        conf.parse_from("127.0.0.1:65540:0,127.0.0.1:65541:0,127.0.0.1:65542:0");   //NOLINT
        butil::Status status = GetLeader(logicPoolId, copysetId, conf, &leader);
        ASSERT_FALSE(status.ok());
        ASSERT_EQ(-1, status.error_code());
    }
    /*Add peer - non-existent peer*/
    {
        Configuration conf;
        conf.parse_from("127.0.0.1:9030:0,127.0.0.1:9031:0,127.0.0.1:9030:2");
        /*Add a non-existent node*/
        PeerId peerId("127.0.0.1:9039:2");
        butil::Status status = curve::chunkserver::AddPeer(logicPoolId,
                                                           copysetId,
                                                           conf,
                                                           peerId,
                                                           opt);
        ASSERT_FALSE(status.ok());
        LOG(INFO) << "add peer: " << status.error_code() << ", "
                  << status.error_str();
    }
    /*Transfer leader - non-existent peer*/
    {
        Configuration conf;
        conf.parse_from("127.0.0.1:9030:0,127.0.0.1:9031:0,127.0.0.1:9032:0");
        PeerId peer1("127.0.0.1:9039:0");
        {
            butil::Status
                status = curve::chunkserver::TransferLeader(logicPoolId,
                                                            copysetId,
                                                            conf,
                                                            peer1,
                                                            opt);
            ASSERT_FALSE(status.ok());
            LOG(INFO) << "add peer: " << status.error_code() << ", "
                      << status.error_str();
        }
    }
}

}  // namespace chunkserver
}  // namespace curve
