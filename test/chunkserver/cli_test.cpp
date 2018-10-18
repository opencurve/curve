/*
 * Project: curve
 * Created Date: 18-9-7
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
#include "src/chunkserver/cli.h"
#include "proto/copyset.pb.h"

namespace curve {
namespace chunkserver {

static std::string Exec(const char *cmd) {
    FILE *pipe = popen(cmd, "r");
    if (!pipe) return "ERROR";
    char buffer[4096];
    std::string result = "";
    while (!feof(pipe)) {
        if (fgets(buffer, 1024, pipe) != NULL)
            result += buffer;
    }
    pclose(pipe);
    return result;
}

class CliTest : public testing::Test {
 protected:
    virtual void SetUp() {
        /* before test: start servers */
        std::string result = Exec(run.c_str());
        std::cout << result << std::endl;
    }

    virtual void TearDown() {
        /* after test: stop servers, debug 的时候可以注释掉以便查看日志 */
        std::string result = Exec(stop.c_str());
        std::cout << result << std::endl;
    }

 private:
    /* 初始化脚本 */
    std::string run = R"(
        killall -9 server-test


        rm -fr 0
        mkdir 0
        rm -fr 1
        mkdir 1
        rm -fr 2
        mkdir 2

        cp -f bazel-bin/test/chunkserver/server-test .
        cp -f server-test ./0
        cd 0
        ./server-test -bthread_concurrency=18 -raft_sync=true -ip=127.0.0.1 -port=8200 -conf=127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0 > std.log 2>&1 &
        cd ..
        sleep 1s

        cp -f server-test ./1
        cd 1
        ./server-test -bthread_concurrency=18 -raft_sync=true -ip=127.0.0.1 -port=8201 -conf=127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0 > std.log 2>&1 &
        cd ..

        cp -f server-test ./2
        cd 2
        ./server-test -bthread_concurrency=18 -raft_sync=true -ip=127.0.0.1 -port=8202 -conf=127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0 > std.log 2>&1 &
        cd ..
        sleep 1s

        ps -ef | grep server-test
    )";
    std::string stop = R"(
        killall -9 server-test
        sleep 1s
        ps -ef | grep server-test
        rm -fr 0 1 2
    )";
    std::string result;
};

TEST_F(CliTest, basic) {
    PeerId leader;
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    Configuration conf;
    conf.parse_from("127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0");

    sleep(2);
    int i = 0;
    while (true) {
        butil::Status status = GetLeader(logicPoolId, copysetId, conf, &leader);
        std::cout << "Leader is: " << leader.to_string() << std::endl;
        if (status.ok()) {
            break;
        }
        usleep(500 * 1000);
        LOG(ERROR) << "Get leader failed, retry times : " << i;
        ++i;
        if (i > 50) {
            ASSERT_TRUE(false);
        }
    }

    braft::cli::CliOptions opt;
    opt.timeout_ms = 1000;
    opt.max_retry = 3;

    /* remove peer */
    {
        PeerId peerId("127.0.0.1:8202:0");
        butil::Status st = curve::chunkserver::RemovePeer(logicPoolId,
                                                          copysetId,
                                                          conf,
                                                          peerId,
                                                          opt);
        ASSERT_TRUE(st.ok());
        sleep(2);
    }
    /*  add peer */
    {
        Configuration conf;
        conf.parse_from("127.0.0.1:8200:0,127.0.0.1:8201:0");
        PeerId peerId("127.0.0.1:8202:0");
        butil::Status st = curve::chunkserver::AddPeer(logicPoolId,
                                                       copysetId,
                                                       conf,
                                                       peerId,
                                                       opt);
        ASSERT_TRUE(st.ok());
    }
    /* remove leader */
    {
        butil::Status status = curve::chunkserver::GetLeader(logicPoolId,
                                                             copysetId,
                                                             conf,
                                                             &leader);
        LOG(INFO) << "get leader:" << status.error_str();
        std::cout << "Leader is: " << leader.to_string() << std::endl;
        ASSERT_TRUE(status.ok());
        butil::Status st = curve::chunkserver::RemovePeer(logicPoolId,
                                                          copysetId,
                                                          conf,
                                                          leader,
                                                          opt);
        ASSERT_TRUE(st.ok());
        sleep(3);
    }

    /* add peer */
    {
        Configuration conf;
        conf.parse_from("127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0");
        std::cout << "Leader is: " << leader.to_string() << std::endl;
        ASSERT_TRUE(conf.remove_peer(leader));
        ASSERT_FALSE(conf.contains(leader));
        ASSERT_EQ(2, conf.size());
        butil::Status st = curve::chunkserver::AddPeer(logicPoolId,
                                                       copysetId,
                                                       conf,
                                                       leader,
                                                       opt);
        LOG(INFO) << "add peer " << st.error_str();
        ASSERT_TRUE(st.ok());
    }
    /*  transfer leader */
    {
        Configuration conf;
        conf.parse_from("127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0");
        PeerId peer1("127.0.0.1:8200:0");
        PeerId peer2("127.0.0.1:8201:0");
        PeerId peer3("127.0.0.1:8202:0");
        {
            butil::Status st = curve::chunkserver::TransferLeader(logicPoolId,
                                                                  copysetId,
                                                                  conf,
                                                                  peer1,
                                                                  opt);
            LOG(INFO) << "transfer leader:" << st.error_str();
            ASSERT_TRUE(st.ok());
            Exec("sleep 1s");
            butil::Status status = curve::chunkserver::GetLeader(logicPoolId,
                                                                 copysetId,
                                                                 conf,
                                                                 &leader);
            LOG(INFO) << "get leader:" << status.error_str();
            ASSERT_TRUE(status.ok());
            ASSERT_STREQ(peer1.to_string().c_str(), leader.to_string().c_str());
        }
        {
            butil::Status st = curve::chunkserver::TransferLeader(logicPoolId,
                                                                  copysetId,
                                                                  conf,
                                                                  peer2,
                                                                  opt);
            LOG(INFO) << "transfer leader:" << st.error_str();
            ASSERT_TRUE(st.ok());
            Exec("sleep 1s");
            butil::Status status = curve::chunkserver::GetLeader(logicPoolId,
                                                                 copysetId,
                                                                 conf,
                                                                 &leader);
            LOG(INFO) << "get leader:" << status.error_str();
            ASSERT_TRUE(status.ok());
            ASSERT_STREQ(peer2.to_string().c_str(), leader.to_string().c_str());
        }
        {
            butil::Status st = curve::chunkserver::TransferLeader(logicPoolId,
                                                                  copysetId,
                                                                  conf,
                                                                  peer3,
                                                                  opt);
            ASSERT_TRUE(st.ok());
            Exec("sleep 1s");
            butil::Status status = curve::chunkserver::GetLeader(logicPoolId,
                                                                 copysetId,
                                                                 conf,
                                                                 &leader);
            LOG(INFO) << "get leader:" << status.error_str();
            ASSERT_TRUE(status.ok());
            ASSERT_STREQ(peer3.to_string().c_str(), leader.to_string().c_str());
        }
        {
            butil::Status st = curve::chunkserver::TransferLeader(logicPoolId,
                                                                  copysetId,
                                                                  conf,
                                                                  peer2,
                                                                  opt);
            ASSERT_TRUE(st.ok());
            Exec("sleep 1s");
            butil::Status status = curve::chunkserver::GetLeader(logicPoolId,
                                                                 copysetId,
                                                                 conf,
                                                                 &leader);
            LOG(INFO) << "get leader:" << status.error_str();
            ASSERT_TRUE(status.ok());
            ASSERT_STREQ(peer2.to_string().c_str(), leader.to_string().c_str());
        }
        {
            butil::Status st = curve::chunkserver::TransferLeader(logicPoolId,
                                                                  copysetId,
                                                                  conf,
                                                                  peer1,
                                                                  opt);
            ASSERT_TRUE(st.ok());
            Exec("sleep 1s");
            butil::Status status = curve::chunkserver::GetLeader(logicPoolId,
                                                                 copysetId,
                                                                 conf,
                                                                 &leader);
            LOG(INFO) << "get leader:" << status.error_str();
            ASSERT_TRUE(status.ok());
            ASSERT_STREQ(peer1.to_string().c_str(), leader.to_string().c_str());
        }
    }
}

}  // namespace chunkserver
}  // namespace curve
