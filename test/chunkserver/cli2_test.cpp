/*
 * Project: curve
 * Created Date: 18-9-7
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <unistd.h>
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
#include "src/chunkserver/cli2.h"
#include "proto/copyset.pb.h"
#include "test/chunkserver/chunkserver_test_util.h"
#include "src/common/uuid.h"

namespace curve {
namespace chunkserver {

using curve::common::UUIDGenerator;

class Cli2Test : public testing::Test {
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

TEST_F(Cli2Test, basic) {
    const char *ip = "127.0.0.1";
    int port = 9033;
    const char *confs = "127.0.0.1:9033:0,127.0.0.1:9034:0,127.0.0.1:9035:0";
    int snapshotInterval = 600;

    /**
     * 设置更大的默认选举超时时间，因为当前 ci 环境很容易出现超时
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

    /* wait for leader & become leader flush config */
    ::usleep(1.5 * 1000 * electionTimeoutMs);
    butil::Status status =
        WaitLeader(logicPoolId, copysetId, conf, &leader, electionTimeoutMs);
    ASSERT_TRUE(status.ok());

    /* 等待 transfer leader 成功 */
    int waitTransferLeader = 3000 * 1000;
    /**
     * 配置变更因为设置一条 log entry 的完成复制，所以设置较长的 timeout
     * 时间，以免在 ci 环境偶尔会出现超时出错
     */
    braft::cli::CliOptions opt;
    opt.timeout_ms = 6000;
    opt.max_retry = 3;

    /* remove peer */
    {
        Peer peer;
        peer.set_address("127.0.0.1:9035:0");
        butil::Status st = curve::chunkserver::RemovePeer(logicPoolId,
                                                          copysetId,
                                                          conf,
                                                          peer,
                                                          opt);
        LOG(INFO) << "remove peer: "
                  << st.error_code() << ", " << st.error_str();
        ASSERT_TRUE(st.ok());
        /* 可能移除的是 leader，如果移除的是 leader，那么需要等到新的 leader 产生，
         * 否则下面的 add peer 测试就会失败， wait 较长时间，是为了保证 remove
         * leader 之后新 leader 选举成功，切 become leader 的 flush config
         * 完成 */
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
        conf.parse_from("127.0.0.1:9033:0,127.0.0.1:9034:0");
        Peer peer;
        peer.set_address("127.0.0.1:9035:0");
        butil::Status st = curve::chunkserver::AddPeer(logicPoolId,
                                                       copysetId,
                                                       conf,
                                                       peer,
                                                       opt);
        LOG(INFO) << "add peer: "
                  << st.error_code() << ", " << st.error_str();
        ASSERT_TRUE(st.ok());
    }
    /* 重复 add 同一个 peer */
    {
        Configuration conf;
        conf.parse_from("127.0.0.1:9033:0,127.0.0.1:9034:0");
        Peer peer;
        peer.set_address("127.0.0.1:9035:0");
        butil::Status st = curve::chunkserver::AddPeer(logicPoolId,
                                                       copysetId,
                                                       conf,
                                                       peer,
                                                       opt);
        LOG(INFO) << "add one peer repeat: "
                  << st.error_code() << ", " << st.error_str();
        ASSERT_TRUE(st.ok());
    }
    /*  transfer leader */
    {
        Configuration conf;
        conf.parse_from("127.0.0.1:9033:0,127.0.0.1:9034:0,127.0.0.1:9035:0");
        Peer peer1;
        peer1.set_address("127.0.0.1:9033:0");
        Peer peer2;
        peer2.set_address("127.0.0.1:9034:0");
        Peer peer3;
        peer3.set_address("127.0.0.1:9035:0");
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
            /* transfer leader 只是讲 rpc 发送给leader，并不会等 leader transfer
             * 成功才返回，所以这里需要等，除此之外，并不能立马去查 leader，因为
             * leader transfer 之后，可能返回之前的 leader，除此之外 transfer
             * leader 成功了之后，become leader 进行时，leader 已经可查，但是
             * become leader 会执行 flush 当前 conf 来充当 noop，如果这个时候
             * 立马进行下一个 transfer leader，会被组织，因为同时只能有一个配置
             * 变更在进行 */
            ::usleep(waitTransferLeader);
            butil::Status status = WaitLeader(logicPoolId,
                                              copysetId,
                                              conf,
                                              &leader,
                                              electionTimeoutMs);
            LOG(INFO) << "get leader: "
                      << status.error_code() << ", " << status.error_str();
            ASSERT_TRUE(status.ok());
            ASSERT_STREQ(peer1.address().c_str(), leader.to_string().c_str());
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
            ASSERT_STREQ(peer2.address().c_str(), leader.to_string().c_str());
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
            ASSERT_STREQ(peer3.address().c_str(), leader.to_string().c_str());
        }
        /* transfer 给 leader 给 leader，仍然返回成功 */
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
            ASSERT_STREQ(peer3.address().c_str(), leader.to_string().c_str());
        }
    }

    /* 异常分支测试 */
    /* get leader - conf empty */
    {
        Configuration conf;
        Peer leader;
        butil::Status status = GetLeader(logicPoolId, copysetId, conf, &leader);
        ASSERT_FALSE(status.ok());
        ASSERT_EQ(EINVAL, status.error_code());
    }
    /* get leader - 非法的地址 */
    {
        Configuration conf;
        Peer leader;
        conf.parse_from("127.0.0.1:65540:0,127.0.0.1:65541:0,127.0.0.1:65542:0");   //NOLINT
        butil::Status status = GetLeader(logicPoolId, copysetId, conf, &leader);
        ASSERT_FALSE(status.ok());
        ASSERT_EQ(-1, status.error_code());
    }
    /* add peer - 不存在的 peer */
    {
        Configuration conf;
        conf.parse_from("127.0.0.1:9033:0,127.0.0.1:9034:0,127.0.0.1:9035:2");
        /* 添加一个根本不存在的节点 */
        Peer peer;
        peer.set_address("127.0.0.1:9039:2");
        butil::Status status = curve::chunkserver::AddPeer(logicPoolId,
                                                           copysetId,
                                                           conf,
                                                           peer,
                                                           opt);
        ASSERT_FALSE(status.ok());
        LOG(INFO) << "add peer: " << status.error_code() << ", "
                  << status.error_str();
    }
    /* transfer leader - 不存在的 peer */
    {
        Configuration conf;
        conf.parse_from("127.0.0.1:9033:0,127.0.0.1:9034:0,127.0.0.1:9035:2");
        Peer peer;
        peer.set_address("127.0.0.1:9039:0");
        {
            butil::Status
                status = curve::chunkserver::TransferLeader(logicPoolId,
                                                            copysetId,
                                                            conf,
                                                            peer,
                                                            opt);
            ASSERT_FALSE(status.ok());
            LOG(INFO) << "add peer: " << status.error_code() << ", "
                      << status.error_str();
        }
    }
}

}  // namespace chunkserver
}  // namespace curve
