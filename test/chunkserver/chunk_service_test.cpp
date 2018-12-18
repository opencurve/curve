/*
 * Project: curve
 * Created Date: 18-10-22
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

#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/cli.h"
#include "proto/copyset.pb.h"
#include "src/sfs/sfsMock.h"
#include "test/chunkserver/chunkserver_test_util.h"

namespace curve {
namespace chunkserver {

class ChunkserverTest : public testing::Test {
 protected:
    virtual void SetUp() {
        Exec("mkdir 0");
        Exec("mkdir 1");
        Exec("mkdir 2");
    }
    virtual void TearDown() {
        Exec("rm -fr 0");
        Exec("rm -fr 1");
        Exec("rm -fr 2");
    }

 public:
    pid_t pid1;
    pid_t pid2;
    pid_t pid3;
};

butil::AtExitManager atExitManager;

TEST_F(ChunkserverTest, normal_read_write) {
    const char *ip = "127.0.0.1";
    int port = 8300;
    const char *confs = "127.0.0.1:8300:0,127.0.0.1:8301:0,127.0.0.1:8302:0";
    int snapshotInterval = 1;

    /**
     * Start three chunk server by fork
     */
    pid1 = fork();
    if (0 > pid1) {
        std::cerr << "fork chunkserver 1 failed" << std::endl;
        ASSERT_TRUE(false);
    } else if (0 == pid1) {
        const char *copysetdir = "local://./0";
        StartChunkserver(ip, port + 0, copysetdir, confs, snapshotInterval);
        return;
    }

    pid2 = fork();
    if (0 > pid2) {
        std::cerr << "fork chunkserver 2 failed" << std::endl;
        ASSERT_TRUE(false);
    } else if (0 == pid2) {
        const char *copysetdir = "local://./1";
        StartChunkserver(ip, port + 1, copysetdir, confs, snapshotInterval);
        return;
    }

    pid3 = fork();
    if (0 > pid3) {
        std::cerr << "fork chunkserver 3 failed" << std::endl;
        ASSERT_TRUE(false);
    } else if (0 == pid3) {
        const char *copysetdir = "local://./2";
        StartChunkserver(ip, port + 2, copysetdir, confs, snapshotInterval);
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

    const uint32_t kMaxChunkSize = 16 * 1024 * 1024;
    PeerId leader;
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    Configuration conf;
    conf.parse_from(confs);

    /* wait for leader election*/
    /* default election timeout */
    int electionTimeoutMs = 1000;
    ::usleep(1.2 * 1000 * electionTimeoutMs);
    butil::Status status =
        WaitLeader(logicPoolId, copysetId, conf, &leader, electionTimeoutMs);
    ASSERT_TRUE(status.ok());

    /* basic read/write/delete */
    {
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leader.addr, NULL));
        ChunkService_Stub stub(&channel);
        char ch = 'a';
        for (int i = 0; i < 25; ++i) {
            /* Write */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(500);
                ChunkRequest request;
                ChunkResponse response;
                uint64_t chunkId = 1;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                request.set_offset(8 * i);
                request.set_size(8);
                cntl.request_attachment().resize(8, ch);
                stub.WriteChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
            }
            /* Read */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(500);
                ChunkRequest request;
                ChunkResponse response;
                uint64_t chunkId = 1;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                request.set_offset(8 * i);
                request.set_size(8);
                stub.ReadChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                ASSERT_STREQ("aaaaaaaa",
                             cntl.response_attachment().to_string().c_str());
            }
            /* Repeat read */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(500);
                ChunkRequest request;
                ChunkResponse response;
                uint64_t chunkId = 1;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                request.set_offset(8 * i);
                request.set_size(8);
                stub.ReadChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                ASSERT_STREQ("aaaaaaaa",
                             cntl.response_attachment().to_string().c_str());
            }
        }
        ::usleep(2000*1000);
        /*  delete */
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(500);
            ChunkRequest request;
            ChunkResponse response;
            uint64_t chunkId = 1;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            stub.DeleteChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                      response.status());
        }
        /* delete 一个不存在的 chunk（重复删除） */
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(500);
            ChunkRequest request;
            ChunkResponse response;
            uint64_t chunkId = 1;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            stub.DeleteChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                      response.status());
        }
    }

    {
        /* 非法参数 request 测试 */
        brpc::Channel channel;
        if (channel.Init(leader.addr, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to " << leader;
        }
        ChunkService_Stub stub(&channel);
        /* read 溢出 */
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(500);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_offset(8);
            request.set_size(kMaxChunkSize);
            cntl.request_attachment().resize(8, 'a');
            stub.ReadChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                      response.status());
        }
        /* read copyset 不存在 */
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(500);
            ChunkRequest request;
            ChunkResponse response;
            uint64_t chunkId = 1;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
            request.set_logicpoolid(logicPoolId + 1);
            request.set_copysetid(copysetId + 1);
            request.set_chunkid(chunkId);
            request.set_offset(0);
            request.set_size(8);
            stub.ReadChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                      response.status());
        }
        /* write 溢出 */
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(500);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_offset(kMaxChunkSize);
            request.set_size(8);
            cntl.request_attachment().resize(8, 'a');
            stub.WriteChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                      response.status());
        }
        /* 未知 Op */
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(500);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_UNKNOWN);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_offset(0);
            request.set_size(8);
            cntl.request_attachment().resize(8, 'a');
            stub.WriteChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                      response.status());
        }
        /* write copyset 不存在 */
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(500);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
            request.set_logicpoolid(logicPoolId + 1);
            request.set_copysetid(copysetId + 1);
            request.set_chunkid(chunkId);
            request.set_offset(8);
            request.set_size(0);
            cntl.request_attachment().resize(8, 'a');
            stub.WriteChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                      response.status());
        }
        /* delete copyset 不存在*/
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(500);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
            request.set_logicpoolid(logicPoolId + 1);
            request.set_copysetid(copysetId + 1);
            request.set_chunkid(chunkId);
            stub.DeleteChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                      response.status());
        }
        /* 不是 leader */
        {
            PeerId peer1;
            PeerId peer2;
            PeerId peer3;
            ASSERT_EQ(0, peer1.parse("127.0.0.1:8300:0"));
            ASSERT_EQ(0, peer2.parse("127.0.0.1:8301:0"));
            ASSERT_EQ(0, peer3.parse("127.0.0.1:8302:0"));

            brpc::Channel channel;
            if (leader.addr.port != peer1.addr.port) {
                ASSERT_EQ(0, channel.Init(peer1.addr, NULL));
                LOG(INFO) << leader.addr.port << " : " << peer1.addr.port
                          << std::endl;
            } else if (leader.addr.port != peer2.addr.port) {
                ASSERT_EQ(0, channel.Init(peer2.addr, NULL));
                LOG(INFO) << leader.addr.port << " : " << peer2.addr.port
                          << std::endl;
            } else {
                ASSERT_EQ(0, channel.Init(peer3.addr, NULL));
                LOG(INFO) << leader.addr.port << " : " << peer3.addr.port
                          << std::endl;
            }
            ChunkService_Stub stub(&channel);
            brpc::Controller cntl;
            cntl.set_timeout_ms(500);
            ChunkRequest request;
            ChunkResponse response;
            uint64_t chunkId = 1;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_offset(0);
            request.set_size(8);
            cntl.request_attachment().resize(8, 'a');
            stub.WriteChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                      response.status());
        }
    }
    /* 多 chunk read/write/delete */
    {
        brpc::Channel channel;
        if (channel.Init(leader.addr, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to " << leader;
        }
        ChunkService_Stub stub(&channel);
        char ch = 'a';
        uint32_t requstSize = 32;
        uint32_t offset = 1345;
        char writeBuffer[33];
        char readBuffer[33];

        ::memset(writeBuffer, ch, requstSize);
        ::memset(readBuffer, ch, requstSize);
        writeBuffer[requstSize] = '\0';
        readBuffer[requstSize] = '\0';
        std::cerr << "readBuffer: " << readBuffer << " , len: "
                  << sizeof(readBuffer) << std::endl;

        const uint32_t kMaxChunk = 100;
        for (uint32_t i = 1; i < kMaxChunk + 1; ++i) {
            /* Write */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(500);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(i);
                request.set_offset(offset);
                request.set_size(requstSize);
                cntl.request_attachment().append(writeBuffer);
                stub.WriteChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
            }
            /* Read */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(500);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(i);
                request.set_offset(offset);
                request.set_size(requstSize);
                stub.ReadChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                ASSERT_STREQ(readBuffer,
                             cntl.response_attachment().to_string().c_str());
            }
        }
        for (uint32_t i = 1; i < kMaxChunk + 1; ++i) {
            /* delete */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(500);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(i);
                stub.DeleteChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
            }
            /* delete 一个不存在的 chunk（重复删除） */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(500);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(i);
                stub.DeleteChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                          response.status());
            }
        }
    }

    /* read 一个不存在的 chunk */
    {
        brpc::Channel channel;
        uint32_t requestSize = 8;
        uint32_t offset = 8;
        if (channel.Init(leader.addr, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to " << leader;
        }
        ChunkService_Stub stub(&channel);
        /* Write */
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(500);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_offset(offset);
            request.set_size(requestSize);
            cntl.request_attachment().resize(requestSize, 'a');
            stub.WriteChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                      response.status());
        }
        /* Read */
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(500);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_offset(offset);
            request.set_size(requestSize);
            stub.ReadChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                      response.status());
            std::cerr << "read size: " << cntl.response_attachment().size()
                      << std::endl;
            ASSERT_EQ(requestSize, cntl.response_attachment().size());
            ASSERT_STREQ("aaaaaaaa",
                         cntl.response_attachment().to_string().c_str());
        }
        /* delete chunk */
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(500);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            stub.DeleteChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                      response.status());
        }
        /* read 一个不存在的 chunk */
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(500);
            ChunkRequest request;
            ChunkResponse response;
            uint64_t chunkId = 1;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_offset(offset);
            request.set_size(requestSize);
            stub.ReadChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                      response.status());
        }
    }
}

}  // namespace chunkserver
}  // namespace curve
