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


TEST_F(ChunkserverTest, normal_read_write_test) {
    const char *ip = "127.0.0.1";
    int port = 8300;
    const char *confs = "127.0.0.1:8300:0,127.0.0.1:8301:0,127.0.0.1:8302:0";
    int rpcTimeoutMs = 3000;
    int snapshotInterval = 600;

    /* wait for leader election*/
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
        const char *copysetdir = "local://./0";
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
        const char *copysetdir = "local://./1";
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
        const char *copysetdir = "local://./2";
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

    const uint32_t kMaxChunkSize = 16 * 1024 * 1024;
    PeerId leader;
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    uint64_t sn = 1;
    Configuration conf;
    conf.parse_from(confs);

    ::usleep(1000 * electionTimeoutMs);

    butil::Status status =
        WaitLeader(logicPoolId, copysetId, conf, &leader, electionTimeoutMs);
    LOG_IF(INFO, status.ok()) << "leader id: " << leader.to_string();
    ASSERT_TRUE(status.ok());

    /* basic read/write/delete */
    {
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leader.addr, NULL));
        ChunkService_Stub stub(&channel);
        char ch = 'a';
        /* read with applied index */
        for (int i = 0; i < 25; ++i) {
            uint64_t appliedIndex = 0;
            /* Write */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                request.set_sn(sn);
                request.set_offset(8 * i);
                request.set_size(8);
                cntl.request_attachment().resize(8, ch);
                stub.WriteChunk(&cntl, &request, &response, nullptr);
                LOG_IF(INFO, cntl.Failed()) << cntl.ErrorText();
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                appliedIndex = response.appliedindex();
                ASSERT_EQ(i + 2 + i, appliedIndex);
            }
            /* Read */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                request.set_sn(sn);
                request.set_offset(8 * i);
                request.set_size(8);
                request.set_appliedindex(appliedIndex);
                stub.ReadChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                ASSERT_STREQ("aaaaaaaa",
                             cntl.response_attachment().to_string().c_str());
                appliedIndex = response.appliedindex();
                ASSERT_EQ(i + 2 + i, appliedIndex);
            }
            /* Repeat read with illegal applied index */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                request.set_sn(sn);
                request.set_offset(8 * i);
                request.set_size(8);
                request.set_appliedindex(appliedIndex + 1);
                stub.ReadChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                ASSERT_STREQ("aaaaaaaa",
                             cntl.response_attachment().to_string().c_str());
            }
        }
        LOG(INFO) << "begin read without applied index test \n";
        /* read without applied index */
        for (int i = 0; i < 10; ++i) {
            /* Write */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                request.set_sn(sn);
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
                cntl.set_timeout_ms(rpcTimeoutMs);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                request.set_sn(sn);
                request.set_offset(8 * i);
                request.set_size(8);
                stub.ReadChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                ASSERT_STREQ("aaaaaaaa",
                             cntl.response_attachment().to_string().c_str());
            }
            /*  delete */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                request.set_sn(sn);
                stub.DeleteChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
            }
            /* delete 一个不存在的 chunk（重复删除） */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                request.set_sn(sn);
                stub.DeleteChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                          response.status());
            }
            /* Read 一个不存在的 Chunk */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                request.set_sn(sn);
                request.set_offset(8 * i);
                request.set_size(8);
                stub.ReadChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST,
                          response.status());
            }
            /* Applied index Read 一个不存在的 Chunk */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                request.set_sn(sn);
                request.set_offset(8 * i);
                request.set_size(8);
                request.set_appliedindex(1);
                stub.ReadChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST,
                          response.status());
            }
            /* Write */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                request.set_sn(sn);
                request.set_offset(8 * i);
                request.set_size(8);
                cntl.request_attachment().resize(8, ch);
                stub.WriteChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
            }
            /* read snapshot */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ_SNAP);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                request.set_sn(sn);
                request.set_offset(8 * i);
                request.set_size(8);
                stub.ReadChunkSnapshot(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                ASSERT_STREQ("aaaaaaaa",
                             cntl.response_attachment().to_string().c_str());
            }
            /*  delete snapshot */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                request.set_sn(sn);
                stub.DeleteChunkSnapshot(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
            }
            /* repeat delete snapshot */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                request.set_sn(sn);
                stub.DeleteChunkSnapshot(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                          response.status());
            }
            /* get chunk info */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                GetChunkInfoRequest request;
                GetChunkInfoResponse response;
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                stub.GetChunkInfo(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                ASSERT_EQ(0, response.chunksn().size());
            }
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

        const uint32_t kMaxChunk = 10;
        for (uint32_t i = 1; i < kMaxChunk + 1; ++i) {
            /* Write */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(i);
                request.set_sn(sn);
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
                cntl.set_timeout_ms(rpcTimeoutMs);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(i);
                request.set_sn(sn);
                request.set_offset(offset);
                request.set_size(requstSize);
                stub.ReadChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                ASSERT_STREQ(readBuffer,
                             cntl.response_attachment().to_string().c_str());
            }
            /* get chunk info */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                GetChunkInfoRequest request;
                GetChunkInfoResponse response;
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(i);
                stub.GetChunkInfo(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                ASSERT_EQ(1, response.chunksn().size());
            }
            /* get chunk info : chunk not exist*/
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                GetChunkInfoRequest request;
                GetChunkInfoResponse response;
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(kMaxChunk + 1);
                stub.GetChunkInfo(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                ASSERT_EQ(0, response.chunksn().size());
            }
        }
        for (uint32_t i = 1; i < kMaxChunk + 1; ++i) {
            /* delete */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(i);
                request.set_sn(sn);
                stub.DeleteChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
            }
            /* delete 一个不存在的 chunk（重复删除） */
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(rpcTimeoutMs);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(i);
                request.set_sn(sn);
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
            cntl.set_timeout_ms(rpcTimeoutMs);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_sn(sn);
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
            cntl.set_timeout_ms(rpcTimeoutMs);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_sn(sn);
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
            cntl.set_timeout_ms(rpcTimeoutMs);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_sn(sn);
            stub.DeleteChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                      response.status());
        }
        /* read 一个不存在的 chunk */
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(rpcTimeoutMs);
            ChunkRequest request;
            ChunkResponse response;
            uint64_t chunkId = 1;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_sn(sn);
            request.set_offset(offset);
            request.set_size(requestSize);
            stub.ReadChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST,
                      response.status());
        }
    }

    /* 非法参数 request 测试 */
    brpc::Channel channel;
    if (channel.Init(leader.addr, NULL) != 0) {
        LOG(ERROR) << "Fail to init channel to " << leader;
    }
    ChunkService_Stub stub(&channel);
    /* read 溢出 */
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(rpcTimeoutMs);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_sn(sn);

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
        cntl.set_timeout_ms(rpcTimeoutMs);
        ChunkRequest request;
        ChunkResponse response;
        uint64_t chunkId = 1;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId + 1);
        request.set_chunkid(chunkId);
        request.set_offset(0);
        request.set_size(8);
        request.set_sn(sn);
        stub.ReadChunk(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  response.status());
    }

    /* read snapshot 溢出 */
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(rpcTimeoutMs);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ_SNAP);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_sn(sn);
        request.set_offset(8);
        request.set_size(kMaxChunkSize);
        cntl.request_attachment().resize(8, 'a');
        stub.ReadChunkSnapshot(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  response.status());
    }
    /* read snapshot copyset 不存在 */
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(rpcTimeoutMs);
        ChunkRequest request;
        ChunkResponse response;
        uint64_t chunkId = 1;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ_SNAP);
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId + 1);
        request.set_chunkid(chunkId);
        request.set_sn(sn);
        request.set_offset(0);
        request.set_size(8);
        stub.ReadChunkSnapshot(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  response.status());
    }
    /* write 溢出 */
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(rpcTimeoutMs);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_offset(kMaxChunkSize);
        request.set_size(8);
        request.set_sn(sn);
        cntl.request_attachment().resize(8, 'a');
        stub.WriteChunk(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  response.status());
    }
    /* write copyset 不存在 */
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(rpcTimeoutMs);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId + 1);
        request.set_chunkid(chunkId);
        request.set_sn(sn);
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
        cntl.set_timeout_ms(rpcTimeoutMs);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId + 1);
        request.set_chunkid(chunkId);
        request.set_sn(sn);
        stub.DeleteChunk(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  response.status());
    }
    /* delete snapshot copyset 不存在*/
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(rpcTimeoutMs);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId + 1);
        request.set_chunkid(chunkId);
        request.set_sn(sn);
        stub.DeleteChunkSnapshot(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  response.status());
    }
    /* get chunk info copyset not exist */
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(rpcTimeoutMs);
        GetChunkInfoRequest request;
        GetChunkInfoResponse response;
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId + 1);
        request.set_chunkid(chunkId);
        stub.GetChunkInfo(&cntl, &request, &response, nullptr);
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
        // write
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(rpcTimeoutMs);
            ChunkRequest request;
            ChunkResponse response;
            uint64_t chunkId = 1;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_sn(sn);
            request.set_offset(0);
            request.set_size(8);
            cntl.request_attachment().resize(8, 'a');
            stub.WriteChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                      response.status());
        }
        // read without applied index
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(rpcTimeoutMs);
            ChunkRequest request;
            ChunkResponse response;
            uint64_t chunkId = 1;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_sn(sn);
            request.set_offset(0);
            request.set_size(8);
            stub.ReadChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                      response.status());
        }
        // read with applied index
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(rpcTimeoutMs);
            ChunkRequest request;
            ChunkResponse response;
            uint64_t chunkId = 1;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_sn(sn);
            request.set_offset(0);
            request.set_size(8);
            request.set_appliedindex(1);
            stub.ReadChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                      response.status());
        }
        // get chunk info
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(rpcTimeoutMs);
            GetChunkInfoRequest request;
            GetChunkInfoResponse response;
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            stub.GetChunkInfo(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                      response.status());
        }
    }
}  // NOLINT

}  // namespace chunkserver
}  // namespace curve
