/*
 * Project: curve
 * Created Date: 18-9-12
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>

#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/cli.h"
#include "proto/copyset.pb.h"
#include "src/chunkserver/type.h"
#include "src/sfs/sfsMock.h"

namespace curve {
namespace chunkserver {



DEFINE_int32(timeout_ms, 500, "Timeout for each request");

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

class ChunkServiceTest : public testing::Test {
 protected:
    virtual void SetUp() {
        // before test: start servers
        std::string result = Exec(run.c_str());
        std::cout << result << std::endl;
    }

    virtual void TearDown() {
        // after test: stop servers, debug 的时候可以注释掉以便查看日志
//        std::string result = Exec(stop.c_str());
//        std::cout << result << std::endl;
    }

 private:
    // 结束脚本
    std::string stop = R"(
        killall -9 server-test
        sleep 2s
        ps -ef | grep server-test
        rm -fr 0 1 2
    )";
    // 初始化脚本
    std::string run = R"(
        killall -9 server-test

        rm -fr 0
        mkdir 0
        rm -fr 1
        mkdir 1
        rm -fr 2
        mkdir 2

        cp -f server-test ./0
        cd 0
        ./server-test -bthread_concurrency=18 -crash_on_fatal_log=true -raft_sync=true -ip=127.0.0.1 -port=8200 -conf=127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0 > std.log 2>&1 &
        cd ..
        sleep 1s

        cp -f server-test ./1
        cd 1
        ./server-test -bthread_concurrency=18 -crash_on_fatal_log=true -raft_sync=true -ip=127.0.0.1 -port=8201 -conf=127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0 > std.log 2>&1 &
        cd ..

        cp -f server-test ./2
        cd 2
        ./server-test -bthread_concurrency=18 -crash_on_fatal_log=true -raft_sync=true -ip=127.0.0.1 -port=8202 -conf=127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0 > std.log 2>&1 &
        cd ..
        sleep 2s

        ps -ef | grep server-test
    )";
};

TEST_F(ChunkServiceTest, normal_read_write) {
    const uint32_t kMaxChunkSize = 4 * 1024 * 1024;
    PeerId leader;
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    Configuration conf;
    conf.parse_from("127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0");

    sleep(2);
    // get leader
    butil::Status status = GetLeader(logicPoolId, copysetId, conf, &leader);
    std::cout << "Leader is: " << leader.to_string() << std::endl;
    ASSERT_TRUE(status.ok());


    /// basic read/write/delete
    {
        brpc::Channel channel;
        if (channel.Init(leader.addr, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to " << leader;
        }
        ChunkService_Stub stub(&channel);
        char ch = 'a';
        for (int i = 0; i < 25; ++i) {
            // Write
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(FLAGS_timeout_ms);
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
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS, response.status());
            }

            // Read
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(FLAGS_timeout_ms);
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
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS, response.status());
                ASSERT_STREQ("aaaaaaaa", cntl.response_attachment().to_string().c_str());
            }
            // Repeat read
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(FLAGS_timeout_ms);
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
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS, response.status());
                ASSERT_STREQ("aaaaaaaa", cntl.response_attachment().to_string().c_str());
            }
        }
        // delete
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
            ChunkRequest request;
            ChunkResponse response;
            uint64_t chunkId = 1;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            stub.DeleteChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS, response.status());
        }
        // delete 一个不存在的 chunk（重复删除）
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
            ChunkRequest request;
            ChunkResponse response;
            uint64_t chunkId = 1;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            stub.DeleteChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN, response.status());
        }
    }

    {
        /// 非法参数 request 测试
        brpc::Channel channel;
        if (channel.Init(leader.addr, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to " << leader;
        }
        ChunkService_Stub stub(&channel);
        /// read 溢出
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
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
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST, response.status());
        }
        /// write 溢出
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_offset(8);
            request.set_size(kMaxChunkSize);
            cntl.request_attachment().resize(8, 'a');
            stub.WriteChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST, response.status());
        }
        // read 不存在的 chunk
    }

    /// 多 chunk read/write/delete
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
        std::cerr << "readBuffer: " << readBuffer << " , len: " << sizeof(readBuffer) << std::endl;

        const uint32_t kMaxChunk = 1000;
        for (uint32_t i = 1; i < kMaxChunk + 1; ++i) {
            // Write
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(FLAGS_timeout_ms);
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
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS, response.status());
            }
            // Read
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(FLAGS_timeout_ms);
                ChunkRequest request;
                ChunkResponse response;
                uint64_t chunkId = 1;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(chunkId);
                request.set_offset(offset);
                request.set_size(requstSize);
                stub.ReadChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS, response.status());
                ASSERT_STREQ(readBuffer, cntl.response_attachment().to_string().c_str());
            }
        }
        for (uint32_t i = 1; i < kMaxChunk + 1; ++i) {
            // delete
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(FLAGS_timeout_ms);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(i);
                stub.DeleteChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS, response.status());
            }
            // delete 一个不存在的 chunk（重复删除）
            {
                brpc::Controller cntl;
                cntl.set_timeout_ms(FLAGS_timeout_ms);
                ChunkRequest request;
                ChunkResponse response;
                request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
                request.set_logicpoolid(logicPoolId);
                request.set_copysetid(copysetId);
                request.set_chunkid(i);
                stub.DeleteChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN, response.status());
            }
        }
    }


    /// read 一个不存在的 chunk
    {
        brpc::Channel channel;
        uint32_t requestSize = 8;
        uint32_t offset = 8;
        if (channel.Init(leader.addr, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to " << leader;
        }
        ChunkService_Stub stub(&channel);
        // Write
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
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
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS, response.status());
        }

        // Read
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
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
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS, response.status());
            std::cerr << "read size: " << cntl.response_attachment().size() << std::endl;
            ASSERT_EQ(requestSize, cntl.response_attachment().size());
            ASSERT_STREQ("aaaaaaaa", cntl.response_attachment().to_string().c_str());
        }
        // delete chunk
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            stub.DeleteChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS, response.status());
        }
        // read 一个不存在的 chunk
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
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
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN, response.status());
        }
    }

    // TODO(wudemiao): read/write 文件系统层失败的情况下
}

}  // namespace chunkserver
}  // namespace curve
