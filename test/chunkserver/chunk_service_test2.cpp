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
 * Created Date: 18-10-22
 * Author: wudemiao
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
#include "src/common/uuid.h"
#include "src/chunkserver/chunk_service.h"

namespace curve {
namespace chunkserver {

using curve::common::UUIDGenerator;

static constexpr uint32_t kOpRequestAlignSize = 4096;

class ChunkService2Test : public testing::Test {
 protected:
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

TEST_F(ChunkService2Test, illegial_parameters_test) {
    const char *ip = "127.0.0.1";
    int port = 9023;
    const char *confs = "127.0.0.1:9023:0,127.0.0.1:9024:0,127.0.0.1:9025:0";
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

    const uint32_t kMaxChunkSize = 16 * 1024 * 1024;
    PeerId leader;
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    uint64_t sn = 1;
    char expectData[kOpRequestAlignSize + 1];
    ::memset(expectData, 'a', kOpRequestAlignSize);
    expectData[kOpRequestAlignSize] = '\0';
    Configuration conf;
    conf.parse_from(confs);

    ::usleep(1000 * electionTimeoutMs);

    butil::Status status =
        WaitLeader(logicPoolId, copysetId, conf, &leader, electionTimeoutMs);
    LOG_IF(INFO, status.ok()) << "leader id: " << leader.to_string();
    ASSERT_TRUE(status.ok());

    /*Illegal parameter request test*/
    brpc::Channel channel;
    if (channel.Init(leader.addr, NULL) != 0) {
        LOG(ERROR) << "Fail to init channel to " << leader;
    }
    ChunkService_Stub stub(&channel);
    /*Read overflow*/
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
        request.set_offset(kOpRequestAlignSize);
        request.set_size(kMaxChunkSize);
        stub.ReadChunk(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  response.status());
    }
    /*Read offset not aligned*/
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
        request.set_offset(kOpRequestAlignSize - 1);
        request.set_size(kOpRequestAlignSize);
        stub.ReadChunk(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  response.status());
    }
    /*Read size not aligned*/
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
        request.set_offset(0);
        request.set_size(kOpRequestAlignSize - 1);
        stub.ReadChunk(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  response.status());
    }
    /*Read copyset does not exist*/
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
        request.set_size(kOpRequestAlignSize);
        request.set_sn(sn);
        stub.ReadChunk(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  response.status());
    }
    /*Read snapshot overflow*/
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
        request.set_offset(kOpRequestAlignSize);
        request.set_size(kMaxChunkSize);
        stub.ReadChunkSnapshot(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  response.status());
    }
    /*Read snapshot offset not aligned*/
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
        request.set_offset(kOpRequestAlignSize - 1);
        request.set_size(kOpRequestAlignSize);
        stub.ReadChunkSnapshot(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  response.status());
    }
    /*Read snapshot size not aligned*/
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
        request.set_size(kOpRequestAlignSize - 1);
        stub.ReadChunkSnapshot(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  response.status());
    }
    /*Read snapshot copyset does not exist*/
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
        request.set_size(kOpRequestAlignSize);
        stub.ReadChunkSnapshot(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  response.status());
    }
    /*Write overflow*/
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
        request.set_size(kOpRequestAlignSize);
        request.set_sn(sn);
        cntl.request_attachment().resize(kOpRequestAlignSize, 'a');
        stub.WriteChunk(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  response.status());
    }
    /*Write offset not aligned*/
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
        request.set_offset(kOpRequestAlignSize - 1);
        request.set_size(kOpRequestAlignSize);
        cntl.request_attachment().resize(kOpRequestAlignSize, 'a');
        stub.WriteChunk(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  response.status());
    }
    /*Write size not aligned*/
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
        request.set_offset(kOpRequestAlignSize);
        request.set_size(kOpRequestAlignSize - 1);
        cntl.request_attachment().resize(kOpRequestAlignSize - 1, 'a');
        stub.WriteChunk(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  response.status());
    }
    /*The write copyset does not exist*/
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
        request.set_offset(0);
        request.set_size(kOpRequestAlignSize);
        cntl.request_attachment().resize(kOpRequestAlignSize, 'a');
        stub.WriteChunk(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  response.status());
    }
    /*Delete copyset does not exist*/
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
    /*Delete snapshot copyset does not exist*/
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(rpcTimeoutMs);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId + 1);
        request.set_chunkid(chunkId);
        request.set_correctedsn(sn);
        stub.DeleteChunkSnapshotOrCorrectSn(&cntl,
                                            &request,
                                            &response,
                                            nullptr);
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
    /*Not a leader*/
    {
        PeerId peer1;
        PeerId peer2;
        PeerId peer3;
        ASSERT_EQ(0, peer1.parse("127.0.0.1:9023:0"));
        ASSERT_EQ(0, peer2.parse("127.0.0.1:9024:0"));
        ASSERT_EQ(0, peer3.parse("127.0.0.1:9025:0"));

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
            request.set_size(kOpRequestAlignSize);
            cntl.request_attachment().resize(kOpRequestAlignSize, 'a');
            stub.WriteChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                      response.status());
            // ASSERT_EQ(response.redirect(), leader.to_string());
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
            request.set_size(kOpRequestAlignSize);
            stub.ReadChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                      response.status());
            // ASSERT_EQ(response.redirect(), leader.to_string());
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
            request.set_size(kOpRequestAlignSize);
            request.set_appliedindex(1);
            stub.ReadChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                      response.status());
            // ASSERT_EQ(response.redirect(), leader.to_string());
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
            // ASSERT_EQ(response.redirect(), leader.to_string());
        }
    }
}

class ChunkServiceTestClosure : public ::google::protobuf::Closure {
 public:
    explicit ChunkServiceTestClosure(int sleepUs = 0) : sleep_(sleepUs) {
    }
    virtual ~ChunkServiceTestClosure() = default;

    void Run() override {
        if (0 != sleep_) {
            //Sleep test, overload
            ::usleep(sleep_);
            LOG(INFO) << "return rpc";
        }
    }

 private:
    int sleep_;
};

class UpdateEpochTestClosure : public ::google::protobuf::Closure {
 public:
    explicit UpdateEpochTestClosure(int sleepUs = 0) : sleep_(sleepUs) {
    }
    virtual ~UpdateEpochTestClosure() = default;

    void Run() override {
        if (0 != sleep_) {
            //Sleep test, overload
            ::usleep(sleep_);
            LOG(INFO) << "return rpc";
        }
    }

 private:
    int sleep_;
};

TEST_F(ChunkService2Test, overload_test) {
    CopysetNodeOptions copysetNodeOptions;
    copysetNodeOptions.maxChunkSize = 16 * 1024 * 1024;

    // inflight throttle
    uint64_t maxInflight = 0;
    std::shared_ptr<InflightThrottle> inflightThrottle
        = std::make_shared<InflightThrottle>(maxInflight);
    CHECK(nullptr != inflightThrottle) << "new inflight throttle failed";

    // chunk service
    CopysetNodeManager &nodeManager = CopysetNodeManager::GetInstance();
    ChunkServiceOptions chunkServiceOptions;
    chunkServiceOptions.copysetNodeManager = &nodeManager;
    chunkServiceOptions.inflightThrottle = inflightThrottle;
    auto epochMap = std::make_shared<EpochMap>();
    ChunkServiceImpl chunkService(chunkServiceOptions, epochMap);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10000;
    ChunkID chunkId = 1;

    // write chunk
    {
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.WriteChunk(&cntl, &request, &response, &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // read chunk
    {
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.ReadChunk(&cntl, &request, &response, &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // delete chunk
    {
        LogicPoolID logicPoolId = 1;
        CopysetID copysetId = 10000;
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.DeleteChunk(&cntl, &request, &response, &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // read snapshot
    {
        LogicPoolID logicPoolId = 1;
        CopysetID copysetId = 10000;
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ_SNAP);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.ReadChunkSnapshot(&cntl, &request, &response, &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // delete snapshot
    {
        LogicPoolID logicPoolId = 1;
        CopysetID copysetId = 10000;
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.DeleteChunkSnapshotOrCorrectSn(&cntl,
                                                    &request,
                                                    &response,
                                                    &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // create clone chunk
    {
        LogicPoolID logicPoolId = 1;
        CopysetID copysetId = 10000;
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_CREATE_CLONE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.CreateCloneChunk(&cntl, &request, &response, &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // recover chunk
    {
        LogicPoolID logicPoolId = 1;
        CopysetID copysetId = 10000;
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_RECOVER);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.RecoverChunk(&cntl, &request, &response, &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // get chunk info
    {
        brpc::Controller cntl;
        GetChunkInfoRequest request;
        GetChunkInfoResponse response;
        ChunkServiceTestClosure done;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.GetChunkInfo(&cntl, &request, &response, &done);

        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }
}

TEST_F(ChunkService2Test, overload_concurrency_test) {
    CopysetNodeOptions copysetNodeOptions;
    copysetNodeOptions.maxChunkSize = 16 * 1024 * 1024;

    // inflight throttle
    uint64_t maxInflight = 10;
    std::shared_ptr<InflightThrottle> inflightThrottle
        = std::make_shared<InflightThrottle>(maxInflight);
    CHECK(nullptr != inflightThrottle) << "new inflight throttle failed";

    // chunk service
    CopysetNodeManager &nodeManager = CopysetNodeManager::GetInstance();
    ChunkServiceOptions chunkServiceOptions;
    chunkServiceOptions.copysetNodeManager = &nodeManager;
    chunkServiceOptions.inflightThrottle = inflightThrottle;
    auto epochMap = std::make_shared<EpochMap>();
    ChunkServiceImpl chunkService(chunkServiceOptions, epochMap);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10000;
    ChunkID chunkId = 1;

    auto writeFunc = [&] {
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done(1000 * 1000);
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.WriteChunk(&cntl, &request, &response, &done);
        ASSERT_NE(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    };

    std::vector<std::thread> threads;
    //Start 10 threads to fully load the chunkserver
    for (int i = 0; i < 10; ++i) {
        std::thread t1(writeFunc);
        threads.push_back(std::move(t1));
    }

    //Waiting for the process to start
    ::usleep(500 * 1000);
    ASSERT_FALSE(inflightThrottle->IsOverLoad());

    //All requests received after the chunkserver is filled will be rejected
    // write chunk
    {
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.WriteChunk(&cntl, &request, &response, &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // read chunk
    {
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.ReadChunk(&cntl, &request, &response, &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // delete chunk
    {
        LogicPoolID logicPoolId = 1;
        CopysetID copysetId = 10000;
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.DeleteChunk(&cntl, &request, &response, &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // read snapshot
    {
        LogicPoolID logicPoolId = 1;
        CopysetID copysetId = 10000;
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ_SNAP);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.ReadChunkSnapshot(&cntl, &request, &response, &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // delete snapshot
    {
        LogicPoolID logicPoolId = 1;
        CopysetID copysetId = 10000;
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.DeleteChunkSnapshotOrCorrectSn(&cntl,
                                                    &request,
                                                    &response,
                                                    &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // create clone chunk
    {
        LogicPoolID logicPoolId = 1;
        CopysetID copysetId = 10000;
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_CREATE_CLONE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.CreateCloneChunk(&cntl, &request, &response, &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // recover chunk
    {
        LogicPoolID logicPoolId = 1;
        CopysetID copysetId = 10000;
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_RECOVER);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.RecoverChunk(&cntl, &request, &response, &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // get chunk info
    {
        brpc::Controller cntl;
        GetChunkInfoRequest request;
        GetChunkInfoResponse response;
        ChunkServiceTestClosure done;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.GetChunkInfo(&cntl, &request, &response, &done);

        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    //Wait for the request processing to complete, and then chunkserver can receive new requests again
    for (auto it = threads.begin(); it != threads.end(); ++it) {
        it->join();
    }

    ASSERT_FALSE(inflightThrottle->IsOverLoad());

    // write chunk
    {
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.WriteChunk(&cntl, &request, &response, &done);
        ASSERT_NE(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // read chunk
    {
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.ReadChunk(&cntl, &request, &response, &done);
        ASSERT_NE(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // delete chunk
    {
        LogicPoolID logicPoolId = 1;
        CopysetID copysetId = 10000;
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.DeleteChunk(&cntl, &request, &response, &done);
        ASSERT_NE(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // read snapshot
    {
        LogicPoolID logicPoolId = 1;
        CopysetID copysetId = 10000;
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ_SNAP);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.ReadChunkSnapshot(&cntl, &request, &response, &done);
        ASSERT_NE(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // delete snapshot
    {
        LogicPoolID logicPoolId = 1;
        CopysetID copysetId = 10000;
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.DeleteChunkSnapshotOrCorrectSn(&cntl,
                                                    &request,
                                                    &response,
                                                    &done);
        ASSERT_NE(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // create clone chunk
    {
        LogicPoolID logicPoolId = 1;
        CopysetID copysetId = 10000;
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_CREATE_CLONE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.CreateCloneChunk(&cntl, &request, &response, &done);
        ASSERT_NE(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // recover chunk
    {
        LogicPoolID logicPoolId = 1;
        CopysetID copysetId = 10000;
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_RECOVER);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.RecoverChunk(&cntl, &request, &response, &done);
        ASSERT_NE(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }

    // get chunk info
    {
        brpc::Controller cntl;
        GetChunkInfoRequest request;
        GetChunkInfoResponse response;
        ChunkServiceTestClosure done;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.GetChunkInfo(&cntl, &request, &response, &done);

        ASSERT_NE(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD, response.status());
    }
}

TEST_F(ChunkService2Test, CheckEpochTest) {
    CopysetNodeOptions copysetNodeOptions;
    copysetNodeOptions.maxChunkSize = 16 * 1024 * 1024;

    // inflight throttle
    uint64_t maxInflight = 10000;
    std::shared_ptr<InflightThrottle> inflightThrottle
        = std::make_shared<InflightThrottle>(maxInflight);
    CHECK(nullptr != inflightThrottle) << "new inflight throttle failed";

    // chunk service
    CopysetNodeManager &nodeManager = CopysetNodeManager::GetInstance();
    ChunkServiceOptions chunkServiceOptions;
    chunkServiceOptions.copysetNodeManager = &nodeManager;
    chunkServiceOptions.inflightThrottle = inflightThrottle;
    auto epochMap = std::make_shared<EpochMap>();
    ChunkServiceImpl chunkService(chunkServiceOptions, epochMap);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10000;
    ChunkID chunkId = 1;

    // write chunk request have no epoch
    {
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        chunkService.WriteChunk(&cntl, &request, &response, &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
            response.status());
    }

    // write chunk request have epoch, but epoch map have no epoch
    {
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_fileid(1);
        request.set_epoch(1);
        chunkService.WriteChunk(&cntl, &request, &response, &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
            response.status());
    }
    // update epoch map to {(1, 1) , (2, 2)}
    {
        brpc::Controller cntl;
        UpdateEpochRequest request;
        UpdateEpochResponse response;
        UpdateEpochTestClosure done;
        request.set_fileid(1);
        request.set_epoch(1);
        chunkService.UpdateEpoch(&cntl, &request, &response, &done);

        request.set_fileid(2);
        request.set_epoch(2);
        chunkService.UpdateEpoch(&cntl, &request, &response, &done);
    }
    // write chunk check epoch success
    {
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_fileid(1);
        request.set_epoch(1);
        chunkService.WriteChunk(&cntl, &request, &response, &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
            response.status());
    }
    // write chunk check epoch failed
    {
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_fileid(2);
        request.set_epoch(1);
        chunkService.WriteChunk(&cntl, &request, &response, &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_EPOCH_TOO_OLD,
            response.status());
    }

    // update epoch map to {(1, 2) , (2, 2)}
    {
        brpc::Controller cntl;
        UpdateEpochRequest request;
        UpdateEpochResponse response;
        UpdateEpochTestClosure done;
        request.set_fileid(1);
        request.set_epoch(2);
        chunkService.UpdateEpoch(&cntl, &request, &response, &done);
    }

    // write chunk check epoch failed 2
    {
        brpc::Controller cntl;
        ChunkRequest request;
        ChunkResponse response;
        ChunkServiceTestClosure done;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_fileid(1);
        request.set_epoch(1);
        chunkService.WriteChunk(&cntl, &request, &response, &done);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_EPOCH_TOO_OLD,
            response.status());
    }
}

}  // namespace chunkserver
}  // namespace curve
