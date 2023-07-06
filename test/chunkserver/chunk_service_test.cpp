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


#include <gmock/gmock-spec-builders.h>
#include <unistd.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <memory>

#include "include/chunkserver/chunkserver_common.h"
#include "proto/auth.pb.h"
#include "src/chunkserver/copyset_node.h"
#include "proto/copyset.pb.h"
#include "src/common/authenticator.h"
#include "test/chunkserver/chunkserver_test_util.h"
#include "src/common/uuid.h"

namespace curve {
namespace chunkserver {

static constexpr uint32_t kOpRequestAlignSize = 4096;

using curve::common::UUIDGenerator;
using ::testing::_;
using ::testing::Return;
using curve::common::UUIDGenerator;

class ChunkserverTest : public testing::Test {
 protected:
    virtual void SetUp() {
        UUIDGenerator uuidGenerator;
        dir1 = uuidGenerator.GenerateUUID();
        dir2 = uuidGenerator.GenerateUUID();
        dir3 = uuidGenerator.GenerateUUID();
        Exec(("mkdir " + dir1).c_str());
        Exec(("mkdir " + dir2).c_str());
        Exec(("mkdir " + dir3).c_str());

        // auth
        curve::common::ServerAuthOption authOption;
        authOption.enable = true;
        authOption.key = "1122334455667788";
        authOption.lastKey = "1122334455667788";
        authOption.lastKeyTTL = 1800;
        authOption.requestTTL = 15;
        std::string cId = "client";
        std::string sId = "chunkserver";
        std::string sk = "123456789abcdefg";
        auto now = curve::common::TimeUtility::GetTimeofDaySec();
        curve::common::Authenticator::GetInstance().Init(
            curve::common::ZEROIV, authOption);
        curve::mds::auth::Ticket ticket;
        ticket.set_cid(cId);
        ticket.set_sessionkey(sk);
        ticket.set_expiration(now + 100);
        ticket.set_caps("*");
        ticket.set_sid(sId);
        std::string ticketStr;
        ASSERT_TRUE(ticket.SerializeToString(&ticketStr));
        std::string encticket;
        ASSERT_EQ(0, curve::common::Encryptor::AESEncrypt(
            authOption.key, curve::common::ZEROIV, ticketStr, &encticket));
        curve::mds::auth::ClientIdentity clientIdentity;
        clientIdentity.set_cid(cId);
        clientIdentity.set_timestamp(now);
        std::string cIdStr;
        ASSERT_TRUE(clientIdentity.SerializeToString(&cIdStr));
        std::string encCId;
        ASSERT_EQ(0, curve::common::Encryptor::AESEncrypt(
            sk, curve::common::ZEROIV, cIdStr, &encCId));
        token_.set_encticket(encticket);
        token_.set_encclientidentity(encCId);
        fakeToken_.set_encticket("fake");
        fakeToken_.set_encclientidentity("fake");
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

    curve::mds::auth::Token token_;
    curve::mds::auth::Token fakeToken_;
};

butil::AtExitManager atExitManager;


TEST_F(ChunkserverTest, normal_read_write_test) {
    const char *ip = "127.0.0.1";
    int port = 9020;
    const char *confs = "127.0.0.1:9020:0,127.0.0.1:9021:0,127.0.0.1:9022:0";
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
    uint64_t chunkId = 1;
    uint64_t sn = 1;
    char ch = 'a';
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

    /* basic read/write/delete */
    {
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leader.addr, NULL));
        ChunkService_Stub stub(&channel);
        /* read with applied index */
        for (int i = 0; i < 10; ++i) {
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
                request.set_offset(kOpRequestAlignSize * i);
                request.set_size(kOpRequestAlignSize);
                cntl.request_attachment().resize(kOpRequestAlignSize, ch);
                // auth fail, miss token
                stub.WriteChunk(&cntl, &request, &response, nullptr);
                LOG_IF(INFO, cntl.Failed()) << cntl.ErrorText();
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_AUTH_FAIL,
                          response.status());
                // auth fail, fake token
                cntl.Reset();
                request.mutable_authtoken()->set_encticket(
                    fakeToken_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    fakeToken_.encclientidentity());
                cntl.request_attachment().resize(kOpRequestAlignSize, ch);
                stub.WriteChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_AUTH_FAIL,
                          response.status());
                // success
                cntl.Reset();
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
                cntl.request_attachment().resize(kOpRequestAlignSize, ch);
                stub.WriteChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                appliedIndex = response.appliedindex();
                ASSERT_EQ(i + 2, appliedIndex);
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
                request.set_offset(kOpRequestAlignSize * i);
                request.set_size(kOpRequestAlignSize);
                request.set_appliedindex(appliedIndex);
                // auth fail, miss token
                stub.ReadChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_AUTH_FAIL,
                          response.status());
                // auth fail, fake token
                cntl.Reset();
                request.mutable_authtoken()->set_encticket(
                    fakeToken_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    fakeToken_.encclientidentity());
                stub.ReadChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_AUTH_FAIL,
                          response.status());
                // success
                cntl.Reset();
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
                stub.ReadChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                ASSERT_STREQ(expectData,
                             cntl.response_attachment().to_string().c_str());
                appliedIndex = response.appliedindex();
                ASSERT_EQ(i + 2, appliedIndex);
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
                request.set_offset(kOpRequestAlignSize * i);
                request.set_size(kOpRequestAlignSize);
                // lease read will not exec log read,
                // so log index only plus 1 in one turn
                request.set_appliedindex(appliedIndex + 1);
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
                stub.ReadChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                ASSERT_STREQ(expectData,
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
                request.set_offset(kOpRequestAlignSize * i);
                request.set_size(kOpRequestAlignSize);
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
                cntl.request_attachment().resize(kOpRequestAlignSize, ch);
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
                request.set_offset(kOpRequestAlignSize * i);
                request.set_size(kOpRequestAlignSize);
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
                stub.ReadChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                ASSERT_STREQ(expectData,
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
                // auth fail, miss token
                stub.DeleteChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_AUTH_FAIL,
                          response.status());
                // auth fail, fake token
                cntl.Reset();
                request.mutable_authtoken()->set_encticket(
                    fakeToken_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    fakeToken_.encclientidentity());
                stub.DeleteChunk(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_AUTH_FAIL,
                          response.status());
                // success
                cntl.Reset();
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
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
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
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
                request.set_offset(kOpRequestAlignSize * i);
                request.set_size(kOpRequestAlignSize);
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
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
                request.set_offset(kOpRequestAlignSize * i);
                request.set_size(kOpRequestAlignSize);
                request.set_appliedindex(1);
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
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
                request.set_offset(kOpRequestAlignSize * i);
                request.set_size(kOpRequestAlignSize);
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
                cntl.request_attachment().resize(kOpRequestAlignSize, ch);
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
                request.set_offset(kOpRequestAlignSize * i);
                request.set_size(kOpRequestAlignSize);
                // auth fail, miss token
                stub.ReadChunkSnapshot(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_AUTH_FAIL,
                          response.status());
                // auth fail, fake token
                cntl.Reset();
                request.mutable_authtoken()->set_encticket(
                    fakeToken_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    fakeToken_.encclientidentity());
                stub.ReadChunkSnapshot(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_AUTH_FAIL,
                          response.status());
                // success
                cntl.Reset();
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
                stub.ReadChunkSnapshot(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                ASSERT_STREQ(expectData,
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
                request.set_correctedsn(sn);
                // auth fail, miss token
                stub.DeleteChunkSnapshotOrCorrectSn(&cntl,
                                                    &request,
                                                    &response,
                                                    nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_AUTH_FAIL,
                          response.status());
                // auth fail, fake token
                cntl.Reset();
                request.mutable_authtoken()->set_encticket(
                    fakeToken_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    fakeToken_.encclientidentity());
                stub.DeleteChunkSnapshotOrCorrectSn(&cntl,
                                                    &request,
                                                    &response,
                                                    nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_AUTH_FAIL,
                          response.status());
                // success
                cntl.Reset();
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
                stub.DeleteChunkSnapshotOrCorrectSn(&cntl,
                                                    &request,
                                                    &response,
                                                    nullptr);
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
                request.set_correctedsn(sn);
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
                stub.DeleteChunkSnapshotOrCorrectSn(&cntl,
                                                    &request,
                                                    &response,
                                                    nullptr);
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
                // auth fail, miss token
                stub.GetChunkInfo(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_AUTH_FAIL,
                          response.status());
                // auth fail, fake token
                cntl.Reset();
                request.mutable_authtoken()->set_encticket(
                    fakeToken_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    fakeToken_.encclientidentity());
                stub.GetChunkInfo(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_AUTH_FAIL,
                          response.status());
                // success
                cntl.Reset();
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
                stub.GetChunkInfo(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed());
                ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                          response.status());
                ASSERT_EQ(1, response.chunksn().size());
            }
        }
    }

    // get hash
    {
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(leader.addr, NULL));
        ChunkService_Stub stub(&channel);

        // get hash : 访问不存在的chunk
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(rpcTimeoutMs);
            GetChunkHashRequest request;
            GetChunkHashResponse response;
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId + 100);
            request.set_offset(0);
            request.set_length(kOpRequestAlignSize);
            // auth fail, miss token
            stub.GetChunkHash(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_AUTH_FAIL,
                      response.status());
            // auth fail, fake token
            cntl.Reset();
            request.mutable_authtoken()->set_encticket(
                fakeToken_.encticket());
            request.mutable_authtoken()->set_encclientidentity(
                fakeToken_.encclientidentity());
            stub.GetChunkHash(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_AUTH_FAIL,
                      response.status());
            // success
            cntl.Reset();
            request.mutable_authtoken()->set_encticket(
                token_.encticket());
            request.mutable_authtoken()->set_encclientidentity(
                token_.encclientidentity());
            stub.GetChunkHash(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                      response.status());
            ASSERT_STREQ("0", response.hash().c_str());
        }

        // get hash : 非法的offset和length
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(rpcTimeoutMs);
            GetChunkHashRequest request;
            GetChunkHashResponse response;
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId + 100);
            request.set_offset(3);
            request.set_length(kOpRequestAlignSize);
            request.mutable_authtoken()->set_encticket(
                token_.encticket());
            request.mutable_authtoken()->set_encclientidentity(
                token_.encclientidentity());
            stub.GetChunkHash(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                      response.status());
        }

        // Write
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
            request.set_offset(0);
            request.set_size(kOpRequestAlignSize);
            request.mutable_authtoken()->set_encticket(
                token_.encticket());
            request.mutable_authtoken()->set_encclientidentity(
                token_.encclientidentity());
            cntl.request_attachment().resize(kOpRequestAlignSize, ch);
            stub.WriteChunk(&cntl, &request, &response, nullptr);
            LOG_IF(INFO, cntl.Failed()) << cntl.ErrorText();
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                      response.status());
        }

        // read
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
            request.set_size(kOpRequestAlignSize);
            request.mutable_authtoken()->set_encticket(
                token_.encticket());
            request.mutable_authtoken()->set_encclientidentity(
                token_.encclientidentity());
            stub.ReadChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                      response.status());
            ASSERT_STREQ(expectData,
                         cntl.response_attachment().to_string().c_str());
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
            request.mutable_authtoken()->set_encticket(
                token_.encticket());
            request.mutable_authtoken()->set_encclientidentity(
                token_.encclientidentity());
            stub.GetChunkInfo(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                      response.status());
            ASSERT_EQ(1, response.chunksn().size());
        }

        // get hash : 访问存在的chunk
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(rpcTimeoutMs);
            GetChunkHashRequest request;
            GetChunkHashResponse response;
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_offset(0);
            request.set_length(kOpRequestAlignSize);
            request.mutable_authtoken()->set_encticket(
                token_.encticket());
            request.mutable_authtoken()->set_encclientidentity(
                token_.encclientidentity());
            stub.GetChunkHash(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                      response.status());
            ASSERT_STREQ("650595490", response.hash().c_str());
        }
    }

    /* 多 chunk read/write/delete */
    {
        brpc::Channel channel;
        if (channel.Init(leader.addr, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to " << leader;
        }
        ChunkService_Stub stub(&channel);
        uint32_t requstSize = kOpRequestAlignSize;
        uint32_t offset = 0;
        char writeBuffer[kOpRequestAlignSize + 1];
        char readBuffer[kOpRequestAlignSize + 1];

        ::memset(writeBuffer, ch, requstSize);
        ::memset(readBuffer, ch, requstSize);
        writeBuffer[requstSize] = '\0';
        readBuffer[requstSize] = '\0';

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
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
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
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
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
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
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
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
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
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
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
                request.mutable_authtoken()->set_encticket(
                    token_.encticket());
                request.mutable_authtoken()->set_encclientidentity(
                    token_.encclientidentity());
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
        uint32_t requestSize = kOpRequestAlignSize;
        uint32_t offset = 0;
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
            request.mutable_authtoken()->set_encticket(
                token_.encticket());
            request.mutable_authtoken()->set_encclientidentity(
                token_.encclientidentity());
            cntl.request_attachment().resize(requestSize, ch);
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
            request.mutable_authtoken()->set_encticket(
                token_.encticket());
            request.mutable_authtoken()->set_encclientidentity(
                token_.encclientidentity());
            stub.ReadChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                      response.status());
            std::cerr << "read size: " << cntl.response_attachment().size()
                      << std::endl;
            ASSERT_EQ(requestSize, cntl.response_attachment().size());
            ASSERT_STREQ(expectData,
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
            request.mutable_authtoken()->set_encticket(
                token_.encticket());
            request.mutable_authtoken()->set_encclientidentity(
                token_.encclientidentity());
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
            request.mutable_authtoken()->set_encticket(
                token_.encticket());
            request.mutable_authtoken()->set_encclientidentity(
                token_.encclientidentity());
            stub.ReadChunk(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST,
                      response.status());
        }
    }
}  // NOLINT

}  // namespace chunkserver
}  // namespace curve
