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
 * Date: Wed Jan 13 09:48:12 CST 2021
 * Author: wuhanqing
 */

#include <gmock/gmock-spec-builders.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <brpc/server.h>
#include <cstdint>
#include <memory>
#include <string>
#include "include/client/libcurve.h"
#include "proto/auth.pb.h"
#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "src/client/client_common.h"
#include "test/client/mock/mock_namespace_service.h"
#include "test/client/mock/mock_topology_service.h"
#include "test/client/mock/mock_auth_service.h"
#include "src/client/auth_client.h"
#include "src/client/mds_client.h"
#include "src/client/lease_executor.h"

namespace curve {
namespace client {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

using ::curve::mds::auth::GetTicketResponse;
using ::curve::mds::auth::AuthStatusCode;

constexpr uint64_t kGiB = 1024ull * 1024 * 1024;

namespace {
template <bool FAIL>
struct FakeRpcService {
    template <typename Request, typename Response>
    void operator()(google::protobuf::RpcController* cntl_base,
                    const Request* /*request*/,
                    Response* /*response*/,
                    google::protobuf::Closure* done) const {
        if (FAIL) {
            brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
            cntl->SetFailed(112, "Not connected to");
        }

        done->Run();
    }
};
}  // namespace

class MDSClientTest : public testing::Test {
 protected:
    void SetUp() override {
        const std::string mdsAddr1 = "127.0.0.1:9600";
        const std::string mdsAddr2 = "127.0.0.1:9601";

        ASSERT_EQ(0, server_.AddService(&mockNameService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.AddService(&mockTopoService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.AddService(&mockAuthService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));

        // only start mds on mdsAddr1
        ASSERT_EQ(0, server_.Start(mdsAddr1.c_str(), nullptr));

        option_.rpcRetryOpt.addrs = {mdsAddr2, mdsAddr1};
        option_.rpcRetryOpt.rpcTimeoutMs = 500;            // 500ms
        option_.rpcRetryOpt.maxRPCTimeoutMS = 2000;        // 2s
        option_.rpcRetryOpt.rpcRetryIntervalUS = 1000000;  // 100ms
        option_.mdsMaxRetryMS = 8000;             // 8s
        option_.rpcRetryOpt.maxFailedTimesBeforeChangeAddr = 2;

        authOption_.clientId = "curve_client";
        authOption_.enable = true;
        authOption_.key = "123456789abcdefg";
        authOption_.lastKey = "1122334455667788";

        AuthClient::GetInstance().Init(option_, authOption_);

        mdsClient_ = std::make_shared<MDSClient>();
        ASSERT_EQ(LIBCURVE_ERROR::OK, mdsClient_->Initialize(option_));

        // prepare auth token
        std::string serverId = "mds";
        std::string encTicket = "ticket";
        std::string sk = "1122334455667788";
        TicketAttach info;
        info.set_expiration(curve::common::TimeUtility::GetTimeofDaySec()
            + 1000);
        info.set_sessionkey(sk);
        std::string attachStr;
        EXPECT_TRUE(info.SerializeToString(&attachStr));
        std::string encTicketAttach;
        EXPECT_EQ(0, curve::common::Encryptor::AESEncrypt(
            authOption_.key, curve::common::ZEROIV, attachStr,
            &encTicketAttach));

        successRep_.set_status(AuthStatusCode::AUTH_OK);
        successRep_.set_encticket(encTicket);
        successRep_.set_encticketattach(encTicketAttach);

        failRep_.set_status(AuthStatusCode::AUTH_KEY_NOT_EXIST);
    }

    void TearDown() override {
        AuthClient::GetInstance().Uninit();
        server_.Stop(0);
        LOG(INFO) << "server stopped";
        server_.Join();
        LOG(INFO) << "server joined";
    }

 protected:
    brpc::Server server_;
    curve::mds::MockNameService mockNameService_;
    curve::client::MockTopologyService mockTopoService_;
    std::shared_ptr<MDSClient> mdsClient_;
    MetaServerOption option_;
    curve::mds::auth::MockAuthService mockAuthService_;
    curve::common::AuthClientOption authOption_;
    GetTicketResponse successRep_;
    GetTicketResponse failRep_;
};

TEST_F(MDSClientTest, TestOpenFile) {
    const std::string fileName = "/TestOpenFile";
    UserInfo userInfo;
    userInfo.owner = "test";

    FInfo fileInfo;
    FileEpoch_t fEpoch;
    LeaseSession session;

    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));
    // get auth ticket fail
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
                  mdsClient_->OpenFile(fileName, userInfo,
                      &fileInfo, &fEpoch, &session));
    }

    // rpc always failed
    {
        EXPECT_CALL(mockNameService_, OpenFile(_, _, _, _))
            .WillRepeatedly(Invoke(
                FakeRpcService<true>{}));

        auto startMs = TimeUtility::GetTimeofDayMs();
        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
                  mdsClient_->OpenFile(fileName, userInfo,
                      &fileInfo, &fEpoch, &session));
        auto endMs = TimeUtility::GetTimeofDayMs();
        ASSERT_LE(option_.mdsMaxRetryMS, endMs - startMs);
    }

    // rpc response failed
    {
        curve::mds::OpenFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kFileNotExists);

        EXPECT_CALL(mockNameService_, OpenFile(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
                  mdsClient_->OpenFile(fileName, userInfo,
                      &fileInfo, &fEpoch, &session));
    }

    // open normal file success
    {
        curve::mds::OpenFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);
        response.set_allocated_fileinfo(new curve::mds::FileInfo());

        auto* protoSession = new curve::mds::ProtoSession();
        protoSession->set_sessionid("1");
        protoSession->set_leasetime(1);
        protoSession->set_createtime(1);
        protoSession->set_sessionstatus(curve::mds::SessionStatus::kSessionOK);

        response.set_allocated_protosession(protoSession);

        EXPECT_CALL(mockNameService_, OpenFile(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
                  mdsClient_->OpenFile(fileName, userInfo,
                      &fileInfo, &fEpoch, &session));
    }

    // open a flattened clone file
    {
        curve::mds::OpenFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);

        auto* protoFileInfo = new curve::mds::FileInfo();
        protoFileInfo->set_clonesource("/clone");
        protoFileInfo->set_filestatus(curve::mds::FileStatus::kFileCloned);

        auto* protoSession = new curve::mds::ProtoSession();
        protoSession->set_sessionid("1");
        protoSession->set_leasetime(1);
        protoSession->set_createtime(1);
        protoSession->set_sessionstatus(curve::mds::SessionStatus::kSessionOK);

        response.set_allocated_fileinfo(protoFileInfo);
        response.set_allocated_protosession(protoSession);

        EXPECT_CALL(mockNameService_, OpenFile(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
                  mdsClient_->OpenFile(fileName, userInfo,
                      &fileInfo, &fEpoch, &session));
    }

    // open clone file, but response doesn't contains clone source segment
    {
        curve::mds::OpenFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);

        auto* protoFileInfo = new curve::mds::FileInfo();
        protoFileInfo->set_clonesource("/clone");
        protoFileInfo->set_filestatus(
            curve::mds::FileStatus::kFileCloneMetaInstalled);

        auto* protoSession = new curve::mds::ProtoSession();
        protoSession->set_sessionid("1");
        protoSession->set_leasetime(1);
        protoSession->set_createtime(1);
        protoSession->set_sessionstatus(curve::mds::SessionStatus::kSessionOK);

        response.set_allocated_fileinfo(protoFileInfo);
        response.set_allocated_protosession(protoSession);

        EXPECT_CALL(mockNameService_, OpenFile(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
                  mdsClient_->OpenFile(fileName, userInfo,
                      &fileInfo, &fEpoch, &session));
    }

    // open clone file success
    {
        curve::mds::OpenFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);

        auto* protoFileInfo = new curve::mds::FileInfo();
        protoFileInfo->set_clonesource("/clone");
        protoFileInfo->set_clonelength(10 * kGiB);
        protoFileInfo->set_filestatus(
            curve::mds::FileStatus::kFileCloneMetaInstalled);

        auto* protoSession = new curve::mds::ProtoSession();
        protoSession->set_sessionid("1");
        protoSession->set_leasetime(1);
        protoSession->set_createtime(1);
        protoSession->set_sessionstatus(curve::mds::SessionStatus::kSessionOK);

        auto* cloneSourceSegment = new curve::mds::CloneSourceSegment();
        cloneSourceSegment->set_segmentsize(1ull * 1024 * 1024 * 1024);
        cloneSourceSegment->add_allocatedsegmentoffset(0 * kGiB);
        cloneSourceSegment->add_allocatedsegmentoffset(1 * kGiB);
        cloneSourceSegment->add_allocatedsegmentoffset(9 * kGiB);

        response.set_allocated_fileinfo(protoFileInfo);
        response.set_allocated_protosession(protoSession);
        response.set_allocated_clonesourcesegment(cloneSourceSegment);

        EXPECT_CALL(mockNameService_, OpenFile(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
                  mdsClient_->OpenFile(fileName, userInfo,
                      &fileInfo, &fEpoch, &session));

        ASSERT_EQ(fileInfo.sourceInfo.name, "/clone");
        ASSERT_EQ(fileInfo.sourceInfo.length, 10 * kGiB);
        ASSERT_EQ(fileInfo.sourceInfo.segmentSize, 1 * kGiB);
        ASSERT_EQ(fileInfo.sourceInfo.allocatedSegmentOffsets,
                  std::unordered_set<uint64_t>({0 * kGiB, 1 * kGiB, 9 * kGiB}));
    }
}

TEST_F(MDSClientTest, TestCreateFile) {
    CreateFileContext ctx;

    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));

    // get auth ticket fail
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
                  mdsClient_->CreateFile(ctx));
    }

    // rpc always failed
    {
        EXPECT_CALL(mockNameService_, CreateFile(_, _, _, _))
            .WillRepeatedly(Invoke(
                FakeRpcService<true>{}));

        auto startMs = TimeUtility::GetTimeofDayMs();
        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
                  mdsClient_->CreateFile(ctx));
        auto endMs = TimeUtility::GetTimeofDayMs();
        ASSERT_LE(option_.mdsMaxRetryMS, endMs - startMs);
    }

    // rpc response failed
    {
        curve::mds::CreateFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kFileExists);

        EXPECT_CALL(mockNameService_, CreateFile(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::EXISTS, mdsClient_->CreateFile(ctx));
    }

    // create normal file success
    {
        curve::mds::CreateFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);

        EXPECT_CALL(mockNameService_, CreateFile(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK, mdsClient_->CreateFile(ctx));
    }
}

TEST_F(MDSClientTest, TestRefreshSession) {
    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));

    UserInfo userInfo;
    std::string fileName = "/TestRefreshSession";
    std::string sessionId = "1";
    LeaseRefreshResult result;
    LeaseSession session;

    // get auth ticket fail
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
            mdsClient_->RefreshSession(fileName, userInfo, sessionId,
            &result, &session));
    }

    // rpc always failed
    {
        EXPECT_CALL(mockNameService_, RefreshSession(_, _, _, _))
            .WillRepeatedly(Invoke(
                FakeRpcService<true>{}));

        auto startMs = TimeUtility::GetTimeofDayMs();
        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsClient_->RefreshSession(fileName, userInfo, sessionId,
            &result, &session));
        auto endMs = TimeUtility::GetTimeofDayMs();
        ASSERT_LE(option_.mdsMaxRetryMS, endMs - startMs);
    }

    // rpc response failed
    {
        curve::mds::ReFreshSessionResponse response;
        response.set_statuscode(curve::mds::StatusCode::kAuthFailed);
        response.set_sessionid("1");

        EXPECT_CALL(mockNameService_, RefreshSession(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsClient_->RefreshSession(fileName, userInfo, sessionId,
            &result, &session));
    }

    // refresh session success
    {
        curve::mds::ReFreshSessionResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);
        response.set_sessionid("1");
        response.set_allocated_fileinfo(new curve::mds::FileInfo());
        response.mutable_protosession()->set_sessionid("1");
        response.mutable_protosession()->set_createtime(1);
        response.mutable_protosession()->set_leasetime(1);
        response.mutable_protosession()->set_sessionstatus(
            curve::mds::SessionStatus::kSessionOK);
        EXPECT_CALL(mockNameService_, RefreshSession(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));
        ASSERT_EQ(LIBCURVE_ERROR::OK,
            mdsClient_->RefreshSession(fileName, userInfo, sessionId,
            &result, &session));
    }
}

TEST_F(MDSClientTest, TestCloseFile) {
    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));

    UserInfo userInfo;
    const std::string fileName = "/TestCloseFile";
    const std::string sessionId = "1";

    // get auth ticket fail
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
                  mdsClient_->CloseFile(fileName, userInfo, sessionId));
    }

    // rpc always failed
    {
        EXPECT_CALL(mockNameService_, CloseFile(_, _, _, _))
            .WillRepeatedly(Invoke(
                FakeRpcService<true>{}));

        auto startMs = TimeUtility::GetTimeofDayMs();
        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
                  mdsClient_->CloseFile(fileName, userInfo, sessionId));
        auto endMs = TimeUtility::GetTimeofDayMs();
        ASSERT_LE(option_.mdsMaxRetryMS, endMs - startMs);
    }

    // rpc response failed
    {
        curve::mds::CloseFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kFileNotExists);

        EXPECT_CALL(mockNameService_, CloseFile(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::NOTEXIST,
                  mdsClient_->CloseFile(fileName, userInfo, sessionId));
    }

    // close normal file success
    {
        curve::mds::CloseFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);

        EXPECT_CALL(mockNameService_, CloseFile(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
                  mdsClient_->CloseFile(fileName, userInfo, sessionId));
    }
}

TEST_F(MDSClientTest, TestGetFileInfo) {
    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));

    UserInfo userInfo;
    const std::string fileName = "/TestFile";
    FInfo_t finfo;
    FileEpoch_t fEpoch;

    // get auth ticket fail
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
                  mdsClient_->GetFileInfo(fileName, userInfo, &finfo, &fEpoch));
    }

    // rpc always failed
    {
        EXPECT_CALL(mockNameService_, GetFileInfo(_, _, _, _))
            .WillRepeatedly(Invoke(
                FakeRpcService<true>{}));

        auto startMs = TimeUtility::GetTimeofDayMs();
        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
                  mdsClient_->GetFileInfo(fileName, userInfo, &finfo, &fEpoch));
        auto endMs = TimeUtility::GetTimeofDayMs();
        ASSERT_LE(option_.mdsMaxRetryMS, endMs - startMs);
    }

    // rpc response failed
    {
        curve::mds::GetFileInfoResponse response;
        response.set_statuscode(curve::mds::StatusCode::kFileNotExists);

        EXPECT_CALL(mockNameService_, GetFileInfo(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::NOTEXIST,
                  mdsClient_->GetFileInfo(fileName, userInfo, &finfo, &fEpoch));
    }

    // get fileInfo success
    {
        curve::mds::GetFileInfoResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);

        EXPECT_CALL(mockNameService_, GetFileInfo(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
                  mdsClient_->GetFileInfo(fileName, userInfo, &finfo, &fEpoch));
    }
}

TEST_F(MDSClientTest, TestIncreaseEpoch) {
    const std::string fileName = "/TestOpenFile";
    UserInfo userInfo;
    userInfo.owner = "test";

    FInfo fileInfo;
    FileEpoch_t fEpoch;
    std::list<CopysetPeerInfo<ChunkServerID>> csLocs;

    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));
    // get auth ticket fail
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
                  mdsClient_->IncreaseEpoch(fileName, userInfo,
                      &fileInfo, &fEpoch, &csLocs));
    }

    // rpc always failed
    {
        EXPECT_CALL(mockNameService_, IncreaseFileEpoch(_, _, _, _))
            .WillRepeatedly(Invoke(
                FakeRpcService<true>{}));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
                  mdsClient_->IncreaseEpoch(fileName, userInfo,
                      &fileInfo, &fEpoch, &csLocs));
    }
    // rpc response failed
    {
        curve::mds::IncreaseFileEpochResponse response;
        response.set_statuscode(curve::mds::StatusCode::kFileNotExists);

        EXPECT_CALL(mockNameService_, IncreaseFileEpoch(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));


        ASSERT_EQ(LIBCURVE_ERROR::NOTEXIST,
                  mdsClient_->IncreaseEpoch(fileName, userInfo,
                      &fileInfo, &fEpoch, &csLocs));
    }
    // response not have fileInfo
    {
        curve::mds::IncreaseFileEpochResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);

        EXPECT_CALL(mockNameService_, IncreaseFileEpoch(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));


        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
                  mdsClient_->IncreaseEpoch(fileName, userInfo,
                      &fileInfo, &fEpoch, &csLocs));
    }

    // success 1, not have externalAddr
    {
        uint64_t fileId = 100;
        uint64_t epoch = 10086;
        curve::mds::IncreaseFileEpochResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);
        auto fiOut = new curve::mds::FileInfo();
        fiOut->set_id(fileId);
        fiOut->set_epoch(epoch);
        response.set_allocated_fileinfo(fiOut);

        for (int i = 0; i < 10; i++) {
            curve::common::ChunkServerLocation *lc =
                response.mutable_cslocs()->Add();
            lc->set_chunkserverid(i);
            lc->set_hostip("127.0.0.1");
            lc->set_port(8200 + i);
        }

        EXPECT_CALL(mockNameService_, IncreaseFileEpoch(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
                  mdsClient_->IncreaseEpoch(fileName, userInfo,
                      &fileInfo, &fEpoch, &csLocs));

        ASSERT_EQ(fileId, fEpoch.fileId);
        ASSERT_EQ(epoch, fEpoch.epoch);
        ASSERT_EQ(10, csLocs.size());
        int i = 0;
        for (auto it = csLocs.begin(); it != csLocs.end(); it++) {
            ASSERT_EQ(i, it->peerID);
            ASSERT_STREQ("127.0.0.1",
                butil::ip2str(it->internalAddr.addr_.ip).c_str());
            ASSERT_EQ(8200 + i, it->internalAddr.addr_.port);
            i++;
        }
    }

    // success 2, have externalAddr
    {
        uint64_t fileId = 100;
        uint64_t epoch = 10086;
        curve::mds::IncreaseFileEpochResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);
        auto fiOut = new curve::mds::FileInfo();
        fiOut->set_id(fileId);
        fiOut->set_epoch(epoch);
        response.set_allocated_fileinfo(fiOut);

        for (int i = 0; i < 10; i++) {
            curve::common::ChunkServerLocation *lc =
                response.mutable_cslocs()->Add();
            lc->set_chunkserverid(i);
            lc->set_hostip("127.0.0.1");
            lc->set_port(8200 + i);
            lc->set_externalip("127.0.0.2");
        }

        EXPECT_CALL(mockNameService_, IncreaseFileEpoch(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
                  mdsClient_->IncreaseEpoch(fileName, userInfo,
                      &fileInfo, &fEpoch, &csLocs));

        ASSERT_EQ(fileId, fEpoch.fileId);
        ASSERT_EQ(epoch, fEpoch.epoch);
        ASSERT_EQ(10, csLocs.size());
        int i = 0;
        for (auto it = csLocs.begin(); it != csLocs.end(); it++) {
            ASSERT_EQ(i, it->peerID);
            ASSERT_STREQ("127.0.0.1",
                butil::ip2str(it->internalAddr.addr_.ip).c_str());
            ASSERT_STREQ("127.0.0.2",
                butil::ip2str(it->externalAddr.addr_.ip).c_str());
            ASSERT_EQ(8200 + i, it->internalAddr.addr_.port);
            i++;
        }
    }
}

TEST_F(MDSClientTest, TestRecoverFile) {
    std::string fileName = "/TestRecoverFile";
    UserInfo userInfo;
    uint64_t fileId = 1;

    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));
    // get auth ticket fail
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
                  mdsClient_->RecoverFile(fileName, userInfo, fileId));
    }

    // rpc always failed
    {
        EXPECT_CALL(mockNameService_, RecoverFile(_, _, _, _))
            .WillRepeatedly(Invoke(
                FakeRpcService<true>{}));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
                  mdsClient_->RecoverFile(fileName, userInfo, fileId));
    }

    // rpc response failed
    {
        curve::mds::RecoverFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kFileNotExists);

        EXPECT_CALL(mockNameService_, RecoverFile(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));
        ASSERT_EQ(LIBCURVE_ERROR::NOTEXIST,
                  mdsClient_->RecoverFile(fileName, userInfo, fileId));
    }

    // scucess
    {
        curve::mds::RecoverFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);

        EXPECT_CALL(mockNameService_, RecoverFile(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));
        ASSERT_EQ(LIBCURVE_ERROR::OK,
                  mdsClient_->RecoverFile(fileName, userInfo, fileId));
    }
}

TEST_F(MDSClientTest, TestRenameFile) {
    UserInfo userInfo;
    const std::string srcName = "/TestRenameFile";
    const std::string destName = "/TestRenameFile-New";

    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));
    // get auth ticket fail
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
                  mdsClient_->RenameFile(userInfo, srcName, destName));
    }

    // mds return not support
    {
        curve::mds::RenameFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kNotSupported);
        EXPECT_CALL(mockNameService_, RenameFile(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        auto startMs = TimeUtility::GetTimeofDayMs();
        ASSERT_EQ(LIBCURVE_ERROR::NOT_SUPPORT,
                  mdsClient_->RenameFile(userInfo, srcName, destName));
        auto endMs = TimeUtility::GetTimeofDayMs();
        ASSERT_LE(option_.mdsMaxRetryMS, endMs - startMs);
    }

    // mds return file is occupied
    {
        curve::mds::RenameFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kFileOccupied);

        EXPECT_CALL(mockNameService_, RenameFile(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::FILE_OCCUPIED,
                  mdsClient_->RenameFile(userInfo, srcName, destName));
    }

    // mds first return not support, then success
    {
        curve::mds::RenameFileResponse responseNotSupport;
        responseNotSupport.set_statuscode(
            curve::mds::StatusCode::kNotSupported);
        curve::mds::RenameFileResponse responseOK;
        responseOK.set_statuscode(curve::mds::StatusCode::kOK);

        EXPECT_CALL(mockNameService_, RenameFile(_, _, _, _))
            .WillOnce(DoAll(
                SetArgPointee<2>(responseNotSupport),
                Invoke(FakeRpcService<false>{})))
            .WillOnce(DoAll(
                SetArgPointee<2>(responseOK),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
                  mdsClient_->RenameFile(userInfo, srcName, destName));
    }
}

TEST_F(MDSClientTest, TestDeleteFile) {
    UserInfo userInfo;
    const std::string fileName = "/TestDeleteFile";
    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));
    // get auth ticket fail
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
                  mdsClient_->DeleteFile(fileName, userInfo));
    }

    // mds return not support
    {
        curve::mds::DeleteFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kNotSupported);
        EXPECT_CALL(mockNameService_, DeleteFile(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        auto startMs = TimeUtility::GetTimeofDayMs();
        ASSERT_EQ(LIBCURVE_ERROR::NOT_SUPPORT,
                  mdsClient_->DeleteFile(fileName, userInfo));
        auto endMs = TimeUtility::GetTimeofDayMs();
        ASSERT_LE(option_.mdsMaxRetryMS, endMs - startMs);
    }

    // mds return file is occupied
    {
        curve::mds::DeleteFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kFileOccupied);

        EXPECT_CALL(mockNameService_, DeleteFile(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::FILE_OCCUPIED,
                  mdsClient_->DeleteFile(fileName, userInfo));
    }

    // mds first return not support, then success
    {
        curve::mds::DeleteFileResponse responseNotSupport;
        responseNotSupport.set_statuscode(
            curve::mds::StatusCode::kNotSupported);
        curve::mds::DeleteFileResponse responseOK;
        responseOK.set_statuscode(curve::mds::StatusCode::kOK);

        EXPECT_CALL(mockNameService_, DeleteFile(_, _, _, _))
            .WillOnce(DoAll(
                SetArgPointee<2>(responseNotSupport),
                Invoke(FakeRpcService<false>{})))
            .WillOnce(DoAll(
                SetArgPointee<2>(responseOK),
                Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
                  mdsClient_->DeleteFile(fileName, userInfo));
    }
}

TEST_F(MDSClientTest, TestChangeOwner) {
    UserInfo userInfo;
    const std::string fileName = "/TestChangeOwner";
    const std::string newUser = "newuser";
    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));
    // get auth ticket fail
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
                  mdsClient_->ChangeOwner(fileName, newUser, userInfo));
    }

    // mds return not support
    {
        curve::mds::ChangeOwnerResponse response;
        response.set_statuscode(curve::mds::StatusCode::kNotSupported);
        EXPECT_CALL(mockNameService_, ChangeOwner(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(
                    FakeRpcService<false>{})));

        auto startMs = TimeUtility::GetTimeofDayMs();
        ASSERT_EQ(LIBCURVE_ERROR::NOT_SUPPORT,
                  mdsClient_->ChangeOwner(fileName, newUser, userInfo));
        auto endMs = TimeUtility::GetTimeofDayMs();
        ASSERT_LE(option_.mdsMaxRetryMS, endMs - startMs);
    }

    // mds return file is occupied
    {
        curve::mds::ChangeOwnerResponse response;
        response.set_statuscode(curve::mds::StatusCode::kFileOccupied);

        EXPECT_CALL(mockNameService_, ChangeOwner(_, _, _, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<2>(response),
                Invoke(
                    FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::FILE_OCCUPIED,
                  mdsClient_->ChangeOwner(fileName, newUser, userInfo));
    }

    // mds first return not support, then success
    {
        curve::mds::ChangeOwnerResponse responseNotSupport;
        responseNotSupport.set_statuscode(
            curve::mds::StatusCode::kNotSupported);
        curve::mds::ChangeOwnerResponse responseOK;
        responseOK.set_statuscode(curve::mds::StatusCode::kOK);

        EXPECT_CALL(mockNameService_, ChangeOwner(_, _, _, _))
            .WillOnce(DoAll(
                SetArgPointee<2>(responseNotSupport),
                Invoke(
                    FakeRpcService<false>{})))
            .WillOnce(DoAll(
                SetArgPointee<2>(responseOK),
                Invoke(
                    FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
                  mdsClient_->ChangeOwner(fileName, newUser, userInfo));
    }
}

TEST_F(MDSClientTest, TestListPoolset) {
    std::vector<std::string> out;
    mds::topology::ListPoolsetResponse response;

    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));
    // get auth ticket fail
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
                  mdsClient_->ListPoolset(&out));
    }

    // controller failed
    {
        EXPECT_CALL(mockTopoService_, ListPoolset(_, _, _, _))
            .WillRepeatedly(Invoke(FakeRpcService<true>{}));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED, mdsClient_->ListPoolset(&out));
    }

    // request failed
    {
        response.set_statuscode(-1);
        EXPECT_CALL(mockTopoService_, ListPoolset(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED, mdsClient_->ListPoolset(&out));
    }

    // request success
    {
        response.set_statuscode(0);
        auto* poolset = response.add_poolsetinfos();
        poolset->set_poolsetid(1);
        poolset->set_poolsetname("default");
        poolset->set_type("default");
        poolset = response.add_poolsetinfos();
        poolset->set_poolsetid(2);
        poolset->set_poolsetname("system");
        poolset->set_type("SSD");

        EXPECT_CALL(mockTopoService_, ListPoolset(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        out.clear();
        ASSERT_EQ(LIBCURVE_ERROR::OK, mdsClient_->ListPoolset(&out));
        ASSERT_EQ(2, out.size());
        ASSERT_EQ("default", out[0]);
        ASSERT_EQ("system", out[1]);
    }
}

TEST_F(MDSClientTest, TestCreateSnapShot) {
    std::string fileName = "/TestCreateSnapShot";
    UserInfo userInfo;
    uint64_t seq;

    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));
    // get auth ticket fail
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
                  mdsClient_->CreateSnapShot(fileName, userInfo, &seq));
    }

    // controller failed
    {
        EXPECT_CALL(mockNameService_, CreateSnapShot(_, _, _, _))
            .WillRepeatedly(Invoke(FakeRpcService<true>{}));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
                  mdsClient_->CreateSnapShot(fileName, userInfo, &seq));
    }

    // request failed
    {
        curve::mds::CreateSnapShotResponse response;
        response.set_statuscode(curve::mds::StatusCode::kAuthFailed);
        EXPECT_CALL(mockNameService_, CreateSnapShot(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::AUTH_FAILED,
                  mdsClient_->CreateSnapShot(fileName, userInfo, &seq));
    }

    // request success
    {
        curve::mds::CreateSnapShotResponse response;
        response.mutable_snapshotfileinfo()->set_id(1);
        response.set_statuscode(curve::mds::StatusCode::kOK);
        EXPECT_CALL(mockNameService_, CreateSnapShot(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
                  mdsClient_->CreateSnapShot(fileName, userInfo, &seq));
    }
}

TEST_F(MDSClientTest, TestDeleteSnapShot) {
    std::string fileName = "/TestDeleteSnapShot";
    UserInfo userInfo;
    uint64_t seq = 1;

    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));

    // get auth ticket fail
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
                  mdsClient_->DeleteSnapShot(fileName, userInfo, seq));
    }

    // controller failed
    {
        EXPECT_CALL(mockNameService_, DeleteSnapShot(_, _, _, _))
            .WillRepeatedly(Invoke(FakeRpcService<true>{}));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
                  mdsClient_->DeleteSnapShot(fileName, userInfo, seq));
    }

    // request failed
    {
        curve::mds::DeleteSnapShotResponse response;
        response.set_statuscode(curve::mds::StatusCode::kAuthFailed);
        EXPECT_CALL(mockNameService_, DeleteSnapShot(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::AUTH_FAILED,
                  mdsClient_->DeleteSnapShot(fileName, userInfo, seq));
    }

    // request success
    {
        curve::mds::DeleteSnapShotResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);
        EXPECT_CALL(mockNameService_, DeleteSnapShot(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
                  mdsClient_->DeleteSnapShot(fileName, userInfo, seq));
    }
}

TEST_F(MDSClientTest, TestListSnapShot) {
    std::string fileName = "/TestListSnapShot";
    UserInfo userInfo;
    std::vector<uint64_t> seq;
    std::map<uint64_t, FInfo> snapInfo;

    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));
    // get auth ticket fail
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
            mdsClient_->ListSnapShot(fileName, userInfo, &seq, &snapInfo));
    }

    // controller failed
    {
        EXPECT_CALL(mockNameService_, ListSnapShot(_, _, _, _))
            .WillRepeatedly(Invoke(FakeRpcService<true>{}));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsClient_->ListSnapShot(fileName, userInfo, &seq, &snapInfo));
    }

    // request failed
    {
        curve::mds::ListSnapShotFileInfoResponse response;
        response.set_statuscode(curve::mds::StatusCode::kAuthFailed);
        EXPECT_CALL(mockNameService_, ListSnapShot(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::AUTH_FAILED,
            mdsClient_->ListSnapShot(fileName, userInfo, &seq, &snapInfo));
    }

    // request success
    {
        curve::mds::ListSnapShotFileInfoResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);
        EXPECT_CALL(mockNameService_, ListSnapShot(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
            mdsClient_->ListSnapShot(fileName, userInfo, &seq, &snapInfo));
    }
}

TEST_F(MDSClientTest, TestGetSnapShotSegmentInfo) {
    std::string fileName = "/TestGetSnapShotSegmentInfo";
    UserInfo userInfo;
    uint64_t seq = 1;
    uint64_t offset = 0;
    SegmentInfo segInfo;

    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));
    // get auth ticket info
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
            mdsClient_->GetSnapshotSegmentInfo(fileName, userInfo,
                seq, offset, &segInfo));
    }

    // controller failed
    {
        EXPECT_CALL(mockNameService_, GetSnapShotFileSegment(_, _, _, _))
            .WillRepeatedly(Invoke(FakeRpcService<true>{}));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsClient_->GetSnapshotSegmentInfo(fileName, userInfo,
                seq, offset, &segInfo));
    }

    // request failed
    {
        curve::mds::GetOrAllocateSegmentResponse response;
        response.set_statuscode(curve::mds::StatusCode::kAuthFailed);
        EXPECT_CALL(mockNameService_, GetSnapShotFileSegment(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::AUTH_FAILED,
            mdsClient_->GetSnapshotSegmentInfo(fileName, userInfo,
                seq, offset, &segInfo));
    }

    // request success
    {
        curve::mds::GetOrAllocateSegmentResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);
        EXPECT_CALL(mockNameService_, GetSnapShotFileSegment(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
            mdsClient_->GetSnapshotSegmentInfo(fileName, userInfo,
                seq, offset, &segInfo));
    }
}

TEST_F(MDSClientTest, TestCheckSnapShotStatus) {
    std::string fileName = "/TestCheckSnapShotStatus";
    UserInfo userInfo;
    uint64_t seq = 1;
    FileStatus status;

    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));
    // get auth ticket info
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
            mdsClient_->CheckSnapShotStatus(fileName, userInfo, seq, &status));
    }

    // controller failed
    {
        EXPECT_CALL(mockNameService_, CheckSnapShotStatus(_, _, _, _))
            .WillRepeatedly(Invoke(FakeRpcService<true>{}));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsClient_->CheckSnapShotStatus(fileName, userInfo, seq, &status));
    }

    // request failed
    {
        curve::mds::CheckSnapShotStatusResponse response;
        response.set_statuscode(curve::mds::StatusCode::kAuthFailed);
        EXPECT_CALL(mockNameService_, CheckSnapShotStatus(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::AUTH_FAILED,
            mdsClient_->CheckSnapShotStatus(fileName, userInfo, seq, &status));
    }

    // request success
    {
        curve::mds::CheckSnapShotStatusResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);
        EXPECT_CALL(mockNameService_, CheckSnapShotStatus(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
            mdsClient_->CheckSnapShotStatus(fileName, userInfo, seq, &status));
    }
}

TEST_F(MDSClientTest, TestGetClusterInfo) {
    ClusterContext ctx;
    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));
    // get auth ticket info
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
            mdsClient_->GetClusterInfo(&ctx));
    }

    // controller failed
    {
        EXPECT_CALL(mockTopoService_, GetClusterInfo(_, _, _, _))
            .WillRepeatedly(Invoke(FakeRpcService<true>{}));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsClient_->GetClusterInfo(&ctx));
    }

    // request failed
    {
        curve::mds::topology::GetClusterInfoResponse response;
        response.set_statuscode(-1);
        EXPECT_CALL(mockTopoService_, GetClusterInfo(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsClient_->GetClusterInfo(&ctx));
    }

    // request success
    {
        curve::mds::topology::GetClusterInfoResponse response;
        response.set_statuscode(0);
        response.set_clusterid("1");
        EXPECT_CALL(mockTopoService_, GetClusterInfo(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
            mdsClient_->GetClusterInfo(&ctx));
    }
}

TEST_F(MDSClientTest, TestCreateCloneFile) {
    std::string source = "source";
    std::string destination = "destination";
    UserInfo_t userInfo;
    uint64_t size = 1024;
    uint64_t sn = 1;
    uint32_t chunksize = 16 * 1024 * 1024;
    uint32_t stripeUnit = 16 * 1024 * 1024;
    uint32_t stripeCount = 1;
    std::string poolset = "ssd";
    FInfo fInfo;

    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));

    // get auth ticket info
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
            mdsClient_->CreateCloneFile(source, destination, userInfo, size,
                                        sn, chunksize, stripeUnit,
                                        stripeCount, poolset, &fInfo));
    }

    // controller failed
    {
        EXPECT_CALL(mockNameService_, CreateCloneFile(_, _, _, _))
            .WillRepeatedly(Invoke(FakeRpcService<true>{}));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsClient_->CreateCloneFile(source, destination, userInfo, size,
                                        sn, chunksize, stripeUnit,
                                        stripeCount, poolset, &fInfo));
    }

    // request failed
    {
        curve::mds::CreateCloneFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kAuthFailed);
        EXPECT_CALL(mockNameService_, CreateCloneFile(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::AUTH_FAILED,
            mdsClient_->CreateCloneFile(source, destination, userInfo, size,
                                        sn, chunksize, stripeUnit,
                                        stripeCount, poolset, &fInfo));
    }

    // request success
    {
        curve::mds::CreateCloneFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);
        EXPECT_CALL(mockNameService_, CreateCloneFile(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
            mdsClient_->CreateCloneFile(source, destination, userInfo, size,
                                        sn, chunksize, stripeUnit,
                                        stripeCount, poolset, &fInfo));
    }
}

TEST_F(MDSClientTest, TestSetCloneFileStatus) {
    std::string filename = "filename";
    FileStatus status = FileStatus::BeingCloned;
    UserInfo_t userInfo;
    uint64_t fId = 1;

    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));

    // get auth ticket info
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
            mdsClient_->SetCloneFileStatus(filename, status, userInfo, fId));
    }

    // controller failed
    {
        EXPECT_CALL(mockNameService_, SetCloneFileStatus(_, _, _, _))
            .WillRepeatedly(Invoke(FakeRpcService<true>{}));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsClient_->SetCloneFileStatus(filename, status, userInfo, fId));
    }

    // request failed
    {
        curve::mds::SetCloneFileStatusResponse response;
        response.set_statuscode(curve::mds::StatusCode::kAuthFailed);
        EXPECT_CALL(mockNameService_, SetCloneFileStatus(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::AUTH_FAILED,
            mdsClient_->SetCloneFileStatus(filename, status, userInfo, fId));
    }

    // request success
    {
        curve::mds::SetCloneFileStatusResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);
        EXPECT_CALL(mockNameService_, SetCloneFileStatus(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
            mdsClient_->SetCloneFileStatus(filename, status, userInfo, fId));
    }
}

TEST_F(MDSClientTest, TestDeAllocateSegment) {
    uint64_t offset = 0;
    FInfo_t finfo;

    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));

    // get auth ticket info
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
            mdsClient_->DeAllocateSegment(&finfo, offset));
    }

    // controller failed
    {
        EXPECT_CALL(mockNameService_, DeAllocateSegment(_, _, _, _))
            .WillRepeatedly(Invoke(FakeRpcService<true>{}));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsClient_->DeAllocateSegment(&finfo, offset));
    }

    // request failed
    {
        curve::mds::DeAllocateSegmentResponse response;
        response.set_statuscode(curve::mds::StatusCode::kAuthFailed);
        EXPECT_CALL(mockNameService_, DeAllocateSegment(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::AUTH_FAILED,
            mdsClient_->DeAllocateSegment(&finfo, offset));
    }

    // request success
    {
        curve::mds::DeAllocateSegmentResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);
        EXPECT_CALL(mockNameService_, DeAllocateSegment(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
            mdsClient_->DeAllocateSegment(&finfo, offset));
    }
}

TEST_F(MDSClientTest, TestExtend) {
    std::string filename = "filename";
    UserInfo_t userInfo;
    uint64_t newsize = 1;

    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));

    // get auth ticket info
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
            mdsClient_->Extend(filename, userInfo, newsize));
    }

    // controller failed
    {
        EXPECT_CALL(mockNameService_, ExtendFile(_, _, _, _))
            .WillRepeatedly(Invoke(FakeRpcService<true>{}));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsClient_->Extend(filename, userInfo, newsize));
    }

    // request failed
    {
        curve::mds::ExtendFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kAuthFailed);
        EXPECT_CALL(mockNameService_, ExtendFile(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::AUTH_FAILED,
            mdsClient_->Extend(filename, userInfo, newsize));
    }

    // request success
    {
        curve::mds::ExtendFileResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);
        EXPECT_CALL(mockNameService_, ExtendFile(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
            mdsClient_->Extend(filename, userInfo, newsize));
    }
}

TEST_F(MDSClientTest, TestListDir) {
    std::string dirpath = "/dirpath";
    UserInfo_t userInfo;
    std::vector<FileStatInfo> files;

    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));

    // get auth ticket info
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
            mdsClient_->Listdir(dirpath, userInfo, &files));
    }

    // controller failed
    {
        EXPECT_CALL(mockNameService_, ListDir(_, _, _, _))
            .WillRepeatedly(Invoke(FakeRpcService<true>{}));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsClient_->Listdir(dirpath, userInfo, &files));
    }

    // request failed
    {
        curve::mds::ListDirResponse response;
        response.set_statuscode(curve::mds::StatusCode::kAuthFailed);
        EXPECT_CALL(mockNameService_, ListDir(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::AUTH_FAILED,
            mdsClient_->Listdir(dirpath, userInfo, &files));
    }

    // request success
    {
        curve::mds::ListDirResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);
        EXPECT_CALL(mockNameService_, ListDir(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
            mdsClient_->Listdir(dirpath, userInfo, &files));
    }
}

TEST_F(MDSClientTest, TestGetChunkServerInfo) {
    butil::ip_t ip;
    EXPECT_EQ(0, butil::str2ip("127.0.0.1", &ip));
    PeerAddr csAddr(butil::EndPoint(ip, 8200));
    CopysetPeerInfo<ChunkServerID> chunkserverInfo;

    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));

    // address invalid
    {
        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsClient_->GetChunkServerInfo(PeerAddr(), &chunkserverInfo));
    }

    // get auth ticket info
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
            mdsClient_->GetChunkServerInfo(csAddr, &chunkserverInfo));
    }

    // controller failed
    {
        EXPECT_CALL(mockTopoService_, GetChunkServer(_, _, _, _))
            .WillRepeatedly(Invoke(FakeRpcService<true>{}));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsClient_->GetChunkServerInfo(csAddr, &chunkserverInfo));
    }

    // request failed
    {
        curve::mds::topology::GetChunkServerInfoResponse response;
        response.set_statuscode(-1);
        EXPECT_CALL(mockTopoService_, GetChunkServer(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsClient_->GetChunkServerInfo(csAddr, &chunkserverInfo));
    }

    // request success
    {
        curve::mds::topology::GetChunkServerInfoResponse response;
        response.set_statuscode(0);
        EXPECT_CALL(mockTopoService_, GetChunkServer(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
            mdsClient_->GetChunkServerInfo(csAddr, &chunkserverInfo));
    }
}

TEST_F(MDSClientTest, TestListChunkServerInServer) {
    std::string serverIp = "127.0.0.1";
    std::vector<ChunkServerID> csIDs;
    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(failRep_),
                        Invoke(FakeRpcService<false>{})))
        .WillRepeatedly(DoAll(SetArgPointee<2>(successRep_),
                        Invoke(FakeRpcService<false>{})));
    // get auth ticket info
    {
        ASSERT_EQ(LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL,
            mdsClient_->ListChunkServerInServer(serverIp, &csIDs));
    }

    // controller failed
    {
        EXPECT_CALL(mockTopoService_, ListChunkServer(_, _, _, _))
            .WillRepeatedly(Invoke(FakeRpcService<true>{}));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsClient_->ListChunkServerInServer(serverIp, &csIDs));
    }

    // request failed
    {
        curve::mds::topology::ListChunkServerResponse response;
        response.set_statuscode(-1);
        EXPECT_CALL(mockTopoService_, ListChunkServer(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsClient_->ListChunkServerInServer(serverIp, &csIDs));
    }

    // request success
    {
        curve::mds::topology::ListChunkServerResponse response;
        response.set_statuscode(0);
        EXPECT_CALL(mockTopoService_, ListChunkServer(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(FakeRpcService<false>{})));

        ASSERT_EQ(LIBCURVE_ERROR::OK,
            mdsClient_->ListChunkServerInServer(serverIp, &csIDs));
    }
}

}  // namespace client
}  // namespace curve
