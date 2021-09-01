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
 * Created Date: Mon Sept 4 2021
 * Author: lixiaocui
 */

#include <brpc/server.h>
#include <gtest/gtest.h>
#include "curvefs/src/client/rpcclient/cli2_client.h"
#include "curvefs/proto/cli2.pb.h"
#include "curvefs/test/client/rpcclient/mock_cli2_service.h"

namespace curvefs {
namespace client {
namespace rpcclient {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

using ::curvefs::common::Peer;
using ::curvefs::metaserver::copyset::GetLeaderRequest2;
using ::curvefs::metaserver::copyset::GetLeaderResponse2;

template <typename RpcRequestType, typename RpcResponseType,
          bool RpcFailed = false>
void RpcService(google::protobuf::RpcController *cntl_base,
                const RpcRequestType *request, RpcResponseType *response,
                google::protobuf::Closure *done) {
    if (RpcFailed) {
        brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);
        cntl->SetFailed(112, "Not connected to");
    }
    done->Run();
}

class Cli2ClientImplTest : public testing::Test {
 protected:
    void SetUp() override {
        ASSERT_EQ(0, server_.AddService(&mockCliService2_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));

        curve::client::EndPoint ep;
        butil::str2endpoint("127.0.0.1", 5800, &ep);
        peerAddr_ = curve::client::PeerAddr(ep);
        peerInfoList_.push_back(
            CopysetPeerInfo<MetaserverID>(1, peerAddr_, peerAddr_));
        peerInfoList_.push_back(
            CopysetPeerInfo<MetaserverID>(2, peerAddr_, peerAddr_));
        peerInfoList_.push_back(
            CopysetPeerInfo<MetaserverID>(3, peerAddr_, peerAddr_));
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

 protected:
    Cli2ClientImpl cli2ClientImp_;
    MockCliService2 mockCliService2_;

    std::string addr_ = "127.0.0.1:5800";
    brpc::Server server_;

    curve::client::PeerAddr peerAddr_;
    PeerInfoList peerInfoList_;
};

TEST_F(Cli2ClientImplTest, test_GetLeaderOK) {
    LogicPoolID poolID = 1;
    CopysetID copysetID = 1;
    int16_t currentLeaderIndex = 0;

    PeerAddr leaderAddr;
    MetaserverID metaserverID;

    GetLeaderResponse2 response;
    Peer *peer = new Peer();
    peer->set_address(peerAddr_.ToString());
    peer->set_id(3);
    response.set_allocated_peer(peer);

    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));


    bool ret = cli2ClientImp_.GetLeader(poolID, copysetID, peerInfoList_,
                                        currentLeaderIndex, &leaderAddr,
                                        &metaserverID);
    ASSERT_TRUE(ret);
    ASSERT_EQ(3, metaserverID);
    ASSERT_EQ("127.0.0.1:5800:0", leaderAddr.ToString());
}

TEST_F(Cli2ClientImplTest, test_GetLeader_OneRPCError) {
    LogicPoolID poolID = 1;
    CopysetID copysetID = 1;
    int16_t currentLeaderIndex = 0;

    PeerAddr leaderAddr;
    MetaserverID metaserverID;

    // one error, one success
    GetLeaderResponse2 response;
    Peer *peer = new Peer();
    peer->set_address(peerAddr_.ToString());
    peer->set_id(2);
    response.set_allocated_peer(peer);
    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .Times(2)
        .WillOnce(
            Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2, true>))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));
    bool ret = cli2ClientImp_.GetLeader(poolID, copysetID, peerInfoList_,
                                        currentLeaderIndex, &leaderAddr,
                                        &metaserverID);
    ASSERT_TRUE(ret);
    ASSERT_EQ(2, metaserverID);
    ASSERT_EQ("127.0.0.1:5800:0", leaderAddr.ToString());
}

TEST_F(Cli2ClientImplTest, test_GetLeader_TwoRPCError) {
    LogicPoolID poolID = 1;
    CopysetID copysetID = 1;
    int16_t currentLeaderIndex = 0;

    PeerAddr leaderAddr;
    MetaserverID metaserverID;

    // both error
    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .WillRepeatedly(
            Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2, true>));
    bool ret = cli2ClientImp_.GetLeader(poolID, copysetID, peerInfoList_,
                                        currentLeaderIndex, &leaderAddr,
                                        &metaserverID);
    ASSERT_FALSE(ret);
}


}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs
