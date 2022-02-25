/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: 2022-03-02
 * Author: lixiaocui
 */

#include <brpc/server.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "curvefs/src/metaserver/mdsclient/mds_client.h"
#include "curvefs/test/client/rpcclient/mock_mds_base_client.h"
#include "curvefs/test/client/rpcclient/mock_mds_service.h"
#include "src/client/mds_client.h"
#include "curvefs/proto/topology.pb.h"

namespace curvefs {
namespace metaserver {
namespace mdsclient {

using curvefs::mds::topology::TopoStatusCode;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

using ::curvefs::mds::topology::TopoStatusCode;
using ::curvefs::client::rpcclient::MockMDSBaseClient;
using ::curvefs::client::rpcclient::MockMdsService;


void GetFsInfoByFsnameRpcFailed(const std::string &fsName,
                                GetFsInfoResponse *response,
                                brpc::Controller *cntl,
                                brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}

void GetFsInfoByFsIDRpcFailed(uint32_t fsId, GetFsInfoResponse *response,
                              brpc::Controller *cntl, brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}


class MdsClientImplTest : public testing::Test {
 protected:
    void SetUp() override {
        ::curve::client::MetaServerOption mdsopt;
        mdsopt.rpcRetryOpt.rpcTimeoutMs = 500;            // 500ms
        mdsopt.rpcRetryOpt.maxRPCTimeoutMS = 1000;        // 1s
        mdsopt.rpcRetryOpt.rpcRetryIntervalUS = 1000000;  // 100ms
        mdsopt.mdsMaxRetryMS = 2000;                      // 2s
        mdsopt.rpcRetryOpt.maxFailedTimesBeforeChangeAddr = 2;

        ASSERT_EQ(0, server_.AddService(&mockMdsService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        uint16_t port = 56800;
        int ret = 0;
        while (port < 65535) {
            std::string addr = ip_ + ":" + std::to_string(port);
            ret = server_.Start(addr.c_str(), nullptr);
            if (ret >= 0) {
                mdsopt.rpcRetryOpt.addrs = {addr};
                LOG(INFO) << "service success, listen port = " << port;
                break;
            }
            ++port;
        }
        ASSERT_EQ(0, ret);
        ASSERT_EQ(FSStatusCode::OK, mdsclient_.Init(mdsopt, &mockmdsbasecli_));
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

 protected:
    MdsClientImpl mdsclient_;
    MockMDSBaseClient mockmdsbasecli_;

    MockMdsService mockMdsService_;
    std::string ip_ = "127.0.0.1";
    brpc::Server server_;
};

TEST_F(MdsClientImplTest, test_GetFsInfo_by_fsname) {
    std::string fsName = "test1";
    FsInfo out;

    curvefs::mds::GetFsInfoResponse response;
    auto fsinfo = new curvefs::mds::FsInfo();
    fsinfo->set_fsid(1);
    fsinfo->set_fsname(fsName);
    fsinfo->set_rootinodeid(1);
    fsinfo->set_capacity(10 * 1024 * 1024L);
    fsinfo->set_blocksize(4 * 1024);
    auto vresp = new curvefs::common::Volume();
    vresp->set_volumesize(10 * 1024 * 1024L);
    vresp->set_blocksize(4 * 1024);
    vresp->set_volumename("test1");
    vresp->set_user("test");
    vresp->set_password("test");
    auto detail = new curvefs::mds::FsDetail();
    detail->set_allocated_volume(vresp);
    fsinfo->set_allocated_detail(detail);
    fsinfo->set_mountnum(1);
    fsinfo->add_mountpoints("0.0.0.0:/data");
    response.set_allocated_fsinfo(fsinfo);

    // 1. get fsinfo ok
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsName, _, _, _))
        .WillOnce(SetArgPointee<1>(response));
    ASSERT_EQ(FSStatusCode::OK, mdsclient_.GetFsInfo(fsName, &out));
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

    // 2. get fsinfo not found
    out.Clear();
    response.set_statuscode(curvefs::mds::FSStatusCode::NOT_FOUND);
    EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsName, _, _, _))
        .WillOnce(SetArgPointee<1>(response));
    ASSERT_EQ(FSStatusCode::NOT_FOUND, mdsclient_.GetFsInfo(fsName, &out));
    ASSERT_FALSE(
        google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

    // 3. get rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsName, _, _, _))
        .WillRepeatedly(Invoke(GetFsInfoByFsnameRpcFailed));
    ASSERT_EQ(FSStatusCode::RPC_ERROR, mdsclient_.GetFsInfo(fsName, &out));
}

TEST_F(MdsClientImplTest, test_GetFsInfo_by_fsid) {
    uint32_t fsid = 1;
    FsInfo out;

    curvefs::mds::GetFsInfoResponse response;
    auto fsinfo = new curvefs::mds::FsInfo();
    fsinfo->set_fsid(1);
    fsinfo->set_fsname("test1");
    fsinfo->set_rootinodeid(1);
    fsinfo->set_capacity(10 * 1024 * 1024L);
    fsinfo->set_blocksize(4 * 1024);
    auto vresp = new curvefs::common::Volume();
    vresp->set_volumesize(10 * 1024 * 1024L);
    vresp->set_blocksize(4 * 1024);
    vresp->set_volumename("test1");
    vresp->set_user("test");
    vresp->set_password("test");
    auto detail = new curvefs::mds::FsDetail();
    detail->set_allocated_volume(vresp);
    fsinfo->set_allocated_detail(detail);
    fsinfo->set_mountnum(1);
    fsinfo->add_mountpoints("0.0.0.0:/data");
    response.set_allocated_fsinfo(fsinfo);

    // 1. get file info ok
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsid, _, _, _))
        .WillOnce(SetArgPointee<1>(response));
    ASSERT_EQ(FSStatusCode::OK, mdsclient_.GetFsInfo(fsid, &out));
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

    // 2. get fsinfo unknow error
    out.Clear();
    response.set_statuscode(curvefs::mds::FSStatusCode::UNKNOWN_ERROR);
    EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsid, _, _, _))
        .WillOnce(SetArgPointee<1>(response));
    ASSERT_EQ(FSStatusCode::UNKNOWN_ERROR, mdsclient_.GetFsInfo(fsid, &out));
    ASSERT_FALSE(
        google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

    // 3. get rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsid, _, _, _))
        .WillRepeatedly(Invoke(GetFsInfoByFsIDRpcFailed));
    ASSERT_EQ(FSStatusCode::RPC_ERROR, mdsclient_.GetFsInfo(fsid, &out));
}

}  // namespace mdsclient
}  // namespace metaserver
}  // namespace curvefs
