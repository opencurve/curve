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
 * Created Date: Thur Jun 17 2021
 * Author: lixiaocui
 */

#include <brpc/server.h>
#include <gtest/gtest.h>
#include <google/protobuf/util/message_differencer.h>

#include "curvefs/src/client/mds_client.h"
#include "curvefs/test/client/mock_mds_base_client.h"
#include "curvefs/test/client/mock_mds_service.h"

namespace curvefs {
namespace client {
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

class MdsClientImplTest : public testing::Test {
 protected:
    void SetUp() override {
        MdsOption mdsopt;
        mdsopt.mdsaddr = addr_;
        mdsopt.rpcTimeoutMs = 1000;

        ASSERT_EQ(CURVEFS_ERROR::OK, mdsclient_.Init(mdsopt, &mockmdsbasecli_));

        ASSERT_EQ(0, server_.AddService(&mockMdsService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

 protected:
    MdsClientImpl mdsclient_;
    MockMDSBaseClient mockmdsbasecli_;

    MockMdsService mockMdsService_;
    std::string addr_ = "127.0.0.1:5602";
    brpc::Server server_;
};

TEST_F(MdsClientImplTest, test_CreateFs) {
    std::string fsName = "test1";
    uint64_t blockSize = 4 * 1024;
    curvefs::common::Volume v;
    v.set_volumesize(10 * 1024 * 1024L);
    v.set_blocksize(4 * 1024);
    v.set_volumename("test1");
    v.set_user("test");
    v.set_password("test");

    curvefs::mds::CreateFsResponse response;
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
    fsinfo->set_allocated_volume(vresp);
    fsinfo->set_mountnum(1);
    fsinfo->add_mountpoints("0.0.0.0:/data");
    response.set_allocated_fsinfo(fsinfo);
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, CreateFs(_, _, _, _, _, _))
        .WillOnce(SetArgPointee<3>(response));

    ASSERT_EQ(CURVEFS_ERROR::OK, mdsclient_.CreateFs(fsName, blockSize, v));
}

TEST_F(MdsClientImplTest, test_DeleteFs) {
    std::string fsName = "test1";

    curvefs::mds::DeleteFsResponse response;
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, DeleteFs(_, _, _, _))
        .WillOnce(SetArgPointee<1>(response));

    ASSERT_EQ(CURVEFS_ERROR::OK, mdsclient_.DeleteFs(fsName));
}

TEST_F(MdsClientImplTest, test_MountFs) {
    std::string fsName = "test1";
    std::string mp = "0.0.0.0:/data";
    FsInfo out;

    curvefs::mds::MountFsResponse response;
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
    fsinfo->set_allocated_volume(vresp);
    fsinfo->set_mountnum(1);
    fsinfo->add_mountpoints("0.0.0.0:/data");
    response.set_allocated_fsinfo(fsinfo);
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, MountFs(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));

    ASSERT_EQ(CURVEFS_ERROR::OK, mdsclient_.MountFs(fsName, mp, &out));
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));
}

TEST_F(MdsClientImplTest, test_UmountFs) {
    std::string fsName = "test1";
    std::string mp = "0.0.0.0:/data";

    curvefs::mds::UmountFsResponse response;
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, UmountFs(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));

    ASSERT_EQ(CURVEFS_ERROR::OK, mdsclient_.UmountFs(fsName, mp));
}

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
    fsinfo->set_allocated_volume(vresp);
    fsinfo->set_mountnum(1);
    fsinfo->add_mountpoints("0.0.0.0:/data");
    response.set_allocated_fsinfo(fsinfo);
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsName, _, _, _))
        .WillOnce(SetArgPointee<1>(response));

    ASSERT_EQ(CURVEFS_ERROR::OK, mdsclient_.GetFsInfo(fsName, &out));
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));
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
    fsinfo->set_allocated_volume(vresp);
    fsinfo->set_mountnum(1);
    fsinfo->add_mountpoints("0.0.0.0:/data");
    response.set_allocated_fsinfo(fsinfo);
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsid, _, _, _))
        .WillOnce(SetArgPointee<1>(response));

    ASSERT_EQ(CURVEFS_ERROR::OK, mdsclient_.GetFsInfo(fsid, &out));
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));
}

}  // namespace client
}  // namespace curvefs
