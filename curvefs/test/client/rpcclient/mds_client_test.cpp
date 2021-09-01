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

#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/test/client/rpcclient/mock_mds_base_client.h"
#include "curvefs/test/client/rpcclient/mock_mds_service.h"
#include "src/client/mds_client.h"

namespace curvefs {
namespace client {
namespace rpcclient {
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

void CreateFSRpcFailed(const std::string &fsName, uint64_t blockSize,
                       const Volume &volume, CreateFsResponse *response,
                       brpc::Controller *cntl, brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}

void CreateFSS3RpcFailed(const std::string &fsName, uint64_t blockSize,
                         const S3Info &s3Info, CreateFsResponse *response,
                         brpc::Controller *cntl, brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}
void DeleteFsRpcFailed(const std::string &fsName, DeleteFsResponse *response,
                       brpc::Controller *cntl, brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}

void MountFsRpcFailed(const std::string &fsName, const std::string &mountPt,
                      MountFsResponse *response, brpc::Controller *cntl,
                      brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}

void UmountFsRpcFailed(const std::string &fsName, const std::string &mountPt,
                       UmountFsResponse *response, brpc::Controller *cntl,
                       brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}

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

void GetMetaServerInfoRpcFailed(uint32_t port, std::string ip,
                                GetMetaServerInfoResponse *response,
                                brpc::Controller *cntl,
                                brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}

void GetMetaServerListInCopysetsRpcFailed(
    const LogicPoolID &logicalpooid, const std::vector<CopysetID> &copysetidvec,
    GetMetaServerListInCopySetsResponse *response, brpc::Controller *cntl,
    brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}

class MdsClientImplTest : public testing::Test {
 protected:
    void SetUp() override {
        ::curve::client::MetaServerOption mdsopt;
        mdsopt.rpcRetryOpt.addrs = {addr_};
        mdsopt.rpcRetryOpt.rpcTimeoutMs = 500;            // 500ms
        mdsopt.rpcRetryOpt.maxRPCTimeoutMS = 1000;        // 1s
        mdsopt.rpcRetryOpt.rpcRetryIntervalUS = 1000000;  // 100ms
        mdsopt.mdsMaxRetryMS = 2000;                      // 2s
        mdsopt.rpcRetryOpt.maxFailedTimesBeforeChangeAddr = 2;

        ASSERT_EQ(FSStatusCode::OK, mdsclient_.Init(mdsopt, &mockmdsbasecli_));

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
    auto detail = new curvefs::mds::FsDetail();
    detail->set_allocated_volume(vresp);
    fsinfo->set_allocated_detail(detail);
    fsinfo->set_mountnum(1);
    fsinfo->add_mountpoints("0.0.0.0:/data");
    response.set_allocated_fsinfo(fsinfo);

    // 1. create ok
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, CreateFs(_, _, _, _, _, _))
        .WillOnce(SetArgPointee<3>(response));

    ASSERT_EQ(FSStatusCode::OK, mdsclient_.CreateFs(fsName, blockSize, v));

    // 2. create logic fail
    response.set_statuscode(
        curvefs::mds::FSStatusCode::INSERT_ROOT_INODE_ERROR);
    EXPECT_CALL(mockmdsbasecli_, CreateFs(_, _, _, _, _, _))
        .WillOnce(SetArgPointee<3>(response));
    ASSERT_EQ(FSStatusCode::INSERT_ROOT_INODE_ERROR,
              mdsclient_.CreateFs(fsName, blockSize, v));

    // 3. create rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, CreateFs(_, _, _, _, _, _))
        .WillRepeatedly(Invoke(CreateFSRpcFailed));
    ASSERT_EQ(FSStatusCode::RPC_ERROR,
              mdsclient_.CreateFs(fsName, blockSize, v));
}

TEST_F(MdsClientImplTest, test_CreateFsS3) {
    std::string fsName = "test1";
    uint64_t blockSize = 4 * 1024;
    curvefs::common::S3Info s3Info;
    s3Info.set_ak("hello");
    s3Info.set_sk("word");
    s3Info.set_endpoint("127.0.0.1:8900");
    s3Info.set_bucketname("testS3");
    s3Info.set_blocksize(4 * 1024);
    s3Info.set_chunksize(4 * 1024);

    curvefs::mds::CreateFsResponse response;
    auto fsinfo = new curvefs::mds::FsInfo();
    fsinfo->set_fsid(1);
    fsinfo->set_fsname(fsName);
    fsinfo->set_rootinodeid(1);
    fsinfo->set_capacity(10 * 1024 * 1024L);
    fsinfo->set_blocksize(4 * 1024);
    auto s3Inforesp = new curvefs::common::S3Info();
    s3Inforesp->CopyFrom(s3Info);
    auto detail = new curvefs::mds::FsDetail();
    detail->set_allocated_s3info(s3Inforesp);
    fsinfo->set_allocated_detail(detail);
    fsinfo->set_mountnum(1);
    fsinfo->add_mountpoints("0.0.0.0:/data");
    response.set_allocated_fsinfo(fsinfo);

    // 1. create ok
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, CreateFsS3(_, _, _, _, _, _))
        .WillOnce(SetArgPointee<3>(response));

    ASSERT_EQ(FSStatusCode::OK,
              mdsclient_.CreateFsS3(fsName, blockSize, s3Info));

    // 2. create logic fail
    response.set_statuscode(
        curvefs::mds::FSStatusCode::INSERT_ROOT_INODE_ERROR);
    EXPECT_CALL(mockmdsbasecli_, CreateFsS3(_, _, _, _, _, _))
        .WillOnce(SetArgPointee<3>(response));
    ASSERT_EQ(FSStatusCode::INSERT_ROOT_INODE_ERROR,
              mdsclient_.CreateFsS3(fsName, blockSize, s3Info));

    // 3. create rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, CreateFsS3(_, _, _, _, _, _))
        .WillRepeatedly(Invoke(CreateFSS3RpcFailed));
    ASSERT_EQ(FSStatusCode::RPC_ERROR,
              mdsclient_.CreateFsS3(fsName, blockSize, s3Info));
}

TEST_F(MdsClientImplTest, test_DeleteFs) {
    std::string fsName = "test1";

    curvefs::mds::DeleteFsResponse response;

    // 1. delete ok
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, DeleteFs(_, _, _, _))
        .WillOnce(SetArgPointee<1>(response));
    ASSERT_EQ(FSStatusCode::OK, mdsclient_.DeleteFs(fsName));

    // 2. delete not exist
    response.set_statuscode(curvefs::mds::FSStatusCode::FS_BUSY);
    EXPECT_CALL(mockmdsbasecli_, DeleteFs(_, _, _, _))
        .WillOnce(SetArgPointee<1>(response));
    ASSERT_EQ(FSStatusCode::FS_BUSY, mdsclient_.DeleteFs(fsName));

    // 3. delete rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, DeleteFs(_, _, _, _))
        .WillRepeatedly(Invoke(DeleteFsRpcFailed));
    ASSERT_EQ(FSStatusCode::RPC_ERROR, mdsclient_.DeleteFs(fsName));
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
    auto detail = new curvefs::mds::FsDetail();
    detail->set_allocated_volume(vresp);
    fsinfo->set_allocated_detail(detail);
    fsinfo->set_mountnum(1);
    fsinfo->add_mountpoints("0.0.0.0:/data");
    response.set_allocated_fsinfo(fsinfo);

    // 1. mount ok
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, MountFs(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_EQ(FSStatusCode::OK, mdsclient_.MountFs(fsName, mp, &out));
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

    // 2. mount point not exist
    out.Clear();
    response.set_statuscode(curvefs::mds::FSStatusCode::MOUNT_POINT_NOT_EXIST);
    EXPECT_CALL(mockmdsbasecli_, MountFs(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_EQ(FSStatusCode::MOUNT_POINT_NOT_EXIST,
              mdsclient_.MountFs(fsName, mp, &out));
    ASSERT_FALSE(
        google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

    // 3. umount rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, MountFs(_, _, _, _, _))
        .WillRepeatedly(Invoke(MountFsRpcFailed));
    ASSERT_EQ(FSStatusCode::RPC_ERROR, mdsclient_.MountFs(fsName, mp, &out));
}

TEST_F(MdsClientImplTest, test_UmountFs) {
    std::string fsName = "test1";
    std::string mp = "0.0.0.0:/data";
    curvefs::mds::UmountFsResponse response;

    // 1. umount ok
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, UmountFs(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_EQ(FSStatusCode::OK, mdsclient_.UmountFs(fsName, mp));

    // 2. umount unknown error
    response.set_statuscode(curvefs::mds::FSStatusCode::UNKNOWN_ERROR);
    EXPECT_CALL(mockmdsbasecli_, UmountFs(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_EQ(FSStatusCode::UNKNOWN_ERROR, mdsclient_.UmountFs(fsName, mp));

    // 3. umount rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, UmountFs(_, _, _, _, _))
        .WillRepeatedly(Invoke(UmountFsRpcFailed));
    ASSERT_EQ(FSStatusCode::RPC_ERROR, mdsclient_.UmountFs(fsName, mp));
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

TEST_F(MdsClientImplTest, test_GetMetaServerInfo) {
    // in
    curve::client::EndPoint ep;
    butil::str2endpoint("127.0.0.1", 5000, &ep);
    curve::client::PeerAddr addr(ep);

    // out
    CopysetPeerInfo<MetaserverID> out;

    curvefs::mds::topology::GetMetaServerInfoResponse response;
    auto metaserverInfo = new curvefs::mds::topology::MetaServerInfo();
    metaserverInfo->set_metaserverid(1);
    metaserverInfo->set_hostname("hangzhou");
    metaserverInfo->set_hostip("127.0.0.1");
    metaserverInfo->set_port(5000);
    metaserverInfo->set_externalip("127.0.0.1");
    metaserverInfo->set_externalport(5000);
    metaserverInfo->set_onlinestate(::curvefs::mds::topology::ONLINE);
    response.set_allocated_metaserverinfo(metaserverInfo);

    // 1. get metaserver info ok
    response.set_statuscode(0);
    EXPECT_CALL(mockmdsbasecli_, GetMetaServerInfo(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_TRUE(mdsclient_.GetMetaServerInfo(addr, &out));
    ASSERT_TRUE(out ==
                CopysetPeerInfo<MetaserverID>(1, PeerAddr(ep), PeerAddr(ep)));

    // 2. get metaserver info not found
    response.set_statuscode(12);
    EXPECT_CALL(mockmdsbasecli_, GetMetaServerInfo(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_FALSE(mdsclient_.GetMetaServerInfo(addr, &out));

    // 3. get rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, GetMetaServerInfo(_, _, _, _, _))
        .WillRepeatedly(Invoke(GetMetaServerInfoRpcFailed));
    ASSERT_FALSE(mdsclient_.GetMetaServerInfo(addr, &out));
}

TEST_F(MdsClientImplTest, GetMetaServerListInCopysets) {
    // in
    LogicPoolID poolID = 1;
    std::vector<CopysetID> copysetidvec{1};

    // out
    std::vector<CopysetInfo<MetaserverID>> out;

    curvefs::mds::topology::GetMetaServerListInCopySetsResponse response;
    auto copysetInfo = response.add_csinfo();
    copysetInfo->set_copysetid(1);
    auto l1 = copysetInfo->add_cslocs();
    auto l2 = copysetInfo->add_cslocs();
    auto l3 = copysetInfo->add_cslocs();
    l1->set_metaserverid(1);
    l1->set_hostip("127.0.0.1");
    l1->set_port(9000);
    l1->set_externalip("127.0.0.1");
    l2->CopyFrom(*l1);
    l2->set_metaserverid(2);
    l3->CopyFrom(*l1);
    l3->set_metaserverid(3);

    // 1. get metaserver list in copysets ok
    response.set_statuscode(0);
    EXPECT_CALL(mockmdsbasecli_, GetMetaServerListInCopysets(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_TRUE(
        mdsclient_.GetMetaServerListInCopysets(poolID, copysetidvec, &out));
    ASSERT_EQ(1, out.size());
    ASSERT_EQ(3, out[0].csinfos_.size());
    std::list<int> ids{1, 2, 3};
    for (int i = 0; i <= 2; i++) {
        ASSERT_TRUE(std::find(ids.begin(), ids.end(),
                              out[0].csinfos_[i].peerID) != ids.end());
    }

    // 2. get metaserver list in copyset unknown error
    response.set_statuscode(12);
    EXPECT_CALL(mockmdsbasecli_, GetMetaServerListInCopysets(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_FALSE(
        mdsclient_.GetMetaServerListInCopysets(poolID, copysetidvec, &out));

    // 3. get rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, GetMetaServerListInCopysets(_, _, _, _, _))
        .WillRepeatedly(Invoke(GetMetaServerListInCopysetsRpcFailed));
    ASSERT_FALSE(
        mdsclient_.GetMetaServerListInCopysets(poolID, copysetidvec, &out));
}

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs
