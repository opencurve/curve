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
 * Created Date: Thur Jun 15 2021
 * Author: lixiaocui
 */

#include <brpc/server.h>
#include <gtest/gtest.h>
#include <google/protobuf/util/message_differencer.h>

#include "curvefs/src/client/base_client.h"
#include "curvefs/test/client/mock_mds_service.h"
#include "curvefs/test/client/mock_metaserver_service.h"
#include "curvefs/test/client/mock_spacealloc_service.h"

namespace curvefs {
namespace client {
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;


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

class BaseClientTest : public testing::Test {
 protected:
    void SetUp() override {
        ASSERT_EQ(0, server_.AddService(&mockMetaServerService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.AddService(&mockMdsService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.AddService(&mockSpaceAllocService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

 protected:
    MockMetaServerService mockMetaServerService_;
    MockMdsService mockMdsService_;
    MockSpaceAllocService mockSpaceAllocService_;
    MetaServerBaseClient msbasecli_;
    MDSBaseClient mdsbasecli_;
    SpaceBaseClient spacebasecli_;

    std::string addr_ = "127.0.0.1:5700";
    brpc::Server server_;
};

TEST_F(BaseClientTest, test_GetDentry) {
    uint32_t fsid = 1;
    uint64_t inodeid = 2;
    std::string name = "test1";
    GetDentryResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));

    curvefs::metaserver::GetDentryResponse response;
    auto *d = new curvefs::metaserver::Dentry();
    d->set_fsid(fsid);
    d->set_inodeid(inodeid);
    d->set_parentinodeid(1);
    d->set_name(name);
    d->set_txid(100);
    response.set_allocated_dentry(d);
    response.set_statuscode(curvefs::metaserver::OK);
    EXPECT_CALL(mockMetaServerService_, GetDentry(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<GetDentryRequest, GetDentryResponse>)));

    msbasecli_.GetDentry(fsid, inodeid, name, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_ListDentry) {
    uint32_t fsid = 1;
    uint64_t inodeid = 2;
    std::string last = "test1";
    uint32_t count = 10;
    ListDentryResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));

    curvefs::metaserver::ListDentryResponse response;
    auto *d = response.add_dentrys();
    d->set_fsid(fsid);
    d->set_inodeid(inodeid);
    d->set_parentinodeid(1);
    d->set_name("test11");
    d->set_txid(100);
    response.set_statuscode(curvefs::metaserver::OK);
    EXPECT_CALL(mockMetaServerService_, ListDentry(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<ListDentryRequest, ListDentryResponse>)));

    msbasecli_.ListDentry(fsid, inodeid, last, count, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_CreateDentry) {
    Dentry d;
    d.set_fsid(1);
    d.set_inodeid(2);
    d.set_parentinodeid(1);
    d.set_name("test11");
    d.set_txid(100);
    CreateDentryResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));

    curvefs::metaserver::CreateDentryResponse response;
    response.set_statuscode(curvefs::metaserver::PARAM_ERROR);
    EXPECT_CALL(mockMetaServerService_, CreateDentry(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(RpcService<CreateDentryRequest, CreateDentryResponse>)));

    msbasecli_.CreateDentry(d, &resp, &cntl, &ch);

    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}


TEST_F(BaseClientTest, test_DeleteDentry) {
    uint32_t fsid = 1;
    uint64_t inodeid = 2;
    std::string name = "test";
    DeleteDentryResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));


    curvefs::metaserver::DeleteDentryResponse response;
    response.set_statuscode(curvefs::metaserver::PARAM_ERROR);
    EXPECT_CALL(mockMetaServerService_, DeleteDentry(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(RpcService<DeleteDentryRequest, DeleteDentryResponse>)));

    msbasecli_.DeleteDentry(fsid, inodeid, name, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_GetInode) {
    uint32_t fsid = 1;
    uint64_t inodeid = 2;
    GetInodeResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));


    curvefs::metaserver::GetInodeResponse response;
    auto inode = new curvefs::metaserver::Inode();
    inode->set_inodeid(inodeid);
    inode->set_fsid(fsid);
    inode->set_length(10);
    inode->set_ctime(1623835517);
    inode->set_mtime(1623835517);
    inode->set_atime(1623835517);
    inode->set_uid(1);
    inode->set_gid(1);
    inode->set_mode(1);
    inode->set_nlink(1);
    inode->set_type(curvefs::metaserver::FsFileType::TYPE_FILE);
    inode->set_symlink("test9");
    response.set_statuscode(curvefs::metaserver::OK);
    EXPECT_CALL(mockMetaServerService_, GetInode(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke(RpcService<GetInodeRequest, GetInodeResponse>)));

    msbasecli_.GetInode(fsid, inodeid, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_UpdateInode) {
    curvefs::metaserver::Inode inode;
    inode.set_inodeid(1);
    inode.set_fsid(2);
    inode.set_length(10);
    inode.set_ctime(1623835517);
    inode.set_mtime(1623835517);
    inode.set_atime(1623835517);
    inode.set_uid(1);
    inode.set_gid(1);
    inode.set_mode(1);
    inode.set_nlink(1);
    inode.set_type(curvefs::metaserver::FsFileType::TYPE_FILE);
    inode.set_symlink("test9");
    UpdateInodeResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));


    curvefs::metaserver::UpdateInodeResponse response;
    response.set_statuscode(curvefs::metaserver::OK);
    EXPECT_CALL(mockMetaServerService_, UpdateInode(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<UpdateInodeRequest, UpdateInodeResponse>)));

    msbasecli_.UpdateInode(inode, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_CreateInode) {
    InodeParam inode;
    inode.fsId = 2;
    inode.length = 10;
    inode.uid = 1;
    inode.gid = 1;
    inode.mode = 1;
    inode.type = curvefs::metaserver::FsFileType::TYPE_FILE;
    inode.symlink = "test9";
    CreateInodeResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));


    curvefs::metaserver::CreateInodeResponse response;
    response.set_statuscode(curvefs::metaserver::OK);
    EXPECT_CALL(mockMetaServerService_, CreateInode(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<CreateInodeRequest, CreateInodeResponse>)));

    msbasecli_.CreateInode(inode, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_DeleteInode) {
    uint32_t fsId = 2;
    uint64_t inodeid = 1;
    DeleteInodeResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));


    curvefs::metaserver::DeleteInodeResponse response;
    response.set_statuscode(curvefs::metaserver::OK);
    EXPECT_CALL(mockMetaServerService_, DeleteInode(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<DeleteInodeRequest, DeleteInodeResponse>)));

    msbasecli_.DeleteInode(fsId, inodeid, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_CreateFs) {
    std::string fsName = "test1";
    uint64_t blockSize = 4 * 1024;
    curvefs::common::Volume v;
    v.set_volumesize(10 * 1024 * 1024L);
    v.set_blocksize(4 * 1024);
    v.set_volumename("test1");
    v.set_user("test");
    v.set_password("test");
    CreateFsResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));


    curvefs::mds::CreateFsResponse response;
    auto fsinfo = new curvefs::mds::FsInfo();
    fsinfo->set_fsid(1);
    fsinfo->set_fsname(fsName);
    fsinfo->set_status(FsStatus::NEW);
    fsinfo->set_rootinodeid(1);
    fsinfo->set_capacity(10 * 1024 * 1024L);
    fsinfo->set_blocksize(4 * 1024);
    fsinfo->set_mountnum(1);
    fsinfo->add_mountpoints("0.0.0.0:/data");
    fsinfo->set_fstype(::curvefs::common::FSType::TYPE_VOLUME);
    auto vresp = new curvefs::common::Volume();
    vresp->set_volumesize(10 * 1024 * 1024L);
    vresp->set_blocksize(4 * 1024);
    vresp->set_volumename("test1");
    vresp->set_user("test");
    vresp->set_password("test");
    fsinfo->mutable_detail()->set_allocated_volume(vresp);
    response.set_allocated_fsinfo(fsinfo);
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockMdsService_, CreateFs(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke(RpcService<CreateFsRequest, CreateFsResponse>)));

    mdsbasecli_.CreateFs(fsName, blockSize, v, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_DeleteFs) {
    std::string fsName = "test1";
    DeleteFsResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));


    curvefs::mds::DeleteFsResponse response;
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockMdsService_, DeleteFs(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke(RpcService<DeleteFsRequest, DeleteFsResponse>)));

    mdsbasecli_.DeleteFs(fsName, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_MountFs) {
    std::string fsName = "test1";
    std::string mp = "0.0.0.0:/data";
    MountFsResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));


    curvefs::mds::MountFsResponse response;
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockMdsService_, MountFs(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke(RpcService<MountFsRequest, MountFsResponse>)));

    mdsbasecli_.MountFs(fsName, mp, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_UmountFs) {
    std::string fsName = "test1";
    std::string mp = "0.0.0.0:/data";
    UmountFsResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));


    curvefs::mds::UmountFsResponse response;
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockMdsService_, UmountFs(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke(RpcService<UmountFsRequest, UmountFsResponse>)));

    mdsbasecli_.UmountFs(fsName, mp, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_GetFsInfo_by_fsName) {
    std::string fsName = "test1";
    GetFsInfoResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));

    curvefs::mds::GetFsInfoResponse response;
    auto fsinfo = new curvefs::mds::FsInfo();
    fsinfo->set_fsid(1);
    fsinfo->set_fsname(fsName);
    fsinfo->set_status(FsStatus::NEW);
    fsinfo->set_rootinodeid(1);
    fsinfo->set_capacity(10 * 1024 * 1024L);
    fsinfo->set_blocksize(4 * 1024);
    fsinfo->set_mountnum(1);
    fsinfo->set_fstype(::curvefs::common::FSType::TYPE_VOLUME);
    auto vresp = new curvefs::common::Volume();
    vresp->set_volumesize(10 * 1024 * 1024L);
    vresp->set_blocksize(4 * 1024);
    vresp->set_volumename("test1");
    vresp->set_user("test");
    vresp->set_password("test");
    fsinfo->mutable_detail()->set_allocated_volume(vresp);
    response.set_allocated_fsinfo(fsinfo);
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockMdsService_, GetFsInfo(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<GetFsInfoRequest, GetFsInfoResponse>)));

    mdsbasecli_.GetFsInfo(fsName, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_GetFsInfo_by_fsId) {
    uint32_t fsId = 1;
    GetFsInfoResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));


    curvefs::mds::GetFsInfoResponse response;
    auto fsinfo = new curvefs::mds::FsInfo();
    fsinfo->set_fsid(1);
    fsinfo->set_fsname("test1");
    fsinfo->set_status(FsStatus::NEW);
    fsinfo->set_rootinodeid(1);
    fsinfo->set_capacity(10 * 1024 * 1024L);
    fsinfo->set_blocksize(4 * 1024);
    fsinfo->set_mountnum(1);
    fsinfo->set_fstype(::curvefs::common::FSType::TYPE_VOLUME);
    auto vresp = new curvefs::common::Volume();
    vresp->set_volumesize(10 * 1024 * 1024L);
    vresp->set_blocksize(4 * 1024);
    vresp->set_volumename("test1");
    vresp->set_user("test");
    vresp->set_password("test");
    fsinfo->mutable_detail()->set_allocated_volume(vresp);
    fsinfo->set_mountnum(1);
    response.set_allocated_fsinfo(fsinfo);
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockMdsService_, GetFsInfo(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<GetFsInfoRequest, GetFsInfoResponse>)));

    mdsbasecli_.GetFsInfo(fsId, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_AllocExtents) {
    uint32_t fsId = 1;
    ExtentAllocInfo info;
    info.lOffset = 0;
    info.len = 1024;
    info.leftHintAvailable = true;
    info.pOffsetLeft = 0;
    info.rightHintAvailable = true;
    info.pOffsetRight = 0;
    curvefs::space::AllocateSpaceResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));

    curvefs::space::AllocateSpaceResponse response;
    auto extent = response.add_extents();
    extent->set_offset(0);
    extent->set_length(1024);
    response.set_status(curvefs::space::SpaceStatusCode::SPACE_OK);
    EXPECT_CALL(mockSpaceAllocService_, AllocateSpace(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(RpcService<AllocateSpaceRequest, AllocateSpaceResponse>)));

    spacebasecli_.AllocExtents(fsId, info, curvefs::space::AllocateType::NONE,
                               &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_DeAllocExtents) {
    uint32_t fsId = 1;
    Extent extent;
    extent.set_offset(0);
    extent.set_length(1024);
    std::list<Extent> allocatedExtents;
    allocatedExtents.push_back(extent);
    curvefs::space::DeallocateSpaceResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));

    curvefs::space::DeallocateSpaceResponse response;
    response.set_status(curvefs::space::SpaceStatusCode::SPACE_OK);
    EXPECT_CALL(mockSpaceAllocService_, DeallocateSpace(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(
                RpcService<DeallocateSpaceRequest, DeallocateSpaceResponse>)));

    spacebasecli_.DeAllocExtents(fsId, allocatedExtents, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

}  // namespace client
}  // namespace curvefs
