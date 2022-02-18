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
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "curvefs/src/client/rpcclient/base_client.h"
#include "curvefs/test/client/rpcclient/mock_mds_service.h"
#include "curvefs/test/client/rpcclient/mock_metaserver_service.h"
#include "curvefs/test/client/rpcclient/mock_spacealloc_service.h"
#include "curvefs/test/client/rpcclient/mock_topology_service.h"

namespace curvefs {
namespace client {
namespace rpcclient {
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
        ASSERT_EQ(0, server_.AddService(&mockTopologyService_,
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
    MockTopologyService mockTopologyService_;
    MockSpaceAllocService mockSpaceAllocService_;
    MDSBaseClient mdsbasecli_;
    // TODO(lixiaocui): add base client for curve block storage
    // SpaceBaseClient spacebasecli_;

    std::string addr_ = "127.0.0.1:5600";
    brpc::Server server_;
};

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
    auto detail = new curvefs::mds::FsDetail();
    detail->set_allocated_volume(vresp);
    fsinfo->set_allocated_detail(detail);
    response.set_allocated_fsinfo(fsinfo);
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockMdsService_, GetFsInfo(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<GetFsInfoRequest, GetFsInfoResponse>)));

    mdsbasecli_.GetFsInfo(fsName, &resp, &cntl, &ch);
    LOG(INFO) << " .........." << cntl.ErrorText();
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
    auto detail = new curvefs::mds::FsDetail();
    detail->set_allocated_volume(vresp);
    fsinfo->set_allocated_detail(detail);
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

TEST_F(BaseClientTest, test_CreatePartition) {
    uint32_t fsID = 1;
    uint32_t count = 2;
    CreatePartitionResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));

    PartitionInfo partitioninfo1;
    PartitionInfo partitioninfo2;
    partitioninfo1.set_fsid(fsID);
    partitioninfo1.set_poolid(1);
    partitioninfo1.set_copysetid(2);
    partitioninfo1.set_partitionid(3);
    partitioninfo1.set_start(4);
    partitioninfo1.set_end(5);
    partitioninfo1.set_txid(6);
    partitioninfo1.set_status(PartitionStatus::READWRITE);

    partitioninfo2.set_fsid(fsID);
    partitioninfo2.set_poolid(2);
    partitioninfo2.set_copysetid(3);
    partitioninfo2.set_partitionid(4);
    partitioninfo2.set_start(5);
    partitioninfo2.set_end(6);
    partitioninfo2.set_txid(7);
    partitioninfo2.set_status(PartitionStatus::READWRITE);

    curvefs::mds::topology::CreatePartitionResponse response;
    response.add_partitioninfolist()->CopyFrom(partitioninfo1);
    response.add_partitioninfolist()->CopyFrom(partitioninfo2);

    response.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(mockTopologyService_, CreatePartition(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(
                RpcService<CreatePartitionRequest, CreatePartitionResponse>)));

    mdsbasecli_.CreatePartition(fsID, count, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_ListPartition) {
    uint32_t fsID = 1;
    ListPartitionResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));

    PartitionInfo partitioninfo1;
    PartitionInfo partitioninfo2;
    partitioninfo1.set_fsid(fsID);
    partitioninfo1.set_poolid(1);
    partitioninfo1.set_copysetid(2);
    partitioninfo1.set_partitionid(3);
    partitioninfo1.set_start(4);
    partitioninfo1.set_end(5);
    partitioninfo1.set_txid(6);
    partitioninfo1.set_status(PartitionStatus::READWRITE);

    partitioninfo2.set_fsid(fsID);
    partitioninfo2.set_poolid(2);
    partitioninfo2.set_copysetid(3);
    partitioninfo2.set_partitionid(4);
    partitioninfo2.set_start(5);
    partitioninfo2.set_end(6);
    partitioninfo2.set_txid(7);
    partitioninfo2.set_status(PartitionStatus::READONLY);

    curvefs::mds::topology::ListPartitionResponse response;
    response.add_partitioninfolist()->CopyFrom(partitioninfo1);
    response.add_partitioninfolist()->CopyFrom(partitioninfo2);
    response.set_statuscode(TopoStatusCode::TOPO_OK);

    EXPECT_CALL(mockTopologyService_, ListPartition(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(RpcService<ListPartitionRequest, ListPartitionResponse>)));

    mdsbasecli_.ListPartition(fsID, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_GetCopysetOfPartition) {
    std::vector<uint32_t> partitionIDList{1, 2};
    GetCopysetOfPartitionResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));

    Copyset copyset1;
    Copyset copyset2;
    copyset1.set_poolid(1);
    copyset1.set_copysetid(2);
    Peer peer1;
    peer1.set_id(3);
    peer1.set_address("addr1");
    copyset1.add_peers()->CopyFrom(peer1);

    copyset2.set_poolid(2);
    copyset2.set_copysetid(3);
    Peer peer2;
    peer2.set_id(4);
    peer2.set_address("addr2");
    copyset2.add_peers()->CopyFrom(peer2);

    curvefs::mds::topology::GetCopysetOfPartitionResponse response;
    auto copysetMap = response.mutable_copysetmap();
    (*copysetMap)[1] = copyset1;
    (*copysetMap)[2] = copyset2;
    response.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(mockTopologyService_, GetCopysetOfPartition(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke(RpcService<GetCopysetOfPartitionRequest,
                                          GetCopysetOfPartitionResponse>)));

    mdsbasecli_.GetCopysetOfPartitions(partitionIDList, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

// TEST_F(BaseClientTest, test_AllocExtents) {
//     uint32_t fsId = 1;
//     ExtentAllocInfo info;
//     info.lOffset = 0;
//     info.len = 1024;
//     info.leftHintAvailable = true;
//     info.pOffsetLeft = 0;
//     info.rightHintAvailable = true;
//     info.pOffsetRight = 0;
//     curvefs::space::AllocateSpaceResponse resp;
//     brpc::Controller cntl;
//     cntl.set_timeout_ms(1000);
//     brpc::Channel ch;
//     ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));

//     curvefs::space::AllocateSpaceResponse response;
//     auto extent = response.add_extents();
//     extent->set_offset(0);
//     extent->set_length(1024);
//     response.set_status(curvefs::space::SpaceStatusCode::SPACE_OK);
//     EXPECT_CALL(mockSpaceAllocService_, AllocateSpace(_, _, _, _))
//         .WillOnce(DoAll(
//             SetArgPointee<2>(response),
//             Invoke(RpcService<AllocateSpaceRequest,
//             AllocateSpaceResponse>)));

//     spacebasecli_.AllocExtents(fsId, info,
//     curvefs::space::AllocateType::NONE,
//                                &resp, &cntl, &ch);
//     ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
//     ASSERT_TRUE(
//         google::protobuf::util::MessageDifferencer::Equals(resp, response))
//         << "resp:\n"
//         << resp.ShortDebugString() << "response:\n"
//         << response.ShortDebugString();
// }

// TEST_F(BaseClientTest, test_DeAllocExtents) {
//     uint32_t fsId = 1;
//     Extent extent;
//     extent.set_offset(0);
//     extent.set_length(1024);
//     std::list<Extent> allocatedExtents;
//     allocatedExtents.push_back(extent);
//     curvefs::space::DeallocateSpaceResponse resp;
//     brpc::Controller cntl;
//     cntl.set_timeout_ms(1000);
//     brpc::Channel ch;
//     ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));

//     curvefs::space::DeallocateSpaceResponse response;
//     response.set_status(curvefs::space::SpaceStatusCode::SPACE_OK);
//     EXPECT_CALL(mockSpaceAllocService_, DeallocateSpace(_, _, _, _))
//         .WillOnce(DoAll(
//             SetArgPointee<2>(response),
//             Invoke(
//                 RpcService<DeallocateSpaceRequest,
//                 DeallocateSpaceResponse>)));

//     spacebasecli_.DeAllocExtents(fsId, allocatedExtents, &resp, &cntl, &ch);
//     ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
//     ASSERT_TRUE(
//         google::protobuf::util::MessageDifferencer::Equals(resp, response))
//         << "resp:\n"
//         << resp.ShortDebugString() << "response:\n"
//         << response.ShortDebugString();
// }
}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs
