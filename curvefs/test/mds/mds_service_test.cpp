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
 * @Project: curve
 * @Date: 2021-06-10 10:47:07
 * @Author: chenwei
 */

#include "curvefs/src/mds/mds_service.h"

#include <brpc/channel.h>
#include <brpc/server.h>
#include <gmock/gmock.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>
#include <functional>
#include <string>

#include "curvefs/test/mds/fake_metaserver.h"
#include "curvefs/test/mds/mock/mock_kvstorage_client.h"
#include "curvefs/test/mds/mock/mock_topology.h"
#include "curvefs/test/mds/mock/mock_cli2.h"
#include "test/common/mock_s3_adapter.h"
#include "curvefs/test/mds/mock/mock_space_manager.h"
#include "proto/nameserver2.pb.h"
#include "curvefs/test/mds/utils.h"

using ::curve::common::MockS3Adapter;
using ::curvefs::common::S3Info;
using ::curvefs::common::Volume;
using ::curvefs::mds::RefreshSessionRequest;
using ::curvefs::mds::RefreshSessionResponse;
using ::curvefs::mds::topology::CreatePartitionRequest;
using ::curvefs::mds::topology::CreatePartitionResponse;
using ::curvefs::metaserver::FakeMetaserverImpl;
using ::curvefs::mds::topology::TopologyManager;
using ::curvefs::mds::topology::MockTopologyManager;
using ::curvefs::mds::topology::MockTopology;
using ::curvefs::mds::topology::MockIdGenerator;
using ::curvefs::mds::topology::MockTokenGenerator;
using ::curvefs::mds::topology::MockStorage;
using ::curvefs::mds::topology::TopologyIdGenerator;
using ::curvefs::mds::topology::DefaultIdGenerator;
using ::curvefs::mds::topology::DefaultTokenGenerator;
using ::curvefs::mds::topology::MockEtcdClient;
using ::curvefs::mds::topology::MockIdGenerator;
using ::curvefs::mds::topology::MockStorage;
using ::curvefs::mds::topology::MockTokenGenerator;
using ::curvefs::mds::topology::MockTopology;
using ::curvefs::mds::topology::MockTopologyManager;
using ::curvefs::mds::topology::TopologyIdGenerator;
using ::curvefs::mds::topology::TopologyImpl;
using ::curvefs::mds::topology::TopologyManager;
using ::curvefs::mds::topology::TopologyStorageCodec;
using ::curvefs::mds::topology::TopologyStorageEtcd;
using ::curvefs::mds::topology::TopologyTokenGenerator;
using ::curvefs::mds::topology::TopoStatusCode;
using ::curvefs::metaserver::FakeMetaserverImpl;
using ::curvefs::metaserver::copyset::GetLeaderResponse2;
using ::curvefs::metaserver::copyset::MockCliService2;

using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Matcher;
using ::testing::Mock;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::StrEq;

using ::curve::common::MockS3Adapter;
using ::curvefs::mds::space::MockSpaceManager;
using ::google::protobuf::util::MessageDifferencer;

namespace curvefs {
namespace mds {
class MdsServiceTest : public ::testing::Test {
 protected:
    void SetUp() override {
        kvstorage_ = std::make_shared<MockKVStorageClient>();

        MetaserverOptions metaserverOptions;
        metaserverOptions.metaserverAddr = "127.0.0.1:6703";
        metaserverOptions.rpcTimeoutMs = 5000;
        fsStorage_ = std::make_shared<MemoryFsStorage>();
        metaserverClient_ =
            std::make_shared<MetaserverClient>(metaserverOptions);
        // init mock topology manager
        std::shared_ptr<TopologyIdGenerator> idGenerator_ =
            std::make_shared<DefaultIdGenerator>();
        std::shared_ptr<TopologyTokenGenerator> tokenGenerator_ =
            std::make_shared<DefaultTokenGenerator>();

        spaceManager_ = std::make_shared<MockSpaceManager>();
        auto etcdClient_ = std::make_shared<MockEtcdClient>();
        auto codec = std::make_shared<TopologyStorageCodec>();
        auto topoStorage_ =
            std::make_shared<TopologyStorageEtcd>(etcdClient_, codec);
        topoManager_ = std::make_shared<MockTopologyManager>(
                            std::make_shared<TopologyImpl>(idGenerator_,
                            tokenGenerator_, topoStorage_), metaserverClient_);

        // init fsmanager
        FsManagerOption fsManagerOption;
        fsManagerOption.backEndThreadRunInterSec = 1;
        s3Adapter_ = std::make_shared<MockS3Adapter>();
        fsManager_ = std::make_shared<FsManager>(
            fsStorage_, spaceManager_, metaserverClient_, topoManager_,
            s3Adapter_, nullptr, fsManagerOption);
        ASSERT_TRUE(fsManager_->Init());
    }

    static bool CompareVolume(const Volume& first, const Volume& second) {
#define COMPARE_FIELD(field)                     \
    (first.has_##field() && second.has_##field() \
         ? first.field() == second.field()       \
         : true)

        return COMPARE_FIELD(volumesize) && COMPARE_FIELD(blocksize) &&
               COMPARE_FIELD(volumename) && COMPARE_FIELD(user) &&
               COMPARE_FIELD(password);
    }

    static bool CompareFs(const FsInfo& first, const FsInfo& second) {
        return first.fsid() == second.fsid() &&
               first.fsname() == second.fsname() &&
               first.rootinodeid() == second.rootinodeid() &&
               first.capacity() == second.capacity() &&
               first.blocksize() == second.blocksize() &&
               CompareVolume(first.detail().volume(), second.detail().volume());
    }

    std::shared_ptr<FsManager> fsManager_;
    std::shared_ptr<FsStorage> fsStorage_;
    std::shared_ptr<MockSpaceManager> spaceManager_;
    std::shared_ptr<MetaserverClient> metaserverClient_;
    std::shared_ptr<MockKVStorageClient> kvstorage_;
    std::shared_ptr<MockTopologyManager> topoManager_;
    std::shared_ptr<MockS3Adapter> s3Adapter_;
};

template <typename RpcRequestType, typename RpcResponseType,
          bool RpcFailed = false>
void RpcService(google::protobuf::RpcController* cntl_base,
                const RpcRequestType* request, RpcResponseType* response,
                google::protobuf::Closure* done) {
    if (RpcFailed) {
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        cntl->SetFailed(112, "Not connected to");
    }
    done->Run();
}

TEST_F(MdsServiceTest, test1) {
    brpc::Server server;
    // add metaserver service
    MdsServiceImpl mdsService(fsManager_, nullptr);
    ASSERT_EQ(server.AddService(&mdsService, brpc::SERVER_DOESNT_OWN_SERVICE),
              0);

    FakeMetaserverImpl metaserverService;
    ASSERT_EQ(
        server.AddService(&metaserverService, brpc::SERVER_DOESNT_OWN_SERVICE),
        0);

    MockCliService2 mockCliService2;
    ASSERT_EQ(
        server.AddService(&mockCliService2, brpc::SERVER_DOESNT_OWN_SERVICE),
        0);

    FakeCurveFSService fakeCurveFSService;
    ASSERT_EQ(0, server.AddService(&fakeCurveFSService,
                                   brpc::SERVER_DOESNT_OWN_SERVICE));

    // start rpc server
    brpc::ServerOptions option;
    std::string addr = "127.0.0.1:6703";
    std::string leader = "127.0.0.1:6703:0";
    ASSERT_EQ(server.Start(addr.c_str(), &option), 0);

    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(server.listen_address(), nullptr), 0);

    MdsService_Stub stub(&channel);
    brpc::Controller cntl;

    // test CreateFS
    CreateFsRequest createRequest;
    CreateFsResponse createResponse;

    // type if volume, but volume not set
    createRequest.set_fsname("fs1");
    createRequest.set_blocksize(4096);
    createRequest.set_fstype(::curvefs::common::FSType::TYPE_VOLUME);
    createRequest.set_enablesumindir(false);
    auto* detail = createRequest.mutable_fsdetail();  // force allocate detail
    (void)detail;

    const auto capacity = 100ULL << 30;
    createRequest.set_capacity(capacity);
    createRequest.set_owner("test");

    FsInfo fsinfo1;
    stub.CreateFs(&cntl, &createRequest, &createResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), FSStatusCode::PARAM_ERROR);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // type if volume, create ok
    Volume volume;
    volume.set_blocksize(4096);
    volume.set_volumename("volume1");
    volume.set_user("user1");
    volume.set_blockgroupsize(128ull * 1024 * 1024);
    volume.set_bitmaplocation(common::BitmapLocation::AtStart);
    volume.set_slicesize(1ULL * 1024 * 1024 * 1024);
    volume.set_autoextend(false);
    volume.add_cluster("127.0.0.1:6703");

    createRequest.set_fsname("fs1");
    createRequest.set_blocksize(4096);
    createRequest.set_fstype(::curvefs::common::FSType::TYPE_VOLUME);
    createRequest.mutable_fsdetail()->mutable_volume()->CopyFrom(volume);

    EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));
    std::set<std::string> addrs;
    addrs.emplace(addr);
    EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(addrs),
            Return(TopoStatusCode::TOPO_OK)));
    GetLeaderResponse2 getLeaderResponse;
    getLeaderResponse.mutable_leader()->set_address(leader);
    EXPECT_CALL(mockCliService2, GetLeader(_, _, _, _))
        .WillOnce(DoAll(
        SetArgPointee<2>(getLeaderResponse),
        Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));

    cntl.Reset();
    stub.CreateFs(&cntl, &createRequest, &createResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(createResponse.has_fsinfo());
        fsinfo1 = createResponse.fsinfo();
        ASSERT_EQ(fsinfo1.fsid(), 0);
        ASSERT_EQ(fsinfo1.fsname(), "fs1");
        ASSERT_EQ(fsinfo1.rootinodeid(), 1);
        ASSERT_EQ(fsinfo1.capacity(), capacity);
        ASSERT_EQ(fsinfo1.blocksize(), 4096);
        ASSERT_EQ(fsinfo1.mountnum(), 0);
        ASSERT_EQ(fsinfo1.mountpoints_size(), 0);
        ASSERT_TRUE(CompareVolume(volume, fsinfo1.detail().volume()))
            << "Request:\n" << volume.DebugString()
            << ", response:\n" << fsinfo1.detail().volume().DebugString();
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // volume exist, create fail
    cntl.Reset();
    stub.CreateFs(&cntl, &createRequest, &createResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), FSStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // create s3 fs, s3info not set
    cntl.Reset();
    FsInfo fsinfo2;
    createRequest.set_fsname("fs2");
    createRequest.set_fstype(FSType::TYPE_S3);
    createRequest.mutable_fsdetail();
    stub.CreateFs(&cntl, &createRequest, &createResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), FSStatusCode::PARAM_ERROR);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // create s3 fs, OK
    cntl.Reset();
    createRequest.set_fsname("fs2");
    createRequest.set_fstype(FSType::TYPE_S3);
    S3Info s3info;
    s3info.set_ak("ak");
    s3info.set_sk("sk");
    s3info.set_endpoint("endpoint");
    s3info.set_bucketname("bucketname");
    s3info.set_blocksize(4096);
    s3info.set_chunksize(4096);
    createRequest.mutable_fsdetail()->mutable_s3info()->CopyFrom(s3info);

    EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));
    EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(addrs),
            Return(TopoStatusCode::TOPO_OK)));
    EXPECT_CALL(mockCliService2, GetLeader(_, _, _, _))
        .WillOnce(DoAll(
        SetArgPointee<2>(getLeaderResponse),
        Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));
    EXPECT_CALL(*s3Adapter_, BucketExist()).WillOnce(Return(true));

    cntl.set_timeout_ms(5000);
    stub.CreateFs(&cntl, &createRequest, &createResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(createResponse.has_fsinfo());
        fsinfo2 = createResponse.fsinfo();
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // TODO(huyao): create hybrid fs
    cntl.Reset();
    createRequest.set_fsname("hybrid");
    createRequest.set_fstype(FSType::TYPE_HYBRID);
    stub.CreateFs(&cntl, &createRequest, &createResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), FSStatusCode::UNKNOWN_ERROR);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // test MountFs
    cntl.Reset();
    Mountpoint mountPoint;
    mountPoint.set_hostname("host1");
    mountPoint.set_port(9000);
    mountPoint.set_path("/a/b/c");
    MountFsRequest mountRequest;
    MountFsResponse mountResponse;
    mountRequest.set_fsname("fs1");
    mountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint));
    stub.MountFs(&cntl, &mountRequest, &mountResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(mountResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(mountResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(mountResponse.fsinfo(), fsinfo1));
        ASSERT_EQ(mountResponse.fsinfo().mountnum(), 1);
        ASSERT_EQ(mountResponse.fsinfo().mountpoints_size(), 1);
        ASSERT_EQ(MessageDifferencer::Equals(
                      mountResponse.fsinfo().mountpoints(0), mountPoint), true);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    stub.MountFs(&cntl, &mountRequest, &mountResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(mountResponse.statuscode(), FSStatusCode::MOUNT_POINT_EXIST);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    Mountpoint mountPoint2;
    mountPoint2.set_hostname("host1");
    mountPoint2.set_port(9000);
    mountPoint2.set_path("/a/b/d");
    mountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint2));
    stub.MountFs(&cntl, &mountRequest, &mountResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(mountResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(mountResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(mountResponse.fsinfo(), fsinfo1));
        ASSERT_EQ(mountResponse.fsinfo().mountnum(), 2);
        ASSERT_EQ(mountResponse.fsinfo().mountpoints_size(), 2);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    Mountpoint mountPoint3;
    mountPoint3.set_hostname("host2");
    mountPoint3.set_port(9000);
    mountPoint3.set_path("/a/b/d");
    mountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint3));
    stub.MountFs(&cntl, &mountRequest, &mountResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(mountResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(mountResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(mountResponse.fsinfo(), fsinfo1));
        ASSERT_EQ(mountResponse.fsinfo().mountnum(), 3);
        ASSERT_EQ(mountResponse.fsinfo().mountpoints_size(), 3);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    mountPoint.set_hostname("host2");
    mountPoint.set_port(9000);
    mountPoint.set_path("/a/b/c");
    mountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint));
    stub.MountFs(&cntl, &mountRequest, &mountResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(mountResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(mountResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(mountResponse.fsinfo(), fsinfo1));
        ASSERT_EQ(mountResponse.fsinfo().mountnum(), 4);
        ASSERT_EQ(mountResponse.fsinfo().mountpoints_size(), 4);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // TEST GetFsInfo
    // no fsid and no fsname
    cntl.Reset();
    GetFsInfoRequest getRequest;
    GetFsInfoResponse getResponse;
    stub.GetFsInfo(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::PARAM_ERROR);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // fsid1
    cntl.Reset();
    getRequest.set_fsid(fsinfo1.fsid());
    stub.GetFsInfo(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(getResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(getResponse.fsinfo(), fsinfo1));
        ASSERT_EQ(getResponse.fsinfo().mountnum(), 4);
        ASSERT_EQ(getResponse.fsinfo().mountpoints_size(), 4);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // fsid2
    cntl.Reset();
    getRequest.set_fsid(fsinfo2.fsid());
    stub.GetFsInfo(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(getResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(getResponse.fsinfo(), fsinfo2));
        ASSERT_EQ(getResponse.fsinfo().mountnum(), 0);
        ASSERT_EQ(getResponse.fsinfo().mountpoints_size(), 0);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // wrong fsid
    cntl.Reset();
    getRequest.set_fsid(fsinfo2.fsid() + 1);
    stub.GetFsInfo(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::NOT_FOUND);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // fsname1
    cntl.Reset();
    getRequest.clear_fsid();
    getRequest.set_fsname(fsinfo1.fsname());
    stub.GetFsInfo(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(getResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(getResponse.fsinfo(), fsinfo1));
        ASSERT_EQ(getResponse.fsinfo().mountnum(), 4);
        ASSERT_EQ(getResponse.fsinfo().mountpoints_size(), 4);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // fsname2
    cntl.Reset();
    getRequest.clear_fsid();
    getRequest.set_fsname(fsinfo2.fsname());
    stub.GetFsInfo(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(getResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(getResponse.fsinfo(), fsinfo2));
        ASSERT_EQ(getResponse.fsinfo().mountnum(), 0);
        ASSERT_EQ(getResponse.fsinfo().mountpoints_size(), 0);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // wrong fsname conflict
    cntl.Reset();
    getRequest.clear_fsid();
    getRequest.set_fsname("wrongName");
    stub.GetFsInfo(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::NOT_FOUND);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // both fsid and fsname
    cntl.Reset();
    getRequest.set_fsid(fsinfo2.fsid());
    getRequest.set_fsname(fsinfo2.fsname());
    stub.GetFsInfo(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(getResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(getResponse.fsinfo(), fsinfo2));
        ASSERT_EQ(getResponse.fsinfo().mountnum(), 0);
        ASSERT_EQ(getResponse.fsinfo().mountpoints_size(), 0);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // fsid and fsname conflict
    cntl.Reset();
    getRequest.set_fsid(fsinfo2.fsid());
    getRequest.set_fsname(fsinfo1.fsname());
    stub.GetFsInfo(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::PARAM_ERROR);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // fsid and fsname conflict
    cntl.Reset();
    getRequest.set_fsid(fsinfo1.fsid());
    getRequest.set_fsname(fsinfo2.fsname());
    stub.GetFsInfo(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::PARAM_ERROR);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // TEST unmount
    cntl.Reset();
    UmountFsRequest umountRequest;
    UmountFsResponse umountResponse;
    umountRequest.set_fsname(fsinfo1.fsname());
    mountPoint.set_hostname("host1");
    mountPoint.set_port(9000);
    mountPoint.set_path("/a/b/c");
    umountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint));
    stub.UmountFs(&cntl, &umountRequest, &umountResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(umountResponse.statuscode(), FSStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    stub.UmountFs(&cntl, &umountRequest, &umountResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(umountResponse.statuscode(),
                  FSStatusCode::MOUNT_POINT_NOT_EXIST);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    mountPoint.set_hostname("host2");
    mountPoint.set_port(9000);
    mountPoint.set_path("/a/b/c");
    umountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint));
    stub.UmountFs(&cntl, &umountRequest, &umountResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(umountResponse.statuscode(), FSStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    getRequest.clear_fsid();
    getRequest.set_fsname(fsinfo1.fsname());
    stub.GetFsInfo(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(getResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(getResponse.fsinfo(), fsinfo1));
        ASSERT_EQ(getResponse.fsinfo().mountnum(), 2);
        ASSERT_EQ(getResponse.fsinfo().mountpoints_size(), 2);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // test refresh session
    cntl.Reset();
    RefreshSessionRequest refreshSessionRequest;
    RefreshSessionResponse refreshSessionResponse;
    PartitionTxId tmp;
    tmp.set_partitionid(1);
    tmp.set_txid(1);
    std::vector<PartitionTxId> partitionList({std::move(tmp)});
    std::string fsName = "fs1";
    Mountpoint mountpoint;
    mountpoint.set_hostname("127.0.0.1");
    mountpoint.set_port(9000);
    mountpoint.set_path("/mnt");
    *refreshSessionRequest.mutable_txids() = {partitionList.begin(),
                                              partitionList.end()};
    refreshSessionRequest.set_fsname(fsName);
    *refreshSessionRequest.mutable_mountpoint() = mountpoint;
    EXPECT_CALL(*topoManager_, GetLatestPartitionsTxId(_, _))
        .WillOnce(SetArgPointee<1>(partitionList));
    stub.RefreshSession(&cntl, &refreshSessionRequest, &refreshSessionResponse,
                        NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(refreshSessionResponse.statuscode(), FSStatusCode::OK);
        ASSERT_EQ(1, refreshSessionResponse.latesttxidlist_size());
        std::pair<std::string, uint64_t> tpair;
        std::string mountpath = "127.0.0.1:9000:/mnt";
        ASSERT_TRUE(fsManager_->GetClientAliveTime(mountpath, &tpair));
        ASSERT_EQ(fsName, tpair.first);
        // RefreshSession will add a mountpoint to fs1
        cntl.Reset();
        UmountFsRequest umountRequest;
        UmountFsResponse umountResponse;
        umountRequest.set_fsname("fs1");
        mountPoint.set_hostname("127.0.0.1");
        mountPoint.set_port(9000);
        mountPoint.set_path("/mnt");
        umountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint));
        stub.UmountFs(&cntl, &umountRequest, &umountResponse, NULL);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // test delete fs
    cntl.Reset();
    DeleteFsRequest deleteRequest;
    DeleteFsResponse deleteResponse;
    deleteRequest.set_fsname(fsinfo2.fsname());
    stub.DeleteFs(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), FSStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    deleteRequest.set_fsname(fsinfo1.fsname());
    stub.DeleteFs(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), FSStatusCode::FS_BUSY);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    mountPoint.set_hostname("host1");
    mountPoint.set_port(9000);
    mountPoint.set_path("/a/b/d");
    umountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint));
    stub.UmountFs(&cntl, &umountRequest, &umountResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(umountResponse.statuscode(), FSStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    mountPoint.set_hostname("host2");
    mountPoint.set_port(9000);
    mountPoint.set_path("/a/b/d");
    umountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint));
    stub.UmountFs(&cntl, &umountRequest, &umountResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(umountResponse.statuscode(), FSStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    deleteRequest.set_fsname(fsinfo1.fsname());
    stub.DeleteFs(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), FSStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // stop rpc server
    server.Stop(10);
    server.Join();
}
}  // namespace mds
}  // namespace curvefs
