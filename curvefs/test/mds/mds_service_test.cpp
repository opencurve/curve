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
#include <memory>
#include <string>

#include "curvefs/test/mds/fake_metaserver.h"
#include "curvefs/test/mds/mock/mock_kvstorage_client.h"
#include "curvefs/test/mds/mock/mock_topology.h"
#include "curvefs/test/mds/mock/mock_cli2.h"
#include "test/common/mock_s3_adapter.h"
#include "curvefs/test/mds/mock/mock_fs_stroage.h"
#include "curvefs/test/mds/mock/mock_space_manager.h"
#include "curvefs/test/mds/mock/mock_volume_space.h"
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
using ::curvefs::mds::MockMemoryFsStorage;
using ::curvefs::mds::space::MockSpaceManager;
using ::curvefs::mds::space::MockVolumeSpace;
using ::google::protobuf::util::MessageDifferencer;

namespace curvefs {
namespace mds {

template <bool FAIL>
struct RpcService {
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

class MdsServiceTest : public ::testing::Test {
 protected:
    void SetUp() override {
        kvstorage_ = std::make_shared<MockKVStorageClient>();

        MetaserverOptions metaserverOptions;
        metaserverOptions.metaserverAddr = kMetaServerAddr;
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

        mdsService_ = std::make_shared<MdsServiceImpl>(fsManager_, nullptr);
        // add metaserver service
        ASSERT_EQ(server.AddService(mdsService_.get(),
                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);

        ASSERT_EQ(server.AddService(&metaserverService,
                                    brpc::SERVER_DOESNT_OWN_SERVICE), 0);

        ASSERT_EQ(
            server.AddService(&mockCliService2,
                              brpc::SERVER_DOESNT_OWN_SERVICE), 0);

        ASSERT_EQ(0, server.AddService(&fakeCurveFSService,
                                    brpc::SERVER_DOESNT_OWN_SERVICE));

        // start rpc server
        ASSERT_EQ(server.Start(kRpcServerAddr.c_str(), &option), 0);

        // init client
        ASSERT_EQ(channel.Init(server.listen_address(), nullptr), 0);
        stub_ = std::unique_ptr<MdsService_Stub>(new MdsService_Stub(&channel));

        volume.set_blocksize(4096);
        volume.set_volumename("volume1");
        volume.set_user("user1");
        volume.set_blockgroupsize(128ull * 1024 * 1024);
        volume.set_bitmaplocation(common::BitmapLocation::AtStart);
        volume.set_slicesize(1ULL * 1024 * 1024 * 1024);
        volume.set_autoextend(false);
        volume.add_cluster(kClusterAddr);

        EXPECT_CALL(*spaceManager_, GetVolumeSpace(_))
            .WillRepeatedly(Return(&volumeSpace));
        EXPECT_CALL(volumeSpace,
                    ReleaseBlockGroups(Matcher<const std::string &>(_)))
            .WillRepeatedly(Return(space::SpaceOk));

        getLeaderResponse.mutable_leader()->set_address(kRpcServerLeaderAddr);

        kCopysetAddrs.emplace(kRpcServerAddr);

        CreateVolumeFs();
        CreateS3F3();

        cntl.Reset();
    }

    void CreateS3F3() {
        CreateFsRequest createRequest;
        CreateFsResponse createResponse;

        cntl.Reset();
        createRequest.set_fsname(kS3FsName);
        createRequest.set_blocksize(4096);
        createRequest.set_fstype(FSType::TYPE_S3);
        createRequest.mutable_fsdetail();
        createRequest.set_enablesumindir(false);
        createRequest.set_capacity(FS_CAPACITY);
        createRequest.set_owner(kFsOwner);
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
                SetArgPointee<2>(kCopysetAddrs),
                Return(TopoStatusCode::TOPO_OK)));
        EXPECT_CALL(mockCliService2, GetLeader(_, _, _, _))
            .WillOnce(DoAll(
            SetArgPointee<2>(getLeaderResponse),
            Invoke(RpcService<false>{})));
        EXPECT_CALL(*s3Adapter_, PutObject(_, _)).WillOnce(Return(0));
        EXPECT_CALL(*s3Adapter_, DeleteObject(_)).WillOnce(Return(0));

        cntl.set_timeout_ms(5000);
        stub_->CreateFs(&cntl, &createRequest, &createResponse, nullptr);
        if (!cntl.Failed()) {
            ASSERT_EQ(createResponse.statuscode(), FSStatusCode::OK);
            ASSERT_TRUE(createResponse.has_fsinfo());
            fsinfo2 = createResponse.fsinfo();
        } else {
            LOG(ERROR) << "error = " << cntl.ErrorText();
            ASSERT_TRUE(false);
        }
    }

    void CreateVolumeFs() {
        CreateFsRequest createRequest;
        CreateFsResponse createResponse;
        createRequest.set_fsname(kVolumeFsName);
        createRequest.set_blocksize(4096);
        createRequest.set_fstype(::curvefs::common::FSType::TYPE_VOLUME);
        createRequest.mutable_fsdetail()->mutable_volume()->CopyFrom(volume);
        createRequest.set_enablesumindir(false);
        createRequest.set_capacity(FS_CAPACITY);
        createRequest.set_owner(kFsOwner);

        // force allocate detail
        auto* detail = createRequest.mutable_fsdetail();
        (void)detail;

        EXPECT_CALL(*topoManager_,
                    CreatePartitionsAndGetMinPartition(_, _))
            .WillOnce(Return(TopoStatusCode::TOPO_OK));

        EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
                    .WillOnce(DoAll(
                        SetArgPointee<2>(kCopysetAddrs),
                        Return(TopoStatusCode::TOPO_OK)));

        EXPECT_CALL(mockCliService2, GetLeader(_, _, _, _))
            .WillOnce(DoAll(
            SetArgPointee<2>(getLeaderResponse),
            Invoke(RpcService<false>{})));

        cntl.Reset();
        stub_->CreateFs(&cntl, &createRequest, &createResponse, nullptr);
        if (!cntl.Failed()) {
            ASSERT_EQ(createResponse.statuscode(), FSStatusCode::OK);
            ASSERT_TRUE(createResponse.has_fsinfo());
            fsinfo1 = createResponse.fsinfo();
            ASSERT_EQ(fsinfo1.fsid(), 0);
            ASSERT_EQ(fsinfo1.fsname(), fsinfo1.fsname());
            ASSERT_EQ(fsinfo1.rootinodeid(), 1);
            ASSERT_EQ(fsinfo1.capacity(), FS_CAPACITY);
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
    }

    void TearDown() override {
        server.Stop(10);
        server.Join();
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
    std::shared_ptr<MdsServiceImpl> mdsService_;

    std::unique_ptr<MdsService_Stub> stub_;

    MockMemoryFsStorage mockFsStorage_;
    MockVolumeSpace volumeSpace;
    FakeCurveFSService fakeCurveFSService;
    FakeMetaserverImpl metaserverService;
    MockCliService2 mockCliService2;
    brpc::Channel channel;
    brpc::ServerOptions option;
    brpc::Server server;
    brpc::Controller cntl;
    GetLeaderResponse2 getLeaderResponse;
    FsInfo fsinfo1;
    Volume volume;
    FsInfo fsinfo2;
    S3Info s3info;

    uint64_t FS_CAPACITY = 100ULL << 30;
    const std::string kS3FsName = "fs2";
    const std::string kHost1 = "host1";
    const std::string kHost2 = "host2";
    const std::string kPathRootABC = "/a/b/c";
    const std::string kPathRootABD = "/a/b/d";
    const std::string kFsOwner = "test";
    const std::string kVolumeFsName = "fs1";
    const std::string kClusterAddr = "127.0.0.1:6703";
    const std::string kRpcServerAddr = "127.0.0.1:6703";
    const std::string kMetaServerAddr = "127.0.0.1:6703";
    const std::string kRpcServerLeaderAddr = "127.0.0.1:6703:0";
    std::set<std::string> kCopysetAddrs;
};

TEST_F(MdsServiceTest, test_fail_create_fs_missing_volume_for_volume_fstype) {
    // type if volume, but volume not set
    CreateFsRequest createRequest;
    createRequest.set_fsname(fsinfo1.fsname());
    createRequest.set_blocksize(4096);
    createRequest.set_fstype(::curvefs::common::FSType::TYPE_VOLUME);
    createRequest.set_enablesumindir(false);
    createRequest.set_capacity(FS_CAPACITY);
    createRequest.set_owner(kFsOwner);
    auto* detail = createRequest.mutable_fsdetail();  // force allocate detail
    (void)detail;

    CreateFsResponse createResponse;
    stub_->CreateFs(&cntl, &createRequest, &createResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), FSStatusCode::PARAM_ERROR);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_fail_create_fs_missing_s3_info_for_s3_fstype) {
    // create s3 fs, s3info not set
    CreateFsRequest createRequest;
    createRequest.set_fsname("abc");
    createRequest.set_blocksize(4096);
    createRequest.set_fstype(FSType::TYPE_S3);
    createRequest.mutable_fsdetail();
    createRequest.set_enablesumindir(false);
    createRequest.set_capacity(FS_CAPACITY);
    createRequest.set_owner(kFsOwner);
    auto* detail = createRequest.mutable_fsdetail();  // force allocate detail
    (void)detail;
    CreateFsResponse createResponse;
    stub_->CreateFs(&cntl, &createRequest, &createResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), FSStatusCode::PARAM_ERROR);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_fail_retrieve_fsinfo_non_existent_fsid) {
    GetFsInfoRequest getRequest;
    GetFsInfoResponse getResponse;
    // non-existent fsid
    getRequest.set_fsid(fsinfo2.fsid() + 1);
    stub_->GetFsInfo(&cntl, &getRequest, &getResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::NOT_FOUND);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_fail_create_fs_existing_volume_for_volume_fstype) {
    CreateFsRequest createRequest;
    createRequest.set_fsname(fsinfo1.fsname());
    createRequest.set_blocksize(4096);
    createRequest.set_fstype(::curvefs::common::FSType::TYPE_VOLUME);
    createRequest.set_enablesumindir(false);
    createRequest.set_capacity(FS_CAPACITY);
    createRequest.set_owner(kFsOwner);
    auto* detail = createRequest.mutable_fsdetail();  // force allocate detail
    (void)detail;

    CreateFsResponse createResponse;
    stub_->CreateFs(&cntl, &createRequest, &createResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), FSStatusCode::PARAM_ERROR);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_fail_create_fs_hybrid_fstype) {
    // TODO(huyao): create hybrid fs
    CreateFsRequest createRequest;
    createRequest.set_fsname("hybrid");
    createRequest.set_blocksize(4096);
    createRequest.set_fstype(FSType::TYPE_HYBRID);
    createRequest.set_enablesumindir(false);
    createRequest.set_capacity(FS_CAPACITY);
    createRequest.set_owner(kFsOwner);
    s3info.set_ak("ak");
    s3info.set_sk("sk");
    s3info.set_endpoint("endpoint");
    s3info.set_bucketname("bucketname");
    s3info.set_blocksize(4096);
    s3info.set_chunksize(4096);
    createRequest.mutable_fsdetail()->mutable_volume()->CopyFrom(volume);
    createRequest.mutable_fsdetail()->mutable_s3info()->CopyFrom(s3info);
    CreateFsResponse createResponse;
    stub_->CreateFs(&cntl, &createRequest, &createResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), FSStatusCode::UNKNOWN_ERROR);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_fail_get_fsinfo_missing_fsid_and_fsname) {
    GetFsInfoRequest getRequest;
    GetFsInfoResponse getResponse;
    // no fsid and no fsname
    stub_->GetFsInfo(&cntl, &getRequest, &getResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::PARAM_ERROR);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_success_get_fsinfo_fsid_with_volume_fstype) {
    FsInfoWrapper fsInfoWrapper(fsinfo1);
    EXPECT_CALL(mockFsStorage_, Get(_, _))
        .WillRepeatedly(DoAll(
            SetArgPointee<1>(fsInfoWrapper),
            Return(FSStatusCode::OK)));

    GetFsInfoRequest getRequest;
    GetFsInfoResponse getResponse;
    getRequest.set_fsid(fsinfo1.fsid());
    stub_->GetFsInfo(&cntl, &getRequest, &getResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(getResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(getResponse.fsinfo(), fsinfo1));
        ASSERT_EQ(getResponse.fsinfo().mountnum(), 0);
        ASSERT_EQ(getResponse.fsinfo().mountpoints_size(), 0);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_success_get_fsinfo_fsid_with_s3_fstype) {
    GetFsInfoRequest getRequest;
    GetFsInfoResponse getResponse;
    getRequest.set_fsid(fsinfo2.fsid());
    stub_->GetFsInfo(&cntl, &getRequest, &getResponse, nullptr);
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
}

TEST_F(MdsServiceTest, test_success_get_fsinfo_fsname_with_volume_fstype) {
    FsInfoWrapper fsInfoWrapper(fsinfo1);
    EXPECT_CALL(mockFsStorage_, Get(_, _))
        .WillRepeatedly(DoAll(
            SetArgPointee<1>(fsInfoWrapper),
            Return(FSStatusCode::OK)));

    GetFsInfoRequest getRequest;
    GetFsInfoResponse getResponse;
    getRequest.clear_fsid();
    getRequest.set_fsname(fsinfo1.fsname());
    stub_->GetFsInfo(&cntl, &getRequest, &getResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(getResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(getResponse.fsinfo(), fsinfo1));
        ASSERT_EQ(getResponse.fsinfo().mountnum(), 0);
        ASSERT_EQ(getResponse.fsinfo().mountpoints_size(), 0);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_success_get_fsinfo_fsname_with_s3_fstype) {
    GetFsInfoRequest getRequest;
    GetFsInfoResponse getResponse;
    getRequest.clear_fsid();
    getRequest.set_fsname(fsinfo2.fsname());
    stub_->GetFsInfo(&cntl, &getRequest, &getResponse, nullptr);
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
}

TEST_F(MdsServiceTest, test_fail_get_fsinfo_non_existent_fsname) {
    GetFsInfoRequest getRequest;
    GetFsInfoResponse getResponse;
    getRequest.clear_fsid();
    getRequest.set_fsname("wrongName");
    stub_->GetFsInfo(&cntl, &getRequest, &getResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::NOT_FOUND);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_success_get_fsinfo_consistent_fsname_and_fsid) {
    GetFsInfoRequest getRequest;
    GetFsInfoResponse getResponse;
    getRequest.set_fsid(fsinfo2.fsid());
    getRequest.set_fsname(fsinfo2.fsname());
    stub_->GetFsInfo(&cntl, &getRequest, &getResponse, nullptr);
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
}

TEST_F(MdsServiceTest, test_fail_get_fsinfo_inconsistent_fsname_and_fsid) {
    GetFsInfoRequest getRequest;
    GetFsInfoResponse getResponse;
    getRequest.set_fsid(fsinfo2.fsid());
    getRequest.set_fsname(fsinfo1.fsname());
    stub_->GetFsInfo(&cntl, &getRequest, &getResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::PARAM_ERROR);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    getRequest.set_fsid(fsinfo1.fsid());
    getRequest.set_fsname(fsinfo2.fsname());
    stub_->GetFsInfo(&cntl, &getRequest, &getResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::PARAM_ERROR);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_success_mount_then_umount_on_single_path) {
    Mountpoint mountPoint;
    mountPoint.set_hostname(kHost2);
    mountPoint.set_port(9000);
    mountPoint.set_path(kPathRootABC);
    mountPoint.set_cto(false);
    MountFsRequest mountRequest;
    MountFsResponse mountResponse;
    mountRequest.set_fsname(fsinfo1.fsname());
    mountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint));
    stub_->MountFs(&cntl, &mountRequest, &mountResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(mountResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(mountResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(mountResponse.fsinfo(), fsinfo1));
        ASSERT_EQ(mountResponse.fsinfo().mountnum(), 1);
        ASSERT_EQ(mountResponse.fsinfo().mountpoints_size(), 1);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    UmountFsRequest umountRequest;
    UmountFsResponse umountResponse;
    umountRequest.set_fsname(fsinfo1.fsname());
    mountPoint.set_hostname(kHost2);
    mountPoint.set_port(9000);
    mountPoint.set_path(kPathRootABC);
    umountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint));
    stub_->UmountFs(&cntl, &umountRequest, &umountResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(umountResponse.statuscode(), FSStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_success_delete_fs_no_mount_paths) {
    // test delete fs
    DeleteFsRequest deleteRequest;
    DeleteFsResponse deleteResponse;
    deleteRequest.set_fsname(fsinfo2.fsname());
    stub_->DeleteFs(&cntl, &deleteRequest, &deleteResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), FSStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_fail_delete_fs_mount_paths_still_exist) {
    Mountpoint mountPoint3;
    mountPoint3.set_hostname(kHost2);
    mountPoint3.set_port(9000);
    mountPoint3.set_path(kPathRootABD);
    mountPoint3.set_cto(false);
    MountFsRequest mountRequest;
    MountFsResponse mountResponse;
    mountRequest.set_fsname(fsinfo1.fsname());
    mountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint3));
    stub_->MountFs(&cntl, &mountRequest, &mountResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(mountResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(mountResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(mountResponse.fsinfo(), fsinfo1));
        ASSERT_EQ(mountResponse.fsinfo().mountnum(), 1);
        ASSERT_EQ(mountResponse.fsinfo().mountpoints_size(), 1);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    FsInfoWrapper fsInfoWrapper(fsinfo1);
    EXPECT_CALL(mockFsStorage_, Get(_, _))
        .WillRepeatedly(DoAll(
            SetArgPointee<1>(fsInfoWrapper),
            Return(FSStatusCode::OK)));

    cntl.Reset();
    DeleteFsRequest deleteRequest;
    DeleteFsResponse deleteResponse;
    deleteRequest.set_fsname(fsinfo1.fsname());
    stub_->DeleteFs(&cntl, &deleteRequest, &deleteResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), FSStatusCode::FS_BUSY);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_fail_repeatedly_mount_fs_on_same_path) {
    Mountpoint mountPoint;
    mountPoint.set_hostname(kHost1);
    mountPoint.set_port(9000);
    mountPoint.set_path(kPathRootABC);
    mountPoint.set_cto(false);
    MountFsRequest mountRequest;
    MountFsResponse mountResponse;
    mountRequest.set_fsname(fsinfo1.fsname());
    mountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint));
    stub_->MountFs(&cntl, &mountRequest, &mountResponse, nullptr);
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
    stub_->MountFs(&cntl, &mountRequest, &mountResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(mountResponse.statuscode(),
                FSStatusCode::MOUNT_POINT_CONFLICT);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_fail_repeatedly_umount_fs_on_same_path) {
    Mountpoint mountPoint;
    mountPoint.set_hostname(kHost1);
    mountPoint.set_port(9000);
    mountPoint.set_path(kPathRootABC);
    mountPoint.set_cto(false);
    MountFsRequest mountRequest;
    MountFsResponse mountResponse;
    mountRequest.set_fsname(fsinfo1.fsname());
    mountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint));
    stub_->MountFs(&cntl, &mountRequest, &mountResponse, nullptr);
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
    UmountFsRequest umountRequest;
    UmountFsResponse umountResponse;
    umountRequest.set_fsname(fsinfo1.fsname());
    mountPoint.set_hostname(kHost1);
    mountPoint.set_port(9000);
    mountPoint.set_path(kPathRootABC);
    umountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint));
    stub_->UmountFs(&cntl, &umountRequest, &umountResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(umountResponse.statuscode(), FSStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    umountRequest.set_fsname(fsinfo1.fsname());
    umountRequest.set_fsname(fsinfo1.fsname());
    mountPoint.set_hostname(kHost1);
    mountPoint.set_port(9000);
    mountPoint.set_path(kPathRootABC);
    stub_->UmountFs(&cntl, &umountRequest, &umountResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(umountResponse.statuscode(),
                  FSStatusCode::MOUNT_POINT_NOT_EXIST);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_success_refresh_sessions) {
    PartitionTxId tmp;
    tmp.set_partitionid(1);
    tmp.set_txid(1);
    std::vector<PartitionTxId> partitionList({std::move(tmp)});
    std::string fsName = fsinfo1.fsname();
    Mountpoint mountpoint;
    Mountpoint mountPoint;
    mountpoint.set_hostname("127.0.0.1");
    mountpoint.set_port(9000);
    mountpoint.set_path("/mnt");
    RefreshSessionRequest refreshSessionRequest;
    RefreshSessionResponse refreshSessionResponse;
    *refreshSessionRequest.mutable_txids() = {partitionList.begin(),
                                              partitionList.end()};
    refreshSessionRequest.set_fsname(fsName);
    *refreshSessionRequest.mutable_mountpoint() = mountpoint;
    EXPECT_CALL(*topoManager_, GetLatestPartitionsTxId(_, _))
        .WillOnce(SetArgPointee<1>(partitionList));

    stub_->RefreshSession(&cntl, &refreshSessionRequest,
                          &refreshSessionResponse, nullptr);
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
        umountRequest.set_fsname(fsName);
        mountPoint.set_hostname("127.0.0.1");
        mountPoint.set_port(9000);
        mountPoint.set_path("/mnt");
        umountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint));
        stub_->UmountFs(&cntl, &umountRequest, &umountResponse, nullptr);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_success_delete_fs_after_umounting_all_paths) {
    Mountpoint mountPoint2;
    mountPoint2.set_hostname(kHost1);
    mountPoint2.set_port(9000);
    mountPoint2.set_path(kPathRootABD);
    mountPoint2.set_cto(false);
    MountFsRequest mountRequest;
    MountFsResponse mountResponse;
    mountRequest.set_fsname(fsinfo1.fsname());
    mountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint2));
    stub_->MountFs(&cntl, &mountRequest, &mountResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(mountResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(mountResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(mountResponse.fsinfo(), fsinfo1));
        ASSERT_EQ(mountResponse.fsinfo().mountnum(), 1);
        ASSERT_EQ(mountResponse.fsinfo().mountpoints_size(), 1);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    Mountpoint mountPoint;
    mountPoint.set_hostname(kHost1);
    mountPoint.set_port(9000);
    mountPoint.set_path(kPathRootABD);
    mountPoint.set_cto(false);
    UmountFsRequest umountRequest;
    UmountFsResponse umountResponse;
    umountRequest.set_fsname(fsinfo1.fsname());
    umountRequest.set_allocated_mountpoint(new Mountpoint(mountPoint));
    stub_->UmountFs(&cntl, &umountRequest, &umountResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(umountResponse.statuscode(), FSStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    DeleteFsRequest deleteRequest;
    DeleteFsResponse deleteResponse;
    deleteRequest.set_fsname(fsinfo1.fsname());
    stub_->DeleteFs(&cntl, &deleteRequest, &deleteResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), FSStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_update_fsinfo_success) {
    FsInfo fs;

    auto get = [&]() {
        cntl.Reset();
        GetFsInfoRequest getRequest;
        GetFsInfoResponse getResponse;
        getRequest.set_fsid(fsinfo1.fsid());
        stub_->GetFsInfo(&cntl, &getRequest, &getResponse, nullptr);
        if (!cntl.Failed()) {
            ASSERT_EQ(getResponse.statuscode(), FSStatusCode::OK);
            fs = getResponse.fsinfo();
        } else {
            LOG(ERROR) << "error = " << cntl.ErrorText();
            ASSERT_TRUE(false);
        }
    };

    // 1. check current fsinfo
    get();
    ASSERT_EQ(fs.capacity(), fsinfo1.capacity());

    // 2. update fsinfo
    cntl.Reset();
    UpdateFsInfoRequest updateRequest;
    UpdateFsInfoResponse updateResponse;
    updateRequest.set_fsname(fsinfo1.fsname());
    updateRequest.set_capacity(0);
    stub_->UpdateFsInfo(&cntl, &updateRequest, &updateResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(updateResponse.statuscode(), FSStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // 3. check updated fsinfo
    get();
    ASSERT_EQ(fs.capacity(), 0);
}

TEST_F(MdsServiceTest, test_update_fsinfo_parameter_error) {
    UpdateFsInfoRequest updateRequest;
    UpdateFsInfoResponse updateResponse;
    updateRequest.set_fsname(fsinfo1.fsname());
    stub_->UpdateFsInfo(&cntl, &updateRequest, &updateResponse, nullptr);
    if (!cntl.Failed()) {
        ASSERT_EQ(updateResponse.statuscode(), FSStatusCode::PARAM_ERROR);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
}

TEST_F(MdsServiceTest, test_tso) {
    TsoRequest tsoRequest;
    TsoResponse tsoResponse;
    for (int i = 1; i < 5; i++) {
        cntl.Reset();
        stub_->Tso(&cntl, &tsoRequest, &tsoResponse, nullptr);
        if (!cntl.Failed()) {
            ASSERT_EQ(tsoResponse.statuscode(), FSStatusCode::OK);
            ASSERT_EQ(tsoResponse.ts(), i);
        } else {
            LOG(ERROR) << "error = " << cntl.ErrorText();
            ASSERT_TRUE(false);
        }
    }
}

}  // namespace mds
}  // namespace curvefs
