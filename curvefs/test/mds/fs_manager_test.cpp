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
 * @Date: 2021-06-10 10:04:37
 * @Author: chenwei
 */
#include <brpc/channel.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <gmock/gmock.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>
#include "absl/memory/memory.h"
#include "curvefs/src/mds/fs_manager.h"
#include "curvefs/test/mds/mock/mock_cli2.h"
#include "curvefs/test/mds/mock/mock_fs_stroage.h"
#include "curvefs/test/mds/mock/mock_metaserver.h"
#include "curvefs/test/mds/mock/mock_topology.h"
#include "curvefs/test/mds/mock/mock_space_manager.h"
#include "curvefs/test/mds/mock/mock_volume_space.h"
#include "curvefs/test/mds/utils.h"
#include "test/common/mock_s3_adapter.h"

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SaveArg;
using ::testing::Mock;
using ::testing::Invoke;
using ::testing::Matcher;
using ::curvefs::metaserver::MockMetaserverService;
using ::curvefs::metaserver::CreateRootInodeRequest;
using ::curvefs::metaserver::CreateRootInodeResponse;
using ::curvefs::metaserver::CreateManageInodeRequest;
using ::curvefs::metaserver::CreateManageInodeResponse;
using ::curvefs::metaserver::CreateDentryRequest;
using ::curvefs::metaserver::CreateDentryResponse;
using ::curvefs::metaserver::DeletePartitionRequest;
using ::curvefs::metaserver::DeletePartitionResponse;
using ::curvefs::metaserver::DeleteInodeRequest;
using ::curvefs::metaserver::DeleteInodeResponse;
using ::curvefs::metaserver::MetaStatusCode;
using ::google::protobuf::util::MessageDifferencer;
using ::curvefs::common::S3Info;
using ::curvefs::common::Volume;
using ::curvefs::mds::topology::TopologyManager;
using ::curvefs::mds::topology::MockTopologyManager;
using ::curvefs::mds::topology::MockTopology;
using ::curvefs::mds::topology::MockIdGenerator;
using ::curvefs::mds::topology::MockTokenGenerator;
using ::curvefs::mds::topology::MockStorage;
using ::curvefs::mds::topology::TopologyIdGenerator;
using ::curvefs::mds::topology::DefaultIdGenerator;
using ::curvefs::mds::topology::TopologyTokenGenerator;
using ::curvefs::mds::topology::DefaultTokenGenerator;
using ::curvefs::mds::topology::MockEtcdClient;
using ::curvefs::mds::topology::MockTopologyManager;
using ::curvefs::mds::topology::TopologyStorageCodec;
using ::curvefs::mds::topology::TopologyStorageEtcd;
using ::curvefs::mds::topology::TopologyImpl;
using ::curvefs::mds::topology::CreatePartitionRequest;
using ::curvefs::mds::topology::CreatePartitionResponse;
using ::curvefs::mds::topology::TopoStatusCode;
using ::curvefs::mds::topology::FsIdType;
using ::curvefs::metaserver::copyset::MockCliService2;
using ::curvefs::metaserver::copyset::GetLeaderResponse2;
using ::curve::common::MockS3Adapter;
using ::curvefs::mds::space::MockSpaceManager;
using ::curvefs::mds::space::MockVolumeSpace;

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

class FSManagerTest : public ::testing::Test {
 protected:
    void SetUp() override {
        addr_ = "127.0.0.1:6704";
        MetaserverOptions metaserverOptions;
        metaserverOptions.metaserverAddr = addr_;
        metaserverOptions.rpcTimeoutMs = 500;
        fsStorage_ = std::make_shared<MemoryFsStorage>();
        spaceManager_ = std::make_shared<MockSpaceManager>();
        metaserverClient_ =
            std::make_shared<MetaserverClient>(metaserverOptions);
        // init mock topology manager
        std::shared_ptr<TopologyIdGenerator> idGenerator_ =
            std::make_shared<DefaultIdGenerator>();
        std::shared_ptr<TopologyTokenGenerator> tokenGenerator_ =
            std::make_shared<DefaultTokenGenerator>();

        auto etcdClient_ = std::make_shared<MockEtcdClient>();
        auto codec = std::make_shared<TopologyStorageCodec>();
        auto topoStorage_ =
            std::make_shared<TopologyStorageEtcd>(etcdClient_, codec);
        topoManager_ = std::make_shared<MockTopologyManager>(
            std::make_shared<TopologyImpl>(idGenerator_, tokenGenerator_,
                                           topoStorage_),
            metaserverClient_);
        // init fsmanager
        FsManagerOption fsManagerOption;
        fsManagerOption.backEndThreadRunInterSec = 1;
        fsManagerOption.clientTimeoutSec = 1;
        fsManagerOption.mdsListenAddr = addr_;
        s3Adapter_ = std::make_shared<MockS3Adapter>();
        fsManager_ = std::make_shared<FsManager>(fsStorage_, spaceManager_,
                                                 metaserverClient_,
                                                 topoManager_, s3Adapter_,
                                                 nullptr,
                                                 fsManagerOption);
        ASSERT_TRUE(fsManager_->Init());

        ASSERT_EQ(0, server_.AddService(&mockMetaserverService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.AddService(&mockCliService2_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.AddService(&fakeCurveFsService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));

        ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));

        volumeSize = fakeCurveFsService_.volumeSize;

        bool enableSumInDir = false;
        curvefs::common::Volume volume;
        volume.set_blocksize(4096);
        volume.set_volumename("volume1");
        volume.set_user("user1");
        volume.add_cluster(addr_);

        FsDetail detail;
        detail.set_allocated_volume(new Volume(volume));

        req.set_fsname(kFsName1);
        req.set_blocksize(kBlockSize);
        req.set_fstype(FSType::TYPE_VOLUME);
        req.set_allocated_fsdetail(new FsDetail(detail));
        req.set_enablesumindir(enableSumInDir);
        req.set_owner("test");
        req.set_capacity((uint64_t)100*1024*1024*1024);

        std::string leader = addr_ + ":0";
        getLeaderResponse.mutable_leader()->set_address(leader);

        // create s3 test
        std::set<std::string> addrs{addr_};
        curvefs::common::S3Info s3Info;
        s3Info.set_ak("ak");
        s3Info.set_sk("sk");
        s3Info.set_endpoint("endpoint");
        s3Info.set_bucketname("bucketname");
        s3Info.set_blocksize(4096);
        s3Info.set_chunksize(4096);
        CreateRootInodeResponse response2;
        FsDetail detail2;
        detail2.set_allocated_s3info(new S3Info(s3Info));

        s3Req.set_fsname(kFsName2);
        s3Req.set_allocated_fsdetail(new FsDetail(detail2));
        s3Req.set_fstype(FSType::TYPE_S3);
        s3Req.set_blocksize(kBlockSize);
        s3Req.set_enablesumindir(enableSumInDir);
        s3Req.set_owner("test");
        s3Req.set_capacity((uint64_t)100*1024*1024*1024);

        s3MountPoint.set_hostname("host");
        s3MountPoint.set_port(90000);
        s3MountPoint.set_path("/a/b/c");
        s3MountPoint.set_cto(false);

        volMountPoint.set_hostname("host");
        volMountPoint.set_port(90000);
        volMountPoint.set_path("/a/b/c");
        volMountPoint.set_cto(false);
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
        fsManager_->Uninit();
    }

    FSStatusCode CreateVolFs() {
        CreateRootInodeResponse response;

        std::set<std::string> addrs { addr_ };

        // create vol fs ok
        EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
            .WillOnce(Return(TopoStatusCode::TOPO_OK));
        EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
            .WillOnce(
                DoAll(SetArgPointee<2>(addrs),
                      Return(TopoStatusCode::TOPO_OK)));
        EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
            .WillOnce(
                DoAll(SetArgPointee<2>(getLeaderResponse),
                    Invoke(RpcService<false>{})));

        response.set_statuscode(MetaStatusCode::OK);
        EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
            .WillOnce(DoAll(
                SetArgPointee<2>(response),
                Invoke(RpcService<false>{})));

        return fsManager_->CreateFs(&req, &volumeFsInfo1);
    }

    FSStatusCode CreateS3Fs() {
        CreateRootInodeResponse response;

        std::set<std::string> addrs { addr_ };

        // create s3 fs ok
        EXPECT_CALL(*topoManager_,
                    CreatePartitionsAndGetMinPartition(_, _))
            .WillOnce(Return(TopoStatusCode::TOPO_OK));
        EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
            .WillOnce(
                DoAll(SetArgPointee<2>(addrs),
                      Return(TopoStatusCode::TOPO_OK)));
        EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
            .WillOnce(
                DoAll(SetArgPointee<2>(getLeaderResponse),
                    Invoke(RpcService<false>{})));

        response.set_statuscode(MetaStatusCode::OK);
        EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
            .WillOnce(DoAll(
                SetArgPointee<2>(response),
                Invoke(RpcService<false>{})));
        EXPECT_CALL(*s3Adapter_, PutObject(_, _)).WillOnce(Return(0));
        EXPECT_CALL(*s3Adapter_, DeleteObject(_)).WillOnce(Return(0));

        return fsManager_->CreateFs(&s3Req, &s3FsInfo);
    }

    static bool CompareVolume(const Volume& first, const Volume& second) {
        return MessageDifferencer::Equals(first, second);
    }

    static bool CompareVolumeFs(const FsInfo& first, const FsInfo& second) {
        return first.fsid() == second.fsid() &&
               first.fsname() == second.fsname() &&
               first.rootinodeid() == second.rootinodeid() &&
               first.capacity() == second.capacity() &&
               first.blocksize() == second.blocksize() &&
               first.fstype() == second.fstype() &&
               first.detail().has_volume() && second.detail().has_volume() &&
               CompareVolume(first.detail().volume(), second.detail().volume());
    }

    static bool CompareS3Info(const S3Info& first, const S3Info& second) {
        return MessageDifferencer::Equals(first, second);
    }

    static bool CompareS3Fs(const FsInfo& first, const FsInfo& second) {
        return first.fsid() == second.fsid() &&
               first.fsname() == second.fsname() &&
               first.rootinodeid() == second.rootinodeid() &&
               first.capacity() == second.capacity() &&
               first.blocksize() == second.blocksize() &&
               first.fstype() == second.fstype() &&
               first.detail().has_s3info() && second.detail().has_s3info() &&
               CompareS3Info(first.detail().s3info(), second.detail().s3info());
        return MessageDifferencer::Equals(first, second);
    }

 protected:
    std::shared_ptr<FsManager> fsManager_;
    std::shared_ptr<FsStorage> fsStorage_;
    std::shared_ptr<MockSpaceManager> spaceManager_;
    std::shared_ptr<MetaserverClient> metaserverClient_;
    MockMetaserverService mockMetaserverService_;
    MockCliService2 mockCliService2_;
    std::shared_ptr<MockTopologyManager> topoManager_;
    brpc::Server server_;
    std::shared_ptr<MockS3Adapter> s3Adapter_;
    FakeCurveFSService fakeCurveFsService_;
    std::string addr_;

    const std::string kFsName1 = "fs1";
    const uint64_t kBlockSize = 4096;
    const std::string kFsName2 = "fs2";
    uint64_t volumeSize;
    CreateFsRequest req;
    CreateFsRequest s3Req;
    GetLeaderResponse2 getLeaderResponse;
    FsInfo s3FsInfo;
    Mountpoint s3MountPoint;
    Mountpoint volMountPoint;
    FsInfo volumeFsInfo1;
};

TEST_F(FSManagerTest,
       test_success_cleanup_after_fail_create_volume_fs_on_inode_error) {
    std::string leader = addr_ + ":0";
    std::set<std::string> addrs{addr_};

    FsInfo fsInfo;
    FsInfoWrapper wrapper;

    // create volume fs create recycle inode fail
    EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));
    EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(addrs), Return(TopoStatusCode::TOPO_OK)));
    GetLeaderResponse2 getLeaderResponse;
    getLeaderResponse.mutable_leader()->set_address(leader);
    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .Times(2)
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(getLeaderResponse),
                  Invoke(RpcService<false>{})));

    CreateRootInodeResponse createRootInodeResponse;
    createRootInodeResponse.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(createRootInodeResponse),
            Invoke(RpcService<false>{})));

    CreateManageInodeResponse createManageInodeResponse;
    createManageInodeResponse.set_statuscode(MetaStatusCode::UNKNOWN_ERROR);
    EXPECT_CALL(mockMetaserverService_, CreateManageInode(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(createManageInodeResponse),
                        Invoke(RpcService<false>{})));

    // delete inode, partition and fs in order
    DeleteInodeResponse deleteInodeResponse;
    deleteInodeResponse.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaserverService_, DeleteInode(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(deleteInodeResponse),
                  Invoke(RpcService<false>{})));
    EXPECT_CALL(*topoManager_, DeletePartition(_))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));

    req.set_recycletimehour(10);

    ASSERT_EQ(FSStatusCode::INSERT_MANAGE_INODE_FAIL,
              fsManager_->CreateFs(&req, &fsInfo));
    ASSERT_EQ(FSStatusCode::NOT_FOUND, fsStorage_->Get(kFsName1, &wrapper));
}

TEST_F(FSManagerTest,
       test_success_cleanup_after_fail_create_volume_fs_on_dentry_error) {
    std::string leader = addr_ + ":0";
    std::set<std::string> addrs{addr_};

    FsInfo fsInfo;
    FsInfoWrapper wrapper;

    // create volume fs create recycle dentry fail
    EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));
    EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(addrs), Return(TopoStatusCode::TOPO_OK)));

    GetLeaderResponse2 getLeaderResponse;
    getLeaderResponse.mutable_leader()->set_address(leader);
    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .Times(3)
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(getLeaderResponse),
                  Invoke(RpcService<false>{})));

    CreateRootInodeResponse createRootInodeResponse;
    createRootInodeResponse.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(createRootInodeResponse),
            Invoke(RpcService<false>{})));

    CreateManageInodeResponse createManageInodeResponse;
    createManageInodeResponse.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaserverService_, CreateManageInode(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(createManageInodeResponse),
                        Invoke(RpcService<false>{})));

    CreateDentryResponse createDentryResponse;
    createDentryResponse.set_statuscode(MetaStatusCode::UNKNOWN_ERROR);
    EXPECT_CALL(mockMetaserverService_, CreateDentry(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(createDentryResponse),
            Invoke(RpcService<false>{})));

    // delete inode, partition and fs in order
    DeleteInodeResponse deleteInodeResponse;
    deleteInodeResponse.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaserverService_, DeleteInode(_, _, _, _))
        .Times(2)
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(deleteInodeResponse),
                  Invoke(RpcService<false>{})));
    EXPECT_CALL(*topoManager_, DeletePartition(_))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));

    req.set_recycletimehour(10);

    ASSERT_EQ(FSStatusCode::INSERT_DENTRY_FAIL,
              fsManager_->CreateFs(&req, &fsInfo));
    ASSERT_EQ(FSStatusCode::NOT_FOUND, fsStorage_->Get(kFsName1, &wrapper));
}

TEST_F(FSManagerTest, test_success_create_volume_fs) {
    FSStatusCode ret = CreateVolFs();

    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_EQ(volumeFsInfo1.fsid(), 0);
    ASSERT_EQ(volumeFsInfo1.fsname(), kFsName1);
    ASSERT_EQ(volumeFsInfo1.status(), FsStatus::INITED);
    ASSERT_EQ(volumeFsInfo1.rootinodeid(), ROOTINODEID);
    ASSERT_EQ(volumeFsInfo1.capacity(), volumeSize);
    ASSERT_EQ(volumeFsInfo1.blocksize(), kBlockSize);
    ASSERT_EQ(volumeFsInfo1.mountnum(), 0);
    ASSERT_EQ(volumeFsInfo1.fstype(), ::curvefs::common::FSType::TYPE_VOLUME);
}

TEST_F(FSManagerTest, test_success_create_s3_fs) {
    FSStatusCode ret = CreateS3Fs();

    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_EQ(s3FsInfo.fsid(), 0);
    ASSERT_EQ(s3FsInfo.fsname(), kFsName2);
    ASSERT_EQ(s3FsInfo.status(), FsStatus::INITED);
    ASSERT_EQ(s3FsInfo.rootinodeid(), ROOTINODEID);
    ASSERT_EQ(s3FsInfo.capacity(), (uint64_t)100 * 1024 * 1024 * 1024);
    ASSERT_EQ(s3FsInfo.blocksize(), kBlockSize);
    ASSERT_EQ(s3FsInfo.mountnum(), 0);
    ASSERT_EQ(s3FsInfo.fstype(), FSType::TYPE_S3);
}

TEST_F(FSManagerTest,
       test_success_monotonically_increasing_fsid_for_multiple_fs) {
    CreateVolFs();
    CreateS3Fs();

    ASSERT_EQ(volumeFsInfo1.fsid(), 0);
    ASSERT_EQ(s3FsInfo.fsid(), 1);
}

TEST_F(FSManagerTest, test_fail_create_volume_fs_on_fail_partition_creation) {
    FsInfo volumeFsInfo1;
    FSStatusCode ret;
    FsInfoWrapper wrapper;

    // create volume fs create partition fail
    EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_CREATE_PARTITION_FAIL));

    ret = fsManager_->CreateFs(&req, &volumeFsInfo1);
    ASSERT_EQ(ret, FSStatusCode::CREATE_PARTITION_ERROR);
    ASSERT_EQ(fsStorage_->Get(kFsName1, &wrapper), FSStatusCode::NOT_FOUND);
}

TEST_F(FSManagerTest, test_fail_create_volume_fs_on_failed_root_node_creation) {
    FsInfo volumeFsInfo1;
    FSStatusCode ret;
    FsInfoWrapper wrapper;
    CreateRootInodeResponse response;

    // create volume fs create root inode fail
    EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));

    std::set<std::string> addrs { addr_ };
    EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(addrs), Return(TopoStatusCode::TOPO_OK)));
    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(getLeaderResponse),
                  Invoke(RpcService<false>{})));
    response.set_statuscode(MetaStatusCode::UNKNOWN_ERROR);
    EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(RpcService<false>{})));
    EXPECT_CALL(*topoManager_, DeletePartition(_))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));
    ret = fsManager_->CreateFs(&req, &volumeFsInfo1);
    ASSERT_EQ(ret, FSStatusCode::INSERT_ROOT_INODE_ERROR);
    ASSERT_EQ(fsStorage_->Get(kFsName1, &wrapper), FSStatusCode::NOT_FOUND);
}

TEST_F(FSManagerTest, test_fail_create_s3_fs_on_failed_root_node_creation) {
    std::set<std::string> addrs { addr_ };
    CreateRootInodeResponse response;
    FSStatusCode ret;
    FsInfoWrapper wrapper;
    // create s3 fs create root inode fail
    EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));
    EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(addrs), Return(TopoStatusCode::TOPO_OK)));
    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(getLeaderResponse),
                    Invoke(RpcService<false>{})));
    response.set_statuscode(MetaStatusCode::UNKNOWN_ERROR);
    EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(RpcService<false>{})));
    EXPECT_CALL(*s3Adapter_, PutObject(_, _)).WillOnce(Return(0));
    EXPECT_CALL(*s3Adapter_, DeleteObject(_)).WillOnce(Return(0));
    EXPECT_CALL(*topoManager_, DeletePartition(_))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));
    ret = fsManager_->CreateFs(&s3Req, &s3FsInfo);
    ASSERT_EQ(ret, FSStatusCode::INSERT_ROOT_INODE_ERROR);
    ASSERT_EQ(fsStorage_->Get(kFsName2, &wrapper), FSStatusCode::NOT_FOUND);
}

TEST_F(FSManagerTest, test_fail_create_duplicate_s3_fs_with_different_fsname) {
    CreateS3Fs();
    FSStatusCode ret;
    // create s3 fs fail
    std::string fsName3 = "fs3";
    EXPECT_CALL(*s3Adapter_, PutObject(_, _)).WillOnce(Return(-1));

    s3Req.set_fsname(fsName3);
    ret = fsManager_->CreateFs(&s3Req, &s3FsInfo);
    ASSERT_EQ(ret, FSStatusCode::S3_INFO_ERROR);
}

TEST_F(FSManagerTest, test_success_get_s3_fsinfo_by_fsname) {
    CreateS3Fs();
    FSStatusCode ret;
    FsInfo fsInfo2;
    ret = fsManager_->GetFsInfo(kFsName2, &fsInfo2);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareS3Fs(fsInfo2, s3FsInfo));
}

TEST_F(FSManagerTest, test_success_get_s3_fsinfo_by_fsid) {
    CreateS3Fs();
    FSStatusCode ret;
    FsInfo fsInfo2;
    ret = fsManager_->GetFsInfo(s3FsInfo.fsid(), &fsInfo2);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareS3Fs(fsInfo2, s3FsInfo));
}

TEST_F(FSManagerTest, test_success_get_s3_fsinfo_by_consistent_fsid_fsname) {
    CreateS3Fs();
    FSStatusCode ret;
    FsInfo fsInfo2;
    ret = fsManager_->GetFsInfo(kFsName2, s3FsInfo.fsid(), &fsInfo2);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareS3Fs(fsInfo2, s3FsInfo));
}

TEST_F(FSManagerTest, test_success_get_s3_fsinfo_by_inconsistent_fsid_fsname) {
    CreateVolFs();
    CreateS3Fs();
    FSStatusCode ret;
    FsInfo fsInfo2;
    ret = fsManager_->GetFsInfo(kFsName1, s3FsInfo.fsid(), &fsInfo2);
    ASSERT_EQ(ret, FSStatusCode::PARAM_ERROR);
}

TEST_F(FSManagerTest, test_success_mount_s3_fs) {
    CreateS3Fs();
    FSStatusCode ret;
    FsInfo fsInfo4;
    ret = fsManager_->MountFs(kFsName2, s3MountPoint, &fsInfo4);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareS3Fs(s3FsInfo, fsInfo4));
    ASSERT_EQ(MessageDifferencer::Equals(fsInfo4.mountpoints(0), s3MountPoint),
              true);
}

TEST_F(FSManagerTest, test_fail_repeated_mount_s3_fs) {
    CreateS3Fs();
    FSStatusCode ret;
    FsInfo fsInfo4;
    ret = fsManager_->MountFs(kFsName2, s3MountPoint, &fsInfo4);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ret = fsManager_->MountFs(kFsName2, s3MountPoint, &fsInfo4);
    ASSERT_EQ(ret, FSStatusCode::MOUNT_POINT_CONFLICT);
}

TEST_F(FSManagerTest, test_fail_delete_s3_fs_with_existing_mount_path) {
    CreateS3Fs();
    FSStatusCode ret;
    FsInfo fsInfo4;
    ret = fsManager_->MountFs(kFsName2, s3MountPoint, &fsInfo4);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ret = fsManager_->DeleteFs(kFsName2);
    ASSERT_EQ(ret, FSStatusCode::FS_BUSY);
}

TEST_F(FSManagerTest, test_success_umount_s3_fs_with_mount_path) {
    CreateS3Fs();
    FSStatusCode ret;
    FsInfo fsInfo4;
    ret = fsManager_->MountFs(kFsName2, s3MountPoint, &fsInfo4);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ret = fsManager_->UmountFs(kFsName2, s3MountPoint);
    ASSERT_EQ(ret, FSStatusCode::OK);
}

TEST_F(FSManagerTest, test_success_delete_s3_fs_without_mount_path) {
    CreateS3Fs();
    FSStatusCode ret;
    FsInfo fsInfo4;
    ret = fsManager_->MountFs(kFsName2, s3MountPoint, &fsInfo4);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ret = fsManager_->UmountFs(kFsName2, s3MountPoint);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ret = fsManager_->DeleteFs(kFsName2);
    ASSERT_EQ(ret, FSStatusCode::OK);
}

TEST_F(FSManagerTest, test_success_create_volume_fs_when_volume_fs_exists) {
    CreateVolFs();
    FsInfo volumeFsInfo1;
    FSStatusCode ret;

    // create volume fs exist
    FsInfo volumeFsInfo2;
    ret = fsManager_->CreateFs(&req, &volumeFsInfo1);
    ASSERT_EQ(ret, FSStatusCode::OK);
}

TEST_F(FSManagerTest, test_success_get_volume_fsinfo_by_fsname) {
    CreateVolFs();

    FSStatusCode ret;

    FsInfo fsInfo1;
    ret = fsManager_->GetFsInfo(kFsName1, &fsInfo1);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareVolumeFs(fsInfo1, volumeFsInfo1));
}

TEST_F(FSManagerTest, test_success_get_volume_fsinfo_by_fsid) {
    CreateVolFs();

    FSStatusCode ret;

    FsInfo fsInfo1;
    ret = fsManager_->GetFsInfo(volumeFsInfo1.fsid(), &fsInfo1);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareVolumeFs(fsInfo1, volumeFsInfo1));
}

TEST_F(FSManagerTest,
       test_success_get_volume_fsinfo_by_consistent_fsname_fsid) {
    FSStatusCode ret;
    CreateVolFs();

    FsInfo fsInfo1;
    ret = fsManager_->GetFsInfo(kFsName1, volumeFsInfo1.fsid(), &fsInfo1);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareVolumeFs(fsInfo1, volumeFsInfo1));
}

TEST_F(FSManagerTest, test_fail_mount_volume_fs_on_space_creation_error) {
    CreateVolFs();
    FSStatusCode ret;
    FsInfo fsInfo3;

    EXPECT_CALL(*spaceManager_, AddVolume(_))
        .WillOnce(Return(space::SpaceErrCreate));
    ret = fsManager_->MountFs(kFsName1, volMountPoint, &fsInfo3);
    ASSERT_EQ(ret, FSStatusCode::INIT_SPACE_ERROR);
}

TEST_F(FSManagerTest, test_success_mount_volume_fs) {
    CreateVolFs();
    FSStatusCode ret;
    FsInfo fsInfo3;

    EXPECT_CALL(*spaceManager_, AddVolume(_))
        .WillOnce(Return(space::SpaceOk));
    ret = fsManager_->MountFs(kFsName1, volMountPoint, &fsInfo3);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareVolumeFs(volumeFsInfo1, fsInfo3));
    ASSERT_EQ(MessageDifferencer::Equals(fsInfo3.mountpoints(0),
              volMountPoint), true);
    std::pair<std::string, uint64_t> tpair;
    std::string mountpath = "host:90000:/a/b/c";
    ASSERT_TRUE(fsManager_->GetClientAliveTime(mountpath, &tpair));
    ASSERT_EQ(kFsName1, tpair.first);
}

TEST_F(FSManagerTest, test_fail_repeated_mount_volume_fs) {
    CreateVolFs();
    FSStatusCode ret;
    FsInfo fsInfo3;

    EXPECT_CALL(*spaceManager_, AddVolume(_))
        .WillOnce(Return(space::SpaceOk));
    ret = fsManager_->MountFs(kFsName1, volMountPoint, &fsInfo3);
    // mount volumefs mountpoint exist
    ret = fsManager_->MountFs(kFsName1, volMountPoint, &fsInfo3);
    ASSERT_EQ(ret, FSStatusCode::MOUNT_POINT_CONFLICT);
}

TEST_F(FSManagerTest, test_fail_umount_volume_fs_on_failed_uninit_space) {
    CreateVolFs();
    FSStatusCode ret;
    FsInfo fsInfo3;

    EXPECT_CALL(*spaceManager_, AddVolume(_))
        .WillOnce(Return(space::SpaceOk));
    ret = fsManager_->MountFs(kFsName1, volMountPoint, &fsInfo3);
    EXPECT_CALL(*spaceManager_, GetVolumeSpace(_)).WillOnce(Return(nullptr));
    ret = fsManager_->UmountFs(kFsName1, volMountPoint);
    ASSERT_EQ(ret, FSStatusCode::UNINIT_SPACE_ERROR);
}

TEST_F(FSManagerTest, test_success_umount_volume_fs_after_uninit) {
    CreateVolFs();
    FSStatusCode ret;
    FsInfo fsInfo3;
    auto volumeSpace = absl::make_unique<MockVolumeSpace>();

    std::pair<std::string, uint64_t> tpair;
    std::string mountpath = "host:90000:/a/b/c";
    // for persistence consider
    // umount UnInitSpace success
    EXPECT_CALL(*spaceManager_, GetVolumeSpace(_))
        .WillOnce(Return(volumeSpace.get()));
    EXPECT_CALL(*volumeSpace,
                ReleaseBlockGroups(Matcher<const std::string &>(_)))
        .WillOnce(Return(space::SpaceOk));
    ret = fsManager_->MountFs(kFsName1, volMountPoint, &fsInfo3);
    ret = fsManager_->UmountFs(kFsName1, volMountPoint);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_FALSE(fsManager_->GetClientAliveTime(mountpath, &tpair));
}

TEST_F(FSManagerTest, test_fail_umount_volume_fs_non_existent_mount_path) {
    CreateVolFs();
    FSStatusCode ret;
    FsInfo fsInfo3;
    auto volumeSpace = absl::make_unique<MockVolumeSpace>();

    std::pair<std::string, uint64_t> tpair;
    std::string mountpath = "host:90000:/a/b/c";

    EXPECT_CALL(*spaceManager_, GetVolumeSpace(_))
        .WillOnce(Return(volumeSpace.get()));
    EXPECT_CALL(*volumeSpace,
                ReleaseBlockGroups(Matcher<const std::string &>(_)))
        .WillOnce(Return(space::SpaceOk));
    ret = fsManager_->MountFs(kFsName1, volMountPoint, &fsInfo3);
    ret = fsManager_->UmountFs(kFsName1, volMountPoint);
    // umount not exist mountpoint
    ret = fsManager_->UmountFs(kFsName1, volMountPoint);
    ASSERT_EQ(ret, FSStatusCode::MOUNT_POINT_NOT_EXIST);
}

TEST_F(FSManagerTest,
       test_success_delete_volume_fs_after_umount_existing_mount_path) {
    CreateVolFs();
    FSStatusCode ret;
    FsInfo fsInfo3;
    auto volumeSpace = absl::make_unique<MockVolumeSpace>();

    std::pair<std::string, uint64_t> tpair;
    std::string mountpath = "host:90000:/a/b/c";

    EXPECT_CALL(*spaceManager_, GetVolumeSpace(_))
        .WillOnce(Return(volumeSpace.get()));
    EXPECT_CALL(*volumeSpace,
                ReleaseBlockGroups(Matcher<const std::string &>(_)))
        .WillOnce(Return(space::SpaceOk));
    ret = fsManager_->MountFs(kFsName1, volMountPoint, &fsInfo3);
    ret = fsManager_->UmountFs(kFsName1, volMountPoint);
    ret = fsManager_->DeleteFs(kFsName1);
    ASSERT_EQ(ret, FSStatusCode::OK);
}

TEST_F(FSManagerTest, test_success_delete_volume_fs_without_mount) {
    CreateVolFs();
    FSStatusCode ret;

    ret = fsManager_->DeleteFs(kFsName1);
    ASSERT_EQ(ret, FSStatusCode::OK);
}

TEST_F(FSManagerTest, test_fail_repeated_delete_volume_fs) {
    CreateVolFs();
    FSStatusCode ret;

    ret = fsManager_->DeleteFs(kFsName1);
    ret = fsManager_->DeleteFs(kFsName1);
    ASSERT_EQ(ret, FSStatusCode::NOT_FOUND);
}

TEST_F(FSManagerTest, test_success_restore_session_after_client_timeout) {
    CreateVolFs();

    fsManager_->Run();

    sleep(4);
    FsInfo info;
    ASSERT_EQ(FSStatusCode::OK, fsManager_->GetFsInfo(kFsName1, &info));
    ASSERT_EQ(0, info.mountpoints_size());

    RefreshSessionRequest request;
    RefreshSessionResponse response;
    request.set_fsname(kFsName1);
    *request.mutable_mountpoint() = volMountPoint;
    fsManager_->RefreshSession(&request, &response);
    ASSERT_EQ(FSStatusCode::OK, fsManager_->GetFsInfo(kFsName1, &info));
    ASSERT_EQ(1, info.mountpoints_size());
    ASSERT_EQ(MessageDifferencer::Equals(info.mountpoints(0), volMountPoint),
        true);
    fsManager_->Stop();
}

TEST_F(FSManagerTest, test_success_background_thread_lifecycle) {
    fsManager_->Run();
    fsManager_->Run();
    fsManager_->Run();
    fsManager_->Uninit();
    fsManager_->Uninit();
    fsManager_->Run();
    fsManager_->Uninit();
}

TEST_F(FSManagerTest, test_success_background_thread_delete_fs) {
    fsManager_->Run();

    CreateS3Fs();
    CreateVolFs();

    std::set<std::string> addrs { addr_ };
    FSStatusCode ret;

    // TEST GetFsInfo
    FsInfo fsInfo1;
    ret = fsManager_->GetFsInfo(kFsName1, &fsInfo1);

    FsInfo fsInfo2;
    ret = fsManager_->GetFsInfo(kFsName2, &fsInfo2);

    // TEST DeleteFs, delete fs1
    std::list<PartitionInfo> emptyList;
    std::list<PartitionInfo> oneReadableWriteabePartitionList;
    std::list<PartitionInfo> deletingList;

    PartitionInfo partition;
    uint32_t poolId1 = 1;
    uint32_t copysetId1 = 2;
    uint32_t partitionId1 = 3;
    partition.set_status(PartitionStatus::READWRITE);
    partition.set_poolid(poolId1);
    partition.set_copysetid(copysetId1);
    partition.set_partitionid(partitionId1);
    oneReadableWriteabePartitionList.push_back(partition);

    PartitionInfo deletingPartition = partition;
    deletingPartition.set_status(PartitionStatus::DELETING);
    deletingList.push_back(deletingPartition);

    EXPECT_CALL(*topoManager_, ListPartitionOfFs(fsInfo1.fsid(), _))
        .WillOnce(SetArgPointee<1>(oneReadableWriteabePartitionList))
        .WillOnce(SetArgPointee<1>(deletingList))
        .WillOnce(SetArgPointee<1>(emptyList));

    EXPECT_CALL(*topoManager_, GetCopysetMembers(poolId1, copysetId1, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(addrs), Return(TopoStatusCode::TOPO_OK)));

    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(getLeaderResponse),
                  Invoke(RpcService<false>{})));

    DeletePartitionResponse response3;
    response3.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaserverService_, DeletePartition(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response3),
            Invoke(RpcService<false>{})));

    EXPECT_CALL(*topoManager_, UpdatePartitionStatus(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));

    EXPECT_CALL(*spaceManager_, DeleteVolume(_))
        .WillOnce(Return(space::SpaceOk));
    ret = fsManager_->DeleteFs(kFsName1);
    ASSERT_EQ(ret, FSStatusCode::OK);

    sleep(4);

    // query fs deleted
    FsInfo fsInfo3;
    ret = fsManager_->GetFsInfo(fsInfo1.fsid(), &fsInfo3);
    ASSERT_EQ(ret, FSStatusCode::NOT_FOUND);

    // TEST DeleteFs, delete fs2
    uint32_t poolId2 = 4;
    uint32_t copysetId2 = 5;
    uint32_t partitionId2 = 6;
    partition.set_poolid(poolId2);
    partition.set_copysetid(copysetId2);
    partition.set_partitionid(partitionId2);
    partition.set_status(PartitionStatus::DELETING);

    deletingList.clear();
    deletingList.push_back(partition);

    EXPECT_CALL(*topoManager_, ListPartitionOfFs(fsInfo2.fsid(), _))
        .WillOnce(SetArgPointee<1>(deletingList))
        .WillOnce(SetArgPointee<1>(emptyList));

    ret = fsManager_->DeleteFs(kFsName2);
    ASSERT_EQ(ret, FSStatusCode::OK);

    sleep(3);

    ret = fsManager_->GetFsInfo(fsInfo2.fsid(), &fsInfo3);
    ASSERT_EQ(ret, FSStatusCode::NOT_FOUND);
}

TEST_F(FSManagerTest,
       test_success_refresh_session_with_outdated_partition_txid) {
    PartitionTxId tmp;
    tmp.set_partitionid(1);
    tmp.set_txid(1);
    std::string fsName = "fs1";
    Mountpoint mountpoint;
    mountpoint.set_hostname("127.0.0.1");
    mountpoint.set_port(9000);
    mountpoint.set_path("/mnt");

    LOG(INFO) << "### case1: partition txid need update ###";
    RefreshSessionRequest request;
    RefreshSessionResponse response;
    std::vector<PartitionTxId> txidlist({std::move(tmp)});
    *request.mutable_txids() = {txidlist.begin(), txidlist.end()};
    request.set_fsname(fsName);
    *request.mutable_mountpoint() = mountpoint;
    EXPECT_CALL(*topoManager_, GetLatestPartitionsTxId(_, _))
        .WillOnce(SetArgPointee<1>(txidlist));
    fsManager_->RefreshSession(&request, &response);
    ASSERT_EQ(1, response.latesttxidlist_size());
}

TEST_F(FSManagerTest,
       test_success_refresh_session_with_up_to_date_partition_txid) {
    PartitionTxId tmp;
    tmp.set_partitionid(1);
    tmp.set_txid(1);
    std::string fsName = "fs1";
    Mountpoint mountpoint;
    mountpoint.set_hostname("127.0.0.1");
    mountpoint.set_port(9000);
    mountpoint.set_path("/mnt");

    LOG(INFO) << "### case2: partition txid do not need update ###";
    RefreshSessionResponse response;
    RefreshSessionRequest request;
    request.set_fsname(fsName);
    *request.mutable_mountpoint() = mountpoint;
    fsManager_->RefreshSession(&request, &response);
    ASSERT_EQ(0, response.latesttxidlist_size());
}

TEST_F(FSManagerTest, test_fail_get_latest_txid_without_fsid) {
    GetLatestTxIdRequest request;
    GetLatestTxIdResponse response;
    fsManager_->GetLatestTxId(&request, &response);
    ASSERT_EQ(response.statuscode(), FSStatusCode::PARAM_ERROR);
}

TEST_F(FSManagerTest, test_success_get_latest_txid_with_fsid) {
    GetLatestTxIdRequest request;
    GetLatestTxIdResponse response;
    request.set_fsid(1);
    EXPECT_CALL(*topoManager_, ListPartitionOfFs(_, _))
        .WillOnce(Invoke([&](FsIdType fsId,
                                std::list<PartitionInfo>* list) {
            if (fsId != 1) {
                return;
            }
            PartitionInfo partition;
            partition.set_fsid(0);
            partition.set_poolid(0);
            partition.set_copysetid(0);
            partition.set_partitionid(0);
            partition.set_start(0);
            partition.set_end(0);
            partition.set_txid(0);
            list->push_back(partition);
        }));
    fsManager_->GetLatestTxId(&request, &response);
    ASSERT_EQ(response.statuscode(), FSStatusCode::OK);
    ASSERT_EQ(response.txids_size(), 1);
}

TEST_F(FSManagerTest, test_GetClientMdsAddrsOverride) {
    ASSERT_EQ(fsManager_->GetClientMdsAddrsOverride(), std::string());
}

TEST_F(FSManagerTest, test_SetClientMdsAddrsOverride) {
    std::string addr("127.0.0.1:9999,127.0.0.1:10000");
    fsManager_->SetClientMdsAddrsOverride(addr);
    ASSERT_EQ(fsManager_->GetClientMdsAddrsOverride(), addr + "," + addr_);
}

TEST_F(FSManagerTest, test_refresh_session_with_mdsoverride) {
    CreateS3Fs();
    // set override
    std::string mds_new("127.0.0.1:9999");
    fsManager_->SetClientMdsAddrsOverride(mds_new);

    PartitionTxId tmp;
    tmp.set_partitionid(1);
    tmp.set_txid(1);
    std::string fsName = kFsName2;
    Mountpoint mountpoint;
    mountpoint.set_hostname("127.0.0.1");
    mountpoint.set_port(9000);
    mountpoint.set_path("/mnt");

    RefreshSessionResponse response;
    RefreshSessionRequest request;
    request.set_fsname(fsName);
    *request.mutable_mountpoint() = mountpoint;
    request.set_mdsaddrs(addr_);
    fsManager_->RefreshSession(&request, &response);
    ASSERT_EQ(response.mdsaddrsoverride(), mds_new + "," + addr_);
    fsManager_->SetClientMdsAddrsOverride(std::string());
}

TEST_F(FSManagerTest, test_refresh_session_with_same_mdsoverride) {
    CreateS3Fs();
    // set override
    std::string mds_new("127.0.0.1:9999");
    fsManager_->SetClientMdsAddrsOverride(mds_new);
    PartitionTxId tmp;
    tmp.set_partitionid(1);
    tmp.set_txid(1);
    std::string fsName = kFsName2;
    Mountpoint mountpoint;
    mountpoint.set_hostname("127.0.0.1");
    mountpoint.set_port(9000);
    mountpoint.set_path("/mnt");

    RefreshSessionResponse response;
    RefreshSessionRequest request;
    request.set_fsname(fsName);
    *request.mutable_mountpoint() = mountpoint;
    request.set_mdsaddrs(mds_new + "," + addr_);
    fsManager_->RefreshSession(&request, &response);
    ASSERT_FALSE(response.has_mdsaddrsoverride());
    fsManager_->SetClientMdsAddrsOverride(std::string());
}

TEST_F(FSManagerTest, test_refresh_session_with_old_client) {
    CreateS3Fs();
    // set override
    auto mds_new = "127.0.0.1:9999";
    fsManager_->SetClientMdsAddrsOverride(mds_new);

    PartitionTxId tmp;
    tmp.set_partitionid(1);
    tmp.set_txid(1);
    std::string fsName = kFsName2;
    Mountpoint mountpoint;
    mountpoint.set_hostname("127.0.0.1");
    mountpoint.set_port(9000);
    mountpoint.set_path("/mnt");

    RefreshSessionResponse response;
    RefreshSessionRequest request;
    request.set_fsname(fsName);
    *request.mutable_mountpoint() = mountpoint;
    // old client do not have mdsaddr in request
    fsManager_->RefreshSession(&request, &response);
    ASSERT_FALSE(response.has_mdsaddrsoverride());
    fsManager_->SetClientMdsAddrsOverride(std::string());
}

}  // namespace mds
}  // namespace curvefs
