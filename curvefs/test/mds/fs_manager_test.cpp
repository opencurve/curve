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
#include "curvefs/src/mds/fs_manager.h"
#include <brpc/channel.h>
#include <brpc/server.h>
#include <gmock/gmock.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>
#include "curvefs/test/mds/mock/mock_cli2.h"
#include "curvefs/test/mds/mock/mock_fs_stroage.h"
#include "curvefs/test/mds/mock/mock_metaserver.h"
#include "curvefs/test/mds/mock/mock_topology.h"
#include "test/common/mock_s3_adapter.h"
#include "curvefs/test/mds/mock/mock_space_manager.h"

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
using ::curvefs::metaserver::MockMetaserverService;
using curvefs::metaserver::CreateRootInodeRequest;
using curvefs::metaserver::CreateRootInodeResponse;
using curvefs::metaserver::DeletePartitionRequest;
using curvefs::metaserver::DeletePartitionResponse;
using curvefs::metaserver::MetaStatusCode;
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

namespace curvefs {
namespace mds {
class FSManagerTest : public ::testing::Test {
 protected:
    void SetUp() override {
        std::string addr = "127.0.0.1:6704";

        MetaserverOptions metaserverOptions;
        metaserverOptions.metaserverAddr = addr;
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
        ASSERT_EQ(0, server_.Start(addr.c_str(), nullptr));

        return;
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
        fsManager_->Uninit();
        return;
    }

    bool CompareVolume(const Volume& first, const Volume& second) {
        return MessageDifferencer::Equals(first, second);
    }

    bool CompareVolumeFs(const FsInfo& first, const FsInfo& second) {
        return first.fsid() == second.fsid() &&
               first.fsname() == second.fsname() &&
               first.rootinodeid() == second.rootinodeid() &&
               first.capacity() == second.capacity() &&
               first.blocksize() == second.blocksize() &&
               first.fstype() == second.fstype() &&
               first.detail().has_volume() && second.detail().has_volume() &&
               CompareVolume(first.detail().volume(), second.detail().volume());
    }

    bool CompareS3Info(const S3Info& first, const S3Info& second) {
        return MessageDifferencer::Equals(first, second);
    }

    bool CompareS3Fs(const FsInfo& first, const FsInfo& second) {
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

TEST_F(FSManagerTest, test1) {
    std::string addr = "127.0.0.1:6704";
    std::string leader = "127.0.0.1:6704:0";
    FSStatusCode ret;
    std::string fsName1 = "fs1";
    uint64_t blockSize = 4096;
    bool enableSumInDir = false;
    curvefs::common::Volume volume;
    uint64_t volumeSize = 4096 * 10000;
    volume.set_volumesize(volumeSize);
    volume.set_blocksize(4096);
    volume.set_volumename("volume1");
    volume.set_user("user1");

    FsInfo volumeFsInfo1;
    FsDetail detail;
    detail.set_allocated_volume(new Volume(volume));

    CreateFsRequest req;
    req.set_fsname(fsName1);
    req.set_blocksize(blockSize);
    req.set_fstype(FSType::TYPE_VOLUME);
    req.set_allocated_fsdetail(new FsDetail(detail));
    req.set_enablesumindir(enableSumInDir);
    req.set_owner("test");
    req.set_capacity((uint64_t)100*1024*1024*1024);

    // create volume fs create partition fail
    EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_CREATE_PARTITION_FAIL));

    ret = fsManager_->CreateFs(&req, &volumeFsInfo1);
    ASSERT_EQ(ret, FSStatusCode::CREATE_PARTITION_ERROR);

    // create volume fs create root inode fail
    EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));

    std::set<std::string> addrs;
    addrs.emplace(addr);
    EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(addrs), Return(TopoStatusCode::TOPO_OK)));
    GetLeaderResponse2 getLeaderResponse;
    getLeaderResponse.mutable_leader()->set_address(leader);
    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(getLeaderResponse),
                  Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));
    CreateRootInodeResponse response;
    response.set_statuscode(MetaStatusCode::UNKNOWN_ERROR);
    EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(
                RpcService<CreateRootInodeRequest, CreateRootInodeResponse>)));
    ret = fsManager_->CreateFs(&req, &volumeFsInfo1);
    ASSERT_EQ(ret, FSStatusCode::INSERT_ROOT_INODE_ERROR);

    // create volume fs ok
    EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));
    EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(addrs), Return(TopoStatusCode::TOPO_OK)));
    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(getLeaderResponse),
                  Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));

    response.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(
                RpcService<CreateRootInodeRequest, CreateRootInodeResponse>)));

    ret = fsManager_->CreateFs(&req, &volumeFsInfo1);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_EQ(volumeFsInfo1.fsid(), 2);
    ASSERT_EQ(volumeFsInfo1.fsname(), fsName1);
    ASSERT_EQ(volumeFsInfo1.status(), FsStatus::INITED);
    ASSERT_EQ(volumeFsInfo1.rootinodeid(), ROOTINODEID);
    ASSERT_EQ(volumeFsInfo1.capacity(), volumeSize);
    ASSERT_EQ(volumeFsInfo1.blocksize(), blockSize);
    ASSERT_EQ(volumeFsInfo1.mountnum(), 0);
    ASSERT_EQ(volumeFsInfo1.fstype(), ::curvefs::common::FSType::TYPE_VOLUME);

    // create volume fs exist
    FsInfo volumeFsInfo2;
    ret = fsManager_->CreateFs(&req, &volumeFsInfo1);
    ASSERT_EQ(ret, FSStatusCode::OK);

    // create s3 test
    std::string fsName2 = "fs2";
    curvefs::common::S3Info s3Info;
    FsInfo s3FsInfo;
    s3Info.set_ak("ak");
    s3Info.set_sk("sk");
    s3Info.set_endpoint("endpoint");
    s3Info.set_bucketname("bucketname");
    s3Info.set_blocksize(4096);
    s3Info.set_chunksize(4096);
    CreateRootInodeResponse response2;
    FsDetail detail2;
    detail2.set_allocated_s3info(new S3Info(s3Info));

    // create s3 fs create root inode fail
    EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));
    EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(addrs), Return(TopoStatusCode::TOPO_OK)));
    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(getLeaderResponse),
                  Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));

    response.set_statuscode(MetaStatusCode::UNKNOWN_ERROR);
    EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(
                RpcService<CreateRootInodeRequest, CreateRootInodeResponse>)));
    EXPECT_CALL(*s3Adapter_, BucketExist()).WillOnce(Return(true));

    req.set_fsname(fsName2);
    req.set_allocated_fsdetail(new FsDetail(detail2));
    req.set_fstype(FSType::TYPE_S3);
    ret = fsManager_->CreateFs(&req, &s3FsInfo);
    ASSERT_EQ(ret, FSStatusCode::INSERT_ROOT_INODE_ERROR);

    // create s3 fs ok
    EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));
    EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(addrs), Return(TopoStatusCode::TOPO_OK)));
    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(getLeaderResponse),
                  Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));

    response.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(
                RpcService<CreateRootInodeRequest, CreateRootInodeResponse>)));
    EXPECT_CALL(*s3Adapter_, BucketExist()).WillOnce(Return(true));

    ret = fsManager_->CreateFs(&req, &s3FsInfo);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_EQ(s3FsInfo.fsid(), 4);
    ASSERT_EQ(s3FsInfo.fsname(), fsName2);
    ASSERT_EQ(s3FsInfo.status(), FsStatus::INITED);
    ASSERT_EQ(s3FsInfo.rootinodeid(), ROOTINODEID);
    ASSERT_EQ(s3FsInfo.capacity(), (uint64_t)100 * 1024 * 1024 * 1024);
    ASSERT_EQ(s3FsInfo.blocksize(), blockSize);
    ASSERT_EQ(s3FsInfo.mountnum(), 0);
    ASSERT_EQ(s3FsInfo.fstype(), FSType::TYPE_S3);

    // create s3 fs fail
    std::string fsName3 = "fs3";
    EXPECT_CALL(*s3Adapter_, BucketExist()).WillOnce(Return(false));

    req.set_fsname(fsName3);
    ret = fsManager_->CreateFs(&req, &s3FsInfo);
    ASSERT_EQ(ret, FSStatusCode::S3_INFO_ERROR);

    // TEST GetFsInfo
    FsInfo fsInfo1;
    ret = fsManager_->GetFsInfo(fsName1, &fsInfo1);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareVolumeFs(fsInfo1, volumeFsInfo1));

    ret = fsManager_->GetFsInfo(volumeFsInfo1.fsid(), &fsInfo1);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareVolumeFs(fsInfo1, volumeFsInfo1));

    ret = fsManager_->GetFsInfo(fsName1, volumeFsInfo1.fsid(), &fsInfo1);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareVolumeFs(fsInfo1, volumeFsInfo1));

    FsInfo fsInfo2;
    ret = fsManager_->GetFsInfo(fsName2, &fsInfo2);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareS3Fs(fsInfo2, s3FsInfo));

    ret = fsManager_->GetFsInfo(s3FsInfo.fsid(), &fsInfo2);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareS3Fs(fsInfo2, s3FsInfo));

    ret = fsManager_->GetFsInfo(fsName2, s3FsInfo.fsid(), &fsInfo2);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareS3Fs(fsInfo2, s3FsInfo));

    ret = fsManager_->GetFsInfo(fsName1, s3FsInfo.fsid(), &fsInfo2);
    ASSERT_EQ(ret, FSStatusCode::PARAM_ERROR);

    // TEST MountFs
    Mountpoint mountPoint;
    mountPoint.set_hostname("host");
    mountPoint.set_port(90000);
    mountPoint.set_path("/a/b/c");
    mountPoint.set_cto(false);
    FsInfo fsInfo3;

    // mount volumefs initspace fail
    EXPECT_CALL(*spaceManager_, AddVolume(_))
        .WillOnce(Return(space::SpaceErrCreate));
    ret = fsManager_->MountFs(fsName1, mountPoint, &fsInfo3);
    ASSERT_EQ(ret, FSStatusCode::INIT_SPACE_ERROR);

    // mount volumefs success
    EXPECT_CALL(*spaceManager_, AddVolume(_))
        .WillOnce(Return(space::SpaceOk));
    ret = fsManager_->MountFs(fsName1, mountPoint, &fsInfo3);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareVolumeFs(volumeFsInfo1, fsInfo3));
    ASSERT_EQ(MessageDifferencer::Equals(fsInfo3.mountpoints(0), mountPoint),
              true);
    std::pair<std::string, uint64_t> tpair;
    std::string mountpath = "host:90000:/a/b/c";
    ASSERT_TRUE(fsManager_->GetClientAliveTime(mountpath, &tpair));
    ASSERT_EQ(fsName1, tpair.first);
    // test client timeout and restore session later
    {
        fsManager_->Run();
        EXPECT_CALL(*spaceManager_, RemoveVolume(_))
            .WillOnce(Return(space::SpaceOk));
        // clientTimeoutSec in option
        sleep(4);
        FsInfo info;
        ASSERT_EQ(FSStatusCode::OK, fsManager_->GetFsInfo(fsName1, &info));
        ASSERT_EQ(0, info.mountpoints_size());

        RefreshSessionRequest request;
        RefreshSessionResponse response;
        request.set_fsname(fsName1);
        *request.mutable_mountpoint() = mountPoint;
        fsManager_->RefreshSession(&request, &response);
        ASSERT_EQ(FSStatusCode::OK, fsManager_->GetFsInfo(fsName1, &info));
        ASSERT_EQ(1, info.mountpoints_size());
        ASSERT_EQ(MessageDifferencer::Equals(info.mountpoints(0), mountPoint),
            true);
        fsManager_->Stop();
    }

    // mount volumefs mountpoint exist
    ret = fsManager_->MountFs(fsName1, mountPoint, &fsInfo3);
    ASSERT_EQ(ret, FSStatusCode::MOUNT_POINT_CONFLICT);

    // mount s3 fs success
    FsInfo fsInfo4;
    ret = fsManager_->MountFs(fsName2, mountPoint, &fsInfo4);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareS3Fs(s3FsInfo, fsInfo4));
    ASSERT_EQ(MessageDifferencer::Equals(fsInfo4.mountpoints(0), mountPoint),
              true);

    // mount s3 fs mount point exist
    ret = fsManager_->MountFs(fsName2, mountPoint, &fsInfo4);
    ASSERT_EQ(ret, FSStatusCode::MOUNT_POINT_CONFLICT);

    // TEST UmountFs
    // umount UnInitSpace fail
    EXPECT_CALL(*spaceManager_, RemoveVolume(_))
        .WillOnce(Return(space::SpaceErrNotFound));
    ret = fsManager_->UmountFs(fsName1, mountPoint);
    ASSERT_EQ(ret, FSStatusCode::UNINIT_SPACE_ERROR);

    // for persistence consider
    // umount UnInitSpace success
    EXPECT_CALL(*spaceManager_, RemoveVolume(_))
        .WillOnce(Return(space::SpaceOk));
    ret = fsManager_->UmountFs(fsName1, mountPoint);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_FALSE(fsManager_->GetClientAliveTime(mountpath, &tpair));

    // umount not exist mountpoint
    ret = fsManager_->UmountFs(fsName1, mountPoint);
    ASSERT_EQ(ret, FSStatusCode::MOUNT_POINT_NOT_EXIST);

    // TEST DeleteFs
    ret = fsManager_->DeleteFs(fsName1);
    ASSERT_EQ(ret, FSStatusCode::OK);

    ret = fsManager_->DeleteFs(fsName1);
    ASSERT_EQ(ret, FSStatusCode::NOT_FOUND);

    ret = fsManager_->DeleteFs(fsName2);
    ASSERT_EQ(ret, FSStatusCode::FS_BUSY);

    ret = fsManager_->UmountFs(fsName2, mountPoint);
    ASSERT_EQ(ret, FSStatusCode::OK);

    ret = fsManager_->DeleteFs(fsName2);
    ASSERT_EQ(ret, FSStatusCode::OK);
}

TEST_F(FSManagerTest, backgroud_thread_test) {
    fsManager_->Run();
    fsManager_->Run();
    fsManager_->Run();
    fsManager_->Uninit();
    fsManager_->Uninit();
    fsManager_->Run();
    fsManager_->Uninit();
}

TEST_F(FSManagerTest, background_thread_deletefs_test) {
    fsManager_->Run();
    std::string addr = "127.0.0.1:6704";
    std::string leader = "127.0.0.1:6704:0";
    FSStatusCode ret;
    std::string fsName1 = "fs1";
    uint64_t blockSize = 4096;
    bool enableSumInDir = false;
    curvefs::common::Volume volume;
    uint64_t volumeSize = 4096 * 10000;
    volume.set_volumesize(volumeSize);
    volume.set_blocksize(4096);
    volume.set_volumename("volume1");
    volume.set_user("user1");

    FsInfo volumeFsInfo1;
    FsDetail detail;
    detail.set_allocated_volume(new Volume(volume));

    CreateFsRequest req;
    req.set_fsname(fsName1);
    req.set_blocksize(blockSize);
    req.set_fstype(FSType::TYPE_VOLUME);
    req.set_allocated_fsdetail(new FsDetail(detail));
    req.set_enablesumindir(enableSumInDir);
    req.set_owner("test");
    req.set_capacity((uint64_t)100*1024*1024*1024);

    // create volume fs ok
    std::set<std::string> addrs;
    addrs.emplace(addr);
    EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));
    EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(addrs), Return(TopoStatusCode::TOPO_OK)));
    GetLeaderResponse2 getLeaderResponse;
    getLeaderResponse.mutable_leader()->set_address(leader);
    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(getLeaderResponse),
                  Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));

    CreateRootInodeResponse response;
    response.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(
                RpcService<CreateRootInodeRequest, CreateRootInodeResponse>)));

    req.set_fsname(fsName1);
    ret = fsManager_->CreateFs(&req, &volumeFsInfo1);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_EQ(volumeFsInfo1.fsid(), 0);
    ASSERT_EQ(volumeFsInfo1.fsname(), fsName1);
    ASSERT_EQ(volumeFsInfo1.status(), FsStatus::INITED);
    ASSERT_EQ(volumeFsInfo1.rootinodeid(), ROOTINODEID);
    ASSERT_EQ(volumeFsInfo1.capacity(), volumeSize);
    ASSERT_EQ(volumeFsInfo1.blocksize(), blockSize);
    ASSERT_EQ(volumeFsInfo1.mountnum(), 0);
    ASSERT_EQ(volumeFsInfo1.fstype(), ::curvefs::common::FSType::TYPE_VOLUME);

    // create s3 test
    std::string fsName2 = "fs2";
    curvefs::common::S3Info s3Info;
    FsInfo s3FsInfo;
    s3Info.set_ak("ak");
    s3Info.set_sk("sk");
    s3Info.set_endpoint("endpoint");
    s3Info.set_bucketname("bucketname");
    s3Info.set_blocksize(4096);
    s3Info.set_chunksize(4096);
    CreateRootInodeResponse response2;
    FsDetail detail2;
    detail2.set_allocated_s3info(new S3Info(s3Info));

    // create s3 fs ok
    EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));
    EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(addrs), Return(TopoStatusCode::TOPO_OK)));
    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(getLeaderResponse),
                  Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));

    response.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(
                RpcService<CreateRootInodeRequest, CreateRootInodeResponse>)));
    EXPECT_CALL(*s3Adapter_, BucketExist()).WillOnce(Return(true));

    req.set_fsname(fsName2);
    req.set_fstype(FSType::TYPE_S3);
    req.set_allocated_fsdetail(new FsDetail(detail2));
    ret = fsManager_->CreateFs(&req, &s3FsInfo);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_EQ(s3FsInfo.fsid(), 1);
    ASSERT_EQ(s3FsInfo.fsname(), fsName2);
    ASSERT_EQ(s3FsInfo.status(), FsStatus::INITED);
    ASSERT_EQ(s3FsInfo.rootinodeid(), ROOTINODEID);
    ASSERT_EQ(s3FsInfo.capacity(), (uint64_t)100 * 1024 * 1024 * 1024);
    ASSERT_EQ(s3FsInfo.blocksize(), blockSize);
    ASSERT_EQ(s3FsInfo.mountnum(), 0);
    ASSERT_EQ(s3FsInfo.fstype(), FSType::TYPE_S3);

    // TEST GetFsInfo
    FsInfo fsInfo1;
    ret = fsManager_->GetFsInfo(fsName1, &fsInfo1);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareVolumeFs(fsInfo1, volumeFsInfo1));

    FsInfo fsInfo2;
    ret = fsManager_->GetFsInfo(fsName2, &fsInfo2);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareS3Fs(fsInfo2, s3FsInfo));

    // TEST DeleteFs, delete fs1
    std::list<PartitionInfo> list;
    std::list<PartitionInfo> list2;
    std::list<PartitionInfo> list3;

    PartitionInfo partition;
    uint32_t poolId1 = 1;
    uint32_t copysetId1 = 2;
    uint32_t partitionId1 = 3;
    partition.set_status(PartitionStatus::READWRITE);
    partition.set_poolid(poolId1);
    partition.set_copysetid(copysetId1);
    partition.set_partitionid(partitionId1);
    list2.push_back(partition);

    PartitionInfo partition2 = partition;
    partition2.set_status(PartitionStatus::DELETING);
    list3.push_back(partition2);

    EXPECT_CALL(*topoManager_, ListPartitionOfFs(fsInfo1.fsid(), _))
        .WillOnce(SetArgPointee<1>(list2))
        .WillOnce(SetArgPointee<1>(list3))
        .WillOnce(SetArgPointee<1>(list));

    EXPECT_CALL(*topoManager_, GetCopysetMembers(poolId1, copysetId1, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(addrs), Return(TopoStatusCode::TOPO_OK)));

    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(getLeaderResponse),
                  Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));

    DeletePartitionResponse response3;
    response3.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaserverService_, DeletePartition(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response3),
            Invoke(
                RpcService<DeletePartitionRequest, DeletePartitionResponse>)));

    EXPECT_CALL(*topoManager_, UpdatePartitionStatus(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));

    EXPECT_CALL(*spaceManager_, DeleteVolume(_))
        .WillOnce(Return(space::SpaceOk));
    ret = fsManager_->DeleteFs(fsName1);
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
    std::list<PartitionInfo> list4;
    list4.push_back(partition);

    EXPECT_CALL(*topoManager_, ListPartitionOfFs(fsInfo2.fsid(), _))
        .WillOnce(SetArgPointee<1>(list4))
        .WillOnce(SetArgPointee<1>(list));

    ret = fsManager_->DeleteFs(fsName2);
    ASSERT_EQ(ret, FSStatusCode::OK);

    sleep(3);

    ret = fsManager_->GetFsInfo(fsInfo2.fsid(), &fsInfo3);
    ASSERT_EQ(ret, FSStatusCode::NOT_FOUND);
}

TEST_F(FSManagerTest, test_refreshSession) {
    PartitionTxId tmp;
    tmp.set_partitionid(1);
    tmp.set_txid(1);
    std::string fsName = "fs1";
    Mountpoint mountpoint;
    mountpoint.set_hostname("127.0.0.1");
    mountpoint.set_port(9000);
    mountpoint.set_path("/mnt");

    {
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
    {
        LOG(INFO) << "### case2: partition txid do not need update ###";
        RefreshSessionResponse response;
        RefreshSessionRequest request;
        request.set_fsname(fsName);
        *request.mutable_mountpoint() = mountpoint;
        fsManager_->RefreshSession(&request, &response);
        ASSERT_EQ(0, response.latesttxidlist_size());
    }
}

TEST_F(FSManagerTest, GetLatestTxId_ParamFsId) {
    // CASE 1: GetLatestTxId without fsid param
    {
        GetLatestTxIdRequest request;
        GetLatestTxIdResponse response;
        fsManager_->GetLatestTxId(&request, &response);
        ASSERT_EQ(response.statuscode(), FSStatusCode::PARAM_ERROR);
    }

    // CASE 2: GetLatestTxId with fsid
    {
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
}

}  // namespace mds
}  // namespace curvefs
