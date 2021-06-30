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
#include <gtest/gtest.h>
#include "curvefs/test/mds/mock_metaserver.h"
#include "curvefs/test/mds/mock_space.h"

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
using ::curvefs::space::MockSpaceService;
using ::curvefs::metaserver::MockMetaserverService;
using curvefs::metaserver::CreateRootInodeRequest;
using curvefs::metaserver::CreateRootInodeResponse;
using curvefs::space::InitSpaceRequest;
using curvefs::space::InitSpaceResponse;
using curvefs::space::UnInitSpaceRequest;
using curvefs::space::UnInitSpaceResponse;
using curvefs::metaserver::MetaStatusCode;
using curvefs::space::SpaceStatusCode;

namespace curvefs {
namespace mds {
class FSManagerTest : public ::testing::Test {
 protected:
    void SetUp() override {
        std::string addr = "127.0.0.1:6704";
        SpaceOptions spaceOptions;
        spaceOptions.spaceAddr = addr;
        spaceOptions.rpcTimeoutMs = 500;
        MetaserverOptions metaserverOptions;
        metaserverOptions.metaserverAddr = addr;
        metaserverOptions.rpcTimeoutMs = 500;
        fsStorage_ = std::make_shared<MemoryFsStorage>();
        spaceClient_ = std::make_shared<SpaceClient>(spaceOptions);
        metaserverClient_ =
            std::make_shared<MetaserverClient>(metaserverOptions);
        fsManager_ = std::make_shared<FsManager>(fsStorage_, spaceClient_,
                                                 metaserverClient_);
        ASSERT_TRUE(fsManager_->Init());

        ASSERT_EQ(0, server_.AddService(&mockSpaceService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.AddService(&mockMetaserverService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(addr.c_str(), nullptr));

        return;
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
        return;
    }

    bool CompareVolume(const Volume& first, const Volume& second) {
        return first.volumesize() == second.volumesize() &&
               first.blocksize() == second.blocksize() &&
               first.volumename() == second.volumename() &&
               first.user() == second.user() &&
               first.has_password() == second.has_password();
    }

    bool CompareVolumeFs(const FsInfo& first, const FsInfo& second) {
        return first.fsid() == second.fsid() &&
               first.fsname() == second.fsname() &&
               first.rootinodeid() == second.rootinodeid() &&
               first.capacity() == second.capacity() &&
               first.blocksize() == second.blocksize() &&
               first.fstype() == second.fstype() &&
               CompareVolume(first.volume(), second.volume());
    }

    bool CompareS3Info(const S3Info& first, const S3Info& second) {
        return first.ak() == second.ak() && first.sk() == second.sk() &&
               first.endpoint() == second.endpoint() &&
               first.bucketname() == second.bucketname() &&
               first.blocksize() == second.blocksize() &&
               first.chunksize() == second.chunksize();
    }

    bool CompareS3Fs(const FsInfo& first, const FsInfo& second) {
        return first.fsid() == second.fsid() &&
               first.fsname() == second.fsname() &&
               first.rootinodeid() == second.rootinodeid() &&
               first.capacity() == second.capacity() &&
               first.blocksize() == second.blocksize() &&
               first.fstype() == second.fstype() &&
               CompareS3Info(first.s3info(), second.s3info());
    }

 protected:
    std::shared_ptr<FsManager> fsManager_;
    std::shared_ptr<FsStorage> fsStorage_;
    std::shared_ptr<SpaceClient> spaceClient_;
    std::shared_ptr<MetaserverClient> metaserverClient_;
    MockSpaceService mockSpaceService_;
    MockMetaserverService mockMetaserverService_;
    brpc::Server server_;
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
    FSStatusCode ret;
    std::string fsName1 = "fs1";
    uint64_t blockSize = 4096;
    curvefs::common::Volume volume;
    uint64_t volumeSize = 4096 * 10000;
    volume.set_volumesize(volumeSize);
    volume.set_blocksize(4096);
    volume.set_volumename("volume1");
    volume.set_user("user1");

    FsInfo volumeFsInfo1;
    CreateRootInodeResponse response;

    // create volume fs create root inode fail
    response.set_statuscode(MetaStatusCode::UNKNOWN_ERROR);
    EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(
                RpcService<CreateRootInodeRequest, CreateRootInodeResponse>)));
    ret = fsManager_->CreateFs(fsName1, blockSize, volume, &volumeFsInfo1);
    ASSERT_EQ(ret, FSStatusCode::INSERT_ROOT_INODE_ERROR);

    // create volume fs ok
    response.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(
                RpcService<CreateRootInodeRequest, CreateRootInodeResponse>)));

    ret = fsManager_->CreateFs(fsName1, blockSize, volume, &volumeFsInfo1);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_EQ(volumeFsInfo1.fsid(), 2);
    ASSERT_EQ(volumeFsInfo1.fsname(), fsName1);
    ASSERT_EQ(volumeFsInfo1.status(), FsStatus::INITED);
    ASSERT_EQ(volumeFsInfo1.rootinodeid(), ROOTINODEID);
    ASSERT_EQ(volumeFsInfo1.capacity(), volumeSize);
    ASSERT_EQ(volumeFsInfo1.blocksize(), blockSize);
    ASSERT_EQ(volumeFsInfo1.mountnum(), 0);
    ASSERT_EQ(volumeFsInfo1.fstype(), FSType::TYPE_VOLUME);

    // create volume fs exist
    FsInfo volumeFsInfo2;
    ret = fsManager_->CreateFs(fsName1, blockSize, volume, &volumeFsInfo2);
    ASSERT_EQ(ret, FSStatusCode::FS_EXIST);

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
    uint64_t fsSize = std::numeric_limits<uint64_t>::max();
    CreateRootInodeResponse response2;

    // create s3 fs create root inode fail
    response.set_statuscode(MetaStatusCode::UNKNOWN_ERROR);
    EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(
                RpcService<CreateRootInodeRequest, CreateRootInodeResponse>)));
    ret = fsManager_->CreateFs(fsName2, blockSize, s3Info, &s3FsInfo);
    ASSERT_EQ(ret, FSStatusCode::INSERT_ROOT_INODE_ERROR);

    // create s3 fs ok
    response.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(
                RpcService<CreateRootInodeRequest, CreateRootInodeResponse>)));

    ret = fsManager_->CreateFs(fsName2, blockSize, s3Info, &s3FsInfo);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_EQ(s3FsInfo.fsid(), 4);
    ASSERT_EQ(s3FsInfo.fsname(), fsName2);
    ASSERT_EQ(s3FsInfo.status(), FsStatus::INITED);
    ASSERT_EQ(s3FsInfo.rootinodeid(), ROOTINODEID);
    ASSERT_EQ(s3FsInfo.capacity(), fsSize);
    ASSERT_EQ(s3FsInfo.blocksize(), blockSize);
    ASSERT_EQ(s3FsInfo.mountnum(), 0);
    ASSERT_EQ(s3FsInfo.fstype(), FSType::TYPE_S3);

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
    std::string mountPoint = "host:/a/b/c";
    FsInfo fsInfo3;

    // mount volumefs initspace fail
    InitSpaceResponse initSpaceResponse;
    initSpaceResponse.set_status(SpaceStatusCode::SPACE_UNKNOWN_ERROR);
    EXPECT_CALL(mockSpaceService_, InitSpace(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(initSpaceResponse),
                  Invoke(RpcService<InitSpaceRequest, InitSpaceResponse>)));
    ret = fsManager_->MountFs(fsName1, mountPoint, &fsInfo3);
    ASSERT_EQ(ret, FSStatusCode::INIT_SPACE_ERROR);

    // mount volumefs success
    initSpaceResponse.set_status(SpaceStatusCode::SPACE_OK);
    EXPECT_CALL(mockSpaceService_, InitSpace(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(initSpaceResponse),
                  Invoke(RpcService<InitSpaceRequest, InitSpaceResponse>)));
    ret = fsManager_->MountFs(fsName1, mountPoint, &fsInfo3);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareS3Fs(volumeFsInfo1, fsInfo3));
    ASSERT_EQ(fsInfo3.mountpoints(0), mountPoint);

    // mount volumefs mountpoint exist
    ret = fsManager_->MountFs(fsName1, mountPoint, &fsInfo3);
    ASSERT_EQ(ret, FSStatusCode::MOUNT_POINT_EXIST);

    // mount s3 fs initspace fail
    FsInfo fsInfo4;
    initSpaceResponse.set_status(SpaceStatusCode::SPACE_UNKNOWN_ERROR);
    EXPECT_CALL(mockSpaceService_, InitSpace(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(initSpaceResponse),
                  Invoke(RpcService<InitSpaceRequest, InitSpaceResponse>)));
    ret = fsManager_->MountFs(fsName2, mountPoint, &fsInfo4);
    ASSERT_EQ(ret, FSStatusCode::INIT_SPACE_ERROR);

    // mount s3 fs success
    initSpaceResponse.set_status(SpaceStatusCode::SPACE_OK);
    EXPECT_CALL(mockSpaceService_, InitSpace(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(initSpaceResponse),
                  Invoke(RpcService<InitSpaceRequest, InitSpaceResponse>)));
    ret = fsManager_->MountFs(fsName2, mountPoint, &fsInfo4);
    ASSERT_EQ(ret, FSStatusCode::OK);
    ASSERT_TRUE(CompareS3Fs(s3FsInfo, fsInfo4));
    ASSERT_EQ(fsInfo4.mountpoints(0), mountPoint);

    // mount s3 fs mount point exist
    ret = fsManager_->MountFs(fsName2, mountPoint, &fsInfo4);
    ASSERT_EQ(ret, FSStatusCode::MOUNT_POINT_EXIST);

    // TEST UmountFs
    // umount UnInitSpace fail
    UnInitSpaceResponse uninitSpaceResponse;
    uninitSpaceResponse.set_status(SpaceStatusCode::SPACE_UNKNOWN_ERROR);
    EXPECT_CALL(mockSpaceService_, UnInitSpace(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(uninitSpaceResponse),
                  Invoke(RpcService<UnInitSpaceRequest, UnInitSpaceResponse>)));
    ret = fsManager_->UmountFs(fsName1, mountPoint);
    ASSERT_EQ(ret, FSStatusCode::UNINIT_SPACE_ERROR);

    // for persistence consider
    // // umount UnInitSpace success
    // uninitSpaceResponse.set_status(SpaceStatusCode::SPACE_OK);
    // EXPECT_CALL(mockSpaceService_, UnInitSpace(_, _, _, _))
    //     .WillOnce(
    //         DoAll(SetArgPointee<2>(uninitSpaceResponse),
    //               Invoke(RpcService<UnInitSpaceRequest,
    //               UnInitSpaceResponse>)));
    // ret = fsManager_->UmountFs(fsName1, mountPoint);
    // ASSERT_EQ(ret, FSStatusCode::OK);

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

    uninitSpaceResponse.set_status(SpaceStatusCode::SPACE_OK);
    EXPECT_CALL(mockSpaceService_, UnInitSpace(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(uninitSpaceResponse),
                  Invoke(RpcService<UnInitSpaceRequest, UnInitSpaceResponse>)));
    ret = fsManager_->UmountFs(fsName2, mountPoint);
    ASSERT_EQ(ret, FSStatusCode::OK);

    ret = fsManager_->DeleteFs(fsName2);
    ASSERT_EQ(ret, FSStatusCode::OK);
}
}  // namespace mds
}  // namespace curvefs
