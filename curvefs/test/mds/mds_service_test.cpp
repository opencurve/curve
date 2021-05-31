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

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include "curvefs/src/mds/mds_service.h"
#include "curvefs/test/mds/fake_space.h"

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SaveArg;
using ::testing::Mock;
// using ::curvefs::space::MockSpaceService;
using ::curvefs::space::FakeSpaceImpl;
using ::curvefs::space::SpaceStatusCode;
using ::curvefs::space::InitSpaceResponse;

namespace curvefs {
namespace mds {
class MdsServiceTest: public ::testing::Test {
 protected:
    void SetUp() override {
        SpaceOptions options;
        options.spaceAddr = "127.0.0.1:6700";
        options.rpcTimeoutMs = 500;
        fsStorage_ = std::make_shared<MemoryFsStorage>();
        spaceClient_ = std::make_shared<SpaceClient>(options);
        fsManager_ = std::make_shared<FsManager>(fsStorage_, spaceClient_);
        spaceClient_->Init();
        return;
    }

    void TearDown() override {
        return;
    }

    bool CompareVolume(const Volume& first, const Volume& second) {
        return first.volumesize() == second.volumesize()
               && first.blocksize() == second.blocksize()
               && first.volumename() == second.volumename()
               && first.user() == second.user()
               && first.has_password() == second.has_password();
    }

    bool CompareFs(const FsInfo& first, const FsInfo& second) {
        return first.fsid() == second.fsid()
                && first.fsname() == second.fsname()
                && first.rootinodeid() == second.rootinodeid()
                && first.capacity() == second.capacity()
                && first.blocksize() == second.blocksize()
                && CompareVolume(first.volume(), second.volume());
    }

    bool CompareMountPoint(const MountPoint& first, const MountPoint& second) {
        return first.host() == second.host()
                && first.mountdir() == second.mountdir();
    }

    std::shared_ptr<FsManager> fsManager_;
    std::shared_ptr<FsStorage> fsStorage_;
    std::shared_ptr<SpaceClient> spaceClient_;
};

TEST_F(MdsServiceTest, test1) {
    brpc::Server server;
    // add metaserver service
    MdsServiceImpl mdsService(fsManager_);
    ASSERT_EQ(server.AddService(&mdsService,
                          brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    // add mocke space service
    // MockSpaceService spaceService;
    FakeSpaceImpl spaceService;
    ASSERT_EQ(server.AddService(&spaceService,
                          brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    // start rpc server
    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    std::string addr = "127.0.0.1:6700";
    ASSERT_EQ(server.Start(addr.c_str(), &option), 0);


    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(server.listen_address(), nullptr), 0);

    MdsService_Stub stub(&channel);
    brpc::Controller cntl;

    // test CreateFS
    CreateFsRequest createRequest;
    CreateFsResponse createResponse;

    Volume volume;
    volume.set_volumesize(4096 * 4096);
    volume.set_blocksize(4096);
    volume.set_volumename("volume1");
    volume.set_user("user1");

    createRequest.set_fsname("fs1");
    createRequest.set_blocksize(4096);
    createRequest.mutable_volume()->CopyFrom(volume);

    FsInfo fsinfo1;
    stub.CreateFs(&cntl, &createRequest, &createResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(createResponse.has_fsinfo());
        fsinfo1 = createResponse.fsinfo();
        ASSERT_EQ(fsinfo1.fsid(), 1);
        ASSERT_EQ(fsinfo1.fsname(), "fs1");
        ASSERT_EQ(fsinfo1.rootinodeid(), 0);
        ASSERT_EQ(fsinfo1.capacity(), 4096 * 4096);
        ASSERT_EQ(fsinfo1.blocksize(), 4096);
        ASSERT_EQ(fsinfo1.mountnum(), 0);
        ASSERT_EQ(fsinfo1.mountpoints_size(), 0);
        ASSERT_TRUE(CompareVolume(volume, fsinfo1.volume()));
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    stub.CreateFs(&cntl, &createRequest, &createResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), FSStatusCode::FS_EXIST);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    FsInfo fsinfo2;
    createRequest.set_fsname("fs2");
    stub.CreateFs(&cntl, &createRequest, &createResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(createResponse.has_fsinfo());
        fsinfo2 = createResponse.fsinfo();
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // test MountFs
    cntl.Reset();
    MountPoint mountPoint;
    mountPoint.set_host("host1");
    mountPoint.set_mountdir("/a/b/c");
    MountFsRequest mountRequest;
    MountFsResponse mountResponse;
    mountRequest.set_fsname("fs1");
    mountRequest.mutable_mountpoint()->CopyFrom(mountPoint);
    stub.MountFs(&cntl, &mountRequest, &mountResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(mountResponse.statuscode(), FSStatusCode::OK);
        ASSERT_TRUE(mountResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(mountResponse.fsinfo(), fsinfo1));
        ASSERT_EQ(mountResponse.fsinfo().mountnum(), 1);
        ASSERT_EQ(mountResponse.fsinfo().mountpoints_size(), 1);
        ASSERT_TRUE(CompareMountPoint(mountResponse.fsinfo().mountpoints(0),
                                        mountPoint));
        ASSERT_EQ(spaceService.initCount, 1);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    stub.MountFs(&cntl, &mountRequest, &mountResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(mountResponse.statuscode(), FSStatusCode::MOUNT_POINT_EXIST);
        ASSERT_EQ(spaceService.initCount, 1);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    MountPoint mountPoint2;
    mountPoint2.set_host("host1");
    mountPoint2.set_mountdir("/a/b/d");
    mountRequest.mutable_mountpoint()->CopyFrom(mountPoint2);
    stub.MountFs(&cntl, &mountRequest, &mountResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(mountResponse.statuscode(), FSStatusCode::OK);
        ASSERT_EQ(spaceService.initCount, 1);
        ASSERT_TRUE(mountResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(mountResponse.fsinfo(), fsinfo1));
        ASSERT_EQ(mountResponse.fsinfo().mountnum(), 2);
        ASSERT_EQ(mountResponse.fsinfo().mountpoints_size(), 2);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    MountPoint mountPoint3;
    mountPoint3.set_host("host2");
    mountPoint3.set_mountdir("/a/b/d");
    mountRequest.mutable_mountpoint()->CopyFrom(mountPoint3);
    stub.MountFs(&cntl, &mountRequest, &mountResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(mountResponse.statuscode(), FSStatusCode::OK);
        ASSERT_EQ(spaceService.initCount, 1);
        ASSERT_TRUE(mountResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(mountResponse.fsinfo(), fsinfo1));
        ASSERT_EQ(mountResponse.fsinfo().mountnum(), 3);
        ASSERT_EQ(mountResponse.fsinfo().mountpoints_size(), 3);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    mountPoint.set_host("host2");
    mountRequest.mutable_mountpoint()->CopyFrom(mountPoint);
    stub.MountFs(&cntl, &mountRequest, &mountResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(mountResponse.statuscode(), FSStatusCode::OK);
        ASSERT_EQ(spaceService.initCount, 1);
        ASSERT_TRUE(mountResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(mountResponse.fsinfo(), fsinfo1));
        ASSERT_EQ(mountResponse.fsinfo().mountnum(), 4);
        ASSERT_EQ(mountResponse.fsinfo().mountpoints_size(), 4);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // TEST GetFsInfo
    // fsid1
    cntl.Reset();
    GetFsInfoRequest getRequest;
    GetFsInfoResponse getResponse;
    getRequest.set_fsid(fsinfo1.fsid());
    stub.GetFsInfo(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), FSStatusCode::OK);
        ASSERT_EQ(spaceService.initCount, 1);
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
        ASSERT_EQ(spaceService.initCount, 1);
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
        ASSERT_EQ(spaceService.initCount, 1);
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
        ASSERT_EQ(spaceService.initCount, 1);
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
        ASSERT_EQ(spaceService.initCount, 1);
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
    mountPoint.set_host("host1");
    mountPoint.set_mountdir("/a/b/c");
    umountRequest.mutable_mountpoint()->CopyFrom(mountPoint);
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
        ASSERT_EQ(umountResponse.statuscode(), FSStatusCode::NOT_FOUND);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    mountPoint.set_host("host2");
    mountPoint.set_mountdir("/a/b/c");
    umountRequest.mutable_mountpoint()->CopyFrom(mountPoint);
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
        ASSERT_EQ(spaceService.initCount, 1);
        ASSERT_TRUE(getResponse.has_fsinfo());
        ASSERT_TRUE(CompareFs(getResponse.fsinfo(), fsinfo1));
        ASSERT_EQ(getResponse.fsinfo().mountnum(), 2);
        ASSERT_EQ(getResponse.fsinfo().mountpoints_size(), 2);
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
    mountPoint.set_host("host1");
    mountPoint.set_mountdir("/a/b/d");
    umountRequest.mutable_mountpoint()->CopyFrom(mountPoint);
    stub.UmountFs(&cntl, &umountRequest, &umountResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(umountResponse.statuscode(), FSStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    mountPoint.set_host("host2");
    mountPoint.set_mountdir("/a/b/d");
    umountRequest.mutable_mountpoint()->CopyFrom(mountPoint);
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
