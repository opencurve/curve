/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "curvefs/src/client/fuse_volume_client.h"
#include "curvefs/src/client/fuse_s3_client.h"
#include "curvefs/test/client/mock_mds_client.h"
#include "curvefs/test/client/mock_metaserver_client.h"
#include "curvefs/test/client/mock_block_device_client.h"
#include "curvefs/test/client/mock_extent_manager.h"
#include "curvefs/test/client/mock_space_client.h"
#include "curvefs/test/client/mock_inode_cache_manager.h"
#include "curvefs/test/client/mock_dentry_cache_mamager.h"
#include "curvefs/test/client/mock_client_s3_adaptor.h"

struct fuse_req {
    struct fuse_ctx *ctx;
};

namespace curvefs {
namespace client {

using ::testing::Return;
using ::testing::_;
using ::testing::Contains;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;
using ::curve::common::Configuration;

using rpcclient::MockMetaServerClient;
using rpcclient::MockMdsClient;

class TestFuseVolumeClient : public ::testing::Test {
 protected:
    TestFuseVolumeClient() {}
    ~TestFuseVolumeClient() {}

    virtual void SetUp() {
        mdsClient_ = std::make_shared<MockMdsClient>();
        metaClient_ = std::make_shared<MockMetaServerClient>();
        spaceClient_ = std::make_shared<MockSpaceClient>();
        blockDeviceClient_ = std::make_shared<MockBlockDeviceClient>();
        inodeManager_ = std::make_shared<MockInodeCacheManager>();
        dentryManager_ = std::make_shared<MockDentryCacheManager>();
        extManager_ = std::make_shared<MockExtentManager>();
        preAllocSize_ = 65536;
        bigFileSize_ = 1048576;
        fuseClientOption_.extentManagerOpt.preAllocSize = preAllocSize_;
        fuseClientOption_.bigFileSize = bigFileSize_;
        client_  = std::make_shared<FuseVolumeClient>(mdsClient_,
            metaClient_,
            spaceClient_,
            inodeManager_,
            dentryManager_,
            extManager_,
            blockDeviceClient_);
        client_->Init(fuseClientOption_);
        PrepareFsInfo();
    }

    virtual void TearDown() {
        mdsClient_ = nullptr;
        metaClient_ = nullptr;
        spaceClient_ = nullptr;
        blockDeviceClient_ = nullptr;
        extManager_ = nullptr;
    }

    void PrepareFsInfo() {
        auto fsInfo = std::make_shared<FsInfo>();
        fsInfo->set_fsid(fsId);
        fsInfo->set_fsname("xxx");

        client_->SetFsInfo(fsInfo);
    }

 protected:
    const uint32_t fsId = 100u;

    std::shared_ptr<MockMdsClient> mdsClient_;
    std::shared_ptr<MockMetaServerClient> metaClient_;
    std::shared_ptr<MockSpaceClient> spaceClient_;
    std::shared_ptr<MockBlockDeviceClient> blockDeviceClient_;
    std::shared_ptr<MockInodeCacheManager> inodeManager_;
    std::shared_ptr<MockDentryCacheManager> dentryManager_;
    std::shared_ptr<MockExtentManager> extManager_;
    std::shared_ptr<FuseVolumeClient> client_;

    uint64_t preAllocSize_;
    uint64_t bigFileSize_;
    FuseClientOption fuseClientOption_;
};

TEST_F(TestFuseVolumeClient, FuseOpInit_when_fs_exist) {
    MountOption mOpts;
    memset(&mOpts, 0, sizeof(mOpts));
    mOpts.mountPoint = "host1:/test";
    mOpts.volume = "xxx";
    mOpts.user = "test";
    mOpts.fsType = "curve";

    std::string volName = mOpts.volume;
    std::string user = mOpts.user;
    std::string fsName = mOpts.volume;

    EXPECT_CALL(*mdsClient_, GetFsInfo(fsName, _))
        .WillOnce(Return(FSStatusCode::OK));

    FsInfo fsInfoExp;
    fsInfoExp.set_fsid(200);
    fsInfoExp.set_fsname(fsName);
    EXPECT_CALL(*mdsClient_, MountFs(fsName, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(fsInfoExp),
                Return(FSStatusCode::OK)));

    EXPECT_CALL(*blockDeviceClient_, Open(volName, user))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    client_->FuseOpInit(&mOpts, nullptr);

    auto fsInfo = client_->GetFsInfo();
    ASSERT_NE(fsInfo, nullptr);

    ASSERT_EQ(fsInfo->fsid(), fsInfoExp.fsid());
    ASSERT_EQ(fsInfo->fsname(), fsInfoExp.fsname());
}

TEST_F(TestFuseVolumeClient, FuseOpInit_when_fs_not_exist) {
    MountOption mOpts;
    memset(&mOpts, 0, sizeof(mOpts));
    mOpts.mountPoint = "host1:/test";
    mOpts.volume = "xxx";
    mOpts.user = "test";
    mOpts.fsType = "curve";

    std::string volName = mOpts.volume;
    std::string user = mOpts.user;
    std::string fsName = mOpts.volume;

    EXPECT_CALL(*mdsClient_, GetFsInfo(fsName, _))
        .WillOnce(Return(FSStatusCode::NOT_FOUND));

    EXPECT_CALL(*blockDeviceClient_, Stat(volName, user, _))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    EXPECT_CALL(*mdsClient_, CreateFs(_, _, _))
        .WillOnce(Return(FSStatusCode::OK));

    FsInfo fsInfoExp;
    fsInfoExp.set_fsid(100);
    fsInfoExp.set_fsname(fsName);
    EXPECT_CALL(*mdsClient_, MountFs(fsName, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(fsInfoExp),
                Return(FSStatusCode::OK)));

    EXPECT_CALL(*blockDeviceClient_, Open(volName, user))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    client_->FuseOpInit(&mOpts, nullptr);

    auto fsInfo = client_->GetFsInfo();
    ASSERT_NE(fsInfo, nullptr);

    ASSERT_EQ(fsInfo->fsid(), fsInfoExp.fsid());
    ASSERT_EQ(fsInfo->fsname(), fsInfoExp.fsname());
}

TEST_F(TestFuseVolumeClient, FuseOpDestroy) {
    MountOption mOpts;
    memset(&mOpts, 0, sizeof(mOpts));
    mOpts.mountPoint = "host1:/test";
    mOpts.volume = "xxx";
    mOpts.fsType = "curve";

    std::string fsName = mOpts.volume;

    EXPECT_CALL(*mdsClient_, UmountFs(fsName, _))
        .WillOnce(Return(FSStatusCode::OK));

    EXPECT_CALL(*blockDeviceClient_, Close())
        .WillOnce(Return(CURVEFS_ERROR::OK));

    client_->FuseOpDestroy(&mOpts);
}

TEST_F(TestFuseVolumeClient, FuseOpLookup) {
    fuse_req_t req;
    fuse_ino_t parent = 1;
    std::string name = "test";

    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);

    EXPECT_CALL(*dentryManager_, GetDentry(parent, name, _))
        .WillOnce(DoAll(SetArgPointee<2>(dentry),
                Return(CURVEFS_ERROR::OK)));

    Inode inode;
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(inodeid, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpLookup(req, parent, name.c_str(), &e);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpLookupFail) {
    fuse_req_t req;
    fuse_ino_t parent = 1;
    std::string name = "test";

    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);

    EXPECT_CALL(*dentryManager_, GetDentry(parent, name, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgPointee<2>(dentry),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*inodeManager_, GetInode(inodeid, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpLookup(req, parent, name.c_str(), &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpLookup(req, parent, name.c_str(), &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}


TEST_F(TestFuseVolumeClient, FuseOpWrite) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    const char *buf = "xxx";
    size_t size = 4;
    off_t off = 0;
    struct fuse_file_info fi;
    fi.flags = O_WRONLY;
    size_t wSize = 0;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);

    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    std::list<ExtentAllocInfo> toAllocExtents;
    ExtentAllocInfo allocInfo;
    allocInfo.lOffset = 0;
    allocInfo.pOffsetLeft = 0;
    allocInfo.len = preAllocSize_;
    toAllocExtents.push_back(allocInfo);
    EXPECT_CALL(*extManager_, GetToAllocExtents(_, off, size, _))
        .WillOnce(DoAll(SetArgPointee<3>(toAllocExtents),
                Return(CURVEFS_ERROR::OK)));

    std::list<Extent> allocatedExtents;
    Extent ext;
    ext.set_offset(0);
    ext.set_length(preAllocSize_);
    EXPECT_CALL(*spaceClient_, AllocExtents(fsId, _, AllocateType::SMALL, _))
        .WillOnce(DoAll(SetArgPointee<3>(allocatedExtents),
            Return(CURVEFS_ERROR::OK)));

    VolumeExtentList *vlist = new VolumeExtentList();
    VolumeExtent *vext = vlist->add_volumeextents();
    vext->set_fsoffset(0);
    vext->set_volumeoffset(0);
    vext->set_length(preAllocSize_);
    vext->set_isused(false);
    inode.set_allocated_volumeextentlist(vlist);

    EXPECT_CALL(*extManager_, MergeAllocedExtents(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(*vlist),
            Return(CURVEFS_ERROR::OK)));

    std::list<PExtent> pExtents;
    PExtent pext;
    pext.pOffset = 0;
    pext.len = preAllocSize_;
    pExtents.push_back(pext);
    EXPECT_CALL(*extManager_, DivideExtents(_, off, size, _))
        .WillOnce(DoAll(SetArgPointee<3>(pExtents),
            Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*blockDeviceClient_, Write(_, 0, preAllocSize_))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    EXPECT_CALL(*extManager_, MarkExtentsWritten(off, size, _))
        .WillOnce(Return(CURVEFS_ERROR::OK));


    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::OK));

    CURVEFS_ERROR ret = client_->FuseOpWrite(
        req, ino, buf, size, off, &fi, &wSize);

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(size, wSize);
}

TEST_F(TestFuseVolumeClient, FuseOpWriteFailed) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    const char *buf = "xxx";
    size_t size = 4;
    off_t off = 0;
    struct fuse_file_info fi;
    fi.flags = O_WRONLY;
    size_t wSize = 0;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    std::list<ExtentAllocInfo> toAllocExtents;
    ExtentAllocInfo allocInfo;
    allocInfo.lOffset = 0;
    allocInfo.pOffsetLeft = 0;
    allocInfo.len = preAllocSize_;
    toAllocExtents.push_back(allocInfo);
    EXPECT_CALL(*extManager_, GetToAllocExtents(_, off, size, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgPointee<3>(toAllocExtents),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<3>(toAllocExtents),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<3>(toAllocExtents),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<3>(toAllocExtents),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<3>(toAllocExtents),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<3>(toAllocExtents),
                Return(CURVEFS_ERROR::OK)));

    std::list<Extent> allocatedExtents;
    Extent ext;
    ext.set_offset(0);
    ext.set_length(preAllocSize_);
    EXPECT_CALL(*spaceClient_, AllocExtents(fsId, _, _, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgPointee<3>(allocatedExtents),
            Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<3>(allocatedExtents),
            Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<3>(allocatedExtents),
            Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<3>(allocatedExtents),
            Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<3>(allocatedExtents),
            Return(CURVEFS_ERROR::OK)));

    VolumeExtentList *vlist = new VolumeExtentList();
    VolumeExtent *vext = vlist->add_volumeextents();
    vext->set_fsoffset(0);
    vext->set_volumeoffset(0);
    vext->set_length(preAllocSize_);
    vext->set_isused(false);
    inode.set_allocated_volumeextentlist(vlist);

    EXPECT_CALL(*extManager_, MergeAllocedExtents(_, _, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgPointee<2>(*vlist),
            Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(*vlist),
            Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(*vlist),
            Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(*vlist),
            Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*spaceClient_, DeAllocExtents(_, _))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    std::list<PExtent> pExtents;
    PExtent pext;
    pext.pOffset = 0;
    pext.len = preAllocSize_;
    pExtents.push_back(pext);
    EXPECT_CALL(*extManager_, DivideExtents(_, off, size, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgPointee<3>(pExtents),
            Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<3>(pExtents),
            Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<3>(pExtents),
            Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*blockDeviceClient_, Write(_, 0, preAllocSize_))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(Return(CURVEFS_ERROR::OK))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    EXPECT_CALL(*extManager_, MarkExtentsWritten(off, size, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(Return(CURVEFS_ERROR::OK));


    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));

    CURVEFS_ERROR ret = client_->FuseOpWrite(
        req, ino, buf, size, off, &fi, &wSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpWrite(req, ino, buf, size, off, &fi, &wSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpWrite(req, ino, buf, size, off, &fi, &wSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpWrite(req, ino, buf, size, off, &fi, &wSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpWrite(req, ino, buf, size, off, &fi, &wSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpWrite(req, ino, buf, size, off, &fi, &wSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpWrite(req, ino, buf, size, off, &fi, &wSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpWrite(req, ino, buf, size, off, &fi, &wSize);
    ASSERT_EQ(CURVEFS_ERROR::UNKNOWN, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpRead) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    size_t size = 4;
    off_t off = 0;
    struct fuse_file_info fi;
    fi.flags = O_RDONLY;
    std::unique_ptr<char[]> buffer(new char[size]);
    size_t rSize = 0;

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(4096);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    std::list<PExtent> pExtents;
    PExtent pext1, pext2;
    pext1.pOffset = 0;
    pext1.len = 4;
    pext1.UnWritten = false;
    pext2.pOffset = 4;
    pext2.len = 4096;
    pext2.UnWritten = true;
    pExtents.push_back(pext1);
    pExtents.push_back(pext2);

    EXPECT_CALL(*extManager_, DivideExtents(_, off, size, _))
        .WillOnce(DoAll(SetArgPointee<3>(pExtents),
            Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*blockDeviceClient_, Read(_, 0, 4))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    CURVEFS_ERROR ret = client_->FuseOpRead(req, ino, size, off, &fi,
        buffer.get(), &rSize);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(size, rSize);
}

TEST_F(TestFuseVolumeClient, FuseOpReadFailed) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    size_t size = 4;
    off_t off = 0;
    struct fuse_file_info fi;
    fi.flags = O_RDONLY;
    std::unique_ptr<char[]> buffer(new char[size]);
    size_t rSize = 0;

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(4096);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    std::list<PExtent> pExtents;
    PExtent pext1, pext2;
    pext1.pOffset = 0;
    pext1.len = 4;
    pext1.UnWritten = false;
    pext2.pOffset = 4;
    pext2.len = 4096;
    pext2.UnWritten = true;
    pExtents.push_back(pext1);
    pExtents.push_back(pext2);

    EXPECT_CALL(*extManager_, DivideExtents(_, off, size, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgPointee<3>(pExtents),
            Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*blockDeviceClient_, Read(_, 0, 4))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    CURVEFS_ERROR ret = client_->FuseOpRead(req, ino, size, off, &fi,
        buffer.get(), &rSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpRead(req, ino, size, off, &fi,
        buffer.get(), &rSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpRead(req, ino, size, off, &fi,
        buffer.get(), &rSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpOpen) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    struct fuse_file_info fi;
    fi.flags = 0;

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_FILE);
    inode.set_openflag(false);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::OK));

    CURVEFS_ERROR ret = client_->FuseOpOpen(req, ino, &fi);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    ASSERT_EQ(1, inodeWapper->GetOpenCount());

    Inode inode2 = inodeWapper->GetInodeUnlocked();
    ASSERT_EQ(true, inode2.openflag());
}

TEST_F(TestFuseVolumeClient, FuseOpOpenFailed) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    struct fuse_file_info fi;
    fi.flags = 0;

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_FILE);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));

    CURVEFS_ERROR ret = client_->FuseOpOpen(req, ino, &fi);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpOpen(req, ino, &fi);
    ASSERT_EQ(CURVEFS_ERROR::UNKNOWN, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpCreate) {
    fuse_req fakeReq;
    fuse_ctx fakeCtx;
    fakeReq.ctx = &fakeCtx;
    fuse_req_t req = &fakeReq;
    fuse_ino_t parent = 1;
    const char *name = "xxx";
    mode_t mode = 1;
    struct fuse_file_info *fi;

    fuse_ino_t ino = 2;
    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(4096);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, CreateInode(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpCreate(req, parent, name, mode, fi, &e);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpCreateFailed) {
    fuse_req_t req;
    fuse_ino_t parent = 1;
    const char *name = "xxx";
    mode_t mode = 1;
    struct fuse_file_info *fi;

    fuse_ino_t ino = 2;
    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(4096);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, CreateInode(_, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpCreate(req, parent, name, mode, fi, &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpCreate(req, parent, name, mode, fi, &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpUnlink) {
    fuse_req_t req;
    fuse_ino_t parent = 1;
    std::string name = "xxx";
    uint32_t nlink = 100;

    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);

    EXPECT_CALL(*dentryManager_, GetDentry(parent, name, _))
        .WillOnce(DoAll(SetArgPointee<2>(dentry),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, DeleteDentry(parent, name))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_nlink(nlink);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(inodeid, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::OK));

    CURVEFS_ERROR ret = client_->FuseOpUnlink(req, parent, name.c_str());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    Inode inode2 = inodeWapper->GetInodeUnlocked();
    ASSERT_EQ(nlink - 1, inode2.nlink());
}

TEST_F(TestFuseVolumeClient, FuseOpUnlinkFailed) {
    fuse_req_t req;
    fuse_ino_t parent = 1;
    std::string name = "xxx";
    uint32_t nlink = 100;

    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);

    EXPECT_CALL(*dentryManager_, GetDentry(parent, name, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgPointee<2>(dentry),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(dentry),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(dentry),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, DeleteDentry(parent, name))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(Return(CURVEFS_ERROR::OK))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_nlink(nlink);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(inodeid, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));

    CURVEFS_ERROR ret = client_->FuseOpUnlink(req, parent, name.c_str());
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpUnlink(req, parent, name.c_str());
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpUnlink(req, parent, name.c_str());
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpUnlink(req, parent, name.c_str());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpOpenDir) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    struct fuse_file_info fi;

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(4);
    inode.set_type(FsFileType::TYPE_DIRECTORY);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    CURVEFS_ERROR ret = client_->FuseOpOpenDir(req, ino, &fi);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpOpenDirFaild) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    struct fuse_file_info fi;

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(4);
    inode.set_type(FsFileType::TYPE_DIRECTORY);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::INTERNAL)));

    CURVEFS_ERROR ret = client_->FuseOpOpenDir(req, ino, &fi);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpOpenAndFuseOpReadDir) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    size_t size = 100;
    off_t off = 0;
    struct fuse_file_info *fi = new fuse_file_info();
    fi->fh = 0;
    char *buffer;
    size_t rSize = 0;

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(0);
    inode.set_type(FsFileType::TYPE_DIRECTORY);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    CURVEFS_ERROR ret = client_->FuseOpOpenDir(req, ino, fi);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    std::list<Dentry> dentryList;
    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name("xxx");
    dentry.set_parentinodeid(ino);
    dentry.set_inodeid(2);
    dentryList.push_back(dentry);

    EXPECT_CALL(*dentryManager_, ListDentry(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(dentryList),
                Return(CURVEFS_ERROR::OK)));

    ret = client_->FuseOpReadDir(req, ino, size, off, fi,
        &buffer, &rSize);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    delete fi;
}

TEST_F(TestFuseVolumeClient, FuseOpOpenAndFuseOpReadDirFailed) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    size_t size = 100;
    off_t off = 0;
    struct fuse_file_info *fi = new fuse_file_info();
    fi->fh = 0;
    char *buffer;
    size_t rSize = 0;

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(0);
    inode.set_type(FsFileType::TYPE_DIRECTORY);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    CURVEFS_ERROR ret = client_->FuseOpOpenDir(req, ino, fi);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    std::list<Dentry> dentryList;
    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name("xxx");
    dentry.set_parentinodeid(ino);
    dentry.set_inodeid(2);
    dentryList.push_back(dentry);

    EXPECT_CALL(*dentryManager_, ListDentry(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(dentryList),
                Return(CURVEFS_ERROR::INTERNAL)));

    ret = client_->FuseOpReadDir(req, ino, size, off, fi,
        &buffer, &rSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    delete fi;
}

TEST_F(TestFuseVolumeClient, FuseOpGetAttr) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    struct fuse_file_info *fi;
    struct stat attr;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    CURVEFS_ERROR ret = client_->FuseOpGetAttr(req, ino, fi, &attr);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpGetAttrFailed) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    struct fuse_file_info *fi;
    struct stat attr;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::INTERNAL)));

    CURVEFS_ERROR ret = client_->FuseOpGetAttr(req, ino, fi, &attr);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpSetAttr) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    struct stat attr;
    int to_set;
    struct fuse_file_info *fi;
    struct stat attrOut;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::OK));

    attr.st_mode = 1;
    attr.st_uid = 2;
    attr.st_gid = 3;
    attr.st_size = 4;
    attr.st_atime = 5;
    attr.st_mtime = 6;
    attr.st_ctime = 7;

    to_set = FUSE_SET_ATTR_MODE |
             FUSE_SET_ATTR_UID |
             FUSE_SET_ATTR_GID |
             FUSE_SET_ATTR_SIZE |
             FUSE_SET_ATTR_ATIME |
             FUSE_SET_ATTR_MTIME |
             FUSE_SET_ATTR_CTIME;

    CURVEFS_ERROR ret = client_->FuseOpSetAttr(
        req, ino, &attr, to_set, fi, &attrOut);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(attr.st_mode,  attrOut.st_mode);
    ASSERT_EQ(attr.st_uid,  attrOut.st_uid);
    ASSERT_EQ(attr.st_gid,  attrOut.st_gid);
    ASSERT_EQ(attr.st_size,  attrOut.st_size);
    ASSERT_EQ(attr.st_atime,  attrOut.st_atime);
    ASSERT_EQ(attr.st_mtime,  attrOut.st_mtime);
    ASSERT_EQ(attr.st_ctime,  attrOut.st_ctime);
}

TEST_F(TestFuseVolumeClient, FuseOpSetAttrFailed) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    struct stat attr;
    int to_set;
    struct fuse_file_info *fi;
    struct stat attrOut;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));

    attr.st_mode = 1;
    attr.st_uid = 2;
    attr.st_gid = 3;
    attr.st_size = 4;
    attr.st_atime = 5;
    attr.st_mtime = 6;
    attr.st_ctime = 7;

    to_set = FUSE_SET_ATTR_MODE |
             FUSE_SET_ATTR_UID |
             FUSE_SET_ATTR_GID |
             FUSE_SET_ATTR_SIZE |
             FUSE_SET_ATTR_ATIME |
             FUSE_SET_ATTR_MTIME |
             FUSE_SET_ATTR_CTIME;

    CURVEFS_ERROR ret = client_->FuseOpSetAttr(
        req, ino, &attr, to_set, fi, &attrOut);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpSetAttr(req, ino, &attr, to_set, fi, &attrOut);
    ASSERT_EQ(CURVEFS_ERROR::UNKNOWN, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpSymlink) {
    fuse_req fakeReq;
    fuse_ctx fakeCtx;
    fakeReq.ctx = &fakeCtx;
    fuse_req_t req = &fakeReq;
    fuse_ino_t parent = 1;
    const char *name = "xxx";
    const char *link = "/a/b/xxx";

    fuse_ino_t ino = 2;
    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(std::strlen(link));
    inode.set_mode(0777);
    inode.set_type(FsFileType::TYPE_SYM_LINK);
    inode.set_symlink(link);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, CreateInode(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpSymlink(req, link, parent,
        name, &e);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpSymlinkFailed) {
    fuse_req fakeReq;
    fuse_ctx fakeCtx;
    fakeReq.ctx = &fakeCtx;
    fuse_req_t req = &fakeReq;
    fuse_ino_t parent = 1;
    const char *name = "xxx";
    const char *link = "/a/b/xxx";

    fuse_ino_t ino = 2;
    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(std::strlen(link));
    inode.set_mode(0777);
    inode.set_type(FsFileType::TYPE_SYM_LINK);
    inode.set_symlink(link);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, CreateInode(_, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpSymlink(req, link, parent,
        name, &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpSymlink(req, link, parent,
        name, &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpLink) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    fuse_ino_t newparent = 2;
    const char *newname = "xxxx";

    uint32_t nlink = 100;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    inode.set_nlink(nlink);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::OK));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpLink(req, ino, newparent, newname, &e);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    Inode inode2 = inodeWapper->GetInodeUnlocked();
    ASSERT_EQ(nlink + 1, inode2.nlink());
}

TEST_F(TestFuseVolumeClient, FuseOpLinkFailed) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    fuse_ino_t newparent = 2;
    const char *newname = "xxxx";

    uint32_t nlink = 100;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    inode.set_nlink(nlink);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR))
        .WillOnce(Return(MetaStatusCode::OK));

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpLink(req, ino, newparent, newname, &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    Inode inode2 = inodeWapper->GetInodeUnlocked();
    ASSERT_EQ(nlink, inode2.nlink());

    ret = client_->FuseOpLink(req, ino, newparent, newname, &e);
    ASSERT_EQ(CURVEFS_ERROR::UNKNOWN, ret);
    Inode inode3 = inodeWapper->GetInodeUnlocked();
    ASSERT_EQ(nlink, inode3.nlink());

    ret = client_->FuseOpLink(req, ino, newparent, newname, &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    Inode inode4 = inodeWapper->GetInodeUnlocked();
    ASSERT_EQ(nlink + 1, inode4.nlink());
}

TEST_F(TestFuseVolumeClient, FuseOpReadLink) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    const char *link = "/a/b/xxx";

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(std::strlen(link));
    inode.set_mode(0777);
    inode.set_type(FsFileType::TYPE_SYM_LINK);
    inode.set_symlink(link);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    std::string linkStr;
    CURVEFS_ERROR ret = client_->FuseOpReadLink(req, ino, &linkStr);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_STREQ(link, linkStr.c_str());
}

TEST_F(TestFuseVolumeClient, FuseOpReadLinkFailed) {
    fuse_req_t req;
    fuse_ino_t ino = 1;

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    std::string linkStr;
    CURVEFS_ERROR ret = client_->FuseOpReadLink(req, ino, &linkStr);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpRelease) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    struct fuse_file_info *fi;

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_openflag(true);

    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);
    inodeWapper->SetOpenCount(1);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::OK));

    CURVEFS_ERROR ret = client_->FuseOpRelease(req, ino, fi);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    ASSERT_EQ(0, inodeWapper->GetOpenCount());
    Inode inode2 = inodeWapper->GetInodeUnlocked();
    ASSERT_EQ(false, inode2.openflag());
}

class TestFuseS3Client : public ::testing::Test {
 protected:
    TestFuseS3Client() {}
    ~TestFuseS3Client() {}

    virtual void SetUp() {
        mdsClient_ = std::make_shared<MockMdsClient>();
        metaClient_ = std::make_shared<MockMetaServerClient>();
        spaceClient_ = std::make_shared<MockSpaceClient>();
        s3ClientAdaptor_ = std::make_shared<MockS3ClientAdaptor>();
        inodeManager_ = std::make_shared<MockInodeCacheManager>();
        dentryManager_ = std::make_shared<MockDentryCacheManager>();
        extManager_ = std::make_shared<MockExtentManager>();
        client_  = std::make_shared<FuseS3Client>(mdsClient_,
            metaClient_,
            spaceClient_,
            inodeManager_,
            dentryManager_,
            extManager_,
            s3ClientAdaptor_);
        PrepareFsInfo();
    }

    virtual void TearDown() {
        mdsClient_ = nullptr;
        metaClient_ = nullptr;
        spaceClient_ = nullptr;
        s3ClientAdaptor_ = nullptr;
        extManager_ = nullptr;
    }

    void PrepareFsInfo() {
        auto fsInfo = std::make_shared<FsInfo>();
        fsInfo->set_fsid(fsId);
        fsInfo->set_fsname("s3fs");

        client_->SetFsInfo(fsInfo);
    }

 protected:
    const uint32_t fsId = 100u;

    std::shared_ptr<MockMdsClient> mdsClient_;
    std::shared_ptr<MockMetaServerClient> metaClient_;
    std::shared_ptr<MockSpaceClient> spaceClient_;
    std::shared_ptr<MockS3ClientAdaptor> s3ClientAdaptor_;
    std::shared_ptr<MockInodeCacheManager> inodeManager_;
    std::shared_ptr<MockDentryCacheManager> dentryManager_;
    std::shared_ptr<MockExtentManager> extManager_;
    std::shared_ptr<FuseS3Client> client_;
};

TEST_F(TestFuseS3Client, FuseOpInit_when_fs_exist) {
    MountOption mOpts;
    memset(&mOpts, 0, sizeof(mOpts));
    mOpts.fsName = "s3fs";
    mOpts.mountPoint = "host1:/test";
    mOpts.user = "test";
    mOpts.fsType = "s3";

    std::string fsName = mOpts.fsName;

    EXPECT_CALL(*mdsClient_, GetFsInfo(fsName, _))
        .WillOnce(Return(FSStatusCode::OK));

    FsInfo fsInfoExp;
    fsInfoExp.set_fsid(200);
    fsInfoExp.set_fsname(fsName);
    EXPECT_CALL(*mdsClient_, MountFs(fsName, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(fsInfoExp),
                Return(FSStatusCode::OK)));

    client_->FuseOpInit(&mOpts, nullptr);

    auto fsInfo = client_->GetFsInfo();
    ASSERT_NE(fsInfo, nullptr);

    ASSERT_EQ(fsInfo->fsid(), fsInfoExp.fsid());
    ASSERT_EQ(fsInfo->fsname(), fsInfoExp.fsname());
}

TEST_F(TestFuseS3Client, FuseOpInit_when_fs_not_exist) {
    MountOption mOpts;
    memset(&mOpts, 0, sizeof(mOpts));
    mOpts.fsName = "s3fs";
    mOpts.mountPoint = "host1:/test";
    mOpts.user = "test";
    mOpts.fsType = "s3";

    std::string fsName = mOpts.fsName;

    EXPECT_CALL(*mdsClient_, GetFsInfo(fsName, _))
        .WillOnce(Return(FSStatusCode::NOT_FOUND));

    EXPECT_CALL(*mdsClient_, CreateFsS3(_, _, _))
        .WillOnce(Return(FSStatusCode::OK));

    FsInfo fsInfoExp;
    fsInfoExp.set_fsid(100);
    fsInfoExp.set_fsname(fsName);
    EXPECT_CALL(*mdsClient_, MountFs(fsName, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(fsInfoExp),
                Return(FSStatusCode::OK)));

    client_->FuseOpInit(&mOpts, nullptr);

    auto fsInfo = client_->GetFsInfo();
    ASSERT_NE(fsInfo, nullptr);

    ASSERT_EQ(fsInfo->fsid(), fsInfoExp.fsid());
    ASSERT_EQ(fsInfo->fsname(), fsInfoExp.fsname());
}

TEST_F(TestFuseS3Client, FuseOpDestroy) {
    MountOption mOpts;
    memset(&mOpts, 0, sizeof(mOpts));
    mOpts.fsName = "s3fs";
    mOpts.mountPoint = "host1:/test";
    mOpts.user = "test";
    mOpts.fsType = "s3";

    std::string fsName = mOpts.fsName;

    EXPECT_CALL(*mdsClient_, UmountFs(fsName, _))
        .WillOnce(Return(FSStatusCode::OK));

    client_->FuseOpDestroy(&mOpts);
}

TEST_F(TestFuseS3Client, FuseOpWrite) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    const char *buf = "xxx";
    size_t size = 4;
    off_t off = 0;
    struct fuse_file_info fi;
    fi.flags = O_WRONLY;
    size_t wSize = 0;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*s3ClientAdaptor_, Write(_, _, _, _))
        .WillOnce(Return(size));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::OK));

    CURVEFS_ERROR ret = client_->FuseOpWrite(
        req, ino, buf, size, off, &fi, &wSize);

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(size, wSize);
}

TEST_F(TestFuseS3Client, FuseOpWriteFailed) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    const char *buf = "xxx";
    size_t size = 4;
    off_t off = 0;
    struct fuse_file_info fi;
    fi.flags = O_WRONLY;
    size_t wSize = 0;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*s3ClientAdaptor_, Write(_, _, _, _))
        .WillOnce(Return(-1))
        .WillOnce(Return(size));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));

    CURVEFS_ERROR ret = client_->FuseOpWrite(
        req, ino, buf, size, off, &fi, &wSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpWrite(req, ino, buf, size, off, &fi, &wSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpWrite(req, ino, buf, size, off, &fi, &wSize);
    ASSERT_EQ(CURVEFS_ERROR::UNKNOWN, ret);
}

TEST_F(TestFuseS3Client, FuseOpRead) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    size_t size = 4;
    off_t off = 0;
    struct fuse_file_info fi;
    fi.flags = O_RDONLY;
    std::unique_ptr<char[]> buffer(new char[size]);
    size_t rSize = 0;

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(4096);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(Return(size));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::OK));

    CURVEFS_ERROR ret = client_->FuseOpRead(req, ino, size, off, &fi,
        buffer.get(), &rSize);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(size, rSize);
}

TEST_F(TestFuseS3Client, FuseOpReadFailed) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    size_t size = 4;
    off_t off = 0;
    struct fuse_file_info fi;
    fi.flags = O_RDONLY;
    std::unique_ptr<char[]> buffer(new char[size]);
    size_t rSize = 0;

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(4096);
    auto inodeWapper = std::make_shared<InodeWapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWapper),
                Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(Return(-1))
        .WillOnce(Return(size));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));

    CURVEFS_ERROR ret = client_->FuseOpRead(req, ino, size, off, &fi,
        buffer.get(), &rSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpRead(req, ino, size, off, &fi,
        buffer.get(), &rSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpRead(req, ino, size, off, &fi,
        buffer.get(), &rSize);
    ASSERT_EQ(CURVEFS_ERROR::UNKNOWN, ret);
}

}  // namespace client
}  // namespace curvefs
