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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "curvefs/src/client/fuse_s3_client.h"
#include "curvefs/src/client/fuse_volume_client.h"
#include "curvefs/test/client/mock_block_device_client.h"
#include "curvefs/test/client/mock_client_s3_adaptor.h"
#include "curvefs/test/client/mock_dentry_cache_mamager.h"
#include "curvefs/test/client/mock_extent_manager.h"
#include "curvefs/test/client/mock_inode_cache_manager.h"
#include "curvefs/test/client/mock_mds_client.h"
#include "curvefs/test/client/mock_metaserver_client.h"
#include "curvefs/test/client/mock_space_client.h"

struct fuse_req {
    struct fuse_ctx* ctx;
};

namespace curvefs {
namespace client {

using ::curve::common::Configuration;
using ::curvefs::mds::topology::PartitionTxId;
using ::testing::_;
using ::testing::Contains;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;

using rpcclient::MockMdsClient;
using rpcclient::MockMetaServerClient;

#define EQUAL(a) (lhs.a() == rhs.a())

bool operator==(const Dentry& lhs, const Dentry& rhs) {
    return EQUAL(fsid) && EQUAL(parentinodeid) && EQUAL(name) && EQUAL(txid) &&
           EQUAL(inodeid) && EQUAL(flag);
}

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
        listDentryLimit_ = 100;
        fuseClientOption_.extentManagerOpt.preAllocSize = preAllocSize_;
        fuseClientOption_.volumeOpt.bigFileSize = bigFileSize_;
        fuseClientOption_.listDentryLimit = listDentryLimit_;
        fuseClientOption_.maxNameLength = 20u;
        client_ = std::make_shared<FuseVolumeClient>(
            mdsClient_, metaClient_, inodeManager_,
            dentryManager_, spaceClient_,  extManager_, blockDeviceClient_);
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

    Dentry GenDentry(uint32_t fsId, uint64_t parentId, const std::string& name,
                     uint64_t txId, uint64_t inodeId, uint32_t flag) {
        Dentry dentry;
        dentry.set_fsid(fsId);
        dentry.set_parentinodeid(parentId);
        dentry.set_name(name);
        dentry.set_txid(txId);
        dentry.set_inodeid(inodeId);
        dentry.set_flag(flag);
        return dentry;
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
    uint32_t listDentryLimit_;
    FuseClientOption fuseClientOption_;

    static const uint32_t DELETE = DentryFlag::DELETE_MARK_FLAG;
    static const uint32_t FILE = DentryFlag::TYPE_FILE_FLAG;
    static const uint32_t TX_PREPARE = DentryFlag::TRANSACTION_PREPARE_FLAG;
};

TEST_F(TestFuseVolumeClient, FuseOpInit_when_fs_exist) {
    MountOption mOpts;
    memset(&mOpts, 0, sizeof(mOpts));
    mOpts.mountPoint = "host1:/test";
    mOpts.volume = "xxx";
    mOpts.fsName = "xxx";
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
        .WillOnce(DoAll(SetArgPointee<2>(fsInfoExp), Return(FSStatusCode::OK)));

    EXPECT_CALL(*blockDeviceClient_, Open(volName, user))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    CURVEFS_ERROR ret = client_->FuseOpInit(&mOpts, nullptr);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

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
    mOpts.fsName = "xxx";
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
        .WillOnce(DoAll(SetArgPointee<2>(fsInfoExp), Return(FSStatusCode::OK)));

    EXPECT_CALL(*blockDeviceClient_, Open(volName, user))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    CURVEFS_ERROR ret = client_->FuseOpInit(&mOpts, nullptr);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

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
    mOpts.fsName = "xxx";
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
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)));

    Inode inode;
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(inodeid, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

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
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*inodeManager_, GetInode(inodeid, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpLookup(req, parent, name.c_str(), &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpLookup(req, parent, name.c_str(), &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpLookupNameTooLong) {
    fuse_req_t req;
    fuse_ino_t parent = 1;
    std::string name = "aaaaaaaaaaaaaaaaaaaaa";

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpLookup(req, parent, name.c_str(), &e);
    ASSERT_EQ(CURVEFS_ERROR::NAMETOOLONG, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpWrite) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    const char* buf = "xxx";
    size_t size = 4;
    off_t off = 0;
    struct fuse_file_info fi;
    fi.flags = O_WRONLY;
    size_t wSize = 0;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    std::list<ExtentAllocInfo> toAllocExtents;
    ExtentAllocInfo allocInfo;
    allocInfo.lOffset = 0;
    allocInfo.pOffsetLeft = 0;
    allocInfo.len = preAllocSize_;
    toAllocExtents.push_back(allocInfo);
    EXPECT_CALL(*extManager_, GetToAllocExtents(_, off, size, _))
        .WillOnce(
            DoAll(SetArgPointee<3>(toAllocExtents), Return(CURVEFS_ERROR::OK)));

    std::list<Extent> allocatedExtents;
    Extent ext;
    ext.set_offset(0);
    ext.set_length(preAllocSize_);
    EXPECT_CALL(*spaceClient_, AllocExtents(fsId, _, AllocateType::SMALL, _))
        .WillOnce(DoAll(SetArgPointee<3>(allocatedExtents),
                        Return(CURVEFS_ERROR::OK)));

    VolumeExtentList* vlist = new VolumeExtentList();
    VolumeExtent* vext = vlist->add_volumeextents();
    vext->set_fsoffset(0);
    vext->set_volumeoffset(0);
    vext->set_length(preAllocSize_);
    vext->set_isused(false);
    inode.set_allocated_volumeextentlist(vlist);

    EXPECT_CALL(*extManager_, MergeAllocedExtents(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(*vlist), Return(CURVEFS_ERROR::OK)));

    std::list<PExtent> pExtents;
    PExtent pext;
    pext.pOffset = 0;
    pext.len = preAllocSize_;
    pExtents.push_back(pext);
    EXPECT_CALL(*extManager_, DivideExtents(_, off, size, _))
        .WillOnce(DoAll(SetArgPointee<3>(pExtents), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*blockDeviceClient_, Write(_, 0, preAllocSize_))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    EXPECT_CALL(*extManager_, MarkExtentsWritten(off, size, _))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    CURVEFS_ERROR ret =
        client_->FuseOpWrite(req, ino, buf, size, off, &fi, &wSize);

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(size, wSize);
    ASSERT_EQ(true, inodeWrapper->isDirty());
}

TEST_F(TestFuseVolumeClient, FuseOpWriteFailed) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    const char* buf = "xxx";
    size_t size = 4;
    off_t off = 0;
    struct fuse_file_info fi;
    fi.flags = O_WRONLY;
    size_t wSize = 0;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    std::list<ExtentAllocInfo> toAllocExtents;
    ExtentAllocInfo allocInfo;
    allocInfo.lOffset = 0;
    allocInfo.pOffsetLeft = 0;
    allocInfo.len = preAllocSize_;
    toAllocExtents.push_back(allocInfo);
    EXPECT_CALL(*extManager_, GetToAllocExtents(_, off, size, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(
            DoAll(SetArgPointee<3>(toAllocExtents), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgPointee<3>(toAllocExtents), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgPointee<3>(toAllocExtents), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgPointee<3>(toAllocExtents), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgPointee<3>(toAllocExtents), Return(CURVEFS_ERROR::OK)));

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
                        Return(CURVEFS_ERROR::OK)));

    VolumeExtentList* vlist = new VolumeExtentList();
    VolumeExtent* vext = vlist->add_volumeextents();
    vext->set_fsoffset(0);
    vext->set_volumeoffset(0);
    vext->set_length(preAllocSize_);
    vext->set_isused(false);
    inode.set_allocated_volumeextentlist(vlist);

    EXPECT_CALL(*extManager_, MergeAllocedExtents(_, _, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgPointee<2>(*vlist), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(*vlist), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(*vlist), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*spaceClient_, DeAllocExtents(_, _))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    std::list<PExtent> pExtents;
    PExtent pext;
    pext.pOffset = 0;
    pext.len = preAllocSize_;
    pExtents.push_back(pext);
    EXPECT_CALL(*extManager_, DivideExtents(_, off, size, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgPointee<3>(pExtents), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<3>(pExtents), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*blockDeviceClient_, Write(_, 0, preAllocSize_))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    EXPECT_CALL(*extManager_, MarkExtentsWritten(off, size, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    CURVEFS_ERROR ret =
        client_->FuseOpWrite(req, ino, buf, size, off, &fi, &wSize);
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
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

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
        .WillOnce(DoAll(SetArgPointee<3>(pExtents), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*blockDeviceClient_, Read(_, 0, 4))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    CURVEFS_ERROR ret =
        client_->FuseOpRead(req, ino, size, off, &fi, buffer.get(), &rSize);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(size, rSize);
    ASSERT_EQ(true, inodeWrapper->isDirty());
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
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

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
        .WillOnce(DoAll(SetArgPointee<3>(pExtents), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*blockDeviceClient_, Read(_, 0, 4))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    CURVEFS_ERROR ret =
        client_->FuseOpRead(req, ino, size, off, &fi, buffer.get(), &rSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpRead(req, ino, size, off, &fi, buffer.get(), &rSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpRead(req, ino, size, off, &fi, buffer.get(), &rSize);
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
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::OK));

    CURVEFS_ERROR ret = client_->FuseOpOpen(req, ino, &fi);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    ASSERT_EQ(1, inodeWrapper->GetOpenCount());

    Inode inode2 = inodeWrapper->GetInodeUnlocked();
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
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

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
    const char* name = "xxx";
    mode_t mode = 1;
    struct fuse_file_info fi;
    fi.flags = 0;

    fuse_ino_t ino = 2;
    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_FILE);
    inode.set_openflag(false);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, CreateInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    EXPECT_CALL(*inodeManager_, ClearInodeCache(parent));

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::OK));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpCreate(req, parent, name, mode, &fi, &e);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    ASSERT_EQ(1, inodeWrapper->GetOpenCount());

    Inode inode2 = inodeWrapper->GetInodeUnlocked();
    ASSERT_EQ(true, inode2.openflag());
}

TEST_F(TestFuseVolumeClient, FuseOpCreateFailed) {
    fuse_req fakeReq;
    fuse_ctx fakeCtx;
    fakeReq.ctx = &fakeCtx;
    fuse_req_t req = &fakeReq;
    fuse_ino_t parent = 1;
    const char* name = "xxx";
    mode_t mode = 1;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));

    fuse_ino_t ino = 2;
    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(4096);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, CreateInode(_, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpCreate(req, parent, name, mode, &fi, &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpCreate(req, parent, name, mode, &fi, &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpCreateNameTooLong) {
    fuse_req fakeReq;
    fuse_ctx fakeCtx;
    fakeReq.ctx = &fakeCtx;
    fuse_req_t req = &fakeReq;
    fuse_ino_t parent = 1;
    const char* name = "aaaaaaaaaaaaaaaaaaaaa";
    mode_t mode = 1;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpCreate(req, parent, name, mode, &fi, &e);
    ASSERT_EQ(CURVEFS_ERROR::NAMETOOLONG, ret);
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
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, DeleteDentry(parent, name))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    EXPECT_CALL(*inodeManager_, ClearInodeCache(parent));

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_nlink(nlink);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(inodeid, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::OK));

    EXPECT_CALL(*inodeManager_, ClearInodeCache(inodeid))
        .Times(1);

    CURVEFS_ERROR ret = client_->FuseOpUnlink(req, parent, name.c_str());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    Inode inode2 = inodeWrapper->GetInodeUnlocked();
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
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, DeleteDentry(parent, name))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(Return(CURVEFS_ERROR::OK))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    EXPECT_CALL(*inodeManager_, ClearInodeCache(parent))
        .Times(2);

    EXPECT_CALL(*inodeManager_, ClearInodeCache(inodeid))
        .Times(1);

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_nlink(nlink);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(inodeid, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

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

TEST_F(TestFuseVolumeClient, FuseOpUnlinkNameTooLong) {
    fuse_req_t req;
    fuse_ino_t parent = 1;
    std::string name = "aaaaaaaaaaaaaaaaaaaaa";

    CURVEFS_ERROR ret = client_->FuseOpUnlink(req, parent, name.c_str());
    ASSERT_EQ(CURVEFS_ERROR::NAMETOOLONG, ret);
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
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

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
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWrapper),
                        Return(CURVEFS_ERROR::INTERNAL)));

    CURVEFS_ERROR ret = client_->FuseOpOpenDir(req, ino, &fi);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpOpenAndFuseOpReadDir) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    size_t size = 100;
    off_t off = 0;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = 0;
    char* buffer;
    size_t rSize = 0;

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(0);
    inode.set_type(FsFileType::TYPE_DIRECTORY);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .Times(2)
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    CURVEFS_ERROR ret = client_->FuseOpOpenDir(req, ino, &fi);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    std::list<Dentry> dentryList;
    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name("xxx");
    dentry.set_parentinodeid(ino);
    dentry.set_inodeid(2);
    dentryList.push_back(dentry);

    EXPECT_CALL(*dentryManager_, ListDentry(ino, _, listDentryLimit_))
        .WillOnce(
            DoAll(SetArgPointee<1>(dentryList), Return(CURVEFS_ERROR::OK)));

    ret = client_->FuseOpReadDir(req, ino, size, off, &fi, &buffer, &rSize);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpOpenAndFuseOpReadDirFailed) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    size_t size = 100;
    off_t off = 0;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = 0;
    char* buffer;
    size_t rSize = 0;

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(0);
    inode.set_type(FsFileType::TYPE_DIRECTORY);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .Times(2)
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    CURVEFS_ERROR ret = client_->FuseOpOpenDir(req, ino, &fi);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    std::list<Dentry> dentryList;
    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name("xxx");
    dentry.set_parentinodeid(ino);
    dentry.set_inodeid(2);
    dentryList.push_back(dentry);

    EXPECT_CALL(*dentryManager_, ListDentry(ino, _, listDentryLimit_))
        .WillOnce(DoAll(SetArgPointee<1>(dentryList),
                        Return(CURVEFS_ERROR::INTERNAL)));

    ret = client_->FuseOpReadDir(req, ino, size, off, &fi, &buffer, &rSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpRenameBasic) {
    fuse_req_t req;
    fuse_ino_t parent = 1;
    std::string name = "A";
    fuse_ino_t newparent = 3;
    std::string newname = "B";
    uint64_t inodeId = 1000;
    uint32_t srcPartitionId = 1;
    uint32_t dstPartitionId = 2;
    uint64_t srcTxId = 0;
    uint64_t dstTxId = 2;

    // step1: get txid
    EXPECT_CALL(*metaClient_, GetTxId(fsId, parent, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(srcPartitionId),
                        SetArgPointee<3>(srcTxId), Return(MetaStatusCode::OK)));
    EXPECT_CALL(*metaClient_, GetTxId(fsId, newparent, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(dstPartitionId),
                        SetArgPointee<3>(dstTxId), Return(MetaStatusCode::OK)));

    // step2: precheck
    // dentry = { fsid, parentid, name, txid, inodeid, DELETE }
    auto dentry = GenDentry(fsId, parent, name, srcTxId, inodeId, 0);
    EXPECT_CALL(*dentryManager_, GetDentry(parent, name, _))
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, GetDentry(newparent, newname, _))
        .WillOnce(Return(CURVEFS_ERROR::NOTEXIST));

    // step3: prepare tx
    EXPECT_CALL(*metaClient_, PrepareRenameTx(_))
        .WillOnce(Invoke([&](const std::vector<Dentry>& dentrys) {
            auto srcDentry = GenDentry(fsId, parent, name,
                                       srcTxId + 1, inodeId,
                                       DELETE | TX_PREPARE);
            if (dentrys.size() == 1 && dentrys[0] == srcDentry) {
                return MetaStatusCode::OK;
            }
            return MetaStatusCode::UNKNOWN_ERROR;
        }))
        .WillOnce(Invoke([&](const std::vector<Dentry>& dentrys) {
            auto dstDentry = GenDentry(fsId, newparent, newname,
                                       dstTxId + 1, inodeId, TX_PREPARE);
            if (dentrys.size() == 1 && dentrys[0] == dstDentry) {
                return MetaStatusCode::OK;
            }
            return MetaStatusCode::UNKNOWN_ERROR;
        }));

    // step4: commit tx
    EXPECT_CALL(*mdsClient_, CommitTx(_))
        .WillOnce(Invoke([&](const std::vector<PartitionTxId>& txIds) {
            if (txIds.size() == 2 && txIds[0].partitionid() == srcPartitionId &&
                txIds[0].txid() == srcTxId + 1 &&
                txIds[1].partitionid() == dstPartitionId &&
                txIds[1].txid() == dstTxId + 1) {
                return TopoStatusCode::TOPO_OK;
            }
            return TopoStatusCode::TOPO_INTERNAL_ERROR;
        }));

    // step5: update cache
    EXPECT_CALL(*dentryManager_, DeleteCache(parent, name)).Times(1);
    EXPECT_CALL(*dentryManager_, InsertOrReplaceCache(_))
        .WillOnce(Invoke([&](const Dentry& dentry) {
            auto dstDentry =
                GenDentry(fsId, newparent, newname,
                          dstTxId + 1, inodeId, TX_PREPARE);
            ASSERT_TRUE(dentry == dstDentry);
        }));

    // step6: set txid
    EXPECT_CALL(*metaClient_, SetTxId(srcPartitionId, srcTxId + 1)).Times(1);
    EXPECT_CALL(*metaClient_, SetTxId(dstPartitionId, dstTxId + 1)).Times(1);

    auto rc = client_->FuseOpRename(req, parent, name.c_str(), newparent,
                                    newname.c_str());
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
}

TEST_F(TestFuseVolumeClient, FuseOpRenameOverwrite) {
    fuse_req_t req;
    fuse_ino_t parent = 1;
    std::string name = "A";
    fuse_ino_t newparent = 3;
    std::string newname = "B";
    uint64_t oldInodeId = 1001;
    uint64_t inodeId = 1000;
    uint32_t partitionId = 10;  // bleong on partiion
    uint64_t txId = 3;

    // step1: get txid
    EXPECT_CALL(*metaClient_, GetTxId(fsId, parent, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(partitionId), SetArgPointee<3>(txId),
                        Return(MetaStatusCode::OK)));
    EXPECT_CALL(*metaClient_, GetTxId(fsId, newparent, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(partitionId), SetArgPointee<3>(txId),
                        Return(MetaStatusCode::OK)));

    // step2: precheck
    // dentry = { fsid, parentid, name, txid, inodeid, DELETE }
    auto srcDentry = GenDentry(fsId, parent, name, txId, inodeId, FILE);
    auto dstDentry = GenDentry(fsId, newparent, newname,
                               txId, oldInodeId, FILE);
    EXPECT_CALL(*dentryManager_, GetDentry(parent, name, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(srcDentry), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, GetDentry(newparent, newname, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(dstDentry), Return(CURVEFS_ERROR::OK)));

    // step3: prepare tx
    EXPECT_CALL(*metaClient_, PrepareRenameTx(_))
        .WillOnce(Invoke([&](const std::vector<Dentry>& dentrys) {
            auto srcDentry = GenDentry(fsId, parent, name, txId + 1,
                                       inodeId, FILE | DELETE | TX_PREPARE);
            auto dstDentry = GenDentry(fsId, newparent, newname,
                                       txId + 1, inodeId, FILE | TX_PREPARE);
            if (dentrys.size() == 2 && dentrys[0] == srcDentry &&
                dentrys[1] == dstDentry) {
                return MetaStatusCode::OK;
            }
            return MetaStatusCode::UNKNOWN_ERROR;
        }));

    // step4: commit tx
    EXPECT_CALL(*mdsClient_, CommitTx(_))
        .WillOnce(Invoke([&](const std::vector<PartitionTxId>& txIds) {
            if (txIds.size() == 1 && txIds[0].partitionid() == partitionId &&
                txIds[0].txid() == txId + 1) {
                return TopoStatusCode::TOPO_OK;
            }
            return TopoStatusCode::TOPO_INTERNAL_ERROR;
        }));

    // step5: unlink old inode
    Inode inode;
    inode.set_inodeid(oldInodeId);
    inode.set_nlink(1);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(oldInodeId, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWrapper),
                        Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::OK));

    // step6: update cache
    EXPECT_CALL(*dentryManager_, DeleteCache(parent, name)).Times(1);
    EXPECT_CALL(*dentryManager_, InsertOrReplaceCache(_))
        .WillOnce(Invoke([&](const Dentry& dentry) {
            auto dstDentry = GenDentry(fsId, newparent, newname,
                                       txId + 1, inodeId, FILE | TX_PREPARE);
            ASSERT_TRUE(dentry == dstDentry);
        }));

    // step7: set txid
    EXPECT_CALL(*metaClient_, SetTxId(partitionId, txId + 1)).Times(2);

    auto rc = client_->FuseOpRename(req, parent, name.c_str(), newparent,
                                    newname.c_str());
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
}

TEST_F(TestFuseVolumeClient, FuseOpRenameOverwriteDir) {
    fuse_req_t req;
    fuse_ino_t parent = 1;
    std::string name = "A";
    fuse_ino_t newparent = 3;
    std::string newname = "B";
    uint64_t oldInodeId = 1001;
    uint64_t inodeId = 1000;
    uint32_t partitionId = 10;  // bleong on partiion
    uint64_t txId = 3;

    // step1: get txid
    EXPECT_CALL(*metaClient_, GetTxId(fsId, parent, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(partitionId), SetArgPointee<3>(txId),
                        Return(MetaStatusCode::OK)));
    EXPECT_CALL(*metaClient_, GetTxId(fsId, newparent, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(partitionId), SetArgPointee<3>(txId),
                        Return(MetaStatusCode::OK)));

    // step2: precheck
    // dentry = { fsid, parentid, name, txid, inodeid, DELETE }
    auto srcDentry = GenDentry(fsId, parent, name, txId, inodeId, FILE);
    auto dstDentry = GenDentry(fsId, newparent, newname,
                               txId, oldInodeId, 0);
    EXPECT_CALL(*dentryManager_, GetDentry(parent, name, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(srcDentry), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, GetDentry(newparent, newname, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(dstDentry), Return(CURVEFS_ERROR::OK)));

    // step3: list directory
    auto dentrys = std::list<Dentry>();
    dentrys.push_back(Dentry());
    EXPECT_CALL(*dentryManager_, ListDentry(oldInodeId, _, 1))
        .WillOnce(DoAll(SetArgPointee<1>(dentrys), Return(CURVEFS_ERROR::OK)));

    auto rc = client_->FuseOpRename(req, parent, name.c_str(), newparent,
                                    newname.c_str());
    ASSERT_EQ(rc, CURVEFS_ERROR::NOTEMPTY);
}

TEST_F(TestFuseVolumeClient, FuseOpRenameNameTooLong) {
    fuse_req_t req;
    fuse_ino_t parent = 1;
    std::string name1 = "aaaaaaaaaaaaaaaaaaaaa";
    std::string name2 = "xxx";
    fuse_ino_t newparent = 2;
    std::string newname1 = "bbbbbbbbbbbbbbbbbbbbb";
    std::string newname2 = "yyy";

    CURVEFS_ERROR ret = client_->FuseOpRename(req, parent, name1.c_str(),
                                              newparent, newname1.c_str());
    ASSERT_EQ(CURVEFS_ERROR::NAMETOOLONG, ret);

    ret = client_->FuseOpRename(req, parent, name1.c_str(), newparent,
                                newname2.c_str());
    ASSERT_EQ(CURVEFS_ERROR::NAMETOOLONG, ret);

    ret = client_->FuseOpRename(req, parent, name2.c_str(), newparent,
                                newname1.c_str());
    ASSERT_EQ(CURVEFS_ERROR::NAMETOOLONG, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpGetAttr) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    struct stat attr;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    CURVEFS_ERROR ret = client_->FuseOpGetAttr(req, ino, &fi, &attr);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpGetAttrFailed) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    struct stat attr;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWrapper),
                        Return(CURVEFS_ERROR::INTERNAL)));

    CURVEFS_ERROR ret = client_->FuseOpGetAttr(req, ino, &fi, &attr);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpSetAttr) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    struct stat attr;
    int to_set;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    struct stat attrOut;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::OK));

    attr.st_mode = 1;
    attr.st_uid = 2;
    attr.st_gid = 3;
    attr.st_size = 4;
    attr.st_atime = 5;
    attr.st_mtime = 6;
    attr.st_ctime = 7;

    to_set = FUSE_SET_ATTR_MODE | FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID |
             FUSE_SET_ATTR_SIZE | FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME |
             FUSE_SET_ATTR_CTIME;

    CURVEFS_ERROR ret =
        client_->FuseOpSetAttr(req, ino, &attr, to_set, &fi, &attrOut);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(attr.st_mode, attrOut.st_mode);
    ASSERT_EQ(attr.st_uid, attrOut.st_uid);
    ASSERT_EQ(attr.st_gid, attrOut.st_gid);
    ASSERT_EQ(attr.st_size, attrOut.st_size);
    ASSERT_EQ(attr.st_atime, attrOut.st_atime);
    ASSERT_EQ(attr.st_mtime, attrOut.st_mtime);
    ASSERT_EQ(attr.st_ctime, attrOut.st_ctime);
}

TEST_F(TestFuseVolumeClient, FuseOpSetAttrFailed) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    struct stat attr;
    int to_set;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    struct stat attrOut;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));

    attr.st_mode = 1;
    attr.st_uid = 2;
    attr.st_gid = 3;
    attr.st_size = 4;
    attr.st_atime = 5;
    attr.st_mtime = 6;
    attr.st_ctime = 7;

    to_set = FUSE_SET_ATTR_MODE | FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID |
             FUSE_SET_ATTR_SIZE | FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME |
             FUSE_SET_ATTR_CTIME;

    CURVEFS_ERROR ret =
        client_->FuseOpSetAttr(req, ino, &attr, to_set, &fi, &attrOut);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpSetAttr(req, ino, &attr, to_set, &fi, &attrOut);
    ASSERT_EQ(CURVEFS_ERROR::UNKNOWN, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpSymlink) {
    fuse_req fakeReq;
    fuse_ctx fakeCtx;
    fakeReq.ctx = &fakeCtx;
    fuse_req_t req = &fakeReq;
    fuse_ino_t parent = 1;
    const char* name = "xxx";
    const char* link = "/a/b/xxx";

    fuse_ino_t ino = 2;
    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(std::strlen(link));
    inode.set_mode(0777);
    inode.set_type(FsFileType::TYPE_SYM_LINK);
    inode.set_symlink(link);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, CreateInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    EXPECT_CALL(*inodeManager_, ClearInodeCache(parent));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpSymlink(req, link, parent, name, &e);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpSymlinkFailed) {
    fuse_req fakeReq;
    fuse_ctx fakeCtx;
    fakeReq.ctx = &fakeCtx;
    fuse_req_t req = &fakeReq;
    fuse_ino_t parent = 1;
    const char* name = "xxx";
    const char* link = "/a/b/xxx";

    fuse_ino_t ino = 2;
    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(std::strlen(link));
    inode.set_mode(0777);
    inode.set_type(FsFileType::TYPE_SYM_LINK);
    inode.set_symlink(link);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, CreateInode(_, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    fuse_entry_param e;
    // create inode failed
    CURVEFS_ERROR ret = client_->FuseOpSymlink(req, link, parent, name, &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    EXPECT_CALL(*inodeManager_, DeleteInode(ino))
        .WillOnce(Return(CURVEFS_ERROR::OK))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    // create dentry failed
    ret = client_->FuseOpSymlink(req, link, parent, name, &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // also delete inode failed
    ret = client_->FuseOpSymlink(req, link, parent, name, &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpSymlinkNameTooLong) {
    fuse_req fakeReq;
    fuse_ctx fakeCtx;
    fakeReq.ctx = &fakeCtx;
    fuse_req_t req = &fakeReq;
    fuse_ino_t parent = 1;
    const char* name = "aaaaaaaaaaaaaaaaaaaaa";
    const char* link = "/a/b/xxx";

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpSymlink(req, link, parent, name, &e);
    ASSERT_EQ(CURVEFS_ERROR::NAMETOOLONG, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpLink) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    fuse_ino_t newparent = 2;
    const char* newname = "xxxx";

    uint32_t nlink = 100;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    inode.set_nlink(nlink);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::OK));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpLink(req, ino, newparent, newname, &e);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    Inode inode2 = inodeWrapper->GetInodeUnlocked();
    ASSERT_EQ(nlink + 1, inode2.nlink());
}

TEST_F(TestFuseVolumeClient, FuseOpLinkFailed) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    fuse_ino_t newparent = 2;
    const char* newname = "xxxx";

    uint32_t nlink = 100;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    inode.set_nlink(nlink);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR))   // link
        .WillOnce(Return(MetaStatusCode::OK))  // link
        .WillOnce(Return(MetaStatusCode::OK))  // link
        .WillOnce(Return(MetaStatusCode::OK))  // unlink
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));  // unlink

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    fuse_entry_param e;
    // get inode failed
    CURVEFS_ERROR ret = client_->FuseOpLink(req, ino, newparent, newname, &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    Inode inode2 = inodeWrapper->GetInodeUnlocked();
    ASSERT_EQ(nlink, inode2.nlink());

    // link failed
    ret = client_->FuseOpLink(req, ino, newparent, newname, &e);
    ASSERT_EQ(CURVEFS_ERROR::UNKNOWN, ret);
    Inode inode3 = inodeWrapper->GetInodeUnlocked();
    ASSERT_EQ(nlink, inode3.nlink());

    // create dentry failed
    ret = client_->FuseOpLink(req, ino, newparent, newname, &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    Inode inode4 = inodeWrapper->GetInodeUnlocked();
    ASSERT_EQ(nlink, inode4.nlink());

    // also unlink failed
    ret = client_->FuseOpLink(req, ino, newparent, newname, &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    Inode inode5 = inodeWrapper->GetInodeUnlocked();
    ASSERT_EQ(nlink, inode5.nlink());
}

TEST_F(TestFuseVolumeClient, FuseOpReadLink) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    const char* link = "/a/b/xxx";

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(std::strlen(link));
    inode.set_mode(0777);
    inode.set_type(FsFileType::TYPE_SYM_LINK);
    inode.set_symlink(link);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

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
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_openflag(true);

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    inodeWrapper->SetOpenCount(1);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::OK));

    CURVEFS_ERROR ret = client_->FuseOpRelease(req, ino, &fi);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    ASSERT_EQ(0, inodeWrapper->GetOpenCount());
    Inode inode2 = inodeWrapper->GetInodeUnlocked();
    ASSERT_EQ(false, inode2.openflag());
}

class TestFuseS3Client : public ::testing::Test {
 protected:
    TestFuseS3Client() {}
    ~TestFuseS3Client() {}

    virtual void SetUp() {
        mdsClient_ = std::make_shared<MockMdsClient>();
        metaClient_ = std::make_shared<MockMetaServerClient>();
        s3ClientAdaptor_ = std::make_shared<MockS3ClientAdaptor>();
        inodeManager_ = std::make_shared<MockInodeCacheManager>();
        dentryManager_ = std::make_shared<MockDentryCacheManager>();
        client_ = std::make_shared<FuseS3Client>(
            mdsClient_, metaClient_, inodeManager_,
            dentryManager_, s3ClientAdaptor_);
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
        .WillOnce(DoAll(SetArgPointee<2>(fsInfoExp), Return(FSStatusCode::OK)));

    CURVEFS_ERROR ret = client_->FuseOpInit(&mOpts, nullptr);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

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
        .WillOnce(DoAll(SetArgPointee<2>(fsInfoExp), Return(FSStatusCode::OK)));

    CURVEFS_ERROR ret = client_->FuseOpInit(&mOpts, nullptr);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

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

TEST_F(TestFuseS3Client, FuseOpWriteSmallSize) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    const char* buf = "xxx";
    size_t size = 4;
    off_t off = 0;
    struct fuse_file_info fi;
    fi.flags = O_WRONLY;
    size_t wSize = 0;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    size_t smallSize = 3;
    EXPECT_CALL(*s3ClientAdaptor_, Write(_, _, _, _))
        .WillOnce(Return(smallSize));

    CURVEFS_ERROR ret =
        client_->FuseOpWrite(req, ino, buf, size, off, &fi, &wSize);

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(smallSize, wSize);
}

TEST_F(TestFuseS3Client, FuseOpWriteFailed) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    const char* buf = "xxx";
    size_t size = 4;
    off_t off = 0;
    struct fuse_file_info fi;
    fi.flags = O_WRONLY;
    size_t wSize = 0;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*s3ClientAdaptor_, Write(_, _, _, _))
        .WillOnce(Return(4))
        .WillOnce(Return(-1));

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    CURVEFS_ERROR ret =
        client_->FuseOpWrite(req, ino, buf, size, off, &fi, &wSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpWrite(req, ino, buf, size, off, &fi, &wSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseS3Client, FuseOpReadOverRange) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    size_t size = 4;
    off_t off = 5000;
    struct fuse_file_info fi;
    fi.flags = O_RDONLY;
    std::unique_ptr<char[]> buffer(new char[size]);
    size_t rSize = 0;

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(4096);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    CURVEFS_ERROR ret =
        client_->FuseOpRead(req, ino, size, off, &fi, buffer.get(), &rSize);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, rSize);
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
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _)).WillOnce(Return(-1));

    CURVEFS_ERROR ret =
        client_->FuseOpRead(req, ino, size, off, &fi, buffer.get(), &rSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpRead(req, ino, size, off, &fi, buffer.get(), &rSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseS3Client, FuseOpFsync) {
    fuse_req_t req;
    fuse_ino_t ino = 1;
    struct fuse_file_info* fi;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);

    EXPECT_CALL(*s3ClientAdaptor_, Flush(_))
        .WillOnce(Return(CURVEFS_ERROR::OK))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    inodeWrapper->SetUid(32);
    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::OK));

    CURVEFS_ERROR ret = client_->FuseOpFsync(req, ino, 0, fi);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    ret = client_->FuseOpFsync(req, ino, 1, fi);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

}  // namespace client
}  // namespace curvefs
