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

#include "curvefs/src/client/common/common.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/volume/fuse_volume_client.h"
#include "curvefs/src/client/volume/client_volume_adaptor.h"
#include "curvefs/src/common/define.h"
#include "curvefs/test/client/mock_client_volume_adaptor.h"
#include "curvefs/test/client/mock_dentry_cache_mamager.h"
#include "curvefs/test/client/mock_inode_cache_manager.h"
#include "curvefs/test/client/rpcclient/mock_mds_client.h"
#include "curvefs/test/client/mock_metaserver_client.h"
#include "curvefs/test/client/mock_volume_storage.h"
#include "curvefs/test/volume/mock/mock_block_device_client.h"
#include "curvefs/test/volume/mock/mock_space_manager.h"

struct fuse_req {
    struct fuse_ctx *ctx;
};

namespace curvefs {
namespace client {
namespace common {
DECLARE_bool(enableCto);
}  // namespace common
}  // namespace client
}  // namespace curvefs

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
using ::testing::AtLeast;
using ::testing::SetArrayArgument;

using rpcclient::MockMdsClient;
using rpcclient::MockMetaServerClient;
using rpcclient::MetaServerClientDone;
using ::curvefs::volume::MockBlockDeviceClient;
using ::curvefs::volume::MockSpaceManager;
using ::curvefs::client::common::FileHandle;

#define EQUAL(a) (lhs.a() == rhs.a())

static bool operator==(const Dentry &lhs, const Dentry &rhs) {
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
        blockDeviceClient_ = std::make_shared<MockBlockDeviceClient>();
        inodeManager_ = std::make_shared<MockInodeCacheManager>();
        dentryManager_ = std::make_shared<MockDentryCacheManager>();
        preAllocSize_ = 65536;
        bigFileSize_ = 1048576;
        listDentryLimit_ = 100;
        listDentryThreads_ = 2;
        fuseClientOption_.extentManagerOpt.preAllocSize = preAllocSize_;
        fuseClientOption_.volumeOpt.bigFileSize = bigFileSize_;
        fuseClientOption_.listDentryLimit = listDentryLimit_;
        fuseClientOption_.listDentryThreads = listDentryThreads_;
        fuseClientOption_.maxNameLength = 20u;
        fuseClientOption_.dummyServerStartPort = 5000;
        fuseClientOption_.s3Opt.s3ClientAdaptorOpt.readCacheMaxByte = 104857600;
        fuseClientOption_.s3Opt.s3ClientAdaptorOpt.writeCacheMaxByte
          = 10485760000;
        fuseClientOption_.s3Opt.s3ClientAdaptorOpt.readCacheThreads = 2;
        fuseClientOption_.s3Opt.s3ClientAdaptorOpt.diskCacheOpt.diskCacheType
          = DiskCacheType::Disable;
        fuseClientOption_.s3Opt.s3AdaptrOpt.asyncThreadNum = 1;

        fuseClientOption_.warmupThreadsNum = 1;
        fuseClientOption_.s3Opt.s3ClientAdaptorOpt.prefetchExecQueueNum = 1;
        // if not set, then there will be a lot of threads in gdb
        fuseClientOption_.s3Opt.s3ClientAdaptorOpt.chunkFlushThreads = 1;
        storageAdaptor_ = std::make_shared<MockVolumeClientAdaptor>();

        spaceManager_ = new MockSpaceManager();
        volumeStorage_ = new MockVolumeStorage();
        client_ = std::make_shared<FuseVolumeClient>(
            mdsClient_, metaClient_, inodeManager_, dentryManager_,
            blockDeviceClient_, storageAdaptor_);

        auto fsInfo = std::make_shared<FsInfo>();
        fsInfo->set_fsid(fsId);
        fsInfo->set_fsname("s3fs");
        fsInfo->set_blocksize(4096);

        Volume volume;
        volume.set_blocksize(4096);
        volume.set_slicesize(4*4096);

        curvefs::mds::FsDetail fsDetail;
        fsDetail.set_allocated_volume(new Volume(volume));

        fsInfo->set_allocated_detail(new curvefs::mds::FsDetail(fsDetail));

        client_->SetFsInfo(fsInfo);
        client_->Init(fuseClientOption_);
        PrepareFsInfo();
        PrepareEnv();
    }

    virtual void TearDown() {
        client_->UnInit();
        mdsClient_ = nullptr;
        metaClient_ = nullptr;
        blockDeviceClient_ = nullptr;
    }

    void PrepareEnv() {
        client_->SetSpaceManagerForTesting(spaceManager_);
        client_->SetVolumeStorageForTesting(volumeStorage_);
    }

    void PrepareFsInfo() {
        auto fsInfo = std::make_shared<FsInfo>();
        fsInfo->set_fsid(fsId);
        fsInfo->set_fsname("xxx");
        fsInfo->set_blocksize(4096);

        client_->SetFsInfo(fsInfo);
        client_->SetMounted(true);
    }

    Dentry GenDentry(uint32_t fsId, uint64_t parentId, const std::string &name,
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
    std::shared_ptr<MockBlockDeviceClient> blockDeviceClient_;
    std::shared_ptr<MockInodeCacheManager> inodeManager_;
    std::shared_ptr<MockDentryCacheManager> dentryManager_;
    std::shared_ptr<FuseVolumeClient> client_;
    std::shared_ptr<MockVolumeClientAdaptor> storageAdaptor_;

    MockVolumeStorage *volumeStorage_;
    MockSpaceManager *spaceManager_;
    ExtentCacheOption option_;

    uint64_t preAllocSize_;
    uint64_t bigFileSize_;
    uint32_t listDentryLimit_;
    uint32_t listDentryThreads_;
    FuseClientOption fuseClientOption_;

    static const uint32_t DELETE = DentryFlag::DELETE_MARK_FLAG;
    static const uint32_t FILE = DentryFlag::TYPE_FILE_FLAG;
    static const uint32_t TX_PREPARE = DentryFlag::TRANSACTION_PREPARE_FLAG;
};

TEST_F(TestFuseVolumeClient, FuseOpInit_when_fs_exist) {
    MountOption mOpts;
    memset(&mOpts, 0, sizeof(mOpts));
    mOpts.mountPoint = "host1:/test";
    mOpts.fsName = "xxx";
    mOpts.fsType = "curve";

    std::string fsName = mOpts.fsName;

    FsInfo fsInfoExp;
    fsInfoExp.set_fsid(200);
    fsInfoExp.set_fsname(fsName);

    auto* vol = fsInfoExp.mutable_detail()->mutable_volume();
    vol->set_blocksize(4096);
    vol->set_slicesize(1ULL * 1024 * 1024 * 1024);
    EXPECT_CALL(*mdsClient_, MountFs(fsName, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(fsInfoExp), Return(FSStatusCode::OK)));
    CURVEFS_ERROR ret = client_->SetMountStatus(&mOpts);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    EXPECT_CALL(*storageAdaptor_, FuseOpInit(_, _))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    ret = client_->FuseOpInit(&mOpts, nullptr);

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
    mOpts.fsName = "xxx";
    mOpts.fsType = "volume";

    std::string fsName = mOpts.fsName;

    VLOG(9) << "fuse op destroy failed";
    EXPECT_CALL(*mdsClient_, UmountFs(fsName, _))
        .WillOnce(Return(FSStatusCode::INTERNAL_ERROR));
    client_->FuseOpDestroy(&mOpts);

    VLOG(9) << "fuse op destroy sucess";
    EXPECT_CALL(*mdsClient_, UmountFs(fsName, _))
        .WillOnce(Return(FSStatusCode::MOUNT_POINT_NOT_EXIST));
    client_->FuseOpDestroy(&mOpts);

    VLOG(9) << "fuse op destroy success";
    EXPECT_CALL(*mdsClient_, UmountFs(fsName, _))
        .WillOnce(Return(FSStatusCode::OK));
    client_->FuseOpDestroy(&mOpts);
}

TEST_F(TestFuseVolumeClient, FuseOpLookup) {
    fuse_req_t req = nullptr;
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

    InodeAttr inode;
    EXPECT_CALL(*inodeManager_, GetInodeAttr(inodeid, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpLookup(req, parent, name.c_str(), &e);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpLookupFail) {
    fuse_req_t req = nullptr;
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

    EXPECT_CALL(*inodeManager_, GetInodeAttr(inodeid, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpLookup(req, parent, name.c_str(), &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpLookup(req, parent, name.c_str(), &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpLookupNameTooLong) {
    fuse_req_t req = nullptr;
    fuse_ino_t parent = 1;
    std::string name = "aaaaaaaaaaaaaaaaaaaaa";

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpLookup(req, parent, name.c_str(), &e);
    ASSERT_EQ(CURVEFS_ERROR::NAMETOOLONG, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpWrite) {
    fuse_req_t req{};
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

    VLOG(9) << "write failed";
    EXPECT_CALL(*storageAdaptor_, Write(_, _, _, _))
        .WillOnce(Return(-1));

    ASSERT_EQ(CURVEFS_ERROR::INTERNAL,
                  client_->FuseOpWrite(req, ino, buf, size, off, &fi, &wSize));

    VLOG(9) << "write success";
    EXPECT_CALL(*storageAdaptor_, Write(_, _, _, _))
        .WillOnce(Return(1));
    inode.set_type(FsFileType::TYPE_FILE);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    ASSERT_EQ(CURVEFS_ERROR::OK,
      client_->FuseOpWrite(req, ino, buf, size, off, &fi, &wSize));
}

TEST_F(TestFuseVolumeClient, FuseOpRead) {
    fuse_req_t req{};
    fuse_ino_t ino = 1;
    size_t size = 4;
    off_t off = 0;
    struct fuse_file_info fi;
    fi.flags = O_RDONLY;
    std::unique_ptr<char[]> buf(new char[size]);
    size_t rSize = 0;

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(4096);
    VLOG(9) << "read failed";
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
              Return(CURVEFS_ERROR::NOTEMPTY)));

    ASSERT_EQ(CURVEFS_ERROR::NOTEMPTY, client_->FuseOpRead(
        req, ino, size, off, &fi, buf.get(), &rSize));

    VLOG(9) << "read failed";
    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
     EXPECT_CALL(*storageAdaptor_, Read(_, _, _, _))
        .WillOnce(Return(-1));
     ASSERT_EQ(CURVEFS_ERROR::INTERNAL, client_->FuseOpRead(
        req, ino, size, off, &fi, buf.get(), &rSize));

    VLOG(9) << "read success";
    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
     EXPECT_CALL(*storageAdaptor_, Read(_, _, _, _))
        .WillOnce(Return(1));
     ASSERT_EQ(CURVEFS_ERROR::OK, client_->FuseOpRead(
        req, ino, size, off, &fi, buf.get(), &rSize));
}

TEST_F(TestFuseVolumeClient, FuseOpOpen) {
    fuse_req_t req = nullptr;
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
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    CURVEFS_ERROR ret = client_->FuseOpOpen(req, ino, &fi);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpOpenFailed) {
    fuse_req_t req = nullptr;
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
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    CURVEFS_ERROR ret = client_->FuseOpOpen(req, ino, &fi);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpCreate) {
    fuse_req fakeReq;
    fuse_ctx fakeCtx;
    fakeReq.ctx = &fakeCtx;
    fuse_req_t req = &fakeReq;
    fuse_ino_t parent = 1;
    const char *name = "xxx";
    mode_t mode = 1;
    struct fuse_file_info fi;
    fi.flags = 0;

    fuse_ino_t ino = 2;
    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_FILE);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, CreateInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    Inode parentInode;
    parentInode.set_fsid(fsId);
    parentInode.set_inodeid(parent);
    parentInode.set_type(FsFileType::TYPE_DIRECTORY);
    parentInode.set_nlink(2);
    auto parentInodeWrapper =
        std::make_shared<InodeWrapper>(parentInode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(parentInodeWrapper),
                        Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpCreate(req, parent, name, mode, &fi, &e);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpMkDir) {
    fuse_req fakeReq;
    fuse_ctx fakeCtx;
    fakeReq.ctx = &fakeCtx;
    fuse_req_t req = &fakeReq;
    fuse_ino_t parent = 1;
    const char *name = "xxx";
    mode_t mode = 1;

    fuse_ino_t ino = 2;
    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_DIRECTORY);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, CreateInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    Inode parentInode;
    parentInode.set_fsid(fsId);
    parentInode.set_inodeid(parent);
    parentInode.set_type(FsFileType::TYPE_DIRECTORY);
    parentInode.set_nlink(2);
    auto parentInodeWrapper =
        std::make_shared<InodeWrapper>(parentInode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(parentInodeWrapper),
                        Return(CURVEFS_ERROR::OK)));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpMkDir(req, parent, name, mode, &e);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpCreateFailed) {
    fuse_req fakeReq;
    fuse_ctx fakeCtx;
    fakeReq.ctx = &fakeCtx;
    fuse_req_t req = &fakeReq;
    fuse_ino_t parent = 1;
    const char *name = "xxx";
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
    const char *name = "aaaaaaaaaaaaaaaaaaaaa";
    mode_t mode = 1;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpCreate(req, parent, name, mode, &fi, &e);
    ASSERT_EQ(CURVEFS_ERROR::NAMETOOLONG, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpUnlink) {
    fuse_req_t req = nullptr;
    fuse_ino_t parent = 1;
    std::string name = "xxx";
    uint32_t nlink = 100;

    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);
    dentry.set_type(FsFileType::TYPE_FILE);

    EXPECT_CALL(*dentryManager_, GetDentry(parent, name, _))
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, DeleteDentry(
        parent, name, FsFileType::TYPE_FILE))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_nlink(nlink);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    InodeAttr attr;
    attr.set_fsid(fsId);
    attr.set_inodeid(inodeid);
    attr.set_length(4096);
    attr.set_nlink(nlink);

    Inode parentInode;
    parentInode.set_fsid(fsId);
    parentInode.set_inodeid(parent);
    parentInode.set_type(FsFileType::TYPE_DIRECTORY);
    parentInode.set_nlink(3);
    auto parentInodeWrapper =
        std::make_shared<InodeWrapper>(parentInode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(parentInodeWrapper),
                        Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, GetInodeAttr(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(attr), Return(MetaStatusCode::OK)));
    EXPECT_CALL(*metaClient_, UpdateInodeAttr(_, _, _))
        .WillOnce(Return(MetaStatusCode::OK));

    EXPECT_CALL(*inodeManager_, ClearInodeCache(inodeid)).Times(1);

    CURVEFS_ERROR ret = client_->FuseOpUnlink(req, parent, name.c_str());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    Inode inode2 = inodeWrapper->GetInode();
    ASSERT_EQ(nlink - 1, inode2.nlink());
}

TEST_F(TestFuseVolumeClient, FuseOpRmDir) {
    fuse_req_t req = nullptr;
    fuse_ino_t parent = 1;
    std::string name = "xxx";
    uint32_t nlink = 100;

    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);
    dentry.set_type(FsFileType::TYPE_DIRECTORY);

    EXPECT_CALL(*dentryManager_, GetDentry(parent, name, _))
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_,
        DeleteDentry(parent, name, FsFileType::TYPE_DIRECTORY))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_nlink(nlink);
    inode.set_type(FsFileType::TYPE_DIRECTORY);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    InodeAttr attr;
    attr.set_fsid(fsId);
    attr.set_inodeid(inodeid);
    attr.set_length(4096);
    attr.set_nlink(nlink);
    attr.set_type(FsFileType::TYPE_DIRECTORY);

    Inode parentInode;
    parentInode.set_fsid(fsId);
    parentInode.set_inodeid(parent);
    parentInode.set_type(FsFileType::TYPE_DIRECTORY);
    parentInode.set_nlink(3);
    auto parentInodeWrapper =
        std::make_shared<InodeWrapper>(parentInode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(parentInodeWrapper),
                        Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, GetInodeAttr(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(attr), Return(MetaStatusCode::OK)));
    EXPECT_CALL(*metaClient_, UpdateInodeAttr(_, _, _))
        .WillOnce(Return(MetaStatusCode::OK));

    EXPECT_CALL(*inodeManager_, ClearInodeCache(inodeid)).Times(1);

    CURVEFS_ERROR ret = client_->FuseOpRmDir(req, parent, name.c_str());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    Inode inode2 = inodeWrapper->GetInode();
    ASSERT_EQ(nlink - 1, inode2.nlink());
    ASSERT_EQ(2, parentInodeWrapper->GetNlinkLocked());
}

TEST_F(TestFuseVolumeClient, FuseOpUnlinkFailed) {
    fuse_req_t req = nullptr;
    fuse_ino_t parent = 1;
    std::string name = "xxx";
    uint32_t nlink = 100;

    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);
    dentry.set_type(FsFileType::TYPE_FILE);

    EXPECT_CALL(*dentryManager_, GetDentry(parent, name, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_,
        DeleteDentry(parent, name, FsFileType::TYPE_FILE))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(Return(CURVEFS_ERROR::OK))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_nlink(nlink);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    InodeAttr attr;
    attr.set_fsid(fsId);
    attr.set_inodeid(inodeid);
    attr.set_length(4096);
    attr.set_nlink(nlink);

    Inode parentInode;
    parentInode.set_fsid(fsId);
    parentInode.set_inodeid(parent);
    parentInode.set_type(FsFileType::TYPE_DIRECTORY);
    parentInode.set_nlink(3);
    auto parentInodeWrapper =
        std::make_shared<InodeWrapper>(parentInode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(parentInodeWrapper),
                        Return(CURVEFS_ERROR::OK)))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(DoAll(SetArgReferee<1>(parentInodeWrapper),
                        Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, GetInodeAttr(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(attr), Return(MetaStatusCode::OK)));
    EXPECT_CALL(*metaClient_, UpdateInodeAttr(_, _, _))
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));

    EXPECT_CALL(*inodeManager_, ClearInodeCache(inodeid)).Times(1);

    CURVEFS_ERROR ret = client_->FuseOpUnlink(req, parent, name.c_str());
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpUnlink(req, parent, name.c_str());
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpUnlink(req, parent, name.c_str());
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpUnlink(req, parent, name.c_str());
    ASSERT_EQ(CURVEFS_ERROR::UNKNOWN, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpUnlinkNameTooLong) {
    fuse_req_t req = nullptr;
    fuse_ino_t parent = 1;
    std::string name = "aaaaaaaaaaaaaaaaaaaaa";

    CURVEFS_ERROR ret = client_->FuseOpUnlink(req, parent, name.c_str());
    ASSERT_EQ(CURVEFS_ERROR::NAMETOOLONG, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpOpenDir) {
    fuse_req_t req = nullptr;
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
    fuse_req_t req = nullptr;
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
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    size_t size = 100;
    off_t off = 0;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = 0;
    char *buffer;
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

    EXPECT_CALL(*dentryManager_, ListDentry(ino, _, listDentryLimit_, _, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(dentryList), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetInodeAttrAsync(ino, _, _))
        .Times(1)
        .WillOnce(Return(CURVEFS_ERROR::OK));

    ret = client_->FuseOpReadDirPlus(req, ino, size, off, &fi, &buffer,
                                     &rSize, true);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpOpenAndFuseOpReadDirFailed) {
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    size_t size = 100;
    off_t off = 0;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = 0;
    char *buffer;
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

    EXPECT_CALL(*dentryManager_, ListDentry(ino, _, listDentryLimit_, _, _))
        .WillOnce(DoAll(SetArgPointee<1>(dentryList),
                        Return(CURVEFS_ERROR::INTERNAL)));

    ret = client_->FuseOpReadDirPlus(req, ino, size, off, &fi, &buffer,
                                     &rSize, false);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpRenameBasic) {
    fuse_req_t req = nullptr;
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

    // step2: link dest parent inode
    Inode destParentInode;
    destParentInode.set_inodeid(newparent);
    destParentInode.set_nlink(2);
    InodeAttr dattr;
    dattr.set_inodeid(newparent);
    dattr.set_nlink(2);
    auto inodeWrapper =
        std::make_shared<InodeWrapper>(destParentInode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(newparent, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWrapper),
                  Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, GetInodeAttr(_, newparent, _))
        .WillOnce(DoAll(SetArgPointee<2>(dattr), Return(MetaStatusCode::OK)));
    // include below unlink operate and update inode parent
    EXPECT_CALL(*metaClient_, UpdateInodeAttr(_, _, _))
        .Times(2)
        .WillRepeatedly(Return(MetaStatusCode::OK));

    // step3: precheck
    // dentry = { fsid, parentid, name, txid, inodeid, DELETE }
    auto dentry = GenDentry(fsId, parent, name, srcTxId, inodeId, 0);
    dentry.set_type(FsFileType::TYPE_DIRECTORY);
    EXPECT_CALL(*dentryManager_, GetDentry(parent, name, _))
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, GetDentry(newparent, newname, _))
        .WillOnce(Return(CURVEFS_ERROR::NOTEXIST));

    // step4: prepare tx
    EXPECT_CALL(*metaClient_, PrepareRenameTx(_))
        .WillOnce(Invoke([&](const std::vector<Dentry> &dentrys) {
            auto srcDentry = GenDentry(fsId, parent, name, srcTxId + 1, inodeId,
                                       DELETE | TX_PREPARE);
            srcDentry.set_type(FsFileType::TYPE_DIRECTORY);
            if (dentrys.size() == 1 && dentrys[0] == srcDentry) {
                return MetaStatusCode::OK;
            }
            return MetaStatusCode::UNKNOWN_ERROR;
        }))
        .WillOnce(Invoke([&](const std::vector<Dentry> &dentrys) {
            auto dstDentry = GenDentry(fsId, newparent, newname, dstTxId + 1,
                                       inodeId, TX_PREPARE);
            dstDentry.set_type(FsFileType::TYPE_DIRECTORY);
            if (dentrys.size() == 1 && dentrys[0] == dstDentry) {
                return MetaStatusCode::OK;
            }
            return MetaStatusCode::UNKNOWN_ERROR;
        }));

    // step5: commit tx
    EXPECT_CALL(*mdsClient_, CommitTx(_))
        .WillOnce(Invoke([&](const std::vector<PartitionTxId> &txIds) {
            if (txIds.size() == 2 && txIds[0].partitionid() == srcPartitionId &&
                txIds[0].txid() == srcTxId + 1 &&
                txIds[1].partitionid() == dstPartitionId &&
                txIds[1].txid() == dstTxId + 1) {
                return FSStatusCode::OK;
            }
            return FSStatusCode::UNKNOWN_ERROR;
        }));

    // step6: unlink source parent inode
    Inode srcParentInode;
    srcParentInode.set_inodeid(parent);
    srcParentInode.set_nlink(3);
    InodeAttr sattr;
    sattr.set_inodeid(parent);
    sattr.set_nlink(3);
    EXPECT_CALL(*metaClient_, GetInodeAttr(_, parent, _))
        .WillOnce(DoAll(SetArgPointee<2>(sattr), Return(MetaStatusCode::OK)));
    inodeWrapper = std::make_shared<InodeWrapper>(srcParentInode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(parent, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWrapper),
                  Return(CURVEFS_ERROR::OK)));

    // step7: update inode parent and ctime
    Inode InodeInfo;
    InodeInfo.set_inodeid(inodeId);
    InodeInfo.add_parent(parent);
    inodeWrapper = std::make_shared<InodeWrapper>(InodeInfo, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(inodeId, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgReferee<1>(inodeWrapper),
                  Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, inodeId, _, _, _))
        .Times(2)
        .WillRepeatedly(Return(MetaStatusCode::OK));

    // step8: update cache
    EXPECT_CALL(*dentryManager_, DeleteCache(parent, name)).Times(1);
    EXPECT_CALL(*dentryManager_, InsertOrReplaceCache(_))
        .WillOnce(Invoke([&](const Dentry &dentry) {
            auto dstDentry = GenDentry(fsId, newparent, newname, dstTxId + 1,
                                       inodeId, TX_PREPARE);
            ASSERT_TRUE(dentry == dstDentry);
        }));

    // step9: set txid
    EXPECT_CALL(*metaClient_, SetTxId(srcPartitionId, srcTxId + 1)).Times(1);
    EXPECT_CALL(*metaClient_, SetTxId(dstPartitionId, dstTxId + 1)).Times(1);

    auto rc = client_->FuseOpRename(req, parent, name.c_str(), newparent,
                                    newname.c_str());
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
}

TEST_F(TestFuseVolumeClient, FuseOpRenameOverwrite) {
    fuse_req_t req = nullptr;
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

    // update dest parent inode mtime ctime
    Inode destParentInode;
    destParentInode.set_inodeid(newparent);
    destParentInode.set_nlink(2);
    auto inodeWrapper =
        std::make_shared<InodeWrapper>(destParentInode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(newparent, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWrapper),
                  Return(CURVEFS_ERROR::OK)));

    // step2: precheck
    // dentry = { fsid, parentid, name, txid, inodeid, DELETE }
    auto srcDentry = GenDentry(fsId, parent, name, txId, inodeId, FILE);
    srcDentry.set_type(FsFileType::TYPE_S3);
    auto dstDentry =
        GenDentry(fsId, newparent, newname, txId, oldInodeId, FILE);
    dstDentry.set_type(FsFileType::TYPE_S3);
    EXPECT_CALL(*dentryManager_, GetDentry(parent, name, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(srcDentry), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, GetDentry(newparent, newname, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(dstDentry), Return(CURVEFS_ERROR::OK)));

    // record old inode info if exist
    InodeAttr attr;
    attr.set_inodeid(oldInodeId);
    EXPECT_CALL(*inodeManager_, GetInodeAttr(oldInodeId, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<1>(attr),
            Return(CURVEFS_ERROR::OK)));


    // step3: prepare tx
    EXPECT_CALL(*metaClient_, PrepareRenameTx(_))
        .WillOnce(Invoke([&](const std::vector<Dentry> &dentrys) {
            auto srcDentry = GenDentry(fsId, parent, name, txId + 1, inodeId,
                                       FILE | DELETE | TX_PREPARE);
            auto dstDentry = GenDentry(fsId, newparent, newname, txId + 1,
                                       inodeId, FILE | TX_PREPARE);
            if (dentrys.size() == 2 && dentrys[0] == srcDentry &&
                dentrys[1] == dstDentry) {
                return MetaStatusCode::OK;
            }
            return MetaStatusCode::UNKNOWN_ERROR;
        }));

    // step4: commit tx
    EXPECT_CALL(*mdsClient_, CommitTx(_))
        .WillOnce(Invoke([&](const std::vector<PartitionTxId> &txIds) {
            if (txIds.size() == 1 && txIds[0].partitionid() == partitionId &&
                txIds[0].txid() == txId + 1) {
                return FSStatusCode::OK;
            }
            return FSStatusCode::UNKNOWN_ERROR;
        }));

    // step5: unlink source parent inode
    Inode srcParentInode;
    srcParentInode.set_inodeid(parent);
    srcParentInode.set_nlink(3);
    inodeWrapper =
        std::make_shared<InodeWrapper>(srcParentInode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(parent, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWrapper),
                  Return(CURVEFS_ERROR::OK)));
    // include below unlink old inode and update inode parent
    EXPECT_CALL(*metaClient_, UpdateInodeAttr(_, _, _))
        .Times(1)
        .WillRepeatedly(Invoke([&](uint32_t /*fsId*/, uint64_t /*inodeId*/,
                                   const InodeAttr& /*attr*/) {
            return MetaStatusCode::OK;
        }));
    EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
        .Times(4)
        .WillRepeatedly(Invoke(
            [&](uint32_t /*fsId*/, uint64_t /*inodeId*/,
                const InodeAttr& /*attr*/,
                S3ChunkInfoMap* s3ChunkInfoAdd,
                bool internal) { return MetaStatusCode::OK; }));

    // step6: unlink old inode
    Inode inode;
    inode.set_inodeid(oldInodeId);
    inode.set_nlink(1);
    InodeAttr oldAttr;
    oldAttr.set_inodeid(oldInodeId);
    oldAttr.set_nlink(1);
    inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(oldInodeId, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgReferee<1>(inodeWrapper),
                  Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, GetInodeAttr(_, oldInodeId, _))
        .WillOnce(DoAll(SetArgPointee<2>(oldAttr), Return(MetaStatusCode::OK)));

    // step7: update inode parent
    Inode InodeInfo;
    InodeInfo.set_inodeid(inodeId);
    InodeInfo.add_parent(parent);
    inodeWrapper = std::make_shared<InodeWrapper>(InodeInfo, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(inodeId, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgReferee<1>(inodeWrapper),
                  Return(CURVEFS_ERROR::OK)));

    // step8: update cache
    EXPECT_CALL(*dentryManager_, DeleteCache(parent, name)).Times(1);
    EXPECT_CALL(*dentryManager_, InsertOrReplaceCache(_))
        .WillOnce(Invoke([&](const Dentry &dentry) {
            auto dstDentry = GenDentry(fsId, newparent, newname, txId + 1,
                                       inodeId, FILE | TX_PREPARE);
            ASSERT_TRUE(dentry == dstDentry);
        }));

    // step9: set txid
    EXPECT_CALL(*metaClient_, SetTxId(partitionId, txId + 1)).Times(2);

    auto rc = client_->FuseOpRename(req, parent, name.c_str(), newparent,
                                    newname.c_str());
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
}

TEST_F(TestFuseVolumeClient, FuseOpRenameOverwriteDir) {
    fuse_req_t req = nullptr;
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
    srcDentry.set_type(FsFileType::TYPE_DIRECTORY);
    auto dstDentry = GenDentry(fsId, newparent, newname, txId, oldInodeId, 0);
    dstDentry.set_type(FsFileType::TYPE_DIRECTORY);
    EXPECT_CALL(*dentryManager_, GetDentry(parent, name, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(srcDentry), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, GetDentry(newparent, newname, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(dstDentry), Return(CURVEFS_ERROR::OK)));

    // step3: list directory
    auto dentrys = std::list<Dentry>();
    dentrys.push_back(Dentry());
    EXPECT_CALL(*dentryManager_, ListDentry(oldInodeId, _, 1, _, _))
        .WillOnce(DoAll(SetArgPointee<1>(dentrys), Return(CURVEFS_ERROR::OK)));

    auto rc = client_->FuseOpRename(req, parent, name.c_str(), newparent,
                                    newname.c_str());
    ASSERT_EQ(rc, CURVEFS_ERROR::NOTEMPTY);
}

TEST_F(TestFuseVolumeClient, FuseOpRenameNameTooLong) {
    fuse_req_t req = nullptr;
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

TEST_F(TestFuseVolumeClient, FuseOpRenameParallel) {
    fuse_req_t req = nullptr;
    uint64_t txId = 0;
    auto dentry = GenDentry(1, 1, "A", 0, 10, FILE);
    dentry.set_type(FsFileType::TYPE_DIRECTORY);
    int nThread = 3;
    int timesPerThread = 10000;
    int times = nThread * timesPerThread;
    volatile bool start = false;
    bool success = true;

    // step1: get txid
    EXPECT_CALL(*metaClient_, GetTxId(_, _, _, _))
        .Times(2 * times)
        .WillRepeatedly(DoAll(SetArgPointee<3>(txId),
                        Return(MetaStatusCode::OK)));

    // step2: precheck
    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .Times(2 * times)
        .WillRepeatedly(DoAll(SetArgPointee<2>(dentry),
                        Return(CURVEFS_ERROR::OK)));

    // record old inode info
    InodeAttr attr;
    attr.set_inodeid(10);
    EXPECT_CALL(*inodeManager_, GetInodeAttr(attr.inodeid(), _))
        .Times(times)
        .WillRepeatedly(DoAll(SetArgPointee<1>(attr),
            Return(CURVEFS_ERROR::OK)));

    // step3: prepare tx
    EXPECT_CALL(*metaClient_, PrepareRenameTx(_))
        .Times(times)
        .WillRepeatedly(Return(MetaStatusCode::OK));

    // step4: commit tx
    EXPECT_CALL(*mdsClient_, CommitTx(_))
        .Times(times)
        .WillRepeatedly(Return(FSStatusCode::OK));

    // step5: unlink source directory
    Inode srcParentInode;
    srcParentInode.set_inodeid(1);
    srcParentInode.set_nlink(times + 2);
    InodeAttr srcAttr;
    srcAttr.set_inodeid(1);
    srcAttr.set_nlink(times + 2);
    auto srcParentInodeWrapper =
        std::make_shared<InodeWrapper>(srcParentInode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(srcParentInode.inodeid(), _))
        .Times(times * 2)
        .WillRepeatedly(DoAll(SetArgReferee<1>(srcParentInodeWrapper),
                              Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, GetInodeAttr(_, 1, _))
        .Times(times)
        .WillRepeatedly(DoAll(SetArgPointee<2>(srcAttr),
                        Return(MetaStatusCode::OK)));
    // include below operator which unlink old inode and update inode parent
    EXPECT_CALL(*metaClient_, UpdateInodeAttr(_, _, _))
        .Times(times * 2)
        .WillRepeatedly(Return(MetaStatusCode::OK));

    EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
        .Times(times * 2)
        .WillRepeatedly(Return(MetaStatusCode::OK));

    // step6: unlink old inode
    Inode inode;
    inode.set_inodeid(10);
    inode.set_nlink(times);
    inode.set_type(FsFileType::TYPE_FILE);
    InodeAttr oldAttr;
    oldAttr.set_inodeid(10);
    oldAttr.set_nlink(times);
    oldAttr.set_type(FsFileType::TYPE_FILE);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(inode.inodeid(), _))
        .Times(times * 2)
        .WillRepeatedly(DoAll(SetArgReferee<1>(inodeWrapper),
                              Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, GetInodeAttr(_, 10, _))
        .Times(times)
        .WillRepeatedly(DoAll(SetArgPointee<2>(oldAttr),
                        Return(MetaStatusCode::OK)));

    // step7: update cache
    EXPECT_CALL(*dentryManager_, DeleteCache(_, _)).Times(times);
    EXPECT_CALL(*dentryManager_, InsertOrReplaceCache(_)).Times(times);

    // step8: set txid
    EXPECT_CALL(*metaClient_, SetTxId(_, _))
        .Times(2 * times)
        .WillRepeatedly(Invoke([&](uint32_t partitionId, uint64_t _) {
            txId = txId + 1;
        }));

    auto worker = [&](int count) {
        while (!start) {
            continue;
        }
        for (auto i = 0; i < count; i++) {
            auto rc = client_->FuseOpRename(req, 1, "A", 1, "B");
            if (rc != CURVEFS_ERROR::OK) {
                success = false;
                break;
            }
        }
    };

    std::vector<std::thread> threads;
    for (auto i = 0; i < nThread; i++) {
        threads.emplace_back(std::thread(worker, timesPerThread));
    }
    start = true;
    for (auto i = 0; i < nThread; i++) {
        threads[i].join();
    }

    ASSERT_TRUE(success);
    // in our cases, for each renema, we plus txid twice
    ASSERT_EQ(2 * times, txId);
}

TEST_F(TestFuseVolumeClient, FuseOpGetAttr) {
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    struct stat attr;

    InodeAttr inode;
    inode.set_inodeid(ino);
    inode.set_length(0);

    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));

    CURVEFS_ERROR ret = client_->FuseOpGetAttr(req, ino, &fi, &attr);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpGetAttrFailed) {
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    struct stat attr;

    InodeAttr inode;
    inode.set_inodeid(ino);
    inode.set_length(0);

    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode),
                        Return(CURVEFS_ERROR::INTERNAL)));

    CURVEFS_ERROR ret = client_->FuseOpGetAttr(req, ino, &fi, &attr);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpGetAttrEnableCto) {
    curvefs::client::common::FLAGS_enableCto = true;

    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    struct stat attr;

    InodeAttr inode;
    inode.set_inodeid(ino);
    inode.set_length(0);

    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));

    ASSERT_EQ(CURVEFS_ERROR::OK, client_->FuseOpGetAttr(req, ino, &fi, &attr));

    // need not refresh inode
    fi.fh = static_cast<uint64_t>(FileHandle::kKeepCache);
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));

    ASSERT_EQ(CURVEFS_ERROR::OK, client_->FuseOpGetAttr(req, ino, &fi, &attr));
}

TEST_F(TestFuseVolumeClient, FuseOpSetAttr) {
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    struct stat attr;
    int to_set;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    struct stat attrOut;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    inode.set_type(FsFileType::TYPE_FILE);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
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
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    struct stat attr;
    int to_set;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    struct stat attrOut;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    inode.set_type(FsFileType::TYPE_FILE);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
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
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, CreateInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    Inode parentInode;
    parentInode.set_fsid(fsId);
    parentInode.set_inodeid(parent);
    parentInode.set_type(FsFileType::TYPE_DIRECTORY);
    parentInode.set_nlink(2);
    auto parentInodeWrapper =
        std::make_shared<InodeWrapper>(parentInode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(parent, _))
        .WillOnce(DoAll(SetArgReferee<1>(parentInodeWrapper),
                        Return(CURVEFS_ERROR::OK)));

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
    const char *name = "aaaaaaaaaaaaaaaaaaaaa";
    const char *link = "/a/b/xxx";

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpSymlink(req, link, parent, name, &e);
    ASSERT_EQ(CURVEFS_ERROR::NAMETOOLONG, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpLink) {
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    fuse_ino_t newparent = 2;
    const char *newname = "xxxx";

    uint32_t nlink = 100;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    inode.set_nlink(nlink);
    inode.set_type(FsFileType::TYPE_FILE);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    InodeAttr attr;
    attr.set_inodeid(ino);
    attr.set_length(0);
    attr.set_nlink(nlink);
    attr.set_type(FsFileType::TYPE_FILE);

    Inode parentInode;
    parentInode.set_fsid(fsId);
    parentInode.set_inodeid(newparent);
    parentInode.set_type(FsFileType::TYPE_DIRECTORY);
    parentInode.set_nlink(2);
    auto parentInodeWrapper =
        std::make_shared<InodeWrapper>(parentInode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgReferee<1>(parentInodeWrapper),
                        Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    EXPECT_CALL(*metaClient_, GetInodeAttr(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(attr), Return(MetaStatusCode::OK)));

    EXPECT_CALL(*metaClient_, UpdateInodeAttr(_, _, _))
        .WillOnce(Return(MetaStatusCode::OK));

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpLink(req, ino, newparent, newname, &e);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(nlink + 1, inodeWrapper->GetNlinkLocked());
    ASSERT_EQ(2, parentInodeWrapper->GetNlinkLocked());
}

TEST_F(TestFuseVolumeClient, FuseOpLinkFailed) {
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    fuse_ino_t newparent = 2;
    const char *newname = "xxxx";

    uint32_t nlink = 100;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    inode.set_nlink(nlink);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    InodeAttr attr;
    attr.set_inodeid(ino);
    attr.set_length(0);
    attr.set_nlink(nlink);

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, GetInodeAttr(_, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(attr),
            Return(MetaStatusCode::OK)));

    EXPECT_CALL(*metaClient_, UpdateInodeAttr(_, _, _))
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR))   // link
        .WillOnce(Return(MetaStatusCode::OK))              // link
        .WillOnce(Return(MetaStatusCode::OK))              // link
        .WillOnce(Return(MetaStatusCode::OK))              // unlink
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));  // unlink

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    fuse_entry_param e;
    // get inode failed
    CURVEFS_ERROR ret = client_->FuseOpLink(req, ino, newparent, newname, &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    Inode inode2 = inodeWrapper->GetInode();
    ASSERT_EQ(nlink, inode2.nlink());

    // link failed
    ret = client_->FuseOpLink(req, ino, newparent, newname, &e);
    ASSERT_EQ(CURVEFS_ERROR::UNKNOWN, ret);
    Inode inode3 = inodeWrapper->GetInode();
    ASSERT_EQ(nlink, inode3.nlink());

    // create dentry failed
    ret = client_->FuseOpLink(req, ino, newparent, newname, &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    Inode inode4 = inodeWrapper->GetInode();
    ASSERT_EQ(nlink - 1, inode4.nlink());

    // also unlink failed
    ret = client_->FuseOpLink(req, ino, newparent, newname, &e);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    Inode inode5 = inodeWrapper->GetInode();
    ASSERT_EQ(nlink - 1, inode5.nlink());
}

TEST_F(TestFuseVolumeClient, FuseOpReadLink) {
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    const char *link = "/a/b/xxx";

    InodeAttr inodeAttr;
    inodeAttr.set_fsid(fsId);
    inodeAttr.set_inodeid(ino);
    inodeAttr.set_length(std::strlen(link));
    inodeAttr.set_mode(0777);
    inodeAttr.set_type(FsFileType::TYPE_SYM_LINK);
    inodeAttr.set_symlink(link);

    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inodeAttr), Return(CURVEFS_ERROR::OK)));

    std::string linkStr;
    CURVEFS_ERROR ret = client_->FuseOpReadLink(req, ino, &linkStr);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_STREQ(link, linkStr.c_str());
}

TEST_F(TestFuseVolumeClient, FuseOpReadLinkFailed) {
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;

    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    std::string linkStr;
    CURVEFS_ERROR ret = client_->FuseOpReadLink(req, ino, &linkStr);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpRelease) {
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(ino);

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    ASSERT_EQ(CURVEFS_ERROR::OK, client_->FuseOpRelease(req, ino, &fi));
}

}  // namespace client
}  // namespace curvefs
