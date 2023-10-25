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
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/fuse_volume_client.h"
#include "curvefs/src/common/define.h"
#include "curvefs/test/client/mock_dentry_cache_mamager.h"
#include "curvefs/test/client/mock_inode_cache_manager.h"
#include "curvefs/test/client/rpcclient/mock_mds_client.h"
#include "curvefs/test/client/mock_metaserver_client.h"
#include "curvefs/test/client/mock_volume_storage.h"
#include "curvefs/test/volume/mock/mock_block_device_client.h"
#include "curvefs/test/volume/mock/mock_space_manager.h"
#include "curvefs/src/client/filesystem/error.h"
#include "curvefs/src/client/filesystem/filesystem.h"

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
using ::testing::DoAll;
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

using ::curvefs::client::common::FileSystemOption;
using ::curvefs::client::common::OpenFilesOption;
using ::curvefs::client::filesystem::EntryOut;
using ::curvefs::client::filesystem::AttrOut;

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
        fuseClientOption_.dummyServerStartPort = 5000;
        {
            auto option = FileSystemOption();
            option.maxNameLength = 20u;
            option.rpcOption.listDentryLimit = listDentryLimit_;
            option.openFilesOption.lruSize = 100;
            option.attrWatcherOption.lruSize = 100;
            fuseClientOption_.fileSystemOption = option;
        }

        spaceManager_ = new MockSpaceManager();
        volumeStorage_ = new MockVolumeStorage();
        client_ = std::make_shared<FuseVolumeClient>(
            mdsClient_, metaClient_, inodeManager_, dentryManager_,
            blockDeviceClient_);

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

    MockVolumeStorage *volumeStorage_;
    MockSpaceManager *spaceManager_;

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

    EXPECT_CALL(*blockDeviceClient_, Open(_, _))
        .WillOnce(Return(true));
    CURVEFS_ERROR ret = client_->SetMountStatus(&mOpts);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ret = client_->FuseOpInit(&mOpts, nullptr);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    auto fsInfo = client_->GetFsInfo();
    ASSERT_NE(fsInfo, nullptr);

    ASSERT_EQ(fsInfo->fsid(), fsInfoExp.fsid());
    ASSERT_EQ(fsInfo->fsname(), fsInfoExp.fsname());

    client_->GetFileSystem()->Destory();
}

TEST_F(TestFuseVolumeClient, FuseOpDestroy) {
    MountOption mOpts;
    memset(&mOpts, 0, sizeof(mOpts));
    mOpts.mountPoint = "host1:/test";
    mOpts.fsName = "xxx";
    mOpts.fsType = "curve";

    std::string fsName = mOpts.fsName;

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

    EntryOut entryOut;
    CURVEFS_ERROR ret = client_->FuseOpLookup(req, parent, name.c_str(),
                                              &entryOut);
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

    EntryOut entryOut;
    CURVEFS_ERROR ret = client_->FuseOpLookup(req, parent, name.c_str(),
                                              &entryOut);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpLookup(req, parent, name.c_str(), &entryOut);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpLookupNameTooLong) {
    fuse_req_t req = nullptr;
    fuse_ino_t parent = 1;
    std::string name = "aaaaaaaaaaaaaaaaaaaaa";

    EntryOut entryOut;
    CURVEFS_ERROR ret = client_->FuseOpLookup(req, parent, name.c_str(),
                                              &entryOut);
    ASSERT_EQ(CURVEFS_ERROR::NAME_TOO_LONG, ret);
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

    for (auto ret : {CURVEFS_ERROR::OK, CURVEFS_ERROR::IO_ERROR,
                     CURVEFS_ERROR::NO_SPACE}) {
        EXPECT_CALL(*volumeStorage_, Write(_, _, _, _, _))
            .WillOnce(Return(ret));

        FileOut fileOut;
        auto rc = client_->FuseOpWrite(req, ino, buf, size, off, &fi, &fileOut);
        ASSERT_EQ(ret, rc);

        if (ret == CURVEFS_ERROR::OK) {
            ASSERT_EQ(size, fileOut.nwritten);
        }
    }
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

    for (auto ret : {CURVEFS_ERROR::OK, CURVEFS_ERROR::IO_ERROR,
                     CURVEFS_ERROR::NO_SPACE}) {
        EXPECT_CALL(*volumeStorage_, Read(_, _, _, _))
            .WillOnce(Return(ret));

        ASSERT_EQ(ret, client_->FuseOpRead(req, ino, size, off, &fi, buf.get(),
                                           &rSize));

        if (ret == CURVEFS_ERROR::OK) {
            ASSERT_EQ(size, rSize);
        }
    }
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
    inode.set_mtime(123);
    inode.set_mtime_ns(456);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    {  // mock lookup to remeber attribute mtime
        auto member = client_->GetFileSystem()->BorrowMember();
        auto attrWatcher = member.attrWatcher;
        InodeAttr attr;
        attr.set_inodeid(ino);
        attr.set_mtime(123);
        attr.set_mtime_ns(456);
        attrWatcher->RemeberMtime(attr);
    }

    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgReferee<1>(inodeWrapper),
                              Return(CURVEFS_ERROR::OK)));

    FileOut fileOut;
    CURVEFS_ERROR ret = client_->FuseOpOpen(req, ino, &fi, &fileOut);
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

    FileOut fileOut;
    CURVEFS_ERROR ret = client_->FuseOpOpen(req, ino, &fi, &fileOut);
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

    EntryOut entryOut;
    CURVEFS_ERROR ret = client_->FuseOpCreate(req, parent, name, mode, &fi,
                                              &entryOut);
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

    EntryOut entryOut;
    CURVEFS_ERROR ret = client_->FuseOpMkDir(req, parent, name, mode,
                                             &entryOut);
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

    EntryOut entryOut;
    CURVEFS_ERROR ret = client_->FuseOpCreate(req, parent, name, mode, &fi,
                                              &entryOut);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpCreate(req, parent, name, mode, &fi, &entryOut);
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
    EntryOut entryOut;
    CURVEFS_ERROR ret = client_->FuseOpCreate(req, parent, name, mode, &fi,
                                              &entryOut);
    ASSERT_EQ(CURVEFS_ERROR::NAME_TOO_LONG, ret);
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
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(
            SetArgReferee<1>(parentInodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, GetInodeAttr(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(attr), Return(MetaStatusCode::OK)));
    EXPECT_CALL(*metaClient_, UpdateInodeAttr(_, _, _))
        .WillOnce(Return(MetaStatusCode::OK));

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
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(
            SetArgReferee<1>(parentInodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, GetInodeAttr(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(attr), Return(MetaStatusCode::OK)));
    EXPECT_CALL(*metaClient_, UpdateInodeAttr(_, _, _))
        .WillOnce(Return(MetaStatusCode::OK));

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
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(
            SetArgReferee<1>(parentInodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(
            SetArgReferee<1>(parentInodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, GetInodeAttr(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(attr), Return(MetaStatusCode::OK)));
    EXPECT_CALL(*metaClient_, UpdateInodeAttr(_, _, _))
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));

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
    ASSERT_EQ(CURVEFS_ERROR::NAME_TOO_LONG, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpOpenDir) {
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    struct fuse_file_info fi;

    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    CURVEFS_ERROR ret = client_->FuseOpOpenDir(req, ino, &fi);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpOpenDirFaild) {
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    struct fuse_file_info fi;

    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

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

    // InodeAttr attr;
    // attr.set_inodeid(ino);
    // EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
    //     .WillOnce(DoAll(SetArgReferee<1>(attr),
    //                     Return(CURVEFS_ERROR::OK)));

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

    ret = client_->FuseOpReadDir(req, ino, size, off, &fi, &buffer,
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

    ret = client_->FuseOpReadDir(req, ino, size, off, &fi, &buffer,
                                 &rSize, false);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpRenameBasic) {
    fuse_req_t req = nullptr;
    fuse_ino_t parent = 1;
    std::string name = "A";
    fuse_ino_t newparent = 3;
    std::string newname = "B";
    unsigned int flags = 0;
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
        .WillOnce(Return(CURVEFS_ERROR::NOT_EXIST));

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

    // step8: set txid
    EXPECT_CALL(*metaClient_, SetTxId(srcPartitionId, srcTxId + 1)).Times(1);
    EXPECT_CALL(*metaClient_, SetTxId(dstPartitionId, dstTxId + 1)).Times(1);

    auto rc = client_->FuseOpRename(req, parent, name.c_str(), newparent,
                                    newname.c_str(), flags);
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
    unsigned int flags = 0;

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

    // step8: set txid
    EXPECT_CALL(*metaClient_, SetTxId(partitionId, txId + 1)).Times(2);

    auto rc = client_->FuseOpRename(req, parent, name.c_str(), newparent,
                                    newname.c_str(), flags);
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
    unsigned int flags = 0;

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
                                    newname.c_str(), flags);
    ASSERT_EQ(rc, CURVEFS_ERROR::NOT_EMPTY);
}

TEST_F(TestFuseVolumeClient, FuseOpRenameNameTooLong) {
    fuse_req_t req = nullptr;
    fuse_ino_t parent = 1;
    std::string name1 = "aaaaaaaaaaaaaaaaaaaaa";
    std::string name2 = "xxx";
    fuse_ino_t newparent = 2;
    std::string newname1 = "bbbbbbbbbbbbbbbbbbbbb";
    std::string newname2 = "yyy";
    unsigned int flags = 0;

    CURVEFS_ERROR ret = client_->FuseOpRename(req, parent, name1.c_str(),
                                              newparent, newname1.c_str(),
                                              flags);
    ASSERT_EQ(CURVEFS_ERROR::NAME_TOO_LONG, ret);

    ret = client_->FuseOpRename(req, parent, name1.c_str(), newparent,
                                newname2.c_str(), flags);
    ASSERT_EQ(CURVEFS_ERROR::NAME_TOO_LONG, ret);

    ret = client_->FuseOpRename(req, parent, name2.c_str(), newparent,
                                newname1.c_str(), flags);
    ASSERT_EQ(CURVEFS_ERROR::NAME_TOO_LONG, ret);
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
    unsigned int flags = 0;

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

    // step7: set txid
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
            auto rc = client_->FuseOpRename(req, 1, "A", 1, "B", flags);
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
    AttrOut attrOut;

    InodeAttr inode;
    inode.set_inodeid(ino);
    inode.set_length(0);

    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));

    CURVEFS_ERROR ret = client_->FuseOpGetAttr(req, ino, &fi, &attrOut);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpGetAttrFailed) {
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    AttrOut attrOut;

    InodeAttr inode;
    inode.set_inodeid(ino);
    inode.set_length(0);

    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode),
                        Return(CURVEFS_ERROR::INTERNAL)));

    CURVEFS_ERROR ret = client_->FuseOpGetAttr(req, ino, &fi, &attrOut);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseVolumeClient, FuseOpGetAttrEnableCto) {
    curvefs::client::common::FLAGS_enableCto = true;

    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    AttrOut attrOut;

    InodeAttr inode;
    inode.set_inodeid(ino);
    inode.set_length(0);

    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));

    ASSERT_EQ(CURVEFS_ERROR::OK,
              client_->FuseOpGetAttr(req, ino, &fi, &attrOut));

    // need not refresh inode
    fi.fh = static_cast<uint64_t>(FileHandle::kKeepCache);
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));

    ASSERT_EQ(CURVEFS_ERROR::OK,
              client_->FuseOpGetAttr(req, ino, &fi, &attrOut));
}

TEST_F(TestFuseVolumeClient, FuseOpSetAttr) {
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    struct stat attr;
    int to_set;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    AttrOut attrOut;

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
    ASSERT_EQ(attr.st_mode, attrOut.attr.mode());
    ASSERT_EQ(attr.st_uid, attrOut.attr.uid());
    ASSERT_EQ(attr.st_gid, attrOut.attr.gid());
    ASSERT_EQ(attr.st_size, attrOut.attr.length());
    ASSERT_EQ(attr.st_atime, attrOut.attr.atime());
    ASSERT_EQ(attr.st_mtime, attrOut.attr.mtime());
    ASSERT_EQ(attr.st_ctime, attrOut.attr.ctime());
}

TEST_F(TestFuseVolumeClient, FuseOpSetAttrFailed) {
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    struct stat attr;
    int to_set;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));

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

    AttrOut attrOut;
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

    EntryOut entryOut;
    CURVEFS_ERROR ret = client_->FuseOpSymlink(req, link, parent, name,
                                               &entryOut);
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

    EntryOut entryOut;
    // create inode failed
    CURVEFS_ERROR ret = client_->FuseOpSymlink(req, link, parent, name,
                                               &entryOut);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    EXPECT_CALL(*inodeManager_, DeleteInode(ino))
        .WillOnce(Return(CURVEFS_ERROR::OK))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));

    // create dentry failed
    ret = client_->FuseOpSymlink(req, link, parent, name, &entryOut);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // also delete inode failed
    ret = client_->FuseOpSymlink(req, link, parent, name, &entryOut);
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

    EntryOut entryOut;
    CURVEFS_ERROR ret = client_->FuseOpSymlink(req, link, parent, name,
                                               &entryOut);
    ASSERT_EQ(CURVEFS_ERROR::NAME_TOO_LONG, ret);
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
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    EXPECT_CALL(*metaClient_, GetInodeAttr(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(attr), Return(MetaStatusCode::OK)));

    EXPECT_CALL(*metaClient_, UpdateInodeAttr(_, _, _))
        .WillOnce(Return(MetaStatusCode::OK));

    EntryOut entryOut;
    CURVEFS_ERROR ret = client_->FuseOpLink(req, ino, newparent, newname,
                                            &entryOut);
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

    EntryOut entryOut;
    // get inode failed
    CURVEFS_ERROR ret = client_->FuseOpLink(req, ino, newparent, newname,
                                            &entryOut);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    Inode inode2 = inodeWrapper->GetInode();
    ASSERT_EQ(nlink, inode2.nlink());

    // link failed
    ret = client_->FuseOpLink(req, ino, newparent, newname, &entryOut);
    ASSERT_EQ(CURVEFS_ERROR::UNKNOWN, ret);
    Inode inode3 = inodeWrapper->GetInode();
    ASSERT_EQ(nlink, inode3.nlink());

    // create dentry failed
    ret = client_->FuseOpLink(req, ino, newparent, newname, &entryOut);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    Inode inode4 = inodeWrapper->GetInode();
    ASSERT_EQ(nlink - 1, inode4.nlink());

    // also unlink failed
    ret = client_->FuseOpLink(req, ino, newparent, newname, &entryOut);
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
