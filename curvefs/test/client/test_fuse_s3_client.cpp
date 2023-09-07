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

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <unordered_map>
#include <utility>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/filesystem/error.h"
#include "curvefs/src/client/filesystem/filesystem.h"
#include "curvefs/src/client/filesystem/meta.h"
#include "curvefs/src/client/fuse_s3_client.h"
#include "curvefs/src/client/inode_wrapper.h"
#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "curvefs/src/client/s3/disk_cache_manager_impl.h"
#include "curvefs/src/client/warmup/warmup_manager.h"
#include "curvefs/src/common/define.h"
#include "curvefs/test/client/mock_client_s3.h"
#include "curvefs/test/client/mock_client_s3_adaptor.h"
#include "curvefs/test/client/mock_dentry_cache_mamager.h"
#include "curvefs/test/client/mock_disk_cache_manager.h"
#include "curvefs/test/client/mock_inode_cache_manager.h"
#include "curvefs/test/client/mock_metaserver_client.h"
#include "curvefs/test/client/rpcclient/mock_mds_client.h"
#include "fuse3/fuse_lowlevel.h"

struct fuse_req {
    struct fuse_ctx* ctx;
};

namespace curvefs {
namespace client {
namespace common {
DECLARE_bool(enableCto);
DECLARE_bool(supportKVcache);

DECLARE_uint64(fuseClientAvgWriteBytes);
DECLARE_uint64(fuseClientBurstWriteBytes);
DECLARE_uint64(fuseClientBurstWriteBytesSecs);

DECLARE_uint64(fuseClientAvgReadBytes);
DECLARE_uint64(fuseClientBurstReadBytes);
DECLARE_uint64(fuseClientBurstReadBytesSecs);
}  // namespace common
}  // namespace client
}  // namespace curvefs

namespace curvefs {
namespace client {

using ::curve::common::Configuration;
using ::curvefs::mds::topology::PartitionTxId;
using ::testing::_;
using ::testing::AtLeast;
using ::testing::Contains;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;
using ::testing::SetArrayArgument;

using curvefs::client::common::FileHandle;
using rpcclient::MetaServerClientDone;
using rpcclient::MockMdsClient;
using rpcclient::MockMetaServerClient;

using ::curvefs::client::common::FileSystemOption;
using ::curvefs::client::common::OpenFilesOption;
using ::curvefs::client::filesystem::EntryOut;
using ::curvefs::client::filesystem::FileOut;

#define EQUAL(a) (lhs.a() == rhs.a())

static bool operator==(const Dentry& lhs, const Dentry& rhs) {
    return EQUAL(fsid) && EQUAL(parentinodeid) && EQUAL(name) && EQUAL(txid) &&
           EQUAL(inodeid) && EQUAL(flag);
}

class WarmupManagerS3Test : public warmup::WarmupManagerS3Impl {
 public:
    WarmupManagerS3Test(std::shared_ptr<MetaServerClient> metaClient,
                        std::shared_ptr<InodeCacheManager> inodeManager,
                        std::shared_ptr<DentryCacheManager> dentryManager,
                        std::shared_ptr<FsInfo> fsInfo,
                        warmup::FuseOpReadFunctionType readFunc,
                        warmup::FuseOpReadLinkFunctionType readLinkFunc,
                        std::shared_ptr<KVClientManager> kvClientManager,
                        std::shared_ptr<S3ClientAdaptor> s3Adaptor)
        : warmup::WarmupManagerS3Impl(
              std::move(metaClient), std::move(inodeManager),
              std::move(dentryManager), std::move(fsInfo), std::move(readFunc),
              std::move(readLinkFunc), std::move(kvClientManager),
              std::move(s3Adaptor)) {}
    bool GetInodeSubPathParent(fuse_ino_t inode,
                               std::vector<std::string> subPath,
                               fuse_ino_t* ret, std::string* lastPath,
                               uint32_t* symlink_depth) {
        return warmup::WarmupManagerS3Impl::GetInodeSubPathParent(
            inode, subPath, ret, lastPath, symlink_depth);
    }

    void FetchDentry(fuse_ino_t key, fuse_ino_t ino, const std::string& file,
                     uint32_t symlink_depth) {
        return warmup::WarmupManagerS3Impl::FetchDentry(key, ino, file,
                                                        symlink_depth);
    }
};

class TestFuseS3Client : public ::testing::Test {
 protected:
    TestFuseS3Client() {}
    ~TestFuseS3Client() {}

    virtual void SetUp() {
        Aws::InitAPI(awsOptions_);
        mdsClient_ = std::make_shared<MockMdsClient>();
        metaClient_ = std::make_shared<MockMetaServerClient>();
        s3ClientAdaptor_ = std::make_shared<MockS3ClientAdaptor>();
        inodeManager_ = std::make_shared<MockInodeCacheManager>();
        dentryManager_ = std::make_shared<MockDentryCacheManager>();
        warmupManager_ = std::make_shared<WarmupManagerS3Test>(
            metaClient_, inodeManager_, dentryManager_, nullptr, nullptr,
            nullptr, nullptr, s3ClientAdaptor_);
        client_ = std::make_shared<FuseS3Client>(
            mdsClient_, metaClient_, inodeManager_, dentryManager_,
            s3ClientAdaptor_, warmupManager_);

        auto readFunc = [this](fuse_req_t req, fuse_ino_t ino, size_t size,
                               off_t off, struct fuse_file_info* fi,
                               char* buffer, size_t* rSize) {
            return client_->FuseOpRead(req, ino, size, off, fi, buffer, rSize);
        };
        warmupManager_->SetFuseOpRead(readFunc);
        auto readLinkFunc = [this](fuse_req_t req, fuse_ino_t ino,
                                   std::string* symLink) {
            return client_->FuseOpReadLink(req, ino, symLink);
        };
        warmupManager_->SetFuseOpReadLink(readLinkFunc);

        InitOptionBasic(&fuseClientOption_);
        InitFSInfo(client_);
        fuseClientOption_.s3Opt.s3AdaptrOpt.asyncThreadNum = 1;
        fuseClientOption_.s3Opt.s3AdaptrOpt.userAgent = "S3 Browser";
        fuseClientOption_.dummyServerStartPort = 5000;
        fuseClientOption_.fileSystemOption.maxNameLength = 20u;
        fuseClientOption_.listDentryThreads = 2;
        fuseClientOption_.warmupThreadsNum = 10;
        {  // filesystem option
            auto option = FileSystemOption();
            option.maxNameLength = 20u;
            option.openFilesOption.lruSize = 100;
            option.attrWatcherOption.lruSize = 100;
            fuseClientOption_.fileSystemOption = option;
        }
        auto fsInfo = std::make_shared<FsInfo>();
        fsInfo->set_fsid(fsId);
        fsInfo->set_fsname("s3fs");
        client_->SetFsInfo(fsInfo);
        client_->Init(fuseClientOption_);
        PrepareFsInfo();
    }

    virtual void TearDown() {
        client_->UnInit();
        mdsClient_ = nullptr;
        metaClient_ = nullptr;
        s3ClientAdaptor_ = nullptr;
    }

    void PrepareFsInfo() {
        auto fsInfo = std::make_shared<FsInfo>();
        fsInfo->set_fsid(fsId);
        fsInfo->set_fsname("s3fs");
        fsInfo->set_rootinodeid(1);

        client_->SetFsInfo(fsInfo);
        client_->SetMounted(true);

        warmupManager_->SetFsInfo(fsInfo);
    }

    void InitOptionBasic(FuseClientOption* opt) {
        opt->s3Opt.s3ClientAdaptorOpt.readCacheThreads = 2;
        opt->s3Opt.s3ClientAdaptorOpt.writeCacheMaxByte = 838860800;
        opt->s3Opt.s3AdaptrOpt.asyncThreadNum = 1;
        opt->s3Opt.s3AdaptrOpt.userAgent = "S3 Browser";
        opt->dummyServerStartPort = 5000;
        opt->fileSystemOption.maxNameLength = 20u;
        opt->listDentryThreads = 2;
    }

    void InitFSInfo(std::shared_ptr<FuseS3Client> client) {
        auto fsInfo = std::make_shared<FsInfo>();
        fsInfo->set_fsid(fsId);
        fsInfo->set_fsname("s3fs");
        client->SetFsInfo(fsInfo);
    }

 protected:
    const uint32_t fsId = 100u;

    std::shared_ptr<MockMdsClient> mdsClient_;
    std::shared_ptr<MockMetaServerClient> metaClient_;
    std::shared_ptr<MockS3ClientAdaptor> s3ClientAdaptor_;
    std::shared_ptr<MockInodeCacheManager> inodeManager_;
    std::shared_ptr<MockDentryCacheManager> dentryManager_;
    std::shared_ptr<FuseS3Client> client_;
    FuseClientOption fuseClientOption_;
    Aws::SDKOptions awsOptions_;
    std::shared_ptr<WarmupManagerS3Test> warmupManager_;
};

TEST_F(TestFuseS3Client, test_Init_with_KVCache) {
    curvefs::client::common::FLAGS_supportKVcache = true;
    curvefs::mds::topology::MemcacheClusterInfo memcacheCluster;
    memcacheCluster.set_clusterid(1);
    auto testclient = std::make_shared<FuseS3Client>(
        mdsClient_, metaClient_, inodeManager_, dentryManager_,
        s3ClientAdaptor_, nullptr);
    FuseClientOption opt;
    InitOptionBasic(&opt);
    InitFSInfo(testclient);
    // testclient->SetMounted(true);

    // test init kvcache success
    {
        EXPECT_CALL(*mdsClient_, AllocOrGetMemcacheCluster(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(memcacheCluster), Return(true)));

        ASSERT_EQ(CURVEFS_ERROR::OK, testclient->Init(opt));

        testclient->UnInit();
    }

    // test init kvcache fail
    {
        EXPECT_CALL(*mdsClient_, AllocOrGetMemcacheCluster(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(memcacheCluster), Return(false)));

        ASSERT_EQ(CURVEFS_ERROR::INTERNAL, testclient->Init(opt));
        testclient->UnInit();
    }
    curvefs::client::common::FLAGS_supportKVcache = false;
}

TEST_F(TestFuseS3Client, test_Init_with_cache_size_0) {
    curvefs::client::common::FLAGS_supportKVcache = false;
    auto testClient = std::make_shared<FuseS3Client>(
        mdsClient_, metaClient_, inodeManager_, dentryManager_,
        s3ClientAdaptor_, nullptr);
    FuseClientOption opt;
    InitOptionBasic(&opt);
    InitFSInfo(testClient);

    // test init when write cache is 0
    opt.s3Opt.s3ClientAdaptorOpt.writeCacheMaxByte = 0;
    ASSERT_EQ(CURVEFS_ERROR::CACHE_TOO_SMALL, testClient->Init(opt));
    testClient->UnInit();
}

// GetInode failed; bad fd
TEST_F(TestFuseS3Client, warmUp_inodeBadFd) {
    sleep(1);
    std::string name = "test";
    fuse_ino_t inodeid = 2;

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_S3);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWrapper),
                        Return(CURVEFS_ERROR::BAD_FD)));
    auto old = client_->GetFsInfo()->fstype();
    client_->GetFsInfo()->set_fstype(FSType::TYPE_S3);
    client_->PutWarmFilelistTask(
        inodeid,
        curvefs::client::common::WarmupStorageType::kWarmupStorageTypeDisk, "",
        "", "");
    warmup::WarmupProgress progress;
    bool ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    sleep(5);
    client_->GetFsInfo()->set_fstype(old);
    ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    // After sleeping for 5s, the scan should be completed
    ASSERT_FALSE(ret);
}

// single file (parent is root)
TEST_F(TestFuseS3Client, warmUp_Warmfile_error_GetDentry01) {
    sleep(1);
    fuse_ino_t parent = 1;
    std::string name = "test";
    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);
    dentry.set_type(FsFileType::TYPE_S3);

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_S3);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::BAD_FD)));
    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    size_t len = 20;
    char* tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = 't';
    tmpbuf[2] = 'e';
    tmpbuf[3] = '\n';

    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(
            DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len), Return(len)));

    auto old = client_->GetFsInfo()->fstype();
    client_->GetFsInfo()->set_fstype(FSType::TYPE_S3);
    client_->PutWarmFilelistTask(
        inodeid,
        curvefs::client::common::WarmupStorageType::kWarmupStorageTypeDisk, "",
        "", "");

    warmup::WarmupProgress progress;
    bool ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    ASSERT_TRUE(ret);
    sleep(5);
    client_->GetFsInfo()->set_fstype(old);
    ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    // After sleeping for 5s, the scan should be completed
    ASSERT_FALSE(ret);
}

// warmup failed because of GetDentry failed
TEST_F(TestFuseS3Client, warmUp_Warmfile_error_GetDentry02) {
    sleep(1);
    fuse_ino_t parent = 1;
    std::string name = "test";
    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);
    dentry.set_type(FsFileType::TYPE_S3);

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_S3);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::NOT_EXIST)));
    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    size_t len = 20;
    char* tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = 't';
    tmpbuf[2] = 'e';
    tmpbuf[3] = '\n';

    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(
            DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len), Return(len)));
    auto old = client_->GetFsInfo()->fstype();
    client_->GetFsInfo()->set_fstype(FSType::TYPE_S3);
    client_->PutWarmFilelistTask(
        inodeid,
        curvefs::client::common::WarmupStorageType::kWarmupStorageTypeDisk, "",
        "", "");

    warmup::WarmupProgress progress;
    bool ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    ASSERT_TRUE(ret);
    sleep(5);
    ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    // After sleeping for 5s, the scan should be completed
    ASSERT_FALSE(ret);
    client_->GetFsInfo()->set_fstype(old);
}

// warmup failed because of Getinode failed
TEST_F(TestFuseS3Client, warmUp_fetchDataEnqueue__error_getinode) {
    sleep(1);
    fuse_ino_t parent = 1;
    std::string name = "test";
    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);
    dentry.set_type(FsFileType::TYPE_S3);

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_S3);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(
            SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::NOT_EXIST)));

    size_t len = 20;
    char* tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = 't';
    tmpbuf[2] = 'e';
    tmpbuf[3] = '\n';
    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(
            DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len), Return(len)));
    auto old = client_->GetFsInfo()->fstype();
    client_->GetFsInfo()->set_fstype(FSType::TYPE_S3);
    client_->PutWarmFilelistTask(
        inodeid,
        curvefs::client::common::WarmupStorageType::kWarmupStorageTypeDisk, "",
        "", "");

    warmup::WarmupProgress progress;
    bool ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    ASSERT_TRUE(ret);
    client_->GetFsInfo()->set_fstype(old);
    sleep(5);
    ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    // After sleeping for 5s, the scan should be completed
    ASSERT_FALSE(ret);
}

// chunk is empty
TEST_F(TestFuseS3Client, warmUp_fetchDataEnqueue_chunkempty) {
    sleep(1);
    fuse_ino_t parent = 1;
    std::string name = "test";
    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);
    dentry.set_type(FsFileType::TYPE_S3);

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_S3);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    size_t len = 20;
    char* tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = 't';
    tmpbuf[2] = 'e';
    tmpbuf[3] = '\n';
    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(
            DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len), Return(len)));
    auto old = client_->GetFsInfo()->fstype();
    client_->GetFsInfo()->set_fstype(FSType::TYPE_S3);
    client_->PutWarmFilelistTask(
        inodeid,
        curvefs::client::common::WarmupStorageType::kWarmupStorageTypeDisk, "",
        "", "");

    warmup::WarmupProgress progress;
    bool ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    ASSERT_TRUE(ret);
    client_->GetFsInfo()->set_fstype(old);
    sleep(5);
    ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    // After sleeping for 5s, the scan should be completed
    ASSERT_FALSE(ret);
}

// single file (parent is root); FetchDentry
TEST_F(TestFuseS3Client, warmUp_FetchDentry_TYPE_SYM_LINK) {
    sleep(1);
    fuse_ino_t parent = 1;
    std::string name = "test";
    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);
    dentry.set_type(FsFileType::TYPE_S3);

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_S3);

    Dentry dentry1;
    dentry1.set_fsid(fsId);
    dentry1.set_inodeid(3);
    dentry1.set_name("3");
    dentry1.set_type(FsFileType::TYPE_SYM_LINK);

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(dentry1), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    size_t len = 20;
    char* tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = 't';
    tmpbuf[2] = 'e';
    tmpbuf[3] = '\n';
    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(
            DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len), Return(len)));
    auto old = client_->GetFsInfo()->fstype();
    client_->GetFsInfo()->set_fstype(FSType::TYPE_S3);
    client_->PutWarmFilelistTask(
        inodeid,
        curvefs::client::common::WarmupStorageType::kWarmupStorageTypeDisk, "",
        "", "");

    warmup::WarmupProgress progress;
    bool ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    ASSERT_TRUE(ret);
    client_->GetFsInfo()->set_fstype(old);
    sleep(5);
    ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    // After sleeping for 5s, the scan should be completed
    ASSERT_FALSE(ret);
    client_->GetFsInfo()->set_fstype(old);
}

// fetch dentry failed
TEST_F(TestFuseS3Client, warmUp_FetchDentry_error_TYPE_DIRECTORY) {
    sleep(1);
    fuse_ino_t parent = 1;
    std::string name = "test";
    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);
    dentry.set_type(FsFileType::TYPE_S3);

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_S3);

    Dentry dentry1;
    dentry1.set_fsid(fsId);
    dentry1.set_inodeid(3);
    dentry1.set_name("3");
    dentry1.set_type(FsFileType::TYPE_DIRECTORY);

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(dentry1), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    std::list<Dentry> dlist;
    size_t len = 20;
    char* tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = 't';
    tmpbuf[2] = 'e';
    tmpbuf[3] = '\n';
    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(
            DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len), Return(len)));
    auto old = client_->GetFsInfo()->fstype();
    client_->GetFsInfo()->set_fstype(FSType::TYPE_S3);
    client_->PutWarmFilelistTask(
        inodeid,
        curvefs::client::common::WarmupStorageType::kWarmupStorageTypeDisk, "",
        "", "");

    warmup::WarmupProgress progress;
    bool ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    ASSERT_TRUE(ret);
    client_->GetFsInfo()->set_fstype(old);
    sleep(5);
    ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    // After sleeping for 5s, the scan should be completed
    ASSERT_FALSE(ret);
}

// lookpath
TEST_F(TestFuseS3Client, warmUp_lookpath_multilevel) {
    sleep(1);
    fuse_ino_t parent = 1;
    std::string name = "test";
    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);
    dentry.set_type(FsFileType::TYPE_S3);

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_S3);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    size_t len = 20;
    char* tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = 'a';
    tmpbuf[2] = '/';
    tmpbuf[3] = 'b';
    tmpbuf[4] = '/';
    tmpbuf[5] = 'c';
    tmpbuf[6] = '\n';
    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(
            DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len), Return(len)));
    auto old = client_->GetFsInfo()->fstype();
    client_->GetFsInfo()->set_fstype(FSType::TYPE_S3);
    client_->PutWarmFilelistTask(
        inodeid,
        curvefs::client::common::WarmupStorageType::kWarmupStorageTypeDisk, "",
        "", "");

    warmup::WarmupProgress progress;
    bool ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    ASSERT_TRUE(ret);
    client_->GetFsInfo()->set_fstype(old);
    sleep(5);
    ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    // After sleeping for 5s, the scan should be completed
    ASSERT_FALSE(ret);
}

// lookpath failed; unknown path
TEST_F(TestFuseS3Client, warmUp_lookpath_unkown) {
    sleep(1);
    fuse_ino_t parent = 1;
    std::string name = "test";
    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);
    dentry.set_type(FsFileType::TYPE_S3);

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_S3);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    size_t len = 20;
    char* tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(
            DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len), Return(len)));
    auto old = client_->GetFsInfo()->fstype();
    client_->GetFsInfo()->set_fstype(FSType::TYPE_S3);
    client_->PutWarmFilelistTask(
        inodeid,
        curvefs::client::common::WarmupStorageType::kWarmupStorageTypeDisk, "",
        "", "");

    warmup::WarmupProgress progress;
    bool ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    ASSERT_TRUE(ret);
    client_->GetFsInfo()->set_fstype(old);
    sleep(5);
    ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    // After sleeping for 5s, the scan should be completed
    ASSERT_FALSE(ret);
}

// i am root
TEST_F(TestFuseS3Client, warmUp_FetchChildDentry_error_ListDentry) {
    sleep(1);
    fuse_ino_t parent = 1;
    std::string name = "test";
    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);
    dentry.set_type(FsFileType::TYPE_S3);

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_S3);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    std::list<Dentry> dlist;
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::NOT_EXIST)));

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    size_t len = 20;
    char* tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = '\n';
    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(
            DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len), Return(len)));
    auto old = client_->GetFsInfo()->fstype();
    client_->GetFsInfo()->set_fstype(FSType::TYPE_S3);
    client_->PutWarmFilelistTask(
        inodeid,
        curvefs::client::common::WarmupStorageType::kWarmupStorageTypeDisk, "",
        "", "");

    warmup::WarmupProgress progress;
    bool ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    ASSERT_TRUE(ret);
    client_->GetFsInfo()->set_fstype(old);
    sleep(5);
    ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    // After sleeping for 5s, the scan should be completed
    ASSERT_FALSE(ret);
}

// success
/*
inode 1 (parent)
|
|- inode 5 (test, directory)
|- inode 2 ("2", file)
|- inode 3 (file, unknown type)
\- inode 4 (file, unknown type)
 */
TEST_F(TestFuseS3Client, warmUp_FetchChildDentry_suc_ListDentry) {
    sleep(1);
    fuse_ino_t parent = 1;
    std::string name = "test";
    fuse_ino_t inodeid = 2;

    Dentry dentry2;
    dentry2.set_fsid(fsId);
    dentry2.set_name(name);
    dentry2.set_parentinodeid(parent);
    dentry2.set_inodeid(inodeid);
    dentry2.set_name("2");
    dentry2.set_type(FsFileType::TYPE_S3);

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_S3);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    std::list<Dentry> dlist;
    Dentry dentry5;
    dentry5.set_fsid(fsId);
    dentry5.set_inodeid(inodeid);
    dentry5.set_parentinodeid(parent);
    dentry5.set_name("5");
    dentry5.set_type(FsFileType::TYPE_DIRECTORY);
    dlist.emplace_back(dentry5);

    dlist.emplace_back(dentry2);

    Dentry dentry3;
    dentry3.set_inodeid(3);
    dentry3.set_parentinodeid(parent);
    dentry3.set_name("3");
    dentry3.set_type(FsFileType::TYPE_FILE);
    dlist.emplace_back(dentry3);

    Dentry dentry4;
    dentry4.set_inodeid(4);
    dentry4.set_parentinodeid(parent);
    dentry4.set_name("4");
    dentry4.set_type(FsFileType::TYPE_FILE);  // unknown type
    dlist.emplace_back(dentry4);

    auto GetDentryReplace = [=](fuse_ino_t ino, const std::string& file,
                                Dentry* dentry) {
        if (ino == parent) {
            if (file == "5") {
                *dentry = dentry5;
                return CURVEFS_ERROR::OK;
            } else if (file == "2") {
                *dentry = dentry2;
                return CURVEFS_ERROR::OK;
            } else if (file == "3") {
                *dentry = dentry3;
                return CURVEFS_ERROR::OK;
            } else if (file == "4") {
                *dentry = dentry4;
                return CURVEFS_ERROR::OK;
            }
        }
        return CURVEFS_ERROR::NOT_EXIST;
    };

    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::NOT_EXIST)));

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillRepeatedly(Invoke(GetDentryReplace));

    size_t len = 20;
    char* tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = '\n';
    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(
            DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len), Return(len)));
    auto old = client_->GetFsInfo()->fstype();
    client_->GetFsInfo()->set_fstype(FSType::TYPE_S3);
    client_->PutWarmFilelistTask(
        inodeid,
        curvefs::client::common::WarmupStorageType::kWarmupStorageTypeDisk, "",
        "", "");

    warmup::WarmupProgress progress;
    bool ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    ASSERT_TRUE(ret);
    client_->GetFsInfo()->set_fstype(old);
    sleep(5);
    ret = client_->GetWarmupProgress(inodeid, &progress);
    LOG(INFO) << "ret:" << ret << " Warmup progress: " << progress.ToString();
    // After sleeping for 5s, the scan should be completed
    ASSERT_FALSE(ret);
}

TEST_F(TestFuseS3Client, warmUp_GetInodeSubPathParent_empty) {
    /*
.(1) parent
    */
    fuse_ino_t inode = 0;
    std::string lastPath;
    ASSERT_EQ(warmupManager_->GetInodeSubPathParent(
                  0, std::vector<std::string>(), &inode, &lastPath, nullptr),
              false);
}

TEST_F(TestFuseS3Client, warmUp_GetInodeSubPathParent_1depth) {
    /*
.(1) parent
├── A(2)
    */
    constexpr fuse_ino_t root = 1;
    constexpr fuse_ino_t aInodeid = 2;
    fuse_ino_t ret = 0;

    std::string aName = "A";
    Dentry dentryA;
    dentryA.set_fsid(fsId);
    dentryA.set_name(aName);
    dentryA.set_parentinodeid(root);
    dentryA.set_inodeid(aInodeid);
    dentryA.set_type(FsFileType::TYPE_S3);

    Inode inodeA;
    inodeA.set_fsid(fsId);
    inodeA.set_inodeid(aInodeid);
    inodeA.add_parent(root);
    std::shared_ptr<InodeWrapper> inodeWrapperA =
        std::make_shared<InodeWrapper>(inodeA, metaClient_);

    auto GetInodeReplace =
        [aInodeid, inodeWrapperA](
            uint64_t inodeId,
            std::shared_ptr<InodeWrapper>& out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case aInodeid:
                out = inodeWrapperA;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillRepeatedly(Invoke(GetInodeReplace));

    auto GetDentryReplace = [=](fuse_ino_t parent, const std::string& name,
                                Dentry* out) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                if (name == aName) {
                    *out = dentryA;
                    return CURVEFS_ERROR::OK;
                }
                break;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::OK;
    };

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillOnce(Invoke(GetDentryReplace));

    auto ListDentryReplace = [=](uint64_t parent, std::list<Dentry>* dentryList,
                                 uint32_t limit, bool onlyDir = false,
                                 uint32_t nlink = 0) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                dentryList->emplace_back(dentryA);
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::NOT_EXIST;
    };
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(Invoke(ListDentryReplace));

    std::string lastPath;
    ASSERT_TRUE(warmupManager_->GetInodeSubPathParent(
        root, std::vector<std::string>{aName}, &ret, &lastPath, nullptr));
    ASSERT_EQ(ret, root);
    ASSERT_EQ(lastPath, aName);
}

TEST_F(TestFuseS3Client, warmUp_GetInodeSubPathParent_2depth) {
    /*
.(1) parent
└── A(2)
    ├── B(3)
    */
    constexpr fuse_ino_t root = 1;
    fuse_ino_t ret = 0;

    std::string aName = "A";
    constexpr fuse_ino_t aInodeid = 2;
    Dentry dentryA;
    dentryA.set_fsid(fsId);
    dentryA.set_name(aName);
    dentryA.set_parentinodeid(root);
    dentryA.set_inodeid(aInodeid);
    dentryA.set_type(FsFileType::TYPE_DIRECTORY);

    std::string bName = "B";
    constexpr fuse_ino_t bInodeid = 3;
    Dentry dentryB;
    dentryB.set_fsid(fsId);
    dentryB.set_name(bName);
    dentryB.set_parentinodeid(dentryA.inodeid());
    dentryB.set_inodeid(bInodeid);
    dentryB.set_type(FsFileType::TYPE_DIRECTORY);
    Inode inodeB;
    inodeB.set_fsid(fsId);
    inodeB.set_inodeid(aInodeid);
    inodeB.add_parent(aInodeid);
    std::shared_ptr<InodeWrapper> inodeWrapperB =
        std::make_shared<InodeWrapper>(inodeB, metaClient_);

    auto GetDentryReplace = [=](fuse_ino_t parent, const std::string& name,
                                Dentry* out) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                if (name == aName) {
                    *out = dentryA;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case aInodeid:
                if (name == bName) {
                    *out = dentryB;
                    return CURVEFS_ERROR::OK;
                }
                break;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::OK;
    };

    auto GetInodeReplace =
        [bInodeid, inodeWrapperB](
            uint64_t inodeId,
            std::shared_ptr<InodeWrapper>& out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case bInodeid:
                out = inodeWrapperB;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillRepeatedly(Invoke(GetInodeReplace));

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillOnce(Invoke(GetDentryReplace))
        .WillOnce(Invoke(GetDentryReplace));

    auto ListDentryReplace = [=](uint64_t parent, std::list<Dentry>* dentryList,
                                 uint32_t limit, bool onlyDir = false,
                                 uint32_t nlink = 0) -> CURVEFS_ERROR {
        switch (parent) {
            case aInodeid:
                dentryList->emplace_back(dentryB);
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::NOT_EXIST;
    };
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(Invoke(ListDentryReplace));

    std::string lastName;
    ASSERT_EQ(warmupManager_->GetInodeSubPathParent(
                  root, std::vector<std::string>{aName, bName}, &ret, &lastName,
                  nullptr),
              true);
    ASSERT_EQ(ret, aInodeid);
    ASSERT_EQ(lastName, bName);
}

TEST_F(TestFuseS3Client, warmUp_GetInodeSubPathParent_getDentryFailed) {
    /*
.(1) parent
└── A(2)
    ├── B(3)
    */
    constexpr fuse_ino_t root = 1;
    fuse_ino_t ret = 0;

    std::string aName = "A";
    constexpr fuse_ino_t aInodeid = 2;
    Dentry dentryA;
    dentryA.set_fsid(fsId);
    dentryA.set_name(aName);
    dentryA.set_parentinodeid(root);
    dentryA.set_inodeid(aInodeid);
    dentryA.set_type(FsFileType::TYPE_DIRECTORY);

    std::string bName = "B";
    constexpr fuse_ino_t bInodeid = 3;
    Dentry dentryB;
    dentryB.set_fsid(fsId);
    dentryB.set_name(bName);
    dentryB.set_parentinodeid(dentryA.inodeid());
    dentryB.set_inodeid(bInodeid);
    dentryB.set_type(FsFileType::TYPE_DIRECTORY);

    auto GetDentryReplace = [](fuse_ino_t parent, const std::string& name,
                               Dentry* out) -> CURVEFS_ERROR {
        switch (parent) {
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::OK;
    };

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillOnce(Invoke(GetDentryReplace));

    std::string lastName;
    ASSERT_EQ(warmupManager_->GetInodeSubPathParent(
                  root, std::vector<std::string>{aName, bName}, &ret, &lastName,
                  nullptr),
              false);
}

TEST_F(TestFuseS3Client, warmUp_GetInodeSubPathParent_3depth) {
    /*
.(1) parent
└── A(2)
│   ├── B(3)
│   │   ├──C(4)
    */
    constexpr fuse_ino_t root = 1;
    fuse_ino_t ret = 0;

    std::string aName = "A";
    constexpr fuse_ino_t aInodeid = 2;
    Dentry dentryA;
    dentryA.set_fsid(fsId);
    dentryA.set_name(aName);
    dentryA.set_parentinodeid(root);
    dentryA.set_inodeid(aInodeid);
    dentryA.set_type(FsFileType::TYPE_DIRECTORY);

    std::string bName = "B";
    constexpr fuse_ino_t bInodeid = 3;
    Dentry dentryB;
    dentryB.set_fsid(fsId);
    dentryB.set_name(bName);
    dentryB.set_parentinodeid(dentryA.inodeid());
    dentryB.set_inodeid(bInodeid);
    dentryB.set_type(FsFileType::TYPE_DIRECTORY);

    std::string cName = "C";
    constexpr fuse_ino_t cInodeid = 4;
    Dentry dentryC;
    dentryC.set_fsid(fsId);
    dentryC.set_name(cName);
    dentryC.set_parentinodeid(dentryB.inodeid());
    dentryC.set_inodeid(cInodeid);
    dentryC.set_type(FsFileType::TYPE_DIRECTORY);
    Inode inodeC;
    inodeC.set_fsid(fsId);
    inodeC.set_inodeid(cInodeid);
    inodeC.add_parent(dentryB.inodeid());
    std::shared_ptr<InodeWrapper> inodeWrapperC =
        std::make_shared<InodeWrapper>(inodeC, metaClient_);

    auto GetDentryReplace = [root, aName, dentryA, aInodeid, bName, dentryB,
                             bInodeid, cName, dentryC](
                                fuse_ino_t parent, const std::string& name,
                                Dentry* out) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                if (name == aName) {
                    *out = dentryA;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case aInodeid:
                if (name == bName) {
                    *out = dentryB;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case bInodeid:
                if (name == cName) {
                    *out = dentryC;
                    return CURVEFS_ERROR::OK;
                }
                break;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::OK;
    };

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillRepeatedly(Invoke(GetDentryReplace));

    auto GetInodeReplace =
        [cInodeid, inodeWrapperC](
            uint64_t inodeId,
            std::shared_ptr<InodeWrapper>& out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case cInodeid:
                out = inodeWrapperC;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillRepeatedly(Invoke(GetInodeReplace));

    auto ListDentryReplace = [=](uint64_t parent, std::list<Dentry>* dentryList,
                                 uint32_t limit, bool onlyDir = false,
                                 uint32_t nlink = 0) -> CURVEFS_ERROR {
        switch (parent) {
            case bInodeid:
                dentryList->emplace_back(dentryC);
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::NOT_EXIST;
    };
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(Invoke(ListDentryReplace));

    std::string lastName;
    ASSERT_EQ(warmupManager_->GetInodeSubPathParent(
                  root, std::vector<std::string>{aName, bName, cName}, &ret,
                  &lastName, nullptr),
              true);
    ASSERT_EQ(ret, bInodeid);
    ASSERT_EQ(lastName, cName);
}

TEST_F(TestFuseS3Client,
       warmUp_GetInodeSubPathParent_symLink_getInodeAttrFailed) {
    /*
.(1) parent
└── A(2)
│   ├── B(3)
│   │   └── D(5)
│   ├── C(4) -> B
│   │   └── D(5)
    */
    constexpr fuse_ino_t root = 1;
    fuse_ino_t ret = 0;

    std::string aName = "A";
    constexpr fuse_ino_t aInodeid = 2;
    Dentry dentryA;
    dentryA.set_fsid(fsId);
    dentryA.set_name(aName);
    dentryA.set_parentinodeid(root);
    dentryA.set_inodeid(aInodeid);
    dentryA.set_type(FsFileType::TYPE_DIRECTORY);

    std::string bName = "B";
    constexpr fuse_ino_t bInodeid = 3;
    Dentry dentryB;
    dentryB.set_fsid(fsId);
    dentryB.set_name(bName);
    dentryB.set_parentinodeid(dentryA.inodeid());
    dentryB.set_inodeid(bInodeid);
    dentryB.set_type(FsFileType::TYPE_DIRECTORY);

    std::string cName = "C";
    constexpr fuse_ino_t cInodeid = 4;
    Dentry dentryC;
    dentryC.set_fsid(fsId);
    dentryC.set_name(cName);
    dentryC.set_parentinodeid(dentryA.inodeid());
    dentryC.set_inodeid(cInodeid);
    dentryC.set_type(FsFileType::TYPE_SYM_LINK);

    std::string dName = "D";
    constexpr fuse_ino_t dInodeid = 5;
    Dentry dentryD;
    dentryD.set_fsid(fsId);
    dentryD.set_name(dName);
    dentryD.set_parentinodeid(dentryB.inodeid());
    dentryD.set_inodeid(dInodeid);
    dentryD.set_type(FsFileType::TYPE_DIRECTORY);

    auto GetDentryReplace = [root, aName, dentryA, aInodeid, bName, dentryB,
                             bInodeid, cName, dentryC, dName, dentryD](
                                fuse_ino_t parent, const std::string& name,
                                Dentry* out) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                if (name == aName) {
                    *out = dentryA;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case aInodeid:
                if (name == bName) {
                    *out = dentryB;
                    return CURVEFS_ERROR::OK;
                } else if (name == cName) {
                    *out = dentryC;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case bInodeid:
                if (name == dName) {
                    *out = dentryD;
                    return CURVEFS_ERROR::OK;
                }
                break;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::OK;
    };

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillRepeatedly(Invoke(GetDentryReplace));

    auto GetInodeAttrReplace = [](uint64_t inodeId,
                                  InodeAttr* out) -> CURVEFS_ERROR {
        switch (inodeId) {
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .WillOnce(Invoke(GetInodeAttrReplace));

    std::string lastName;
    ASSERT_EQ(warmupManager_->GetInodeSubPathParent(
                  root, std::vector<std::string>{aName, cName, dName}, &ret,
                  &lastName, nullptr),
              false);
}

TEST_F(TestFuseS3Client, warmUp_FetchDentry_symLink_getInodeAttrFailed) {
    /*
.(1) parent
└── A(2)
│   ├── B(3)
│   │   └── D(5)
│   ├── C(4) -> B
│   │   └── D(5)
    */
    constexpr fuse_ino_t root = 1;
    fuse_ino_t ret = 0;

    std::string aName = "A";
    constexpr fuse_ino_t aInodeid = 2;
    Dentry dentryA;
    dentryA.set_fsid(fsId);
    dentryA.set_name(aName);
    dentryA.set_parentinodeid(root);
    dentryA.set_inodeid(aInodeid);
    dentryA.set_type(FsFileType::TYPE_DIRECTORY);

    std::string bName = "B";
    constexpr fuse_ino_t bInodeid = 3;
    Dentry dentryB;
    dentryB.set_fsid(fsId);
    dentryB.set_name(bName);
    dentryB.set_parentinodeid(dentryA.inodeid());
    dentryB.set_inodeid(bInodeid);
    dentryB.set_type(FsFileType::TYPE_DIRECTORY);

    std::string cName = "C";
    constexpr fuse_ino_t cInodeid = 4;
    Dentry dentryC;
    dentryC.set_fsid(fsId);
    dentryC.set_name(cName);
    dentryC.set_parentinodeid(dentryA.inodeid());
    dentryC.set_inodeid(cInodeid);
    dentryC.set_type(FsFileType::TYPE_SYM_LINK);

    std::string dName = "D";
    constexpr fuse_ino_t dInodeid = 5;
    Dentry dentryD;
    dentryD.set_fsid(fsId);
    dentryD.set_name(dName);
    dentryD.set_parentinodeid(dentryB.inodeid());
    dentryD.set_inodeid(dInodeid);
    dentryD.set_type(FsFileType::TYPE_DIRECTORY);

    auto GetDentryReplace = [root, aName, dentryA, aInodeid, bName, dentryB,
                             bInodeid, cName, dentryC, dName, dentryD](
                                fuse_ino_t parent, const std::string& name,
                                Dentry* out) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                if (name == aName) {
                    *out = dentryA;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case aInodeid:
                if (name == bName) {
                    *out = dentryB;
                    return CURVEFS_ERROR::OK;
                } else if (name == cName) {
                    *out = dentryC;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case bInodeid:
                if (name == dName) {
                    *out = dentryD;
                    return CURVEFS_ERROR::OK;
                }
                break;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::OK;
    };

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillRepeatedly(Invoke(GetDentryReplace));

    auto GetInodeAttrReplace = [](uint64_t inodeId,
                                  InodeAttr* out) -> CURVEFS_ERROR {
        switch (inodeId) {
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .WillOnce(Invoke(GetInodeAttrReplace));

    warmupManager_->FetchDentry(123, aInodeid, cName, 0);
    sleep(5);
}

TEST_F(TestFuseS3Client, warmUp_GetInodeSubPathParent_symLink_1depth) {
    /*
.(1) parent
└── A(2)
│   ├── B(3)
│   │   └── D(5)
│   ├── C(4) -> B
│   │   └── D(5)
    */
    constexpr fuse_ino_t root = 1;
    fuse_ino_t ret = 0;

    std::string aName = "A";
    constexpr fuse_ino_t aInodeid = 2;
    Dentry dentryA;
    dentryA.set_fsid(fsId);
    dentryA.set_name(aName);
    dentryA.set_parentinodeid(root);
    dentryA.set_inodeid(aInodeid);
    dentryA.set_type(FsFileType::TYPE_DIRECTORY);

    std::string bName = "B";
    constexpr fuse_ino_t bInodeid = 3;
    Dentry dentryB;
    dentryB.set_fsid(fsId);
    dentryB.set_name(bName);
    dentryB.set_parentinodeid(dentryA.inodeid());
    dentryB.set_inodeid(bInodeid);
    dentryB.set_type(FsFileType::TYPE_DIRECTORY);

    std::string cName = "C";
    constexpr fuse_ino_t cInodeid = 4;
    Dentry dentryC;
    dentryC.set_fsid(fsId);
    dentryC.set_name(cName);
    dentryC.set_parentinodeid(dentryA.inodeid());
    dentryC.set_inodeid(cInodeid);
    dentryC.set_type(FsFileType::TYPE_SYM_LINK);
    InodeAttr attrC;
    attrC.set_symlink(bName);

    std::string dName = "D";
    constexpr fuse_ino_t dInodeid = 5;
    Dentry dentryD;
    dentryD.set_fsid(fsId);
    dentryD.set_name(dName);
    dentryD.set_parentinodeid(dentryB.inodeid());
    dentryD.set_inodeid(dInodeid);
    dentryD.set_type(FsFileType::TYPE_DIRECTORY);
    Inode inodeD;
    inodeD.set_fsid(fsId);
    inodeD.set_inodeid(dInodeid);
    inodeD.add_parent(dentryB.inodeid());
    std::shared_ptr<InodeWrapper> inodeWrapperD =
        std::make_shared<InodeWrapper>(inodeD, metaClient_);

    auto GetDentryReplace = [root, aName, dentryA, aInodeid, bName, dentryB,
                             bInodeid, cName, dentryC, dName, dentryD](
                                fuse_ino_t parent, const std::string& name,
                                Dentry* out) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                if (name == aName) {
                    *out = dentryA;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case aInodeid:
                if (name == bName) {
                    *out = dentryB;
                    return CURVEFS_ERROR::OK;
                } else if (name == cName) {
                    *out = dentryC;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case bInodeid:
                if (name == dName) {
                    *out = dentryD;
                    return CURVEFS_ERROR::OK;
                }
                break;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::OK;
    };

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillRepeatedly(Invoke(GetDentryReplace));

    auto GetInodeAttrReplace =
        [cInodeid, attrC](uint64_t inodeId, InodeAttr* out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case cInodeid:
                *out = attrC;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .WillOnce(Invoke(GetInodeAttrReplace));

    auto GetInodeReplace =
        [dInodeid, inodeWrapperD](
            uint64_t inodeId,
            std::shared_ptr<InodeWrapper>& out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case dInodeid:
                out = inodeWrapperD;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillRepeatedly(Invoke(GetInodeReplace));

    auto ListDentryReplace = [=](uint64_t parent, std::list<Dentry>* dentryList,
                                 uint32_t limit, bool onlyDir = false,
                                 uint32_t nlink = 0) -> CURVEFS_ERROR {
        switch (parent) {
            case bInodeid:
                dentryList->emplace_back(dentryD);
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::NOT_EXIST;
    };
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(Invoke(ListDentryReplace));

    std::string lastName;
    ASSERT_EQ(warmupManager_->GetInodeSubPathParent(
                  root, std::vector<std::string>{aName, cName, dName}, &ret,
                  &lastName, nullptr),
              true);
    ASSERT_EQ(ret, bInodeid);
    ASSERT_EQ(lastName, dName);
}

TEST_F(TestFuseS3Client, warmUp_GetInodeSubPathParent_symLink_1depth_samePath) {
    /*
.(1) parent
└── A(2)
│   ├── B(3)
│   │   └── D(5)
│   ├── C(4) -> ./B
│   │   └── D(5)
    */
    constexpr fuse_ino_t root = 1;
    fuse_ino_t ret = 0;

    std::string aName = "A";
    constexpr fuse_ino_t aInodeid = 2;
    Dentry dentryA;
    dentryA.set_fsid(fsId);
    dentryA.set_name(aName);
    dentryA.set_parentinodeid(root);
    dentryA.set_inodeid(aInodeid);
    dentryA.set_type(FsFileType::TYPE_DIRECTORY);

    std::string bName = "B";
    constexpr fuse_ino_t bInodeid = 3;
    Dentry dentryB;
    dentryB.set_fsid(fsId);
    dentryB.set_name(bName);
    dentryB.set_parentinodeid(dentryA.inodeid());
    dentryB.set_inodeid(bInodeid);
    dentryB.set_type(FsFileType::TYPE_DIRECTORY);

    std::string cName = "C";
    constexpr fuse_ino_t cInodeid = 4;
    Dentry dentryC;
    dentryC.set_fsid(fsId);
    dentryC.set_name(cName);
    dentryC.set_parentinodeid(dentryA.inodeid());
    dentryC.set_inodeid(cInodeid);
    dentryC.set_type(FsFileType::TYPE_SYM_LINK);
    InodeAttr attrC;
    attrC.set_symlink(fmt::format("./{}", bName));

    std::string dName = "D";
    constexpr fuse_ino_t dInodeid = 5;
    Dentry dentryD;
    dentryD.set_fsid(fsId);
    dentryD.set_name(dName);
    dentryD.set_parentinodeid(dentryB.inodeid());
    dentryD.set_inodeid(dInodeid);
    dentryD.set_type(FsFileType::TYPE_DIRECTORY);
    Inode inodeD;
    inodeD.set_fsid(fsId);
    inodeD.set_inodeid(dInodeid);
    inodeD.add_parent(dentryB.inodeid());
    std::shared_ptr<InodeWrapper> inodeWrapperD =
        std::make_shared<InodeWrapper>(inodeD, metaClient_);

    auto GetDentryReplace = [root, aName, dentryA, aInodeid, bName, dentryB,
                             bInodeid, cName, dentryC, dName, dentryD](
                                fuse_ino_t parent, const std::string& name,
                                Dentry* out) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                if (name == aName) {
                    *out = dentryA;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case aInodeid:
                if (name == bName) {
                    *out = dentryB;
                    return CURVEFS_ERROR::OK;
                } else if (name == cName) {
                    *out = dentryC;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case bInodeid:
                if (name == dName) {
                    *out = dentryD;
                    return CURVEFS_ERROR::OK;
                }
                break;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::OK;
    };

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillRepeatedly(Invoke(GetDentryReplace));

    auto GetInodeAttrReplace =
        [cInodeid, attrC](uint64_t inodeId, InodeAttr* out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case cInodeid:
                *out = attrC;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .WillOnce(Invoke(GetInodeAttrReplace));
    auto GetInodeReplace =
        [dInodeid, inodeWrapperD](
            uint64_t inodeId,
            std::shared_ptr<InodeWrapper>& out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case dInodeid:
                out = inodeWrapperD;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };
    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillRepeatedly(Invoke(GetInodeReplace));

    auto ListDentryReplace = [=](uint64_t parent, std::list<Dentry>* dentryList,
                                 uint32_t limit, bool onlyDir = false,
                                 uint32_t nlink = 0) -> CURVEFS_ERROR {
        switch (parent) {
            case bInodeid:
                dentryList->emplace_back(dentryD);
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::NOT_EXIST;
    };
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(Invoke(ListDentryReplace));

    std::string lastName;
    ASSERT_EQ(warmupManager_->GetInodeSubPathParent(
                  root, std::vector<std::string>{aName, cName, dName}, &ret,
                  &lastName, nullptr),
              true);
    ASSERT_EQ(ret, bInodeid);
    ASSERT_EQ(lastName, dName);
}

TEST_F(TestFuseS3Client, warmUp_FetchDentry_symLink_1depth_1) {
    /*
.(1) parent
└── A(2)
│   ├── B(3)
│   │   └── D(5)
│   ├── C(4) -> ./B
│   │   └── D(5)
    */
    constexpr fuse_ino_t root = 1;
    fuse_ino_t ret = 0;

    std::string aName = "A";
    constexpr fuse_ino_t aInodeid = 2;
    Dentry dentryA;
    dentryA.set_fsid(fsId);
    dentryA.set_name(aName);
    dentryA.set_parentinodeid(root);
    dentryA.set_inodeid(aInodeid);
    dentryA.set_type(FsFileType::TYPE_DIRECTORY);

    std::string bName = "B";
    constexpr fuse_ino_t bInodeid = 3;
    Dentry dentryB;
    dentryB.set_fsid(fsId);
    dentryB.set_name(bName);
    dentryB.set_parentinodeid(dentryA.inodeid());
    dentryB.set_inodeid(bInodeid);
    dentryB.set_type(FsFileType::TYPE_DIRECTORY);
    Inode inodeB;
    inodeB.set_fsid(fsId);
    inodeB.set_inodeid(bInodeid);
    inodeB.add_parent(dentryA.inodeid());
    std::shared_ptr<InodeWrapper> inodeWrapperB =
        std::make_shared<InodeWrapper>(inodeB, metaClient_);

    std::string cName = "C";
    constexpr fuse_ino_t cInodeid = 4;
    Dentry dentryC;
    dentryC.set_fsid(fsId);
    dentryC.set_name(cName);
    dentryC.set_parentinodeid(dentryA.inodeid());
    dentryC.set_inodeid(cInodeid);
    dentryC.set_type(FsFileType::TYPE_SYM_LINK);
    InodeAttr attrC;
    attrC.set_symlink(fmt::format("./{}", bName));

    std::string dName = "D";
    constexpr fuse_ino_t dInodeid = 5;
    Dentry dentryD;
    dentryD.set_fsid(fsId);
    dentryD.set_name(dName);
    dentryD.set_parentinodeid(dentryB.inodeid());
    dentryD.set_inodeid(dInodeid);
    dentryD.set_type(FsFileType::TYPE_DIRECTORY);

    auto GetDentryReplace = [root, aName, dentryA, aInodeid, bName, dentryB,
                             bInodeid, cName, dentryC, dName, dentryD](
                                fuse_ino_t parent, const std::string& name,
                                Dentry* out) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                if (name == aName) {
                    *out = dentryA;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case aInodeid:
                if (name == bName) {
                    *out = dentryB;
                    return CURVEFS_ERROR::OK;
                } else if (name == cName) {
                    *out = dentryC;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case bInodeid:
                if (name == dName) {
                    *out = dentryD;
                    return CURVEFS_ERROR::OK;
                }
                break;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::OK;
    };

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillRepeatedly(Invoke(GetDentryReplace));

    auto GetInodeAttrReplace =
        [cInodeid, attrC](uint64_t inodeId, InodeAttr* out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case cInodeid:
                *out = attrC;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .WillOnce(Invoke(GetInodeAttrReplace));

    auto GetInodeReplace =
        [=](uint64_t inodeId,
            std::shared_ptr<InodeWrapper>& out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case bInodeid:
                out = inodeWrapperB;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };
    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillRepeatedly(Invoke(GetInodeReplace));

    warmupManager_->FetchDentry(123, aInodeid, cName, 0);
}

TEST_F(TestFuseS3Client,
       warmUp_GetInodeSubPathParent_symLink_2depth_GetInodeWrapperFailed) {
    /*
.(1) parent
└── A(2)
│   ├── B(3)
│   │   └── D(5)
│   ├── C(4) -> ../A
│   │   ├── B(3)
│   │   │   └── D(5)
│   │   ├── C(4)
    */
    constexpr fuse_ino_t root = 1;
    fuse_ino_t ret = 0;

    std::string aName = "A";
    constexpr fuse_ino_t aInodeid = 2;
    Dentry dentryA;
    dentryA.set_fsid(fsId);
    dentryA.set_name(aName);
    dentryA.set_parentinodeid(root);
    dentryA.set_inodeid(aInodeid);
    dentryA.set_type(FsFileType::TYPE_DIRECTORY);
    Inode inodeA;
    inodeA.set_fsid(fsId);
    inodeA.set_inodeid(aInodeid);
    std::shared_ptr<InodeWrapper> inodeWrapperA =
        std::make_shared<InodeWrapper>(inodeA, metaClient_);

    std::string bName = "B";
    constexpr fuse_ino_t bInodeid = 3;
    Dentry dentryB;
    dentryB.set_fsid(fsId);
    dentryB.set_name(bName);
    dentryB.set_parentinodeid(dentryA.inodeid());
    dentryB.set_inodeid(bInodeid);
    dentryB.set_type(FsFileType::TYPE_DIRECTORY);

    std::string cName = "C";
    constexpr fuse_ino_t cInodeid = 4;
    Dentry dentryC;
    dentryC.set_fsid(fsId);
    dentryC.set_name(cName);
    dentryC.set_parentinodeid(dentryA.inodeid());
    dentryC.set_inodeid(cInodeid);
    dentryC.set_type(FsFileType::TYPE_SYM_LINK);
    InodeAttr attrC;
    attrC.set_symlink(fmt::format("../{}", aName));

    std::string dName = "D";
    constexpr fuse_ino_t dInodeid = 5;
    Dentry dentryD;
    dentryD.set_fsid(fsId);
    dentryD.set_name(dName);
    dentryD.set_parentinodeid(dentryB.inodeid());
    dentryD.set_inodeid(dInodeid);
    dentryD.set_type(FsFileType::TYPE_DIRECTORY);

    auto GetDentryReplace = [root, aName, dentryA, aInodeid, bName, dentryB,
                             bInodeid, cName, dentryC, dName, dentryD](
                                fuse_ino_t parent, const std::string& name,
                                Dentry* out) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                if (name == aName) {
                    *out = dentryA;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case aInodeid:
                if (name == bName) {
                    *out = dentryB;
                    return CURVEFS_ERROR::OK;
                } else if (name == cName) {
                    *out = dentryC;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case bInodeid:
                if (name == dName) {
                    *out = dentryD;
                    return CURVEFS_ERROR::OK;
                }
                break;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::OK;
    };

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillRepeatedly(Invoke(GetDentryReplace));

    auto GetInodeAttrReplace =
        [cInodeid, attrC](uint64_t inodeId, InodeAttr* out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case cInodeid:
                *out = attrC;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .WillOnce(Invoke(GetInodeAttrReplace));

    auto GetInodeReplace =
        [](uint64_t inodeId,
           std::shared_ptr<InodeWrapper>& out) -> CURVEFS_ERROR {
        switch (inodeId) {
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(Invoke(GetInodeReplace));

    std::string lastName;
    ASSERT_EQ(warmupManager_->GetInodeSubPathParent(
                  root, std::vector<std::string>{aName, cName, bName, dName},
                  &ret, &lastName, nullptr),
              false);
}

TEST_F(TestFuseS3Client,
       warmUp_GetInodeSubPathParent_symLink_2depth_noparents) {
    /*
.(1) parent
└── A(2)
│   ├── B(3)
│   │   └── D(5)
│   ├── C(4) -> ../A
│   │   ├── B(3)
│   │   │   └── D(5)
│   │   ├── C(4)
    */
    constexpr fuse_ino_t root = 1;
    fuse_ino_t ret = 0;

    std::string aName = "A";
    constexpr fuse_ino_t aInodeid = 2;
    Dentry dentryA;
    dentryA.set_fsid(fsId);
    dentryA.set_name(aName);
    dentryA.set_parentinodeid(root);
    dentryA.set_inodeid(aInodeid);
    dentryA.set_type(FsFileType::TYPE_DIRECTORY);
    Inode inodeA;
    inodeA.set_fsid(fsId);
    inodeA.set_inodeid(aInodeid);
    std::shared_ptr<InodeWrapper> inodeWrapperA =
        std::make_shared<InodeWrapper>(inodeA, metaClient_);

    std::string bName = "B";
    constexpr fuse_ino_t bInodeid = 3;
    Dentry dentryB;
    dentryB.set_fsid(fsId);
    dentryB.set_name(bName);
    dentryB.set_parentinodeid(dentryA.inodeid());
    dentryB.set_inodeid(bInodeid);
    dentryB.set_type(FsFileType::TYPE_DIRECTORY);

    std::string cName = "C";
    constexpr fuse_ino_t cInodeid = 4;
    Dentry dentryC;
    dentryC.set_fsid(fsId);
    dentryC.set_name(cName);
    dentryC.set_parentinodeid(dentryA.inodeid());
    dentryC.set_inodeid(cInodeid);
    dentryC.set_type(FsFileType::TYPE_SYM_LINK);
    InodeAttr attrC;
    attrC.set_symlink(fmt::format("../{}", aName));
    Inode inodeC;
    inodeC.set_fsid(fsId);
    inodeC.set_inodeid(cInodeid);
    std::shared_ptr<InodeWrapper> inodeWrapperC =
        std::make_shared<InodeWrapper>(inodeC, metaClient_);

    std::string dName = "D";
    constexpr fuse_ino_t dInodeid = 5;
    Dentry dentryD;
    dentryD.set_fsid(fsId);
    dentryD.set_name(dName);
    dentryD.set_parentinodeid(dentryB.inodeid());
    dentryD.set_inodeid(dInodeid);
    dentryD.set_type(FsFileType::TYPE_DIRECTORY);

    auto GetDentryReplace = [root, aName, dentryA, aInodeid, bName, dentryB,
                             bInodeid, cName, dentryC, dName, dentryD](
                                fuse_ino_t parent, const std::string& name,
                                Dentry* out) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                if (name == aName) {
                    *out = dentryA;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case aInodeid:
                if (name == bName) {
                    *out = dentryB;
                    return CURVEFS_ERROR::OK;
                } else if (name == cName) {
                    *out = dentryC;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case bInodeid:
                if (name == dName) {
                    *out = dentryD;
                    return CURVEFS_ERROR::OK;
                }
                break;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::OK;
    };

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillRepeatedly(Invoke(GetDentryReplace));

    auto GetInodeAttrReplace =
        [cInodeid, attrC](uint64_t inodeId, InodeAttr* out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case cInodeid:
                *out = attrC;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .WillOnce(Invoke(GetInodeAttrReplace));

    auto GetInodeReplace =
        [aInodeid, inodeWrapperA, cInodeid, inodeWrapperC](
            uint64_t inodeId,
            std::shared_ptr<InodeWrapper>& out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case aInodeid:
                out = inodeWrapperA;
                return CURVEFS_ERROR::OK;
            case cInodeid:
                out = inodeWrapperC;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(Invoke(GetInodeReplace));
    std::string lastName;
    ASSERT_EQ(warmupManager_->GetInodeSubPathParent(
                  root, std::vector<std::string>{aName, cName, bName, dName},
                  &ret, &lastName, nullptr),
              false);
}

TEST_F(TestFuseS3Client, warmUp_GetInodeSubPathParent_symLink_2depth) {
    /*
.(1) parent
└── A(2)
│   ├── B(3)
│   │   └── D(5)
│   ├── C(4) -> ../A
│   │   ├── B(3)
│   │   │   └── D(5)
│   │   ├── C(4)
    */
    constexpr fuse_ino_t root = 1;
    fuse_ino_t ret = 0;

    std::string aName = "A";
    constexpr fuse_ino_t aInodeid = 2;
    Dentry dentryA;
    dentryA.set_fsid(fsId);
    dentryA.set_name(aName);
    dentryA.set_parentinodeid(root);
    dentryA.set_inodeid(aInodeid);
    dentryA.set_type(FsFileType::TYPE_DIRECTORY);
    Inode inodeA;
    inodeA.set_fsid(fsId);
    inodeA.set_inodeid(aInodeid);
    inodeA.add_parent(root);
    std::shared_ptr<InodeWrapper> inodeWrapperA =
        std::make_shared<InodeWrapper>(inodeA, metaClient_);

    std::string bName = "B";
    constexpr fuse_ino_t bInodeid = 3;
    Dentry dentryB;
    dentryB.set_fsid(fsId);
    dentryB.set_name(bName);
    dentryB.set_parentinodeid(dentryA.inodeid());
    dentryB.set_inodeid(bInodeid);
    dentryB.set_type(FsFileType::TYPE_DIRECTORY);

    std::string cName = "C";
    constexpr fuse_ino_t cInodeid = 4;
    Dentry dentryC;
    dentryC.set_fsid(fsId);
    dentryC.set_name(cName);
    dentryC.set_parentinodeid(dentryA.inodeid());
    dentryC.set_inodeid(cInodeid);
    dentryC.set_type(FsFileType::TYPE_SYM_LINK);
    InodeAttr attrC;
    attrC.set_symlink(fmt::format("../{}", aName));
    Inode inodeC;
    inodeC.set_fsid(fsId);
    inodeC.set_inodeid(cInodeid);
    inodeC.add_parent(aInodeid);
    std::shared_ptr<InodeWrapper> inodeWrapperC =
        std::make_shared<InodeWrapper>(inodeC, metaClient_);

    std::string dName = "D";
    constexpr fuse_ino_t dInodeid = 5;
    Dentry dentryD;
    dentryD.set_fsid(fsId);
    dentryD.set_name(dName);
    dentryD.set_parentinodeid(dentryB.inodeid());
    dentryD.set_inodeid(dInodeid);
    dentryD.set_type(FsFileType::TYPE_DIRECTORY);
    Inode inodeD;
    inodeD.set_fsid(fsId);
    inodeD.set_inodeid(dInodeid);
    inodeD.add_parent(dentryB.inodeid());
    std::shared_ptr<InodeWrapper> inodeWrapperD =
        std::make_shared<InodeWrapper>(inodeD, metaClient_);

    auto GetDentryReplace = [root, aName, dentryA, aInodeid, bName, dentryB,
                             bInodeid, cName, dentryC, dName, dentryD](
                                fuse_ino_t parent, const std::string& name,
                                Dentry* out) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                if (name == aName) {
                    *out = dentryA;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case aInodeid:
                if (name == bName) {
                    *out = dentryB;
                    return CURVEFS_ERROR::OK;
                } else if (name == cName) {
                    *out = dentryC;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case bInodeid:
                if (name == dName) {
                    *out = dentryD;
                    return CURVEFS_ERROR::OK;
                }
                break;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::OK;
    };

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillRepeatedly(Invoke(GetDentryReplace));

    auto GetInodeAttrReplace =
        [cInodeid, attrC](uint64_t inodeId, InodeAttr* out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case cInodeid:
                *out = attrC;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .WillOnce(Invoke(GetInodeAttrReplace));

    auto GetInodeReplace =
        [aInodeid, inodeWrapperA, cInodeid, inodeWrapperC, dInodeid,
         inodeWrapperD](uint64_t inodeId,
                        std::shared_ptr<InodeWrapper>& out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case aInodeid:
                out = inodeWrapperA;
                return CURVEFS_ERROR::OK;
            case cInodeid:
                out = inodeWrapperC;
                return CURVEFS_ERROR::OK;
            case dInodeid:
                out = inodeWrapperD;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillRepeatedly(Invoke(GetInodeReplace));

    auto ListDentryReplace = [=](uint64_t parent, std::list<Dentry>* dentryList,
                                 uint32_t limit, bool onlyDir = false,
                                 uint32_t nlink = 0) -> CURVEFS_ERROR {
        switch (parent) {
            case bInodeid:
                dentryList->emplace_back(dentryD);
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::NOT_EXIST;
    };
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(Invoke(ListDentryReplace));

    std::string lastName;
    ASSERT_EQ(warmupManager_->GetInodeSubPathParent(
                  root, std::vector<std::string>{aName, cName, bName, dName},
                  &ret, &lastName, nullptr),
              true);
    ASSERT_EQ(ret, bInodeid);
    ASSERT_EQ(lastName, dName);
}

TEST_F(TestFuseS3Client, warmUp_GetInodeSubPathParent_symLink_double_point) {
    /*
.(1) parent
└── A(2)
│   ├── B(3)
│   │   ├── C(4) -> ..
│   │   │   ├── B(3)
    */
    constexpr fuse_ino_t root = 1;

    std::string aName = "A";
    constexpr fuse_ino_t aInodeid = 2;
    Dentry dentryA;
    dentryA.set_fsid(fsId);
    dentryA.set_name(aName);
    dentryA.set_parentinodeid(root);
    dentryA.set_inodeid(aInodeid);
    dentryA.set_type(FsFileType::TYPE_DIRECTORY);
    Inode inodeA;
    inodeA.set_fsid(fsId);
    inodeA.set_inodeid(aInodeid);
    inodeA.add_parent(root);
    std::shared_ptr<InodeWrapper> inodeWrapperA =
        std::make_shared<InodeWrapper>(inodeA, metaClient_);

    std::string bName = "B";
    constexpr fuse_ino_t bInodeid = 3;
    Dentry dentryB;
    dentryB.set_fsid(fsId);
    dentryB.set_name(bName);
    dentryB.set_parentinodeid(dentryA.inodeid());
    dentryB.set_inodeid(bInodeid);
    dentryB.set_type(FsFileType::TYPE_DIRECTORY);
    Inode inodeB;
    inodeB.set_fsid(fsId);
    inodeB.set_inodeid(bInodeid);
    inodeB.add_parent(dentryA.inodeid());
    std::shared_ptr<InodeWrapper> inodeWrapperB =
        std::make_shared<InodeWrapper>(inodeB, metaClient_);

    std::string cName = "C";
    constexpr fuse_ino_t cInodeid = 4;
    Dentry dentryC;
    dentryC.set_fsid(fsId);
    dentryC.set_name(cName);
    dentryC.set_parentinodeid(dentryB.inodeid());
    dentryC.set_inodeid(cInodeid);
    dentryC.set_type(FsFileType::TYPE_SYM_LINK);
    InodeAttr attrC;
    attrC.set_symlink("..");
    Inode inodeC;
    inodeC.set_fsid(fsId);
    inodeC.set_inodeid(cInodeid);
    std::shared_ptr<InodeWrapper> inodeWrapperC =
        std::make_shared<InodeWrapper>(inodeC, metaClient_);

    auto GetDentryReplace = [root, aName, dentryA, aInodeid, bName, dentryB,
                             bInodeid, cName, dentryC](
                                fuse_ino_t parent, const std::string& name,
                                Dentry* out) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                if (name == aName) {
                    *out = dentryA;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case aInodeid:
                if (name == bName) {
                    *out = dentryB;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case bInodeid:
                if (name == cName) {
                    *out = dentryC;
                    return CURVEFS_ERROR::OK;
                }
                break;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::NOT_EXIST;
    };

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillRepeatedly(Invoke(GetDentryReplace));

    auto GetInodeAttrReplace =
        [cInodeid, attrC](uint64_t inodeId, InodeAttr* out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case cInodeid:
                *out = attrC;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .WillRepeatedly(Invoke(GetInodeAttrReplace));

    auto GetInodeReplace =
        [aInodeid, inodeWrapperA, bInodeid, inodeWrapperB, cInodeid,
         inodeWrapperC](uint64_t inodeId,
                        std::shared_ptr<InodeWrapper>& out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case aInodeid:
                out = inodeWrapperA;
                return CURVEFS_ERROR::OK;
            case bInodeid:
                out = inodeWrapperB;
                return CURVEFS_ERROR::OK;
            case cInodeid:
                out = inodeWrapperC;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillRepeatedly(Invoke(GetInodeReplace));

    auto ListDentryReplace = [=](uint64_t parent, std::list<Dentry>* dentryList,
                                 uint32_t limit, bool onlyDir = false,
                                 uint32_t nlink = 0) -> CURVEFS_ERROR {
        switch (parent) {
            case aInodeid:
                dentryList->emplace_back(dentryB);
                return CURVEFS_ERROR::OK;
            case root:
                dentryList->emplace_back(dentryA);
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::NOT_EXIST;
    };
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillRepeatedly(Invoke(ListDentryReplace));

    std::string lastName;
    fuse_ino_t ret = 0;
    ASSERT_EQ(warmupManager_->GetInodeSubPathParent(
                  root, std::vector<std::string>{aName, bName, cName}, &ret,
                  &lastName, nullptr),
              true);
    ASSERT_EQ(ret, root);
    ASSERT_EQ(lastName, aName);
    ASSERT_EQ(warmupManager_->GetInodeSubPathParent(
                  root, std::vector<std::string>{aName, bName, cName, bName},
                  &ret, &lastName, nullptr),
              true);
    ASSERT_EQ(ret, aInodeid);
    ASSERT_EQ(lastName, bName);
}

TEST_F(TestFuseS3Client,
       warmUp_GetInodeSubPathParent_symLink_double_point_to_root) {
    /*
.(1) parent
└── A(2)
│   ├── B(3) -> ..
│   │   ├── A(2)
    */
    constexpr fuse_ino_t root = 1;
    Inode inodeRoot;
    inodeRoot.set_fsid(fsId);
    inodeRoot.set_inodeid(root);
    inodeRoot.add_parent(0);
    std::shared_ptr<InodeWrapper> inodeWrapperRoot =
        std::make_shared<InodeWrapper>(inodeRoot, metaClient_);

    std::string aName = "A";
    constexpr fuse_ino_t aInodeid = 2;
    Dentry dentryA;
    dentryA.set_fsid(fsId);
    dentryA.set_name(aName);
    dentryA.set_parentinodeid(root);
    dentryA.set_inodeid(aInodeid);
    dentryA.set_type(FsFileType::TYPE_DIRECTORY);
    Inode inodeA;
    inodeA.set_fsid(fsId);
    inodeA.set_inodeid(aInodeid);
    inodeA.add_parent(root);
    std::shared_ptr<InodeWrapper> inodeWrapperA =
        std::make_shared<InodeWrapper>(inodeA, metaClient_);

    std::string bName = "B";
    constexpr fuse_ino_t bInodeid = 3;
    Dentry dentryB;
    dentryB.set_fsid(fsId);
    dentryB.set_name(bName);
    dentryB.set_parentinodeid(dentryA.inodeid());
    dentryB.set_inodeid(bInodeid);
    dentryB.set_type(FsFileType::TYPE_SYM_LINK);
    Inode inodeB;
    inodeB.set_fsid(fsId);
    inodeB.set_inodeid(bInodeid);
    inodeB.add_parent(dentryA.inodeid());
    std::shared_ptr<InodeWrapper> inodeWrapperB =
        std::make_shared<InodeWrapper>(inodeB, metaClient_);
    InodeAttr attrB;
    attrB.set_symlink("..");
    Inode inodeC;
    inodeB.set_fsid(fsId);
    inodeB.set_inodeid(bInodeid);
    std::shared_ptr<InodeWrapper> inodeWrapperb =
        std::make_shared<InodeWrapper>(inodeC, metaClient_);

    auto GetDentryReplace = [=](fuse_ino_t parent, const std::string& name,
                                Dentry* out) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                if (name == aName) {
                    *out = dentryA;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case aInodeid:
                if (name == bName) {
                    *out = dentryB;
                    return CURVEFS_ERROR::OK;
                }
                break;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::NOT_EXIST;
    };

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillRepeatedly(Invoke(GetDentryReplace));

    auto GetInodeAttrReplace = [=](uint64_t inodeId,
                                   InodeAttr* out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case bInodeid:
                *out = attrB;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .WillRepeatedly(Invoke(GetInodeAttrReplace));

    auto GetInodeReplace =
        [=](uint64_t inodeId,
            std::shared_ptr<InodeWrapper>& out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case root:
                out = inodeWrapperRoot;
                return CURVEFS_ERROR::OK;
            case aInodeid:
                out = inodeWrapperA;
                return CURVEFS_ERROR::OK;
            case bInodeid:
                out = inodeWrapperB;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillRepeatedly(Invoke(GetInodeReplace));

    auto ListDentryReplace = [=](uint64_t parent, std::list<Dentry>* dentryList,
                                 uint32_t limit, bool onlyDir = false,
                                 uint32_t nlink = 0) -> CURVEFS_ERROR {
        switch (parent) {
            case aInodeid:
                dentryList->emplace_back(dentryB);
                return CURVEFS_ERROR::OK;
            case root:
                dentryList->emplace_back(dentryA);
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::NOT_EXIST;
    };
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillRepeatedly(Invoke(ListDentryReplace));

    std::string lastName;
    fuse_ino_t ret = 0;
    ASSERT_EQ(warmupManager_->GetInodeSubPathParent(
                  root, std::vector<std::string>{aName, bName}, &ret, &lastName,
                  nullptr),
              true);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(lastName, "/");
    ASSERT_EQ(warmupManager_->GetInodeSubPathParent(
                  root, std::vector<std::string>{aName, bName, aName}, &ret,
                  &lastName, nullptr),
              true);
    ASSERT_EQ(ret, root);
    ASSERT_EQ(lastName, aName);
}

TEST_F(TestFuseS3Client,
       warmUp_GetInodeSubPathParent_symLink_doublePointx2_to_root) {
    /*
.(1) parent
└── A(2)
│   ├── B(3)
│   │   ├── C(4) -> ../..
│   │   │   ├── A(2)
    */
    constexpr fuse_ino_t root = 1;
    Inode inodeRoot;
    inodeRoot.set_fsid(fsId);
    inodeRoot.set_inodeid(root);
    inodeRoot.add_parent(0);
    std::shared_ptr<InodeWrapper> inodeWrapperRoot =
        std::make_shared<InodeWrapper>(inodeRoot, metaClient_);

    std::string aName = "A";
    constexpr fuse_ino_t aInodeid = 2;
    Dentry dentryA;
    dentryA.set_fsid(fsId);
    dentryA.set_name(aName);
    dentryA.set_parentinodeid(root);
    dentryA.set_inodeid(aInodeid);
    dentryA.set_type(FsFileType::TYPE_DIRECTORY);
    Inode inodeA;
    inodeA.set_fsid(fsId);
    inodeA.set_inodeid(aInodeid);
    inodeA.add_parent(root);
    std::shared_ptr<InodeWrapper> inodeWrapperA =
        std::make_shared<InodeWrapper>(inodeA, metaClient_);

    std::string bName = "B";
    constexpr fuse_ino_t bInodeid = 3;
    Dentry dentryB;
    dentryB.set_fsid(fsId);
    dentryB.set_name(bName);
    dentryB.set_parentinodeid(dentryA.inodeid());
    dentryB.set_inodeid(bInodeid);
    dentryB.set_type(FsFileType::TYPE_DIRECTORY);
    Inode inodeB;
    inodeB.set_fsid(fsId);
    inodeB.set_inodeid(bInodeid);
    inodeB.add_parent(dentryA.inodeid());
    std::shared_ptr<InodeWrapper> inodeWrapperB =
        std::make_shared<InodeWrapper>(inodeB, metaClient_);

    std::string cName = "C";
    constexpr fuse_ino_t cInodeid = 4;
    Dentry dentryC;
    dentryC.set_fsid(fsId);
    dentryC.set_name(cName);
    dentryC.set_parentinodeid(dentryB.inodeid());
    dentryC.set_inodeid(cInodeid);
    dentryC.set_type(FsFileType::TYPE_SYM_LINK);
    InodeAttr attrC;
    attrC.set_symlink("../..");
    Inode inodeC;
    inodeC.set_fsid(fsId);
    inodeC.set_inodeid(cInodeid);
    std::shared_ptr<InodeWrapper> inodeWrapperC =
        std::make_shared<InodeWrapper>(inodeC, metaClient_);

    auto GetDentryReplace = [root, aName, dentryA, aInodeid, bName, dentryB,
                             bInodeid, cName, dentryC](
                                fuse_ino_t parent, const std::string& name,
                                Dentry* out) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                if (name == aName) {
                    *out = dentryA;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case aInodeid:
                if (name == bName) {
                    *out = dentryB;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case bInodeid:
                if (name == cName) {
                    *out = dentryC;
                    return CURVEFS_ERROR::OK;
                }
                break;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::NOT_EXIST;
    };

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillRepeatedly(Invoke(GetDentryReplace));

    auto GetInodeAttrReplace =
        [cInodeid, attrC](uint64_t inodeId, InodeAttr* out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case cInodeid:
                *out = attrC;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .WillRepeatedly(Invoke(GetInodeAttrReplace));

    auto GetInodeReplace =
        [=](uint64_t inodeId,
            std::shared_ptr<InodeWrapper>& out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case root:
                out = inodeWrapperRoot;
                return CURVEFS_ERROR::OK;
            case aInodeid:
                out = inodeWrapperA;
                return CURVEFS_ERROR::OK;
            case bInodeid:
                out = inodeWrapperB;
                return CURVEFS_ERROR::OK;
            case cInodeid:
                out = inodeWrapperC;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillRepeatedly(Invoke(GetInodeReplace));

    auto ListDentryReplace = [=](uint64_t parent, std::list<Dentry>* dentryList,
                                 uint32_t limit, bool onlyDir = false,
                                 uint32_t nlink = 0) -> CURVEFS_ERROR {
        switch (parent) {
            case aInodeid:
                dentryList->emplace_back(dentryB);
                return CURVEFS_ERROR::OK;
            case root:
                dentryList->emplace_back(dentryA);
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::NOT_EXIST;
    };
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillRepeatedly(Invoke(ListDentryReplace));

    std::string lastName;
    fuse_ino_t ret = 0;
    ASSERT_EQ(warmupManager_->GetInodeSubPathParent(
                  root, std::vector<std::string>{aName, bName, cName}, &ret,
                  &lastName, nullptr),
              true);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(lastName, "/");
    ASSERT_EQ(warmupManager_->GetInodeSubPathParent(
                  root, std::vector<std::string>{aName, bName, cName, aName},
                  &ret, &lastName, nullptr),
              true);
    ASSERT_EQ(ret, root);
    ASSERT_EQ(lastName, aName);
}

TEST_F(TestFuseS3Client, warmUp_Fetch_loop) {
    /*
.(1) parent
└── A(2)
│   ├── B(3)
│   │   ├── C(4) -> ..
│   │   │   ├── B(3)
│   │   │   ├── D(5)
│   │   └── D(5)
    */
    constexpr fuse_ino_t root = 1;

    std::string aName = "A";
    constexpr fuse_ino_t aInodeid = 2;
    Dentry dentryA;
    dentryA.set_fsid(fsId);
    dentryA.set_name(aName);
    dentryA.set_parentinodeid(root);
    dentryA.set_inodeid(aInodeid);
    dentryA.set_type(FsFileType::TYPE_DIRECTORY);
    Inode inodeA;
    inodeA.set_fsid(fsId);
    inodeA.set_inodeid(aInodeid);
    inodeA.add_parent(root);
    std::shared_ptr<InodeWrapper> inodeWrapperA =
        std::make_shared<InodeWrapper>(inodeA, metaClient_);

    std::string bName = "B";
    constexpr fuse_ino_t bInodeid = 3;
    Dentry dentryB;
    dentryB.set_fsid(fsId);
    dentryB.set_name(bName);
    dentryB.set_parentinodeid(dentryA.inodeid());
    dentryB.set_inodeid(bInodeid);
    dentryB.set_type(FsFileType::TYPE_DIRECTORY);
    Inode inodeB;
    inodeB.set_fsid(fsId);
    inodeB.set_inodeid(bInodeid);
    inodeB.add_parent(dentryA.inodeid());
    std::shared_ptr<InodeWrapper> inodeWrapperB =
        std::make_shared<InodeWrapper>(inodeB, metaClient_);

    std::string cName = "C";
    constexpr fuse_ino_t cInodeid = 4;
    Dentry dentryC;
    dentryC.set_fsid(fsId);
    dentryC.set_name(cName);
    dentryC.set_parentinodeid(dentryB.inodeid());
    dentryC.set_inodeid(cInodeid);
    dentryC.set_type(FsFileType::TYPE_SYM_LINK);
    InodeAttr attrC;
    attrC.set_symlink("..");
    Inode inodeC;
    inodeC.set_fsid(fsId);
    inodeC.set_inodeid(cInodeid);
    std::shared_ptr<InodeWrapper> inodeWrapperC =
        std::make_shared<InodeWrapper>(inodeC, metaClient_);

    std::string dName = "D";
    constexpr fuse_ino_t dInodeid = 5;
    Dentry dentryD;
    dentryD.set_fsid(fsId);
    dentryD.set_name(dName);
    dentryD.set_parentinodeid(dentryB.inodeid());
    dentryD.set_inodeid(dInodeid);
    dentryD.set_type(FsFileType::TYPE_S3);
    Inode inodeD;
    inodeD.set_fsid(fsId);
    inodeD.set_inodeid(dInodeid);
    inodeD.add_parent(dentryB.inodeid());
    std::shared_ptr<InodeWrapper> inodeWrapperD =
        std::make_shared<InodeWrapper>(inodeD, metaClient_);

    auto GetDentryReplace = [root, aName, dentryA, aInodeid, bName, dentryB,
                             bInodeid, cName, dentryC](
                                fuse_ino_t parent, const std::string& name,
                                Dentry* out) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                if (name == aName) {
                    *out = dentryA;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case aInodeid:
                if (name == bName) {
                    *out = dentryB;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case bInodeid:
                if (name == cName) {
                    *out = dentryC;
                    return CURVEFS_ERROR::OK;
                }
                break;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::NOT_EXIST;
    };

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillRepeatedly(Invoke(GetDentryReplace));

    auto GetInodeAttrReplace =
        [cInodeid, attrC](uint64_t inodeId, InodeAttr* out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case cInodeid:
                *out = attrC;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .WillRepeatedly(Invoke(GetInodeAttrReplace));

    auto GetInodeReplace =
        [aInodeid, inodeWrapperA, bInodeid, inodeWrapperB, cInodeid,
         inodeWrapperC](uint64_t inodeId,
                        std::shared_ptr<InodeWrapper>& out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case aInodeid:
                out = inodeWrapperA;
                return CURVEFS_ERROR::OK;
            case bInodeid:
                out = inodeWrapperB;
                return CURVEFS_ERROR::OK;
            case cInodeid:
                out = inodeWrapperC;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillRepeatedly(Invoke(GetInodeReplace));

    auto ListDentryReplace = [=](uint64_t parent, std::list<Dentry>* dentryList,
                                 uint32_t limit, bool onlyDir = false,
                                 uint32_t nlink = 0) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                dentryList->emplace_back(dentryA);
                return CURVEFS_ERROR::OK;
            case aInodeid:
                dentryList->emplace_back(dentryB);
                return CURVEFS_ERROR::OK;
            case bInodeid:
                dentryList->emplace_back(dentryC);
                dentryList->emplace_back(dentryD);
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::NOT_EXIST;
    };
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillRepeatedly(Invoke(ListDentryReplace));

    std::string lastName;
    fuse_ino_t ret = 0;

    warmupManager_->FetchDentry(0, bInodeid, cName, 0);
    sleep(1);
}

TEST_F(TestFuseS3Client,
       warmUp_GetInodeSubPathParent_symLink_2depth_outcurvefs) {
    /*
.(1) root
└── A(2)
│   ├── C(4) -> ../../F
│   │   ├── D(3)
    */
    constexpr fuse_ino_t root = 1;
    Inode inodeRoot;
    inodeRoot.set_fsid(fsId);
    inodeRoot.set_inodeid(root);
    std::shared_ptr<InodeWrapper> inodeWrapperRoot =
        std::make_shared<InodeWrapper>(inodeRoot, metaClient_);

    fuse_ino_t ret = 0;

    std::string aName = "A";
    constexpr fuse_ino_t aInodeid = 2;
    Dentry dentryA;
    dentryA.set_fsid(fsId);
    dentryA.set_name(aName);
    dentryA.set_parentinodeid(root);
    dentryA.set_inodeid(aInodeid);
    dentryA.set_type(FsFileType::TYPE_DIRECTORY);
    Inode inodeA;
    inodeA.set_fsid(fsId);
    inodeA.set_inodeid(aInodeid);
    inodeA.add_parent(root);
    std::shared_ptr<InodeWrapper> inodeWrapperA =
        std::make_shared<InodeWrapper>(inodeA, metaClient_);

    std::string cName = "C";
    constexpr fuse_ino_t cInodeid = 4;
    Dentry dentryC;
    dentryC.set_fsid(fsId);
    dentryC.set_name(cName);
    dentryC.set_parentinodeid(dentryA.inodeid());
    dentryC.set_inodeid(cInodeid);
    dentryC.set_type(FsFileType::TYPE_SYM_LINK);
    InodeAttr attrC;
    attrC.set_symlink("../../F");
    Inode inodeC;
    inodeC.set_fsid(fsId);
    inodeC.set_inodeid(cInodeid);
    inodeC.add_parent(aInodeid);
    std::shared_ptr<InodeWrapper> inodeWrapperC =
        std::make_shared<InodeWrapper>(inodeC, metaClient_);

    std::string dName = "D";
    constexpr fuse_ino_t dInodeid = 5;
    Dentry dentryD;
    dentryD.set_fsid(fsId);
    dentryD.set_name(dName);
    dentryD.set_parentinodeid(1234);
    dentryD.set_inodeid(dInodeid);
    dentryD.set_type(FsFileType::TYPE_DIRECTORY);

    auto GetDentryReplace = [root, aName, dentryA, aInodeid, cName, dentryC](
                                fuse_ino_t parent, const std::string& name,
                                Dentry* out) -> CURVEFS_ERROR {
        switch (parent) {
            case root:
                if (name == aName) {
                    *out = dentryA;
                    return CURVEFS_ERROR::OK;
                }
                break;
            case aInodeid:
                if (name == cName) {
                    *out = dentryC;
                    return CURVEFS_ERROR::OK;
                }
                break;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
        return CURVEFS_ERROR::OK;
    };

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillRepeatedly(Invoke(GetDentryReplace));

    auto GetInodeAttrReplace =
        [cInodeid, attrC](uint64_t inodeId, InodeAttr* out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case cInodeid:
                *out = attrC;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .WillOnce(Invoke(GetInodeAttrReplace));

    auto GetInodeReplace =
        [inodeWrapperRoot, aInodeid, inodeWrapperA, cInodeid, inodeWrapperC](
            uint64_t inodeId,
            std::shared_ptr<InodeWrapper>& out) -> CURVEFS_ERROR {
        switch (inodeId) {
            case root:
                out = inodeWrapperRoot;
                return CURVEFS_ERROR::OK;
            case aInodeid:
                out = inodeWrapperA;
                return CURVEFS_ERROR::OK;
            case cInodeid:
                out = inodeWrapperC;
                return CURVEFS_ERROR::OK;
            default:
                return CURVEFS_ERROR::NOT_EXIST;
        }
    };

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillRepeatedly(Invoke(GetInodeReplace));

    std::string lastName;

    ASSERT_EQ(warmupManager_->GetInodeSubPathParent(
                  root, std::vector<std::string>{aName, cName, dName}, &ret,
                  &lastName, nullptr),
              false);
}

TEST_F(TestFuseS3Client, FuseInit_when_fs_exist) {
    MountOption mOpts;
    memset(&mOpts, 0, sizeof(mOpts));
    mOpts.fsName = const_cast<char*>("s3fs");
    mOpts.mountPoint = const_cast<char*>("host1:/test");
    mOpts.fsType = const_cast<char*>("s3");

    std::string fsName = mOpts.fsName;
    FsInfo fsInfoExp;
    fsInfoExp.set_fsid(200);
    fsInfoExp.set_fsname(fsName);
    EXPECT_CALL(*mdsClient_, MountFs(fsName, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(fsInfoExp), Return(FSStatusCode::OK)));
    CURVEFS_ERROR ret = client_->SetMountStatus(&mOpts);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    auto fsInfo = client_->GetFsInfo();
    ASSERT_NE(fsInfo, nullptr);

    ASSERT_EQ(fsInfo->fsid(), fsInfoExp.fsid());
    ASSERT_EQ(fsInfo->fsname(), fsInfoExp.fsname());
}

TEST_F(TestFuseS3Client, FuseOpDestroy) {
    MountOption mOpts;
    memset(&mOpts, 0, sizeof(mOpts));
    mOpts.fsName = const_cast<char*>("s3fs");
    mOpts.mountPoint = const_cast<char*>("host1:/test");
    mOpts.fsType = const_cast<char*>("s3");

    std::string fsName = mOpts.fsName;

    EXPECT_CALL(*mdsClient_, UmountFs(fsName, _))
        .WillOnce(Return(FSStatusCode::OK));

    client_->FuseOpDestroy(&mOpts);
}

TEST_F(TestFuseS3Client, FuseOpWriteSmallSize) {
    fuse_req_t req = nullptr;
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

    FileOut fileOut;
    CURVEFS_ERROR ret =
        client_->FuseOpWrite(req, ino, buf, size, off, &fi, &fileOut);

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(smallSize, fileOut.nwritten);
}

TEST_F(TestFuseS3Client, FuseOpWriteFailed) {
    fuse_req_t req = nullptr;
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

    FileOut fileOut;
    CURVEFS_ERROR ret =
        client_->FuseOpWrite(req, ino, buf, size, off, &fi, &fileOut);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    ret = client_->FuseOpWrite(req, ino, buf, size, off, &fi, &fileOut);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseS3Client, FuseOpReadOverRange) {
    fuse_req_t req = nullptr;
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
    fuse_req_t req = nullptr;
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
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    struct fuse_file_info* fi = nullptr;

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    inode.set_type(FsFileType::TYPE_S3);

    EXPECT_CALL(*s3ClientAdaptor_, Flush(_))
        .WillOnce(Return(CURVEFS_ERROR::OK))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    inodeWrapper->SetUid(32);
    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::OK));

    CURVEFS_ERROR ret = client_->FuseOpFsync(req, ino, 0, fi);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    ret = client_->FuseOpFsync(req, ino, 1, fi);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseS3Client, FuseOpFlush) {
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    struct fuse_file_info* fi = nullptr;
    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(0);
    inode.set_type(FsFileType::TYPE_S3);

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);
    inodeWrapper->SetUid(32);

    LOG(INFO) << "############ case1: test disable cto and s3 flush fail";
    curvefs::client::common::FLAGS_enableCto = false;
    EXPECT_CALL(*s3ClientAdaptor_, Flush(ino))
        .WillOnce(Return(CURVEFS_ERROR::UNKNOWN));
    ASSERT_EQ(CURVEFS_ERROR::UNKNOWN, client_->FuseOpFlush(req, ino, fi));

    LOG(INFO) << "############ case2: test disable cto and flush ok";
    EXPECT_CALL(*s3ClientAdaptor_, Flush(ino))
        .WillOnce(Return(CURVEFS_ERROR::OK));
    ASSERT_EQ(CURVEFS_ERROR::OK, client_->FuseOpFlush(req, ino, fi));

    LOG(INFO)
        << "############ case3: test enable cto, but flush all cache fail";
    curvefs::client::common::FLAGS_enableCto = true;
    EXPECT_CALL(*s3ClientAdaptor_, FlushAllCache(_))
        .WillOnce(Return(CURVEFS_ERROR::UNKNOWN));
    ASSERT_EQ(CURVEFS_ERROR::UNKNOWN, client_->FuseOpFlush(req, ino, fi));

    LOG(INFO) << "############ case4: enable cto and execute ok";
    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*s3ClientAdaptor_, FlushAllCache(_))
        .WillOnce(Return(CURVEFS_ERROR::OK));
    ASSERT_EQ(CURVEFS_ERROR::OK, client_->FuseOpFlush(req, ino, fi));
}

TEST_F(TestFuseS3Client, FuseOpGetXattr_NotSummaryInfo) {
    // in
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    const char name[] = "security.selinux";
    size_t size = 100;
    std::string value;

    CURVEFS_ERROR ret = client_->FuseOpGetXattr(req, ino, name, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::NODATA, ret);
}

TEST_F(TestFuseS3Client, FuseOpGetXattr_NotEnableSumInDir) {
    // in
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    const char rname[] = "curve.dir.rfbytes";
    const char name[] = "curve.dir.fbytes";
    size_t size = 100;
    std::string value;

    // out
    uint32_t fsId = 1;
    uint64_t inodeId1 = 2;
    uint64_t inodeId2 = 3;
    uint64_t inodeId3 = 4;
    std::string name1 = "file1";
    std::string name2 = "file2";
    std::string name3 = "file3";
    uint64_t txId = 1;

    std::list<Dentry> dlist;
    std::list<Dentry> dlist1;
    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_inodeid(inodeId1);
    dentry.set_parentinodeid(ino);
    dentry.set_name(name1);
    dentry.set_txid(txId);
    dentry.set_type(FsFileType::TYPE_DIRECTORY);
    dlist.emplace_back(dentry);

    dentry.set_inodeid(inodeId2);
    dentry.set_name(name2);
    dentry.set_type(FsFileType::TYPE_FILE);
    dlist.emplace_back(dentry);

    dentry.set_inodeid(inodeId3);
    dentry.set_name(name3);
    dentry.set_type(FsFileType::TYPE_FILE);
    dlist1.emplace_back(dentry);

    std::list<InodeAttr> attrs;
    InodeAttr attr;
    attr.set_fsid(fsId);
    attr.set_inodeid(inodeId1);
    attr.set_length(100);
    attr.set_type(FsFileType::TYPE_DIRECTORY);
    attrs.emplace_back(attr);
    attr.set_inodeid(inodeId2);
    attr.set_length(200);
    attr.set_type(FsFileType::TYPE_FILE);
    attrs.emplace_back(attr);

    std::list<InodeAttr> attrs1;
    InodeAttr attr1;
    attr1.set_inodeid(inodeId3);
    attr1.set_length(200);
    attr1.set_type(FsFileType::TYPE_FILE);
    attrs1.emplace_back(attr1);

    InodeAttr inode;
    inode.set_inodeid(ino);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_DIRECTORY);
    inode.mutable_xattr()->insert({XATTR_DIR_FILES, "0"});
    inode.mutable_xattr()->insert({XATTR_DIR_SUBDIRS, "0"});
    inode.mutable_xattr()->insert({XATTR_DIR_ENTRIES, "0"});
    inode.mutable_xattr()->insert({XATTR_DIR_FBYTES, "0"});

    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(dlist1), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetInodeAttr(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(attrs), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(attrs1), Return(CURVEFS_ERROR::OK)));

    CURVEFS_ERROR ret = client_->FuseOpGetXattr(req, ino, rname, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(value, "4596");

    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetInodeAttr(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(attrs), Return(CURVEFS_ERROR::OK)));

    ret = client_->FuseOpGetXattr(req, ino, name, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(value, "4396");
}

TEST_F(TestFuseS3Client, FuseOpGetXattr_NotEnableSumInDir_Failed) {
    // in
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    const char rname[] = "curve.dir.rfbytes";
    const char name[] = "curve.dir.fbytes";
    size_t size = 100;
    std::string value;

    // out
    uint32_t fsId = 1;
    uint64_t inodeId = 2;
    std::string name1 = "file";
    uint64_t txId = 1;

    std::list<Dentry> dlist;
    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_inodeid(inodeId);
    dentry.set_parentinodeid(ino);
    dentry.set_name(name1);
    dentry.set_txid(txId);
    dentry.set_type(FsFileType::TYPE_FILE);
    dlist.emplace_back(dentry);

    std::list<InodeAttr> attrs;
    InodeAttr attr;
    attr.set_fsid(fsId);
    attr.set_inodeid(inodeId);
    attr.set_length(100);
    attr.set_type(FsFileType::TYPE_FILE);
    attrs.emplace_back(attr);

    InodeAttr inode;
    inode.set_inodeid(ino);
    inode.mutable_xattr()->insert({XATTR_DIR_FILES, "aaa"});
    inode.mutable_xattr()->insert({XATTR_DIR_SUBDIRS, "1"});
    inode.mutable_xattr()->insert({XATTR_DIR_ENTRIES, "2"});
    inode.mutable_xattr()->insert({XATTR_DIR_FBYTES, "100"});

    // get inode failed
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::INTERNAL)));
    CURVEFS_ERROR ret = client_->FuseOpGetXattr(req, ino, rname, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // list dentry failed
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(Return(CURVEFS_ERROR::NOT_EXIST));
    ret = client_->FuseOpGetXattr(req, ino, rname, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // BatchGetInodeAttr failed
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetInodeAttr(_, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));
    ret = client_->FuseOpGetXattr(req, ino, rname, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseS3Client, FuseOpGetXattr_EnableSumInDir) {
    client_->SetEnableSumInDir(true);
    // in
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    const char name[] = "curve.dir.rentries";
    size_t size = 100;
    std::string value;

    // out
    uint32_t fsId = 1;
    std::string name1 = "file1";
    uint64_t txId = 1;

    std::list<Dentry> emptyDlist;
    std::list<Dentry> dlist;
    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_inodeid(ino);
    dentry.set_parentinodeid(ino);
    dentry.set_name(name1);
    dentry.set_txid(txId);
    dentry.set_type(FsFileType::TYPE_DIRECTORY);
    dlist.emplace_back(dentry);

    std::list<XAttr> xattrs;
    XAttr xattr;
    xattr.set_fsid(fsId);
    xattr.set_inodeid(ino);
    xattr.mutable_xattrinfos()->insert({XATTR_DIR_FILES, "2"});
    xattr.mutable_xattrinfos()->insert({XATTR_DIR_SUBDIRS, "2"});
    xattr.mutable_xattrinfos()->insert({XATTR_DIR_ENTRIES, "4"});
    xattr.mutable_xattrinfos()->insert({XATTR_DIR_FBYTES, "200"});
    xattrs.emplace_back(xattr);

    InodeAttr inode;
    inode.set_inodeid(ino);
    inode.set_nlink(3);
    inode.mutable_xattr()->insert({XATTR_DIR_FILES, "1"});
    inode.mutable_xattr()->insert({XATTR_DIR_SUBDIRS, "1"});
    inode.mutable_xattr()->insert({XATTR_DIR_ENTRIES, "2"});
    inode.mutable_xattr()->insert({XATTR_DIR_FBYTES, "100"});

    InodeAttr attr = inode;
    attr.set_nlink(2);

    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .Times(3)
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(attr), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgPointee<1>(emptyDlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetXAttr(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(xattrs), Return(CURVEFS_ERROR::OK)));

    CURVEFS_ERROR ret = client_->FuseOpGetXattr(req, ino, name, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(value, "6");
}

TEST_F(TestFuseS3Client, FuseOpGetXattr_EnableSumInDir_Failed) {
    client_->SetEnableSumInDir(true);
    // in
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    const char name[] = "curve.dir.entries";
    const char rname[] = "curve.dir.rentries";
    size_t size = 100;
    std::string value;

    // out
    uint32_t fsId = 1;
    uint64_t inodeId = 2;
    std::string name1 = "file";
    uint64_t txId = 1;

    std::list<Dentry> emptyDlist;
    std::list<Dentry> dlist;
    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_inodeid(inodeId);
    dentry.set_parentinodeid(ino);
    dentry.set_name(name1);
    dentry.set_txid(txId);
    dentry.set_type(FsFileType::TYPE_DIRECTORY);
    dlist.emplace_back(dentry);

    std::list<XAttr> xattrs;
    XAttr xattr;
    xattr.set_fsid(fsId);
    xattr.set_inodeid(inodeId);
    xattr.mutable_xattrinfos()->insert({XATTR_DIR_FILES, "2"});
    xattr.mutable_xattrinfos()->insert({XATTR_DIR_SUBDIRS, "2"});
    xattr.mutable_xattrinfos()->insert({XATTR_DIR_ENTRIES, "4"});
    xattr.mutable_xattrinfos()->insert({XATTR_DIR_FBYTES, "200"});
    xattrs.emplace_back(xattr);

    InodeAttr inode;
    inode.set_inodeid(ino);
    inode.set_nlink(3);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_DIRECTORY);
    inode.mutable_xattr()->insert({XATTR_DIR_FILES, "1"});
    inode.mutable_xattr()->insert({XATTR_DIR_SUBDIRS, "1"});
    inode.mutable_xattr()->insert({XATTR_DIR_ENTRIES, "2"});
    inode.mutable_xattr()->insert({XATTR_DIR_FBYTES, "aaa"});

    // get inode failed
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));
    CURVEFS_ERROR ret = client_->FuseOpGetXattr(req, ino, name, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // AddUllStringToFirst failed
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpGetXattr(req, ino, name, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    inode.mutable_xattr()->find(XATTR_DIR_FBYTES)->second = "100";

    // list dentry failed
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .Times(2)
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(Return(CURVEFS_ERROR::NOT_EXIST));
    ret = client_->FuseOpGetXattr(req, ino, rname, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // BatchGetInodeAttr failed
    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .Times(AtLeast(2))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(emptyDlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetXAttr(_, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));
    ret = client_->FuseOpGetXattr(req, ino, rname, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // AddUllStringToFirst  XATTR_DIR_FILES failed
    inode.mutable_xattr()->find(XATTR_DIR_FILES)->second = "aaa";
    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .Times(AtLeast(2))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(emptyDlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetXAttr(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(xattrs), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpGetXattr(req, ino, rname, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // AddUllStringToFirst  XATTR_DIR_SUBDIRS failed
    inode.mutable_xattr()->find(XATTR_DIR_FILES)->second = "0";
    inode.mutable_xattr()->find(XATTR_DIR_SUBDIRS)->second = "aaa";
    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .Times(AtLeast(2))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(emptyDlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetXAttr(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(xattrs), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpGetXattr(req, ino, rname, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // AddUllStringToFirst  XATTR_DIR_ENTRIES failed
    inode.mutable_xattr()->find(XATTR_DIR_SUBDIRS)->second = "0";
    inode.mutable_xattr()->find(XATTR_DIR_ENTRIES)->second = "aaa";
    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .Times(AtLeast(2))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(emptyDlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetXAttr(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(xattrs), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpGetXattr(req, ino, rname, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseS3Client, FuseOpCreate_EnableSummary) {
    client_->SetEnableSumInDir(true);

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
    inode.set_openmpcount(0);
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
    parentInode.mutable_xattr()->insert({XATTR_DIR_FILES, "1"});
    parentInode.mutable_xattr()->insert({XATTR_DIR_SUBDIRS, "1"});
    parentInode.mutable_xattr()->insert({XATTR_DIR_ENTRIES, "2"});
    parentInode.mutable_xattr()->insert({XATTR_DIR_FBYTES, "100"});

    auto parentInodeWrapper =
        std::make_shared<InodeWrapper>(parentInode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(parentInodeWrapper),
                        Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
        .WillRepeatedly(Return(MetaStatusCode::OK));

    EXPECT_CALL(*inodeManager_, ShipToFlush(_))
        .Times(1);  // update mtime directly

    EntryOut entryOut;
    CURVEFS_ERROR ret =
        client_->FuseOpCreate(req, parent, name, mode, &fi, &entryOut);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    auto p = parentInodeWrapper->GetInodeLocked();
    ASSERT_EQ(p->xattr().find(XATTR_DIR_FILES)->second, "2");
    ASSERT_EQ(p->xattr().find(XATTR_DIR_SUBDIRS)->second, "1");
    ASSERT_EQ(p->xattr().find(XATTR_DIR_ENTRIES)->second, "3");
    ASSERT_EQ(p->xattr().find(XATTR_DIR_FBYTES)->second, "4196");
}

TEST_F(TestFuseS3Client, FuseOpWrite_EnableSummary) {
    client_->SetEnableSumInDir(true);

    fuse_req_t req = nullptr;
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
    inode.add_parent(0);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    Inode parentInode;
    parentInode.set_fsid(1);
    parentInode.set_inodeid(0);
    parentInode.set_type(FsFileType::TYPE_DIRECTORY);
    parentInode.set_nlink(2);
    parentInode.mutable_xattr()->insert({XATTR_DIR_FILES, "1"});
    parentInode.mutable_xattr()->insert({XATTR_DIR_SUBDIRS, "0"});
    parentInode.mutable_xattr()->insert({XATTR_DIR_ENTRIES, "1"});
    parentInode.mutable_xattr()->insert({XATTR_DIR_FBYTES, "0"});

    auto parentInodeWrapper =
        std::make_shared<InodeWrapper>(parentInode, metaClient_);
    EXPECT_CALL(*inodeManager_, ShipToFlush(_)).Times(2);
    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgReferee<1>(parentInodeWrapper),
                        Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*s3ClientAdaptor_, Write(_, _, _, _)).WillOnce(Return(size));

    FileOut fileOut;
    CURVEFS_ERROR ret =
        client_->FuseOpWrite(req, ino, buf, size, off, &fi, &fileOut);

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(size, fileOut.nwritten);

    auto p = parentInodeWrapper->GetInodeLocked();
    ASSERT_EQ(p->xattr().find(XATTR_DIR_FILES)->second, "1");
    ASSERT_EQ(p->xattr().find(XATTR_DIR_SUBDIRS)->second, "0");
    ASSERT_EQ(p->xattr().find(XATTR_DIR_ENTRIES)->second, "1");
    ASSERT_EQ(p->xattr().find(XATTR_DIR_FBYTES)->second, std::to_string(size));
}

TEST_F(TestFuseS3Client, FuseOpLink_EnableSummary) {
    client_->SetEnableSumInDir(true);

    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    fuse_ino_t newparent = 2;
    const char* newname = "xxxx";

    Inode inode;
    inode.set_inodeid(ino);
    inode.set_length(100);
    inode.add_parent(0);
    inode.set_nlink(1);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    Inode pinode;
    pinode.set_inodeid(0);
    pinode.set_length(0);
    pinode.mutable_xattr()->insert({XATTR_DIR_FILES, "0"});
    pinode.mutable_xattr()->insert({XATTR_DIR_SUBDIRS, "0"});
    pinode.mutable_xattr()->insert({XATTR_DIR_ENTRIES, "0"});
    pinode.mutable_xattr()->insert({XATTR_DIR_FBYTES, "0"});
    auto pinodeWrapper = std::make_shared<InodeWrapper>(pinode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(pinodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, UpdateInodeAttr(_, _, _))
        .WillRepeatedly(Return(MetaStatusCode::OK));
    EXPECT_CALL(*dentryManager_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::OK));
    EXPECT_CALL(*inodeManager_, ShipToFlush(_)).Times(1);
    EntryOut entryOut;
    CURVEFS_ERROR ret =
        client_->FuseOpLink(req, ino, newparent, newname, &entryOut);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    auto p = pinodeWrapper->GetInode();
    ASSERT_EQ(p.xattr().find(XATTR_DIR_FILES)->second, "1");
    ASSERT_EQ(p.xattr().find(XATTR_DIR_SUBDIRS)->second, "0");
    ASSERT_EQ(p.xattr().find(XATTR_DIR_ENTRIES)->second, "1");
    ASSERT_EQ(p.xattr().find(XATTR_DIR_FBYTES)->second, "100");
}

TEST_F(TestFuseS3Client, FuseOpUnlink_EnableSummary) {
    client_->SetEnableSumInDir(true);

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
    dentry.set_type(FsFileType::TYPE_S3);

    EXPECT_CALL(*dentryManager_, GetDentry(parent, name, _))
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*dentryManager_,
                DeleteDentry(parent, name, FsFileType::TYPE_S3))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    Inode inode;
    inode.set_fsid(fsId);
    inode.set_inodeid(inodeid);
    inode.set_length(4096);
    inode.set_nlink(nlink);
    inode.set_type(FsFileType::TYPE_FILE);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    Inode parentInode;
    parentInode.set_fsid(fsId);
    parentInode.set_inodeid(parent);
    parentInode.set_type(FsFileType::TYPE_DIRECTORY);
    parentInode.set_nlink(3);
    parentInode.mutable_xattr()->insert({XATTR_DIR_FILES, "1"});
    parentInode.mutable_xattr()->insert({XATTR_DIR_SUBDIRS, "1"});
    parentInode.mutable_xattr()->insert({XATTR_DIR_ENTRIES, "2"});
    parentInode.mutable_xattr()->insert({XATTR_DIR_FBYTES, "4196"});

    InodeAttr attr;
    attr.set_fsid(fsId);
    attr.set_inodeid(inodeid);
    attr.set_nlink(nlink);

    auto parentInodeWrapper =
        std::make_shared<InodeWrapper>(parentInode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(
            SetArgReferee<1>(parentInodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, GetInodeAttr(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(attr), Return(MetaStatusCode::OK)));
    EXPECT_CALL(*metaClient_, UpdateInodeAttr(_, _, _))
        .WillRepeatedly(Return(MetaStatusCode::OK));

    CURVEFS_ERROR ret = client_->FuseOpUnlink(req, parent, name.c_str());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    auto p = parentInodeWrapper->GetInode();
    ASSERT_EQ(3, p.nlink());
    ASSERT_EQ(p.xattr().find(XATTR_DIR_FILES)->second, "0");
    ASSERT_EQ(p.xattr().find(XATTR_DIR_SUBDIRS)->second, "1");
    ASSERT_EQ(p.xattr().find(XATTR_DIR_ENTRIES)->second, "1");
    ASSERT_EQ(p.xattr().find(XATTR_DIR_FBYTES)->second, "100");
}

TEST_F(TestFuseS3Client, FuseOpOpen_Trunc_EnableSummary) {
    client_->SetEnableSumInDir(true);

    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    struct fuse_file_info fi;
    fi.flags = O_TRUNC | O_WRONLY;

    Inode inode;
    inode.set_fsid(1);
    inode.set_inodeid(1);
    inode.set_length(4096);
    inode.set_openmpcount(0);
    inode.add_parent(0);
    inode.set_mtime(123);
    inode.set_mtime_ns(456);
    inode.set_type(FsFileType::TYPE_S3);

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    Inode parentInode;
    parentInode.set_fsid(1);
    parentInode.set_inodeid(0);
    parentInode.set_type(FsFileType::TYPE_DIRECTORY);
    parentInode.set_nlink(3);
    parentInode.mutable_xattr()->insert({XATTR_DIR_FILES, "1"});
    parentInode.mutable_xattr()->insert({XATTR_DIR_SUBDIRS, "1"});
    parentInode.mutable_xattr()->insert({XATTR_DIR_ENTRIES, "2"});
    parentInode.mutable_xattr()->insert({XATTR_DIR_FBYTES, "4196"});

    auto parentInodeWrapper =
        std::make_shared<InodeWrapper>(parentInode, metaClient_);

    uint64_t parentId = 1;

    {  // mock lookup to remeber attribute mtime
        auto member = client_->GetFileSystem()->BorrowMember();
        auto attrWatcher = member.attrWatcher;
        InodeAttr attr;
        attr.set_inodeid(1);
        attr.set_mtime(123);
        attr.set_mtime_ns(456);
        attrWatcher->RemeberMtime(attr);
    }

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgReferee<1>(parentInodeWrapper),
                        Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*s3ClientAdaptor_, Truncate(_, _))
        .WillOnce(Return(CURVEFS_ERROR::OK));
    EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
        .WillRepeatedly(Return(MetaStatusCode::OK));
    EXPECT_CALL(*inodeManager_, ShipToFlush(_)).Times(0);

    FileOut fileOut;
    CURVEFS_ERROR ret = client_->FuseOpOpen(req, ino, &fi, &fileOut);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    auto p = parentInodeWrapper->GetInode();
    ASSERT_EQ(p.xattr().find(XATTR_DIR_FILES)->second, "1");
    ASSERT_EQ(p.xattr().find(XATTR_DIR_SUBDIRS)->second, "1");
    ASSERT_EQ(p.xattr().find(XATTR_DIR_ENTRIES)->second, "2");
    // FIXME: (Wine93)
    // ASSERT_EQ(p.xattr().find(XATTR_DIR_FBYTES)->second, "100");
}

TEST_F(TestFuseS3Client, FuseOpListXattr) {
    char buf[256];
    std::memset(buf, 0, 256);
    size_t size = 0;

    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    InodeAttr inode;
    inode.set_inodeid(ino);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_S3);
    std::string key = "security";
    inode.mutable_xattr()->insert({key, "0"});

    size_t realSize = 0;

    // failed when get inode
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::INTERNAL)));
    CURVEFS_ERROR ret =
        client_->FuseOpListXattr(req, ino, buf, size, &realSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpListXattr(req, ino, buf, size, &realSize);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(realSize, key.length() + 1);

    realSize = 0;
    inode.set_type(FsFileType::TYPE_DIRECTORY);
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpListXattr(req, ino, buf, size, &realSize);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    auto expected = key.length() + 1;
    ASSERT_EQ(realSize, expected);

    realSize = 0;
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpListXattr(req, ino, buf, expected - 1, &realSize);
    ASSERT_EQ(CURVEFS_ERROR::OUT_OF_RANGE, ret);

    realSize = 0;
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpListXattr(req, ino, buf, expected, &realSize);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseS3Client, FuseOpSetXattr_TooLong) {
    // in
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    const char name[] = "security.selinux";
    size_t size = 64 * 1024 + 1;
    char value[64 * 1024 + 1];
    std::memset(value, 0, size);

    CURVEFS_ERROR ret = client_->FuseOpSetXattr(req, ino, name, value, size, 0);
    ASSERT_EQ(CURVEFS_ERROR::OUT_OF_RANGE, ret);
}

TEST_F(TestFuseS3Client, FuseOpSetXattr) {
    // in
    fuse_req_t req = nullptr;
    fuse_ino_t ino = 1;
    const char name[] = "security.selinux";
    size_t size = 100;
    char value[100];
    std::memset(value, 0, 100);

    // get inode failed
    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));
    CURVEFS_ERROR ret = client_->FuseOpSetXattr(req, ino, name, value, size, 0);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // updateInode failed
    auto inodeWrapper = std::make_shared<InodeWrapper>(Inode(), metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::NOT_FOUND));
    ret = client_->FuseOpSetXattr(req, ino, name, value, size, 0);
    ASSERT_EQ(CURVEFS_ERROR::NOT_EXIST, ret);

    // success
    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::OK));
    ret = client_->FuseOpSetXattr(req, ino, name, value, size, 0);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseS3Client, FuseOpWriteQosTest) {
    curvefs::client::common::FLAGS_fuseClientAvgWriteBytes = 100;
    curvefs::client::common::FLAGS_fuseClientBurstWriteBytes = 150;
    curvefs::client::common::FLAGS_fuseClientBurstWriteBytesSecs = 180;
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    auto qosWriteTest = [&](int id, int len) {
        fuse_ino_t ino = id;
        Inode inode;
        inode.set_inodeid(ino);
        inode.set_length(0);
        auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

        EXPECT_CALL(*inodeManager_, GetInode(ino, _))
            .WillOnce(DoAll(SetArgReferee<1>(inodeWrapper),
                            Return(CURVEFS_ERROR::OK)));

        fuse_req_t req = nullptr;
        off_t off = 0;
        struct fuse_file_info fi;
        fi.flags = O_WRONLY;
        std::string buf('a', len);
        size_t size = buf.size();
        size_t s3Size = size;
        FileOut fileOut;

        EXPECT_CALL(*s3ClientAdaptor_, Write(_, _, _, _))
            .WillOnce(Return(s3Size));

        client_->Add(false, size);

        CURVEFS_ERROR ret = client_->FuseOpWrite(req, ino, buf.c_str(), size,
                                                 off, &fi, &fileOut);
        ASSERT_EQ(CURVEFS_ERROR::OK, ret);
        ASSERT_EQ(s3Size, fileOut.nwritten);
    };

    qosWriteTest(1, 90);
    qosWriteTest(2, 100);
    qosWriteTest(3, 160);
    qosWriteTest(4, 200);
}

TEST_F(TestFuseS3Client, FuseOpReadQosTest) {
    curvefs::client::common::FLAGS_fuseClientAvgReadBytes = 100;
    curvefs::client::common::FLAGS_fuseClientBurstReadBytes = 150;
    curvefs::client::common::FLAGS_fuseClientBurstReadBytesSecs = 180;

    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    auto qosReadTest = [&](int id, int size) {
        fuse_req_t req = nullptr;
        fuse_ino_t ino = id;
        off_t off = 0;
        struct fuse_file_info fi;
        fi.flags = O_RDONLY;

        Inode inode;
        inode.set_fsid(fsId);
        inode.set_inodeid(ino);
        inode.set_length(4096);
        auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

        EXPECT_CALL(*inodeManager_, GetInode(ino, _))
            .WillOnce(DoAll(SetArgReferee<1>(inodeWrapper),
                            Return(CURVEFS_ERROR::OK)));

        std::unique_ptr<char[]> buffer(new char[size]);
        size_t rSize = 0;
        client_->Add(true, size);
        CURVEFS_ERROR ret =
            client_->FuseOpRead(req, ino, size, off, &fi, buffer.get(), &rSize);
        ASSERT_EQ(CURVEFS_ERROR::OK, ret);
        ASSERT_EQ(0, rSize);
    };

    qosReadTest(1, 90);
    qosReadTest(2, 100);
    qosReadTest(3, 160);
    qosReadTest(4, 200);
}

}  // namespace client
}  // namespace curvefs
