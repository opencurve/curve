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
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/fuse_s3_client.h"
#include "curvefs/src/common/define.h"
#include "curvefs/test/client/mock_client_s3_adaptor.h"
#include "curvefs/test/client/mock_dentry_cache_mamager.h"
#include "curvefs/test/client/mock_inode_cache_manager.h"
#include "curvefs/test/client/rpcclient/mock_mds_client.h"
#include "curvefs/test/client/mock_metaserver_client.h"

struct fuse_req {
    struct fuse_ctx *ctx;
};

namespace curvefs {
namespace client {
namespace common {
DECLARE_bool(enableCto);
DECLARE_bool(supportKVcache);
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
using curvefs::client::common::FileHandle;

#define EQUAL(a) (lhs.a() == rhs.a())

static bool operator==(const Dentry &lhs, const Dentry &rhs) {
    return EQUAL(fsid) && EQUAL(parentinodeid) && EQUAL(name) && EQUAL(txid) &&
           EQUAL(inodeid) && EQUAL(flag);
}

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
        client_ = std::make_shared<FuseS3Client>(mdsClient_, metaClient_,
                                                 inodeManager_, dentryManager_,
                                                 s3ClientAdaptor_);
        InitOptionBasic(&fuseClientOption_);
        InitFSInfo(client_);
        fuseClientOption_.s3Opt.s3AdaptrOpt.asyncThreadNum = 1;
        fuseClientOption_.dummyServerStartPort = 5000;
        fuseClientOption_.maxNameLength = 20u;
        fuseClientOption_.listDentryThreads = 2;
        fuseClientOption_.warmupThreadsNum = 10;
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
    }

    void InitOptionBasic(FuseClientOption *opt) {
        opt->s3Opt.s3AdaptrOpt.asyncThreadNum = 1;
        opt->dummyServerStartPort = 5000;
        opt->maxNameLength = 20u;
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
};

TEST_F(TestFuseS3Client, test_Init_with_KVCache) {
    curvefs::client::common::FLAGS_supportKVcache = true;
    curvefs::mds::topology::MemcacheClusterInfo memcacheCluster;
    memcacheCluster.set_clusterid(1);
    auto testclient =
        std::make_shared<FuseS3Client>(mdsClient_, metaClient_, inodeManager_,
                                       dentryManager_, s3ClientAdaptor_);
    FuseClientOption opt;
    InitOptionBasic(&opt);
    InitFSInfo(testclient);
    testclient->SetMounted(true);

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

// GetDentry failed; dentry not exist
TEST_F(TestFuseS3Client, warmUp_dentryNotexist) {
    // wait init
    sleep(1);
    std::string warmUpPath = "/test";
    fuse_req_t req;
    fuse_ino_t parent = 1;
    std::string name = "test";
    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);
    dentry.set_type(FsFileType::TYPE_S3);
    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(dentry),
          Return(CURVEFS_ERROR::NOTEXIST)));
    client_->PutWarmTask(warmUpPath);
    sleep(5);
}

// GetDentry failed; bad fd
TEST_F(TestFuseS3Client, warmUp_dentryBadFd) {
    sleep(1);
    std::string warmUpPath = "/test";
    fuse_req_t req;
    fuse_ino_t parent = 1;
    std::string name = "test";
    fuse_ino_t inodeid = 2;

    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_name(name);
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(inodeid);
    dentry.set_type(FsFileType::TYPE_S3);

    EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(dentry),
          Return(CURVEFS_ERROR::BAD_FD)));
    client_->PutWarmTask(warmUpPath);
    sleep(5);
}

// GetInode failed; bad fd
TEST_F(TestFuseS3Client, warmUp_inodeBadFd) {
    sleep(1);
    std::string warmUpPath = "/test";
    fuse_req_t req;
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
            DoAll(SetArgReferee<1>(inodeWrapper),
              Return(CURVEFS_ERROR::BAD_FD)));
    client_->PutWarmTask(warmUpPath);
    sleep(5);
}

// single file (parent is root)
TEST_F(TestFuseS3Client, warmUp_Warmfile_error_GetDentry01) {
    sleep(1);
    std::string warmUpPath = "/test";
    fuse_req_t req;
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
        .WillOnce(DoAll(SetArgPointee<2>(dentry),
          Return(CURVEFS_ERROR::BAD_FD)));
     EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)));

    size_t len = 20;
    char *tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = 't';
    tmpbuf[2] = 'e';
    tmpbuf[3] = '\n';

    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len),
                        Return(len)));
    client_->PutWarmTask(warmUpPath);
    sleep(5);
}

// warmup failed because of GetDentry failed
TEST_F(TestFuseS3Client, warmUp_Warmfile_error_GetDentry02) {
    sleep(1);
    std::string warmUpPath = "/test";
    fuse_req_t req;
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
        .WillOnce(DoAll(SetArgPointee<2>(dentry),
          Return(CURVEFS_ERROR::NOTEXIST)));
     EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)));

    size_t len = 20;
    char *tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = 't';
    tmpbuf[2] = 'e';
    tmpbuf[3] = '\n';

    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len),
                        Return(len)));
    client_->PutWarmTask(warmUpPath);
    sleep(5);
}

// warmup failed because of Getinode failed
TEST_F(TestFuseS3Client, warmUp_fetchDataEnqueue__error_getinode) {
    sleep(1);
    std::string warmUpPath = "/test";
    fuse_req_t req;
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
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)));
     EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::NOTEXIST)));

    size_t len = 20;
    char *tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = 't';
    tmpbuf[2] = 'e';
    tmpbuf[3] = '\n';
    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len),
                        Return(len)));
    client_->PutWarmTask(warmUpPath);
    sleep(5);
}

// chunk is empty
TEST_F(TestFuseS3Client, warmUp_fetchDataEnqueue_chunkempty) {
    sleep(1);
    std::string warmUpPath = "/test";
    fuse_req_t req;
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
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)));
     EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)));

    size_t len = 20;
    char *tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = 't';
    tmpbuf[2] = 'e';
    tmpbuf[3] = '\n';
    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len),
                        Return(len)));
    client_->PutWarmTask(warmUpPath);
    sleep(5);
}

// single file (parent is root); FetchDentry
TEST_F(TestFuseS3Client, warmUp_FetchDentry_TYPE_SYM_LINK) {
    sleep(1);
    std::string warmUpPath = "/test";
    fuse_req_t req;
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
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(dentry1), Return(CURVEFS_ERROR::OK)));
     EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)));
    size_t len = 20;
    char *tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = 't';
    tmpbuf[2] = 'e';
    tmpbuf[3] = '\n';
    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len),
                        Return(len)));
    client_->PutWarmTask(warmUpPath);
    sleep(5);
}

// fetch dentry failed
TEST_F(TestFuseS3Client, warmUp_FetchDentry_error_TYPE_DIRECTORY) {
    sleep(1);
    std::string warmUpPath = "/test";
    fuse_req_t req;
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
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(dentry1), Return(CURVEFS_ERROR::OK)));
     EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)));
    std::list<Dentry> dlist;
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::NOTEXIST)));
    size_t len = 20;
    char *tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = 't';
    tmpbuf[2] = 'e';
    tmpbuf[3] = '\n';
    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len),
                        Return(len)));
    client_->PutWarmTask(warmUpPath);
    sleep(5);
}

// lookpath
TEST_F(TestFuseS3Client, warmUp_lookpath_multilevel) {
    sleep(1);
    std::string warmUpPath = "/test";
    fuse_req_t req;
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
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(dentry), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)));

    size_t len = 20;
    char *tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = 'a';
    tmpbuf[2] = '/';
    tmpbuf[3] = 'b';
    tmpbuf[4] = '/';
    tmpbuf[5] = 'c';
    tmpbuf[6] = '\n';
    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len),
                        Return(len)));
    client_->PutWarmTask(warmUpPath);
    sleep(5);
}

// lookpath failed; unknown path
TEST_F(TestFuseS3Client, warmUp_lookpath_unkown) {
    sleep(1);
    std::string warmUpPath = "/test";
    fuse_req_t req;
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
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)));

    size_t len = 20;
    char *tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len),
                        Return(len)));
    client_->PutWarmTask(warmUpPath);
    sleep(5);
}

// i am root
TEST_F(TestFuseS3Client, warmUp_FetchChildDentry_error_ListDentry) {
    sleep(1);
    std::string warmUpPath = "/test";
    fuse_req_t req;
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

    std::list<Dentry> dlist;
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::NOTEXIST)));

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)));
    size_t len = 20;
    char *tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = '\n';
    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len),
                        Return(len)));
    client_->PutWarmTask(warmUpPath);
    sleep(5);
}

// success
TEST_F(TestFuseS3Client, warmUp_FetchChildDentry_suc_ListDentry) {
    sleep(1);
    std::string warmUpPath = "/test";
    fuse_req_t req;
    fuse_ino_t parent = 1;
    std::string name = "test";
    fuse_ino_t inodeid = 5;

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

    std::list<Dentry> dlist;
    std::list<Dentry> dlist1;
    Dentry dentry1;
    dentry1.set_fsid(fsId);
    dentry1.set_inodeid(inodeid);
    dentry1.set_parentinodeid(parent);
    dentry1.set_name("5");
    dentry1.set_type(FsFileType::TYPE_DIRECTORY);
    dlist.emplace_back(dentry1);

    dentry.set_inodeid(2);
    dentry.set_name("2");
    dentry.set_type(FsFileType::TYPE_S3);
    dlist.emplace_back(dentry);
    Dentry dentry2;
    dentry2.set_inodeid(3);
    dentry2.set_parentinodeid(parent);
    dentry2.set_name("3");
    dentry2.set_type(FsFileType::TYPE_SYM_LINK);
    dlist.emplace_back(dentry2);
    Dentry dentry3;
    dentry3.set_inodeid(4);
    dentry3.set_parentinodeid(parent);
    dentry3.set_name("4");
    dentry3.set_type(FsFileType::TYPE_FILE);
    dlist.emplace_back(dentry3);

    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::NOTEXIST)));

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper),
                Return(CURVEFS_ERROR::OK)));

    size_t len = 20;
    char *tmpbuf = new char[len];
    memset(tmpbuf, '\n', len);
    tmpbuf[0] = '/';
    tmpbuf[1] = '\n';
    EXPECT_CALL(*s3ClientAdaptor_, Read(_, _, _, _))
        .WillOnce(DoAll(SetArrayArgument<3>(tmpbuf, tmpbuf + len),
                        Return(len)));
    client_->PutWarmTask(warmUpPath);
    sleep(5);
}

TEST_F(TestFuseS3Client, FuseOpInit_when_fs_exist) {
    MountOption mOpts;
    memset(&mOpts, 0, sizeof(mOpts));
    mOpts.fsName = "s3fs";
    mOpts.mountPoint = "host1:/test";
    mOpts.fsType = "s3";

    std::string fsName = mOpts.fsName;
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

TEST_F(TestFuseS3Client, FuseOpDestroy) {
    MountOption mOpts;
    memset(&mOpts, 0, sizeof(mOpts));
    mOpts.fsName = "s3fs";
    mOpts.mountPoint = "host1:/test";
    mOpts.fsType = "s3";

    std::string fsName = mOpts.fsName;

    EXPECT_CALL(*mdsClient_, UmountFs(fsName, _))
        .WillOnce(Return(FSStatusCode::OK));

    client_->FuseOpDestroy(&mOpts);
}

TEST_F(TestFuseS3Client, FuseOpWriteSmallSize) {
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
    const char *buf = "xxx";
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
    struct fuse_file_info *fi;

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
    fuse_req_t req;
    fuse_ino_t ino = 1;
    struct fuse_file_info *fi;
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
    fuse_req_t req;
    fuse_ino_t ino = 1;
    const char name[] = "security.selinux";
    size_t size = 100;
    std::string value;

    CURVEFS_ERROR ret = client_->FuseOpGetXattr(req, ino, name, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::NODATA, ret);
}

TEST_F(TestFuseS3Client, FuseOpGetXattr_NotEnableSumInDir) {
    // in
    fuse_req_t req;
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
    inode.mutable_xattr()->insert({XATTRFILES, "0"});
    inode.mutable_xattr()->insert({XATTRSUBDIRS, "0"});
    inode.mutable_xattr()->insert({XATTRENTRIES, "0"});
    inode.mutable_xattr()->insert({XATTRFBYTES, "0"});

    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist1), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetInodeAttr(_, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(attrs), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgPointee<1>(attrs1), Return(CURVEFS_ERROR::OK)));

    CURVEFS_ERROR ret = client_->FuseOpGetXattr(req, ino, rname, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(value, "4596");

    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetInodeAttr(_, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(attrs), Return(CURVEFS_ERROR::OK)));

    ret = client_->FuseOpGetXattr(req, ino, name, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(value, "4396");
}

TEST_F(TestFuseS3Client, FuseOpGetXattr_NotEnableSumInDir_Failed) {
    // in
    fuse_req_t req;
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
    inode.mutable_xattr()->insert({XATTRFILES, "aaa"});
    inode.mutable_xattr()->insert({XATTRSUBDIRS, "1"});
    inode.mutable_xattr()->insert({XATTRENTRIES, "2"});
    inode.mutable_xattr()->insert({XATTRFBYTES, "100"});

    // get inode failed
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode),
                        Return(CURVEFS_ERROR::INTERNAL)));
    CURVEFS_ERROR ret = client_->FuseOpGetXattr(req, ino, rname, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // list dentry failed
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(Return(CURVEFS_ERROR::NOTEXIST));
    ret = client_->FuseOpGetXattr(req, ino, rname, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // BatchGetInodeAttr failed
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetInodeAttr(_, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));
    ret = client_->FuseOpGetXattr(req, ino, rname, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // AddUllStringToFirst  XATTRFILES failed
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetInodeAttr(_, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(attrs), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpGetXattr(req, ino, name, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // AddUllStringToFirst  XATTRSUBDIRS failed
    inode.mutable_xattr()->find(XATTRFILES)->second = "0";
    inode.mutable_xattr()->find(XATTRSUBDIRS)->second = "aaa";
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetInodeAttr(_, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(attrs), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpGetXattr(req, ino, name, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // AddUllStringToFirst  XATTRENTRIES failed
    inode.mutable_xattr()->find(XATTRSUBDIRS)->second = "0";
    inode.mutable_xattr()->find(XATTRENTRIES)->second = "aaa";
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetInodeAttr(_, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(attrs), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpGetXattr(req, ino, name, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // AddUllStringToFirst  XATTRFBYTES failed
    inode.mutable_xattr()->find(XATTRENTRIES)->second = "0";
    inode.mutable_xattr()->find(XATTRFBYTES)->second = "aaa";
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetInodeAttr(_, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(attrs), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpGetXattr(req, ino, name, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
}

TEST_F(TestFuseS3Client, FuseOpGetXattr_EnableSumInDir) {
    client_->SetEnableSumInDir(true);
    // in
    fuse_req_t req;
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
    xattr.mutable_xattrinfos()->insert({XATTRFILES, "2"});
    xattr.mutable_xattrinfos()->insert({XATTRSUBDIRS, "2"});
    xattr.mutable_xattrinfos()->insert({XATTRENTRIES, "4"});
    xattr.mutable_xattrinfos()->insert({XATTRFBYTES, "200"});
    xattrs.emplace_back(xattr);

    InodeAttr inode;
    inode.set_inodeid(ino);
    inode.set_nlink(3);
    inode.mutable_xattr()->insert({XATTRFILES, "1"});
    inode.mutable_xattr()->insert({XATTRSUBDIRS, "1"});
    inode.mutable_xattr()->insert({XATTRENTRIES, "2"});
    inode.mutable_xattr()->insert({XATTRFBYTES, "100"});

    InodeAttr attr = inode;
    attr.set_nlink(2);

    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .Times(3)
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgPointee<1>(attr), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgPointee<1>(emptyDlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetXAttr(_, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(xattrs), Return(CURVEFS_ERROR::OK)));

    CURVEFS_ERROR ret = client_->FuseOpGetXattr(req, ino, name, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(value, "6");
}

TEST_F(TestFuseS3Client, FuseOpGetXattr_EnableSumInDir_Failed) {
    client_->SetEnableSumInDir(true);
    // in
    fuse_req_t req;
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
    xattr.mutable_xattrinfos()->insert({XATTRFILES, "2"});
    xattr.mutable_xattrinfos()->insert({XATTRSUBDIRS, "2"});
    xattr.mutable_xattrinfos()->insert({XATTRENTRIES, "4"});
    xattr.mutable_xattrinfos()->insert({XATTRFBYTES, "200"});
    xattrs.emplace_back(xattr);

    InodeAttr inode;
    inode.set_inodeid(ino);
    inode.set_nlink(3);
    inode.set_length(4096);
    inode.set_type(FsFileType::TYPE_DIRECTORY);
    inode.mutable_xattr()->insert({XATTRFILES, "1"});
    inode.mutable_xattr()->insert({XATTRSUBDIRS, "1"});
    inode.mutable_xattr()->insert({XATTRENTRIES, "2"});
    inode.mutable_xattr()->insert({XATTRFBYTES, "aaa"});

    // get inode failed
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));
    CURVEFS_ERROR ret = client_->FuseOpGetXattr(req, ino, name, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // AddUllStringToFirst failed
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode),
                        Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpGetXattr(req, ino, name, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    inode.mutable_xattr()->find(XATTRFBYTES)->second = "100";

    // list dentry failed
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .Times(2)
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(Return(CURVEFS_ERROR::NOTEXIST));
    ret = client_->FuseOpGetXattr(req, ino, rname, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // BatchGetInodeAttr failed
    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .Times(AtLeast(2))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(emptyDlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetXAttr(_, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));
    ret = client_->FuseOpGetXattr(req, ino, rname, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // AddUllStringToFirst  XATTRFILES failed
    inode.mutable_xattr()->find(XATTRFILES)->second = "aaa";
    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .Times(AtLeast(2))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(emptyDlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetXAttr(_, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(xattrs), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpGetXattr(req, ino, rname, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // AddUllStringToFirst  XATTRSUBDIRS failed
    inode.mutable_xattr()->find(XATTRFILES)->second = "0";
    inode.mutable_xattr()->find(XATTRSUBDIRS)->second = "aaa";
    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .Times(AtLeast(2))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(emptyDlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetXAttr(_, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(xattrs), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpGetXattr(req, ino, rname, &value, size);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // AddUllStringToFirst  XATTRENTRIES failed
    inode.mutable_xattr()->find(XATTRSUBDIRS)->second = "0";
    inode.mutable_xattr()->find(XATTRENTRIES)->second = "aaa";
    EXPECT_CALL(*inodeManager_, GetInodeAttr(_, _))
        .Times(AtLeast(2))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*dentryManager_, ListDentry(_, _, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(dlist), Return(CURVEFS_ERROR::OK)))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(emptyDlist), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*inodeManager_, BatchGetXAttr(_, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(xattrs), Return(CURVEFS_ERROR::OK)));
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
    parentInode.mutable_xattr()->insert({XATTRFILES, "1"});
    parentInode.mutable_xattr()->insert({XATTRSUBDIRS, "1"});
    parentInode.mutable_xattr()->insert({XATTRENTRIES, "2"});
    parentInode.mutable_xattr()->insert({XATTRFBYTES, "100"});

    auto parentInodeWrapper = std::make_shared<InodeWrapper>(
        parentInode, metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(parentInodeWrapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(parentInodeWrapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
        .WillRepeatedly(Return(MetaStatusCode::OK));

    EXPECT_CALL(*inodeManager_, ShipToFlush(_))
        .Times(2);

    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpCreate(req, parent, name, mode, &fi, &e);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    auto p = parentInodeWrapper->GetInodeLocked();
    ASSERT_EQ(p->xattr().find(XATTRFILES)->second, "2");
    ASSERT_EQ(p->xattr().find(XATTRSUBDIRS)->second, "1");
    ASSERT_EQ(p->xattr().find(XATTRENTRIES)->second, "3");
    ASSERT_EQ(p->xattr().find(XATTRFBYTES)->second, "4196");
}

TEST_F(TestFuseS3Client, FuseOpWrite_EnableSummary) {
    client_->SetEnableSumInDir(true);

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
    inode.add_parent(0);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    Inode parentInode;
    parentInode.set_fsid(1);
    parentInode.set_inodeid(0);
    parentInode.set_type(FsFileType::TYPE_DIRECTORY);
    parentInode.set_nlink(2);
    parentInode.mutable_xattr()->insert({XATTRFILES, "1"});
    parentInode.mutable_xattr()->insert({XATTRSUBDIRS, "0"});
    parentInode.mutable_xattr()->insert({XATTRENTRIES, "1"});
    parentInode.mutable_xattr()->insert({XATTRFBYTES, "0"});

    uint64_t parentId = 1;

    auto parentInodeWrapper = std::make_shared<InodeWrapper>(
        parentInode, metaClient_);
    EXPECT_CALL(*inodeManager_, ShipToFlush(_))
        .Times(2);
    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(parentInodeWrapper),
                Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*s3ClientAdaptor_, Write(_, _, _, _))
        .WillOnce(Return(size));

    CURVEFS_ERROR ret =
        client_->FuseOpWrite(req, ino, buf, size, off, &fi, &wSize);

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(size, wSize);

    auto p = parentInodeWrapper->GetInodeLocked();
    ASSERT_EQ(p->xattr().find(XATTRFILES)->second, "1");
    ASSERT_EQ(p->xattr().find(XATTRSUBDIRS)->second, "0");
    ASSERT_EQ(p->xattr().find(XATTRENTRIES)->second, "1");
    ASSERT_EQ(p->xattr().find(XATTRFBYTES)->second, std::to_string(size));
}

TEST_F(TestFuseS3Client, FuseOpLink_EnableSummary) {
    client_->SetEnableSumInDir(true);

    fuse_req_t req;
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
    pinode.mutable_xattr()->insert({XATTRFILES, "0"});
    pinode.mutable_xattr()->insert({XATTRSUBDIRS, "0"});
    pinode.mutable_xattr()->insert({XATTRENTRIES, "0"});
    pinode.mutable_xattr()->insert({XATTRFBYTES, "0"});
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
    EXPECT_CALL(*inodeManager_, ShipToFlush(_))
        .Times(2);
    fuse_entry_param e;
    CURVEFS_ERROR ret = client_->FuseOpLink(req, ino, newparent, newname, &e);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    auto p = pinodeWrapper->GetInode();
    ASSERT_EQ(p.xattr().find(XATTRFILES)->second, "1");
    ASSERT_EQ(p.xattr().find(XATTRSUBDIRS)->second, "0");
    ASSERT_EQ(p.xattr().find(XATTRENTRIES)->second, "1");
    ASSERT_EQ(p.xattr().find(XATTRFBYTES)->second, "100");
}

TEST_F(TestFuseS3Client, FuseOpUnlink_EnableSummary) {
    client_->SetEnableSumInDir(true);

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
    parentInode.mutable_xattr()->insert({XATTRFILES, "1"});
    parentInode.mutable_xattr()->insert({XATTRSUBDIRS, "1"});
    parentInode.mutable_xattr()->insert({XATTRENTRIES, "2"});
    parentInode.mutable_xattr()->insert({XATTRFBYTES, "4196"});

    InodeAttr attr;
    attr.set_fsid(fsId);
    attr.set_inodeid(inodeid);
    attr.set_nlink(nlink);

    auto parentInodeWrapper = std::make_shared<InodeWrapper>(
        parentInode, metaClient_);

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(parentInodeWrapper),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(parentInodeWrapper),
                Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, GetInodeAttr(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(attr), Return(MetaStatusCode::OK)));
    EXPECT_CALL(*metaClient_, UpdateInodeAttr(_, _, _))
        .WillRepeatedly(Return(MetaStatusCode::OK));

    EXPECT_CALL(*inodeManager_, ShipToFlush(_))
        .Times(2);

    EXPECT_CALL(*inodeManager_, ClearInodeCache(inodeid))
        .Times(1);

    CURVEFS_ERROR ret = client_->FuseOpUnlink(req, parent, name.c_str());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    Inode inode2 = inodeWrapper->GetInode();
    ASSERT_EQ(nlink - 1, inode2.nlink());

    auto p = parentInodeWrapper->GetInode();
    ASSERT_EQ(3, p.nlink());
    ASSERT_EQ(p.xattr().find(XATTRFILES)->second, "0");
    ASSERT_EQ(p.xattr().find(XATTRSUBDIRS)->second, "1");
    ASSERT_EQ(p.xattr().find(XATTRENTRIES)->second, "1");
    ASSERT_EQ(p.xattr().find(XATTRFBYTES)->second, "100");
}

TEST_F(TestFuseS3Client, FuseOpOpen_Trunc_EnableSummary) {
    client_->SetEnableSumInDir(true);

    fuse_req_t req;
    fuse_ino_t ino = 1;
    struct fuse_file_info fi;
    fi.flags = O_TRUNC | O_WRONLY;

    Inode inode;
    inode.set_fsid(1);
    inode.set_inodeid(1);
    inode.set_length(4096);
    inode.set_openmpcount(0);
    inode.add_parent(0);
    inode.set_type(FsFileType::TYPE_S3);

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaClient_);

    Inode parentInode;
    parentInode.set_fsid(1);
    parentInode.set_inodeid(0);
    parentInode.set_type(FsFileType::TYPE_DIRECTORY);
    parentInode.set_nlink(3);
    parentInode.mutable_xattr()->insert({XATTRFILES, "1"});
    parentInode.mutable_xattr()->insert({XATTRSUBDIRS, "1"});
    parentInode.mutable_xattr()->insert({XATTRENTRIES, "2"});
    parentInode.mutable_xattr()->insert({XATTRFBYTES, "4196"});

    auto parentInodeWrapper = std::make_shared<InodeWrapper>(
        parentInode, metaClient_);

    uint64_t parentId = 1;

    EXPECT_CALL(*inodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(parentInodeWrapper),
                Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*s3ClientAdaptor_, Truncate(_, _))
        .WillOnce(Return(CURVEFS_ERROR::OK));
    EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
        .WillRepeatedly(Return(MetaStatusCode::OK));
    EXPECT_CALL(*inodeManager_, ShipToFlush(_))
        .Times(1);

    CURVEFS_ERROR ret = client_->FuseOpOpen(req, ino, &fi);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    auto p = parentInodeWrapper->GetInode();
    ASSERT_EQ(p.xattr().find(XATTRFILES)->second, "1");
    ASSERT_EQ(p.xattr().find(XATTRSUBDIRS)->second, "1");
    ASSERT_EQ(p.xattr().find(XATTRENTRIES)->second, "2");
    ASSERT_EQ(p.xattr().find(XATTRFBYTES)->second, "100");
}

TEST_F(TestFuseS3Client, FuseOpListXattr) {
    char buf[256];
    std::memset(buf, 0, 256);
    size_t size = 0;

    fuse_req_t req;
    fuse_ino_t ino = 1;
    struct fuse_file_info fi;
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
            DoAll(SetArgPointee<1>(inode),
                Return(CURVEFS_ERROR::INTERNAL)));
    CURVEFS_ERROR ret = client_->FuseOpListXattr(
        req, ino, buf, size, &realSize);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpListXattr(
        req, ino, buf, size, &realSize);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(realSize, key.length() + 1);

    realSize = 0;
    inode.set_type(FsFileType::TYPE_DIRECTORY);
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpListXattr(
        req, ino, buf, size, &realSize);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    auto expected = key.length() + 1 + strlen(XATTRRFILES) + 1 +
                    strlen(XATTRRSUBDIRS) + 1 + strlen(XATTRRENTRIES) + 1 +
                    strlen(XATTRRFBYTES) + 1;
    ASSERT_EQ(realSize, expected);

    realSize = 0;
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpListXattr(
        req, ino, buf, expected - 1, &realSize);
    ASSERT_EQ(CURVEFS_ERROR::OUT_OF_RANGE, ret);

    realSize = 0;
    EXPECT_CALL(*inodeManager_, GetInodeAttr(ino, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(inode), Return(CURVEFS_ERROR::OK)));
    ret = client_->FuseOpListXattr(
        req, ino, buf, expected, &realSize);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestFuseS3Client, FuseOpSetXattr_TooLong) {
    // in
    fuse_req_t req;
    fuse_ino_t ino = 1;
    const char name[] = "security.selinux";
    size_t size = 64 * 1024 + 1;
    char value[64 * 1024 + 1];
    std::memset(value, 0, size);

    CURVEFS_ERROR ret = client_->FuseOpSetXattr(
        req, ino, name, value, size, 0);
    ASSERT_EQ(CURVEFS_ERROR::OUT_OF_RANGE, ret);
}

TEST_F(TestFuseS3Client, FuseOpSetXattr) {
    // in
    fuse_req_t req;
    fuse_ino_t ino = 1;
    const char name[] = "security.selinux";
    size_t size = 100;
    char value[100];
    std::memset(value, 0, 100);

    // get inode failed
    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));
    CURVEFS_ERROR ret = client_->FuseOpSetXattr(
        req, ino, name, value, size, 0);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    // updateInode failed
    auto inodeWrapper = std::make_shared<InodeWrapper>(Inode(), metaClient_);
    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::NOT_FOUND));
    ret = client_->FuseOpSetXattr(
        req, ino, name, value, size, 0);
    ASSERT_EQ(CURVEFS_ERROR::NOTEXIST, ret);

    // success
    EXPECT_CALL(*inodeManager_, GetInode(ino, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::OK));
    ret = client_->FuseOpSetXattr(
        req, ino, name, value, size, 0);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

}  // namespace client
}  // namespace curvefs
