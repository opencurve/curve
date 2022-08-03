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
 * Project: curve
 * Created Date: Thur Jun 22 2021
 * Author: huyao
 */

#include "curvefs/src/client/s3/client_s3_adaptor.h"

#include <brpc/server.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "curvefs/test/client/mock_client_s3.h"
#include "curvefs/test/client/mock_inode_cache_manager.h"
#include "curvefs/test/client/rpcclient/mock_mds_client.h"
#include "curvefs/test/client/mock_metaserver_service.h"
#include "src/common/curve_define.h"

namespace curvefs {
namespace client {
namespace common {
DECLARE_bool(enableCto);
}  // namespace common
}  // namespace client
}  // namespace curvefs

namespace curvefs {
namespace client {
using ::curve::common::kMB;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;
using ::testing::WithArg;

using rpcclient::MockMdsClient;

template <typename RpcRequestType, typename RpcResponseType,
          bool RpcFailed = false>
void S3RpcService(google::protobuf::RpcController *cntl_base,
                  const RpcRequestType *request, RpcResponseType *response,
                  google::protobuf::Closure *done) {
    if (RpcFailed) {
        brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);
        cntl->SetFailed(112, "Not connected to");
    }
    LOG(INFO) << "run s3 prc service";
    done->Run();
}

struct S3Data {
    char *buf;
    uint64_t len;
};

static std::map<std::string, S3Data> gObjectDataMaps;

int S3Upload(std::string name, const char *buf, uint64_t len) {
    S3Data &tmp = gObjectDataMaps[name];

    tmp.len = len;
    tmp.buf = new char[len];
    strncpy(tmp.buf, buf, len);
    LOG(INFO) << "S3Upload len:" << len << ",name" << name << "buf:" << buf[0];
    return len;
}

int S3Download(std::string name, char *buf, uint64_t offset, uint64_t len) {
    S3Data &tmp = gObjectDataMaps[name];

    assert((offset + len) <= tmp.len);
    strncpy(buf, tmp.buf + offset, len);
    LOG(INFO) << "S3Download offset:" << offset << ",len:" << len
              << ",name:" << name << "buf:" << buf[0];
    return len;
}

class ClientS3IntegrationTest : public testing::Test {
 protected:
    ClientS3IntegrationTest() {}
    ~ClientS3IntegrationTest() {}
    void SetUp() override {
        Aws::InitAPI(awsOptions_);
        ASSERT_EQ(0, server_.AddService(&mockMetaServerService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));
        S3ClientAdaptorOption option;
        option.blockSize = 1 * 1024 * 1024;
        option.chunkSize = 4 * 1024 * 1024;
        option.pageSize = 64 * 1024;
        option.intervalSec = 5000;
        option.flushIntervalSec = 5000;
        option.readCacheMaxByte = 104857600;
        option.writeCacheMaxByte = 10485760000;
        option.diskCacheOpt.diskCacheType = (DiskCacheType)0;
        option.chunkFlushThreads = 5;
        std::shared_ptr<MockInodeCacheManager> mockInodeManager(
            &mockInodeManager_);
        std::shared_ptr<MockMdsClient> mockMdsClient(&mockMdsClient_);
        std::shared_ptr<MockS3Client> mockS3Client(&mockS3Client_);
        s3ClientAdaptor_ = new S3Adaptor();
        auto fsCacheManager = std::make_shared<FsCacheManager>(
            s3ClientAdaptor_, option.readCacheMaxByte,
            option.writeCacheMaxByte);
        s3ClientAdaptor_->Init(option, mockS3Client, mockInodeManager,
                               mockMdsClient, fsCacheManager, nullptr);
        s3ClientAdaptor_->SetFsId(2);
        curvefs::client::common::FLAGS_enableCto = false;
    }

    void TearDown() override {
        Aws::ShutdownAPI(awsOptions_);
        server_.Stop(0);
        server_.Join();
    }

 protected:
    S3Adaptor *s3ClientAdaptor_;
    MockMetaServerService mockMetaServerService_;
    MockS3Client mockS3Client_;
    MockInodeCacheManager mockInodeManager_;
    MockMdsClient mockMdsClient_;
    std::string addr_ = "127.0.0.1:5630";
    brpc::Server server_;
    Aws::SDKOptions awsOptions_;
};
uint64_t gInodeId1 = 1;
void InitInodeForIntegration(Inode *inode) {
    inode->set_inodeid(gInodeId1);
    inode->set_fsid(2);
    inode->set_length(0);
    inode->set_ctime(1623835517);
    inode->set_mtime(1623835517);
    inode->set_atime(1623835517);
    inode->set_uid(1);
    inode->set_gid(1);
    inode->set_mode(1);
    inode->set_nlink(1);
    inode->set_type(curvefs::metaserver::FsFileType::TYPE_S3);
    gInodeId1++;
}

TEST_F(ClientS3IntegrationTest, test_first_write) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    int ret = s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(len, ret);

    delete buf;
    buf = NULL;
}

TEST_F(ClientS3IntegrationTest, test_overlap_write) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    offset = 0;
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    offset = 2 * 1024 * 1024;
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    delete buf;
    buf = NULL;
}

TEST_F(ClientS3IntegrationTest, test_hole_write) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    offset = 2 * 1024 * 1024;
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(2, fsCacheManager->GetDataCacheNum());

    delete buf;
    buf = NULL;
}

TEST_F(ClientS3IntegrationTest, test_append_write) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    offset = offset + len;
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    delete buf;
    buf = NULL;
}

TEST_F(ClientS3IntegrationTest, test_write_more_chunk) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 5 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(2, fsCacheManager->GetDataCacheNum());

    delete buf;
    buf = NULL;
}

/*
       ------                   write1
                   ------       write2
          ------------          write3
*/
TEST_F(ClientS3IntegrationTest, test_write_merge_data1) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    offset = 3 * 1024 * 1024;
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(2, fsCacheManager->GetDataCacheNum());

    offset = 1.5 * 1024 * 1024;
    len = 2 * 1024 * 1024;
    char *buf1 = new char[len];
    memset(buf1, 'b', len);

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf1);
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    delete buf;
    delete buf1;
}

/*
       ------                   write1
                   ------       write2
   -------------------          write3
*/
TEST_F(ClientS3IntegrationTest, test_write_merge_data2) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    offset = 3 * 1024 * 1024;
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(2, fsCacheManager->GetDataCacheNum());

    offset = 0;
    len = 3.5 * 1024 * 1024;
    char *buf1 = new char[len];
    memset(buf1, 'b', len);

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf1);
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    delete buf;
    delete buf1;
}

TEST_F(ClientS3IntegrationTest, test_read_one_chunk) {
    uint64_t readFileLen = 2 * 1024 * 1024;
    curvefs::metaserver::Inode inode;
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    uint64_t chunkIndex = offset / chunkSize;
    InitInodeForIntegration(&inode);
    inode.set_length(readFileLen);

    auto s3ChunkInfoMap = inode.mutable_s3chunkinfomap();
    S3ChunkInfoList *s3ChunkInfoList = new S3ChunkInfoList();
    S3ChunkInfo *s3ChunkInfo = s3ChunkInfoList->add_s3chunks();
    s3ChunkInfo->set_chunkid(25);
    s3ChunkInfo->set_compaction(0);
    s3ChunkInfo->set_offset(0);
    s3ChunkInfo->set_len(readFileLen);
    s3ChunkInfo->set_size(readFileLen);
    s3ChunkInfo->set_zero(false);
    s3ChunkInfoMap->insert({chunkIndex, *s3ChunkInfoList});

    char *buf = new char[len];
    char *tmpbuf = new char[len];
    memset(tmpbuf, 'a', len);

    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<1>(*tmpbuf), Return(1 * 1024 * 1024)))
        .WillOnce(Return(-1));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    int ret = s3ClientAdaptor_->Read(inode.inodeid(), offset, len, buf);
    ASSERT_EQ(len, ret);
    ASSERT_EQ('a', buf[0]);
    offset = offset + len;
    ret = s3ClientAdaptor_->Read(inode.inodeid(), offset, len, buf);
    ASSERT_EQ(-1, ret);

    delete buf;
    buf = NULL;
}
/*
    ------------   a         write1
    ------         b         write2
    ------         b         read1
          ------   a         read2
*/
TEST_F(ClientS3IntegrationTest, test_read_overlap_block1) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    len = 1 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    char *readBuf = new char[len + 1];
    char *expectBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    memset(expectBuf, 'b', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'a', len);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    delete readBuf;
    delete buf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
    ------------           a         write1
          ------------     b         write2
    ------                 a         read1
          ------           b         read2
*/
TEST_F(ClientS3IntegrationTest, test_read_overlap_block2) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    offset = 1 * 1024 * 1024;
    len = 2 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    offset = 0;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'a', len);
    memset(expectBuf + len, 0, 1);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);

    offset = offset + len;
    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    delete readBuf;
    delete buf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
    ------------------     a         write1
          ------           b         write2
    ------                 a         read1
          ------           b         read2
                 ------    a         read3
*/
TEST_F(ClientS3IntegrationTest, test_read_overlap_block3) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 3 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    offset = 1 * 1024 * 1024;
    len = 1 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    offset = 0;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'a', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'a', len);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    delete readBuf;
    delete buf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
    ------                 a         write1
    ------------------     b         write2
    ------                 b         read1
          ------           b         read2
                 ------    b         read3
*/
TEST_F(ClientS3IntegrationTest, test_read_overlap_block4) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t max_len = 3 * 1024 * 1024;
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[max_len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    offset = 0;
    len = 3 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(len);
    offset = 0;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'b', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    delete readBuf;
    delete buf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
          ------------     a         write1
    ------------           b         write2
    ------                 b         read1
          ------           b         read2
                 ------    a         read3
*/
TEST_F(ClientS3IntegrationTest, test_read_overlap_block5) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t max_len = 2 * 1024 * 1024;
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[max_len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    offset = 0;
    len = 2 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    offset = 0;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    char *expectBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    memset(expectBuf, 0, len + 1);

    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'a', len);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    // delete readBuf;
    delete buf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
    ------      ------         read
          ------               writed
*/
TEST_F(ClientS3IntegrationTest, test_read_hole1) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    offset = 0;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 'a', len);
    memset(readBuf + len, 0, 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = 2 * 1024 * 1024;
    inode.set_length(offset + len);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}
/*
    ------------               read
          ------------         writed
*/
TEST_F(ClientS3IntegrationTest, test_read_hole2) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    offset = 0;
    len = 2 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf + 1024 * 1024, 'a', 1024 * 1024);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);

    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}
/*
    ------------         read
          ------         writed
*/
TEST_F(ClientS3IntegrationTest, test_read_hole3) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    offset = 0;
    len = 2 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf + 1024 * 1024, 'a', 1024 * 1024);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
    ------------------     read
    ------      ------     writed
*/
TEST_F(ClientS3IntegrationTest, test_read_hole4) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    offset = 2 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    offset = 0;
    len = 3 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf, 'a', 1024 * 1024);
    memset(expectBuf + 2 * 1024 * 1024, 'b', 1024 * 1024);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);

    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
    ------                 a         write1
            ------         b         write2
    ------------------               read1
*/
TEST_F(ClientS3IntegrationTest, test_read_more_write) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 100;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    offset = 120;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    offset = 0;
    len = 1024;
    inode.set_length(1024);
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf, 'a', 100);
    memset(expectBuf + 120, 'b', 100);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);

    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
    ------                 a         write1
                ------     b         write2
          ------           c         write3
                ------     b         read1
*/
TEST_F(ClientS3IntegrationTest, test_read_more_write2) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 34339840;
    uint64_t len = 512;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    offset = 34340864;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    offset = 34340352;
    memset(buf, 'c', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    offset = 34340864;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);

    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

TEST_F(ClientS3IntegrationTest, test_read_more_chunks) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 4 * 1024 * 1024 - 1024;
    uint64_t len = 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    offset = 4 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    offset = 4 * 1024 * 1024 - 1024;
    len = 2048;
    inode.set_length(4 * 1024 * 1024 + 1024);
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf, 'a', 1024);
    memset(expectBuf + 1024, 'b', 1024);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);

    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
    -----------------    write1
    ------               truncate

*/
TEST_F(ClientS3IntegrationTest, test_truncate_small1) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 5 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    ASSERT_EQ(len, s3ClientAdaptor_->GetFsCacheManager()->GetDataCacheSize());
    len = 1 * 1024 * 1024;
    CURVEFS_ERROR ret = s3ClientAdaptor_->Truncate(&inode, len);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(len, s3ClientAdaptor_->GetFsCacheManager()->GetDataCacheSize());

    // cleanup
    delete buf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
    ---   ---   ---    ---   ---   ---    write
    -------------------            truncate1
    -------                        truncate2

*/
TEST_F(ClientS3IntegrationTest, test_truncate_small2) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 512 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    inode.set_length(8 * 1024 * 1024);
    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    for (int i = 0; i < 6; i++) {
        s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
        offset = offset + len * 2;
    }
    ASSERT_EQ(6, fsCacheManager->GetDataCacheNum());

    uint64_t truncateLen = 3 * 1024 * 1024;
    CURVEFS_ERROR ret = s3ClientAdaptor_->Truncate(&inode, truncateLen);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(3 * len,
              s3ClientAdaptor_->GetFsCacheManager()->GetDataCacheSize());

    truncateLen = 1200 * 1024;
    ret = s3ClientAdaptor_->Truncate(&inode, truncateLen);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(704 * 1024,
              s3ClientAdaptor_->GetFsCacheManager()->GetDataCacheSize());

    // cleanup
    delete buf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
    ---   ---   ---    ---   ---   ---    write
    -------------------            truncate1
    -------                        truncate2

*/
TEST_F(ClientS3IntegrationTest, test_truncate_small3) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 512 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    inode.set_length(8 * 1024 * 1024);
    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    uint64_t chunkId = 25;
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                S3Data &tmp = gObjectDataMaps[context->key];
                tmp.len = context->bufferSize;
                tmp.buf = new char[context->bufferSize];
                strncpy(tmp.buf, context->buffer, context->bufferSize);
                context->retCode = 0;
                context->cb(context);
            }));


    for (int i = 0; i < 6; i++) {
        s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
        offset = offset + len * 2;
    }
    ASSERT_EQ(6, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(6 * len, fsCacheManager->GetDataCacheSize());
    ASSERT_EQ(0, fsCacheManager->GetLruByte());

    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(0, fsCacheManager->GetDataCacheSize());
    ASSERT_EQ(6 * len, fsCacheManager->GetLruByte());

    uint64_t truncateLen = 3 * 1024 * 1024;
    ret = s3ClientAdaptor_->Truncate(&inode, truncateLen);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(3 * len, s3ClientAdaptor_->GetFsCacheManager()->GetLruByte());

    truncateLen = 1200 * 1024;
    ret = s3ClientAdaptor_->Truncate(&inode, truncateLen);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(len, s3ClientAdaptor_->GetFsCacheManager()->GetLruByte());

    // cleanup
    delete buf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}
/*
    file size 0
    -------------------            truncate

*/
TEST_F(ClientS3IntegrationTest, test_truncate_big1) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    CURVEFS_ERROR ret;
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();
    //  mock
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    ret = s3ClientAdaptor_->Truncate(&inode, len);
    inode.set_length(len);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    auto s3InfoListIter = inode.s3chunkinfomap().find(chunkIndex);
    S3ChunkInfo tmp;
    ASSERT_NE(s3InfoListIter, inode.s3chunkinfomap().end());
    ASSERT_EQ(1, s3InfoListIter->second.s3chunks_size());
    tmp = s3InfoListIter->second.s3chunks(0);
    ASSERT_EQ(len, tmp.len());
    ASSERT_EQ(offset, tmp.offset());
    ASSERT_EQ(true, tmp.zero());

    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
    ---                    write
    -------------------    truncate

*/
TEST_F(ClientS3IntegrationTest, test_truncate_big2) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    len = 5 * 1024 * 1024;
    s3ClientAdaptor_->Truncate(&inode, len);
    inode.set_length(len);
    auto s3InfoListIter = inode.s3chunkinfomap().find(chunkIndex);
    ASSERT_NE(s3InfoListIter, inode.s3chunkinfomap().end());
    ASSERT_EQ(1, s3InfoListIter->second.s3chunks_size());

    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf, 'a', 1 * 1024 * 1024);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);

    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
    -------               truncate1
    -------------         truncate2
    ------------          read1
      ---                 read2
           ------------   read3

*/
TEST_F(ClientS3IntegrationTest, test_truncate_big3) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    CURVEFS_ERROR ret;
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();
    //  mock
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

    ret = s3ClientAdaptor_->Truncate(&inode, len);
    inode.set_length(len);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    auto s3InfoListIter = inode.s3chunkinfomap().find(chunkIndex);
    S3ChunkInfo tmp;
    ASSERT_NE(s3InfoListIter, inode.s3chunkinfomap().end());
    ASSERT_EQ(1, s3InfoListIter->second.s3chunks_size());
    tmp = s3InfoListIter->second.s3chunks(0);
    ASSERT_EQ(len, tmp.len());
    ASSERT_EQ(offset, tmp.offset());
    ASSERT_EQ(true, tmp.zero());

    len = 2 * 1024 * 1024;
    ret = s3ClientAdaptor_->Truncate(&inode, len);
    inode.set_length(len);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    s3InfoListIter = inode.s3chunkinfomap().find(chunkIndex);
    ASSERT_NE(s3InfoListIter, inode.s3chunkinfomap().end());
    ASSERT_EQ(2, s3InfoListIter->second.s3chunks_size());
    tmp = s3InfoListIter->second.s3chunks(1);
    ASSERT_EQ(1 * 1024 * 1024, tmp.len());
    ASSERT_EQ(1 * 1024 * 1024, tmp.offset());
    ASSERT_EQ(true, tmp.zero());

    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = 512;
    len = 512;
    char *readBuf1 = new char[len + 1];
    memset(readBuf1, 0, len + 1);
    char *expectBuf1 = new char[len + 1];
    memset(expectBuf1, 0, len + 1);

    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf1);
    EXPECT_STREQ(expectBuf1, readBuf1);

    len = 1 * 1024 * 1024;
    inode.set_length(3 * 1024 * 1024);
    offset = 1.5 * 1024 * 1024;
    char *readBuf2 = new char[len + 1];
    memset(readBuf2, 0, len + 1);
    char *expectBuf2 = new char[len + 1];
    memset(expectBuf2, 0, len + 1);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf2);
    EXPECT_STREQ(expectBuf2, readBuf2);

    // cleanup
    delete readBuf;
    delete expectBuf;
    delete readBuf1;
    delete expectBuf1;
    delete readBuf2;
    delete expectBuf2;

    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

TEST_F(ClientS3IntegrationTest, test_releaseCache) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->ReleaseCache(inode.inodeid());
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    offset = 2 * 1024 * 1024;
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    ASSERT_EQ(2, fsCacheManager->GetDataCacheNum());

    s3ClientAdaptor_->ReleaseCache(inode.inodeid());
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());

    //  cleanup
    delete buf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

TEST_F(ClientS3IntegrationTest, test_flush_first_write) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));

    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                context->retCode = 0;
                context->cb(context);
            }));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    auto s3InfoListIter = inode.s3chunkinfomap().find(chunkIndex);
    S3ChunkInfo tmp;
    ASSERT_NE(s3InfoListIter, inode.s3chunkinfomap().end());
    ASSERT_EQ(1, s3InfoListIter->second.s3chunks_size());
    tmp = s3InfoListIter->second.s3chunks(0);
    ASSERT_EQ(len, tmp.len());
    ASSERT_EQ(offset, tmp.offset());

    //  cleanup
    delete buf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
    -------               write1
       -------            write2
                          flush
*/
TEST_F(ClientS3IntegrationTest, test_flush_overlap_write) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    offset = 1 * 1024 * 1024;
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    auto s3InfoListIter = inode.s3chunkinfomap().find(chunkIndex);
    S3ChunkInfo tmp;
    ASSERT_NE(s3InfoListIter, inode.s3chunkinfomap().end());
    ASSERT_EQ(1, s3InfoListIter->second.s3chunks_size());
    tmp = s3InfoListIter->second.s3chunks(0);
    ASSERT_EQ(3 * 1024 * 1024, tmp.len());
    ASSERT_EQ(0, tmp.offset());

    //  cleanup
    delete buf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/* This is a use case for an inconsistency problem */
TEST_F(ClientS3IntegrationTest, test_flush_overlap_write2) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 16748032;
    uint64_t len = 512;
    char *buf = new char[len];
    memset(buf, 'a', len);
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    offset = 16749056;
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(2, fsCacheManager->GetDataCacheNum());

    offset = 16748544;
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    auto s3InfoListIter = inode.s3chunkinfomap().find(chunkIndex);
    S3ChunkInfo tmp;
    ASSERT_NE(s3InfoListIter, inode.s3chunkinfomap().end());
    ASSERT_EQ(1, s3InfoListIter->second.s3chunks_size());
    tmp = s3InfoListIter->second.s3chunks(0);
    ASSERT_EQ(1536, tmp.len());
    ASSERT_EQ(16748032, tmp.offset());

    //  cleanup
    delete buf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

TEST_F(ClientS3IntegrationTest, test_flush_hole_write) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    offset = 2 * 1024 * 1024;
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(2, fsCacheManager->GetDataCacheNum());

    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    auto s3InfoListIter = inode.s3chunkinfomap().find(chunkIndex);
    S3ChunkInfo tmp;
    S3ChunkInfoList tmpList;
    ASSERT_NE(s3InfoListIter, inode.s3chunkinfomap().end());
    tmpList = s3InfoListIter->second;
    ASSERT_EQ(2, tmpList.s3chunks_size());
    tmp = tmpList.s3chunks(0);
    ASSERT_EQ(1 * 1024 * 1024, tmp.len());
    ASSERT_EQ(0, tmp.offset());
    tmp = tmpList.s3chunks(1);
    ASSERT_EQ(1 * 1024 * 1024, tmp.len());
    ASSERT_EQ(2 * 1024 * 1024, tmp.offset());

    //  cleanup
    delete buf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

TEST_F(ClientS3IntegrationTest, test_flush_write_more_chunk) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 5 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(2, fsCacheManager->GetDataCacheNum());

    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(2, inode.s3chunkinfomap_size());
    auto s3InfoListIter = inode.s3chunkinfomap().find(0);
    S3ChunkInfo tmp;
    S3ChunkInfoList tmpList;
    ASSERT_NE(s3InfoListIter, inode.s3chunkinfomap().end());
    VLOG(9) << "index: " << s3InfoListIter->first;
    tmpList = s3InfoListIter->second;
    ASSERT_EQ(1, tmpList.s3chunks_size());
    tmp = tmpList.s3chunks(0);
    ASSERT_EQ(4 * 1024 * 1024, tmp.len());
    ASSERT_EQ(0, tmp.offset());
    s3InfoListIter = inode.s3chunkinfomap().find(1);
    tmpList = s3InfoListIter->second;
    tmp = tmpList.s3chunks(0);
    ASSERT_EQ(1 * 1024 * 1024, tmp.len());
    ASSERT_EQ(4 * 1024 * 1024, tmp.offset());

    //  cleanup
    delete buf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
    ------         write1
    ---            write2
                   flush
    ---            read1
*/
TEST_F(ClientS3IntegrationTest, test_flush_write_and_read1) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                S3Data &tmp = gObjectDataMaps[context->key];
                tmp.len = context->bufferSize;
                tmp.buf = new char[context->bufferSize];
                strncpy(tmp.buf, context->buffer, context->bufferSize);
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    len = 1 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());
    uint64_t len1 = inode.length();
    inode = inodeWrapper->GetInodeUnlocked();
    inode.set_length(len1);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());
    auto s3InfoListIter = inode.s3chunkinfomap().begin();
    S3ChunkInfo tmp;
    S3ChunkInfoList tmpList;
    ASSERT_NE(s3InfoListIter, inode.s3chunkinfomap().end());
    tmpList = s3InfoListIter->second;
    ASSERT_EQ(1, tmpList.s3chunks_size());
    tmp = tmpList.s3chunks(0);
    ASSERT_EQ(2 * 1024 * 1024, tmp.len());
    ASSERT_EQ(0, tmp.offset());

    char *readBuf = new char[len + 1];
    char *expectBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    memset(expectBuf, 'b', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    //  cleanup
    delete buf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
    ------         write
                   flush
     ---           read cache
     ---           read
*/
TEST_F(ClientS3IntegrationTest, test_flush_write_and_read2) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                S3Data &tmp = gObjectDataMaps[context->key];
                tmp.len = context->bufferSize;
                tmp.buf = new char[context->bufferSize];
                strncpy(tmp.buf, context->buffer, context->bufferSize);
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());
    uint64_t len1 = inode.length();
    inode = inodeWrapper->GetInodeUnlocked();
    inode.set_length(len1);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());
    auto s3InfoListIter = inode.s3chunkinfomap().begin();
    S3ChunkInfo tmp;
    S3ChunkInfoList tmpList;
    ASSERT_NE(s3InfoListIter, inode.s3chunkinfomap().end());
    tmpList = s3InfoListIter->second;
    ASSERT_EQ(1, tmpList.s3chunks_size());
    tmp = tmpList.s3chunks(0);
    ASSERT_EQ(2 * 1024 * 1024, tmp.len());
    ASSERT_EQ(0, tmp.offset());

    offset = 512;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    char *expectBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    memset(expectBuf, 'a', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    s3ClientAdaptor_->ReleaseCache(inode.inodeid());
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);
    //  cleanup
    delete buf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
       ---     a    write
                    flush
    ---        0    read1
          ---  0    read2
    ---------       read3
*/
TEST_F(ClientS3IntegrationTest, test_flush_write_and_read3) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                S3Data &tmp = gObjectDataMaps[context->key];
                tmp.len = context->bufferSize;
                tmp.buf = new char[context->bufferSize];
                strncpy(tmp.buf, context->buffer, context->bufferSize);
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());
    uint64_t len1 = inode.length();
    inode = inodeWrapper->GetInodeUnlocked();
    inode.set_length(len1);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    offset = 0;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 'a', len);
    memset(readBuf + len, 0, 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = 2 * 1024 * 1024;
    inode.set_length(offset + len);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    s3ClientAdaptor_->ReleaseCache(inode.inodeid());

    offset = 0;
    len = 3 * 1024 * 1024;
    char *readBuf1 = new char[len + 1];
    char *expectBuf1 = new char[len + 1];
    memset(readBuf1, 'b', len);
    memset(readBuf1 + len, 0, 1);
    memset(expectBuf1, 0, len + 1);
    memset(expectBuf1 + 1024 * 1024, 'a', 1024 * 1024);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf1);
    EXPECT_STREQ(expectBuf1, readBuf1);

    s3ClientAdaptor_->ReleaseCache(inode.inodeid());
    offset = 1.5 * 1024 * 1024;
    len = 1 * 1024 * 1024;
    char *readBuf2 = new char[len + 1];
    char *expectBuf2 = new char[len + 1];
    memset(readBuf2, 'b', len);
    memset(readBuf2 + len, 0, 1);
    memset(expectBuf2, 0, len + 1);
    memset(expectBuf2, 'a', 0.5 * 1024 * 1024);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf2);
    EXPECT_STREQ(expectBuf2, readBuf2);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    delete readBuf1;
    delete expectBuf1;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
    ------        a     write1
       ------     b     write2 
                       flush
                       releaseReadCache
       ---        b     read

*/
TEST_F(ClientS3IntegrationTest, test_flush_write_and_read4) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0 * 1024 * 1024;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    uint64_t chunkId = 25;
    uint64_t chunkId1 = 26;
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId1), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                S3Data &tmp = gObjectDataMaps[context->key];
                tmp.len = context->bufferSize;
                tmp.buf = new char[context->bufferSize];
                strncpy(tmp.buf, context->buffer, context->bufferSize);
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());
    uint64_t len1 = inode.length();
    inode = inodeWrapper->GetInodeUnlocked();
    inode.set_length(len1);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    offset = 1 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode.inodeid());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    s3ClientAdaptor_->ReleaseCache(inode.inodeid());
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 'c', len);
    memset(readBuf + len, 0, 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'b', len);
    memset(expectBuf + len, 0, 1);
    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/* This is a use case for an inconsistency problem */
TEST_F(ClientS3IntegrationTest, test_flush_write_and_read5) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', 512 * 1024);
    memset(buf + 512 * 1024, 'b', 512 * 1024);

    uint64_t chunkId = 25;
    uint64_t chunkId1 = 26;
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId1), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                S3Data &tmp = gObjectDataMaps[context->key];
                tmp.len = context->bufferSize;
                tmp.buf = new char[context->bufferSize];
                strncpy(tmp.buf, context->buffer, context->bufferSize);
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());
    uint64_t len1 = inode.length();
    inode = inodeWrapper->GetInodeUnlocked();
    inode.set_length(len1);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    offset = 0;
    len = 512 * 1024;
    memset(buf, 'c', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(1024 * 1024);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode.inodeid());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    s3ClientAdaptor_->ReleaseCache(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();
    inode.set_length(1024 * 1024);
    auto s3ChunkInfoMap = inode.mutable_s3chunkinfomap();
    auto s3chunkInfoListIter = s3ChunkInfoMap->find(0);
    ASSERT_EQ(2, s3chunkInfoListIter->second.s3chunks_size());

    offset = 512 * 1024;
    len = 1;
    char *readBuf = new char[len + 1];
    memset(readBuf, 'c', len);
    memset(readBuf + len, 0, 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'b', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
      ------    write1
                flush
       ---      write2
                flush
      ------    read1
*/
TEST_F(ClientS3IntegrationTest, test_flush_write_and_read6) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 1024;
    uint64_t len = 1 * 1024 * 1024 - 2048;
    char *buf = new char[len];
    memset(buf, 'a', len);

    uint64_t chunkId = 25;
    uint64_t chunkId1 = 26;
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId1), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                S3Data &tmp = gObjectDataMaps[context->key];
                tmp.len = context->bufferSize;
                tmp.buf = new char[context->bufferSize];
                strncpy(tmp.buf, context->buffer, context->bufferSize);
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();
    inode.set_length(offset + len);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    offset = 2048;
    len = 512 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode.inodeid());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    s3ClientAdaptor_->ReleaseCache(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();
    inode.set_length(1024 * 1024);
    auto s3ChunkInfoMap = inode.mutable_s3chunkinfomap();
    auto s3chunkInfoListIter = s3ChunkInfoMap->find(0);
    ASSERT_EQ(2, s3chunkInfoListIter->second.s3chunks_size());

    offset = 1024;
    len = 1024 * 1024 - 2048;
    char *readBuf = new char[len + 1];
    memset(readBuf, 'c', len);
    memset(readBuf + len, 0, 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'a', len);
    memset(expectBuf + 1024, 'b', 512 * 1024);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/*
    ------   a  write1
     -       b  write2
       -     c  write3
    ------      read
*/
TEST_F(ClientS3IntegrationTest, test_flush_write_and_read7) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    uint64_t chunkId = 25;
    uint64_t chunkId1 = 26;
    uint64_t chunkId2 = 27;
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId1), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId2), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                S3Data &tmp = gObjectDataMaps[context->key];
                tmp.len = context->bufferSize;
                tmp.buf = new char[context->bufferSize];
                strncpy(tmp.buf, context->buffer, context->bufferSize);
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();
    inode.set_length(offset + len);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    offset = 1024;
    len = 512;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode.inodeid());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    offset = 2048;
    len = 512;
    memset(buf, 'c', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    ret = s3ClientAdaptor_->Flush(inode.inodeid());

    s3ClientAdaptor_->ReleaseCache(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();
    inode.set_length(1024 * 1024);
    auto s3ChunkInfoMap = inode.mutable_s3chunkinfomap();
    auto s3chunkInfoListIter = s3ChunkInfoMap->find(0);
    ASSERT_EQ(3, s3chunkInfoListIter->second.s3chunks_size());

    offset = 0;
    len = 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 'd', len);
    memset(readBuf + len, 0, 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'a', len);
    memset(expectBuf + 1024, 'b', 512);
    memset(expectBuf + 2048, 'c', 512);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/* This is a use case for an inconsistency problem */
TEST_F(ClientS3IntegrationTest, test_flush_write_and_read8) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 20971520;
    uint64_t len = 4194304;
    char *buf = new char[len];
    memset(buf, 'a', len);

    uint64_t chunkId = 25;
    uint64_t chunkId1 = 26;
    uint64_t chunkId2 = 27;
    inode.set_length(offset + len);
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId1), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId2), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                S3Data &tmp = gObjectDataMaps[context->key];
                tmp.len = context->bufferSize;
                tmp.buf = new char[context->bufferSize];
                strncpy(tmp.buf, context->buffer, context->bufferSize);
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);


    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    len = 524288;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode.inodeid());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    len = 1048576;
    memset(buf, 'c', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    ret = s3ClientAdaptor_->Flush(inode.inodeid());

    s3ClientAdaptor_->ReleaseCache(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();

    auto s3ChunkInfoMap = inode.mutable_s3chunkinfomap();
    auto s3chunkInfoListIter = s3ChunkInfoMap->find(5);
    ASSERT_EQ(3, s3chunkInfoListIter->second.s3chunks_size());

    offset = 20971520;
    len = 4194304;
    char *readBuf = new char[len + 1];
    memset(readBuf, 'd', len);
    memset(readBuf + len, 0, 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'a', len);
    memset(expectBuf, 'c', 1048576);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/* This is a use case for an inconsistency problem */
TEST_F(ClientS3IntegrationTest, test_flush_write_and_read9) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 92274688;
    uint64_t len = 4194304;
    char *buf = new char[len];
    memset(buf, 'a', len);
    memset(buf + (93210624 - 92274688), 'b', 1024);

    uint64_t chunkId = 25;
    uint64_t chunkId1 = 26;
    uint64_t chunkId2 = 27;
    inode.set_length(offset + len);
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId1), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId2), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                S3Data &tmp = gObjectDataMaps[context->key];
                tmp.len = context->bufferSize;
                tmp.buf = new char[context->bufferSize];
                strncpy(tmp.buf, context->buffer, context->bufferSize);
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);


    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    offset = 92405760;
    len = 1024;
    memset(buf, 'c', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode.inodeid());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    offset = 93547520;
    len = 1024;
    memset(buf, 'd', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    ret = s3ClientAdaptor_->Flush(inode.inodeid());

    s3ClientAdaptor_->ReleaseCache(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();

    auto s3ChunkInfoMap = inode.mutable_s3chunkinfomap();
    auto s3chunkInfoListIter = s3ChunkInfoMap->find(22);
    ASSERT_EQ(3, s3chunkInfoListIter->second.s3chunks_size());

    offset = 93210624;
    len = 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 'e', len);
    memset(readBuf + len, 0, 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'b', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/* This is a use case for an inconsistency problem */
TEST_F(ClientS3IntegrationTest, test_flush_write_and_read10) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 8388608;
    uint64_t len = 4194304;
    char *buf = new char[len];
    memset(buf, 'a', len);

    uint64_t chunkId = 1;
    uint64_t chunkId1 = 2;
    uint64_t chunkId2 = 3;
    uint64_t chunkId3 = 4;
    uint64_t chunkId4 = 5;
    uint64_t chunkId5 = 6;
    uint64_t chunkId6 = 7;
    inode.set_length(offset + len);
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId1), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId2), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId3), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId4), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId5), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId6), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                S3Data &tmp = gObjectDataMaps[context->key];
                tmp.len = context->bufferSize;
                tmp.buf = new char[context->bufferSize];
                strncpy(tmp.buf, context->buffer, context->bufferSize);
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);


    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    offset = 8818688;
    len = 4096;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode.inodeid());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    offset = 9961472;
    len = 1048576;
    memset(buf, 'c', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    ret = s3ClientAdaptor_->Flush(inode.inodeid());

    offset = 8523776;
    len = 4096;
    memset(buf, 'd', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    ret = s3ClientAdaptor_->Flush(inode.inodeid());

    offset = 8732672;
    len = 512;
    memset(buf, 'e', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    ret = s3ClientAdaptor_->Flush(inode.inodeid());

    offset = 9184768;
    len = 512;
    memset(buf, 'f', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    ret = s3ClientAdaptor_->Flush(inode.inodeid());

    offset = 9437184;
    len = 1048576;
    memset(buf, 'g', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    ret = s3ClientAdaptor_->Flush(inode.inodeid());


    s3ClientAdaptor_->ReleaseCache(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();

    offset = 9437184;
    len = 131072;
    char *readBuf = new char[len + 1];
    memset(readBuf, 'h', len);
    memset(readBuf + len, 0, 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'g', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/* This is a use case for an inconsistency problem */
TEST_F(ClientS3IntegrationTest, test_flush_write_and_read11) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 196608;
    uint64_t len = 131072;
    char *buf = new char[len];
    memset(buf, 'a', len);

    uint64_t chunkId = 1;
    uint64_t chunkId1 = 2;

    inode.set_length(offset + len);
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId1), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                S3Data &tmp = gObjectDataMaps[context->key];
                tmp.len = context->bufferSize;
                tmp.buf = new char[context->bufferSize];
                strncpy(tmp.buf, context->buffer, context->bufferSize);
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);


    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    offset = 323584;
    len = 4096;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode.inodeid());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    s3ClientAdaptor_->ReleaseCache(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();

    offset = 196608;
    len = 131072;
    char *readBuf = new char[len + 1];
    memset(readBuf, 'c', len);
    memset(readBuf + len, 0, 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'a', len);
    memset(expectBuf + 126976, 'b', 4096);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

/* This is a use case for an inconsistency problem */
TEST_F(ClientS3IntegrationTest, test_flush_write_and_read12) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 196608;
    uint64_t len = 131072;
    char *buf = new char[len];
    memset(buf, 'a', len);

    uint64_t chunkId = 1;
    uint64_t chunkId1 = 2;

    inode.set_length(offset + len);
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId1), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                S3Data &tmp = gObjectDataMaps[context->key];
                tmp.len = context->bufferSize;
                tmp.buf = new char[context->bufferSize];
                strncpy(tmp.buf, context->buffer, context->bufferSize);
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);


    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    offset = 323584;
    len = 4096;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode.inodeid());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, inode.s3chunkinfomap_size());

    s3ClientAdaptor_->ReleaseCache(inode.inodeid());
    inode = inodeWrapper->GetInodeUnlocked();

    offset = 196608;
    len = 131072;
    char *readBuf = new char[len + 1];
    memset(readBuf, 'c', len);
    memset(readBuf + len, 0, 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'a', len);
    memset(expectBuf + 126976, 'b', 4096);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode.inodeid(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    // cleanup
    delete buf;
    delete readBuf;
    delete expectBuf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

TEST_F(ClientS3IntegrationTest, test_fssync_success_and_fail) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)));

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWrapper),
                        Return(CURVEFS_ERROR::INTERNAL)))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWrapper),
                        Return(CURVEFS_ERROR::INTERNAL)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                S3Data &tmp = gObjectDataMaps[context->key];
                tmp.len = context->bufferSize;
                tmp.buf = new char[context->bufferSize];
                strncpy(tmp.buf, context->buffer, context->bufferSize);
                context->retCode = 0;
                context->cb(context);
            }));

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    CURVEFS_ERROR ret = s3ClientAdaptor_->FsSync();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    ret = s3ClientAdaptor_->FsSync();
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    ret = s3ClientAdaptor_->FsSync();
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);

    //  cleanup
    delete buf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

TEST_F(ClientS3IntegrationTest, test_fssync_overlap_write) {
    curvefs::metaserver::Inode inode;

    InitInodeForIntegration(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                S3Data &tmp = gObjectDataMaps[context->key];
                tmp.len = context->bufferSize;
                tmp.buf = new char[context->bufferSize];
                strncpy(tmp.buf, context->buffer, context->bufferSize);
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    offset = 1 * 1024 * 1024;
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    CURVEFS_ERROR ret = s3ClientAdaptor_->FsSync();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());

    //  cleanup
    delete buf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

}  // namespace client
}  // namespace curvefs

