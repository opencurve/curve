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

#include <brpc/server.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>
#include <memory>

#include "curvefs/src/client/inode_wrapper.h"
#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/client/kvclient/kvclient_manager.h"
#include "src/common/curve_define.h"
#include "curvefs/test/client/mock_client_s3.h"
#include "curvefs/test/client/mock_inode_cache_manager.h"
#include "curvefs/test/client/mock_metaserver_service.h"
#include "curvefs/test/client/mock_kvclient.h"
#include "curvefs/test/client/rpcclient/mock_mds_client.h"

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
using ::testing::SetArrayArgument;
using ::testing::AtLeast;
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

namespace {

uint64_t gInodeId1 = 1;
std::shared_ptr<InodeWrapper> InitInodeForIntegration() {
    Inode inode;
    inode.set_inodeid(gInodeId1);
    inode.set_fsid(2);
    inode.set_length(0);
    inode.set_ctime(1623835517);
    inode.set_mtime(1623835517);
    inode.set_atime(1623835517);
    inode.set_uid(1);
    inode.set_gid(1);
    inode.set_mode(1);
    inode.set_nlink(1);
    inode.set_type(curvefs::metaserver::FsFileType::TYPE_S3);
    gInodeId1++;

    return std::make_shared<InodeWrapper>(std::move(inode), nullptr);
}
}  // namespace

class ClientS3IntegrationTest : public testing::Test {
 protected:
    void SetUp() override {
        Aws::InitAPI(awsOptions_);
        InitKVClientManager();
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
        s3ClientAdaptor_ = new S3ClientAdaptorImpl();
        auto fsCacheManager = std::make_shared<FsCacheManager>(
            s3ClientAdaptor_, option.readCacheMaxByte,
            option.writeCacheMaxByte, kvClientManager_);
        s3ClientAdaptor_->Init(option, mockS3Client, mockInodeManager,
                               mockMdsClient, fsCacheManager, nullptr,
                               kvClientManager_);
        s3ClientAdaptor_->SetFsId(2);
        curvefs::client::common::FLAGS_enableCto = false;
    }

    void TearDown() override {
        Aws::ShutdownAPI(awsOptions_);
        server_.Stop(0);
        server_.Join();
    }

    void InitKVClientManager() {
        kvClientManager_ = std::make_shared<KVClientManager>();

        KVClientManagerOpt opt;
        std::shared_ptr<MockKVClient> mockKVClient(&mockKVClient_);
        kvClientManager_->Init(opt, mockKVClient);
    }

 protected:
    S3ClientAdaptorImpl *s3ClientAdaptor_;
    MockMetaServerService mockMetaServerService_;
    MockS3Client mockS3Client_;
    MockInodeCacheManager mockInodeManager_;
    MockMdsClient mockMdsClient_;
    MockKVClient mockKVClient_;
    std::string addr_ = "127.0.0.1:5630";
    brpc::Server server_;
    Aws::SDKOptions awsOptions_;

    std::shared_ptr<InodeWrapper> inode{InitInodeForIntegration()};

    std::shared_ptr<KVClientManager> kvClientManager_;
};

TEST_F(ClientS3IntegrationTest, test_first_write) {
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    int ret = s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(len, ret);

    delete buf;
    buf = NULL;
}

TEST_F(ClientS3IntegrationTest, test_overlap_write) {
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    offset = 0;
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    offset = 2 * 1024 * 1024;
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    delete buf;
    buf = NULL;
}

TEST_F(ClientS3IntegrationTest, test_hole_write) {
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);
    offset = 2 * 1024 * 1024;
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(2, fsCacheManager->GetDataCacheNum());

    delete buf;
    buf = NULL;
}

TEST_F(ClientS3IntegrationTest, test_append_write) {
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);
    offset = offset + len;
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    delete buf;
    buf = NULL;
}

TEST_F(ClientS3IntegrationTest, test_write_more_chunk) {
    uint64_t offset = 0;
    uint64_t len = 5 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

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
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    offset = 3 * 1024 * 1024;
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(2, fsCacheManager->GetDataCacheNum());

    offset = 1.5 * 1024 * 1024;
    len = 2 * 1024 * 1024;
    char *buf1 = new char[len];
    memset(buf1, 'b', len);

    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf1);
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
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    offset = 3 * 1024 * 1024;
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(2, fsCacheManager->GetDataCacheNum());

    offset = 0;
    len = 3.5 * 1024 * 1024;
    char *buf1 = new char[len];
    memset(buf1, 'b', len);

    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf1);
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    delete buf;
    delete buf1;
}

TEST_F(ClientS3IntegrationTest, test_read_one_chunk) {
    uint64_t readFileLen = 2 * 1024 * 1024;
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    uint64_t chunkIndex = offset / chunkSize;

    inode->SetLength(readFileLen);

    auto s3ChunkInfoMap = inode->GetChunkInfoMap();
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
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));

    int ret = s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, buf);
    ASSERT_EQ(len, ret);
    ASSERT_EQ('a', buf[0]);
    offset = offset + len;
    ret = s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, buf);
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
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));

    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);
    len = 1 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    char *readBuf = new char[len + 1];
    char *expectBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    memset(expectBuf, 'b', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'a', len);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));

    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);
    offset = 1 * 1024 * 1024;
    len = 2 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    offset = 0;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'a', len);
    memset(expectBuf + len, 0, 1);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);

    offset = offset + len;
    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t offset = 0;
    uint64_t len = 3 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));

    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);
    offset = 1 * 1024 * 1024;
    len = 1 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    offset = 0;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'a', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'a', len);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t max_len = 3 * 1024 * 1024;
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[max_len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));

    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);
    offset = 0;
    len = 3 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(len);
    offset = 0;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'b', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t max_len = 2 * 1024 * 1024;
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[max_len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));

    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);
    offset = 0;
    len = 2 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    offset = 0;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    char *expectBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    memset(expectBuf, 0, len + 1);

    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'a', len);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    offset = 0;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 'a', len);
    memset(readBuf + len, 0, 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = 2 * 1024 * 1024;
    inode->SetLength(offset + len);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    offset = 0;
    len = 2 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf + 1024 * 1024, 'a', 1024 * 1024);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);

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
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    offset = 0;
    len = 2 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf + 1024 * 1024, 'a', 1024 * 1024);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    offset = 2 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    offset = 0;
    len = 3 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf, 'a', 1024 * 1024);
    memset(expectBuf + 2 * 1024 * 1024, 'b', 1024 * 1024);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);

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
    uint64_t offset = 0;
    uint64_t len = 100;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    offset = 120;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    offset = 0;
    len = 1024;
    inode->SetLength(1024);
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf, 'a', 100);
    memset(expectBuf + 120, 'b', 100);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);

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
    uint64_t offset = 34339840;
    uint64_t len = 512;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    offset = 34340864;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    offset = 34340352;
    memset(buf, 'c', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    offset = 34340864;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);

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
    uint64_t offset = 4 * 1024 * 1024 - 1024;
    uint64_t len = 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    offset = 4 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    offset = 4 * 1024 * 1024 - 1024;
    len = 2048;
    inode->SetLength(4 * 1024 * 1024 + 1024);
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf, 'a', 1024);
    memset(expectBuf + 1024, 'b', 1024);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);

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
    uint64_t offset = 0;
    uint64_t len = 5 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);
    ASSERT_EQ(len, s3ClientAdaptor_->GetFsCacheManager()->GetDataCacheSize());
    len = 1 * 1024 * 1024;
    CURVEFS_ERROR ret = s3ClientAdaptor_->Truncate(inode.get(), len);
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
    uint64_t offset = 0;
    uint64_t len = 512 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    inode->SetLength(8 * 1024 * 1024);
    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    for (int i = 0; i < 6; i++) {
        s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
        offset = offset + len * 2;
    }
    ASSERT_EQ(6, fsCacheManager->GetDataCacheNum());

    uint64_t truncateLen = 3 * 1024 * 1024;
    CURVEFS_ERROR ret = s3ClientAdaptor_->Truncate(inode.get(), truncateLen);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(3 * len,
              s3ClientAdaptor_->GetFsCacheManager()->GetDataCacheSize());

    truncateLen = 1200 * 1024;
    ret = s3ClientAdaptor_->Truncate(inode.get(), truncateLen);
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
    uint64_t offset = 0;
    uint64_t len = 512 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    inode->SetLength(8 * 1024 * 1024);
    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    uint64_t chunkId = 25;
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
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
        s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
        offset = offset + len * 2;
    }
    ASSERT_EQ(6, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(6 * len, fsCacheManager->GetDataCacheSize());
    ASSERT_EQ(0, fsCacheManager->GetLruByte());

    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    auto ino = inode->GetInode();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(0, fsCacheManager->GetDataCacheSize());
    ASSERT_EQ(6 * len, fsCacheManager->GetLruByte());

    uint64_t truncateLen = 3 * 1024 * 1024;
    ret = s3ClientAdaptor_->Truncate(inode.get(), truncateLen);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(3 * len, s3ClientAdaptor_->GetFsCacheManager()->GetLruByte());

    truncateLen = 1200 * 1024;
    ret = s3ClientAdaptor_->Truncate(inode.get(), truncateLen);
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
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    CURVEFS_ERROR ret;
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();
    //  mock
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
    ret = s3ClientAdaptor_->Truncate(inode.get(), len);
    inode->SetLength(len);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    auto s3InfoListIter = inode->GetChunkInfoMap()->find(chunkIndex);
    S3ChunkInfo tmp;
    ASSERT_NE(s3InfoListIter, inode->GetChunkInfoMap()->end());
    ASSERT_EQ(1, s3InfoListIter->second.s3chunks_size());
    tmp = s3InfoListIter->second.s3chunks(0);
    ASSERT_EQ(len, tmp.len());
    ASSERT_EQ(offset, tmp.offset());
    ASSERT_EQ(true, tmp.zero());

    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();

    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);
    len = 5 * 1024 * 1024;
    s3ClientAdaptor_->Truncate(inode.get(), len);
    inode->SetLength(len);
    auto ino = inode->GetInode();
    auto s3InfoListIter = ino.s3chunkinfomap().find(chunkIndex);
    ASSERT_NE(s3InfoListIter, ino.s3chunkinfomap().end());
    ASSERT_EQ(1, s3InfoListIter->second.s3chunks_size());

    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf, 'a', 1 * 1024 * 1024);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);

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
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    CURVEFS_ERROR ret;
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();
    //  mock
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));

    ret = s3ClientAdaptor_->Truncate(inode.get(), len);
    inode->SetLength(len);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    auto s3InfoListIter = inode->GetChunkInfoMap()->find(chunkIndex);
    S3ChunkInfo tmp;
    ASSERT_NE(s3InfoListIter, inode->GetChunkInfoMap()->end());
    ASSERT_EQ(1, s3InfoListIter->second.s3chunks_size());
    tmp = s3InfoListIter->second.s3chunks(0);
    ASSERT_EQ(len, tmp.len());
    ASSERT_EQ(offset, tmp.offset());
    ASSERT_EQ(true, tmp.zero());

    len = 2 * 1024 * 1024;
    ret = s3ClientAdaptor_->Truncate(inode.get(), len);
    inode->SetLength(len);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    s3InfoListIter = inode->GetChunkInfoMap()->find(chunkIndex);
    ASSERT_NE(s3InfoListIter, inode->GetChunkInfoMap()->end());
    ASSERT_EQ(2, s3InfoListIter->second.s3chunks_size());
    tmp = s3InfoListIter->second.s3chunks(1);
    ASSERT_EQ(1 * 1024 * 1024, tmp.len());
    ASSERT_EQ(1 * 1024 * 1024, tmp.offset());
    ASSERT_EQ(true, tmp.zero());

    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = 512;
    len = 512;
    char *readBuf1 = new char[len + 1];
    memset(readBuf1, 0, len + 1);
    char *expectBuf1 = new char[len + 1];
    memset(expectBuf1, 0, len + 1);

    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf1);
    EXPECT_STREQ(expectBuf1, readBuf1);

    len = 1 * 1024 * 1024;
    inode->SetLength(3 * 1024 * 1024);
    offset = 1.5 * 1024 * 1024;
    char *readBuf2 = new char[len + 1];
    memset(readBuf2, 0, len + 1);
    char *expectBuf2 = new char[len + 1];
    memset(expectBuf2, 0, len + 1);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf2);
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
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->ReleaseCache(inode->GetInodeId());
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    offset = 2 * 1024 * 1024;
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    ASSERT_EQ(2, fsCacheManager->GetDataCacheNum());

    s3ClientAdaptor_->ReleaseCache(inode->GetInodeId());
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
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));

    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                context->retCode = 0;
                context->cb(context);
            }));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    auto ino = inode->GetInode();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    auto s3InfoListIter = ino.s3chunkinfomap().find(chunkIndex);
    S3ChunkInfo tmp;
    ASSERT_NE(s3InfoListIter, ino.s3chunkinfomap().end());
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
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);
    offset = 1 * 1024 * 1024;
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    auto ino = inode->GetInode();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    auto s3InfoListIter = ino.s3chunkinfomap().find(chunkIndex);
    S3ChunkInfo tmp;
    ASSERT_NE(s3InfoListIter, ino.s3chunkinfomap().end());
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
    uint64_t offset = 16748032;
    uint64_t len = 512;
    char *buf = new char[len];
    memset(buf, 'a', len);
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);
    offset = 16749056;
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(2, fsCacheManager->GetDataCacheNum());

    offset = 16748544;
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    auto ino = inode->GetInode();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    auto s3InfoListIter = ino.s3chunkinfomap().find(chunkIndex);
    S3ChunkInfo tmp;
    ASSERT_NE(s3InfoListIter, ino.s3chunkinfomap().end());
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
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);
    offset = 2 * 1024 * 1024;
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(2, fsCacheManager->GetDataCacheNum());

    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    auto ino = inode->GetInode();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    auto s3InfoListIter = ino.s3chunkinfomap().find(chunkIndex);
    S3ChunkInfo tmp;
    S3ChunkInfoList tmpList;
    ASSERT_NE(s3InfoListIter, ino.s3chunkinfomap().end());
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
    uint64_t offset = 0;
    uint64_t len = 5 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(mockS3Client_, UploadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                context->retCode = 0;
                context->cb(context);
            }));
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(2, fsCacheManager->GetDataCacheNum());

    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    auto ino = inode->GetInode();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(2, ino.s3chunkinfomap_size());
    auto s3InfoListIter = ino.s3chunkinfomap().find(0);
    S3ChunkInfo tmp;
    S3ChunkInfoList tmpList;
    ASSERT_NE(s3InfoListIter, ino.s3chunkinfomap().end());
    VLOG(9) << "index: " << s3InfoListIter->first;
    tmpList = s3InfoListIter->second;
    ASSERT_EQ(1, tmpList.s3chunks_size());
    tmp = tmpList.s3chunks(0);
    ASSERT_EQ(4 * 1024 * 1024, tmp.len());
    ASSERT_EQ(0, tmp.offset());
    s3InfoListIter = ino.s3chunkinfomap().find(1);
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
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
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
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);
    len = 1 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    uint64_t len1 = inode->GetLength();
    auto ino = inode->GetInode();
    inode->SetLength(len1);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());
    auto s3InfoListIter = ino.s3chunkinfomap().begin();
    S3ChunkInfo tmp;
    S3ChunkInfoList tmpList;
    ASSERT_NE(s3InfoListIter, ino.s3chunkinfomap().end());
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

    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
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
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    uint64_t len1 = inode->GetLength();
    auto ino = inode->GetInode();
    inode->SetLength(len1);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());
    auto s3InfoListIter = ino.s3chunkinfomap().begin();
    S3ChunkInfo tmp;
    S3ChunkInfoList tmpList;
    ASSERT_NE(s3InfoListIter, ino.s3chunkinfomap().end());
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

    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    s3ClientAdaptor_->ReleaseCache(inode->GetInodeId());
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
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
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    uint64_t len1 = inode->GetLength();
    auto ino = inode->GetInode();
    inode->SetLength(len1);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    offset = 0;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 'a', len);
    memset(readBuf + len, 0, 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = 2 * 1024 * 1024;
    inode->SetLength(offset + len);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    s3ClientAdaptor_->ReleaseCache(inode->GetInodeId());

    offset = 0;
    len = 3 * 1024 * 1024;
    char *readBuf1 = new char[len + 1];
    char *expectBuf1 = new char[len + 1];
    memset(readBuf1, 'b', len);
    memset(readBuf1 + len, 0, 1);
    memset(expectBuf1, 0, len + 1);
    memset(expectBuf1 + 1024 * 1024, 'a', 1024 * 1024);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf1);
    EXPECT_STREQ(expectBuf1, readBuf1);

    s3ClientAdaptor_->ReleaseCache(inode->GetInodeId());
    offset = 1.5 * 1024 * 1024;
    len = 1 * 1024 * 1024;
    char *readBuf2 = new char[len + 1];
    char *expectBuf2 = new char[len + 1];
    memset(readBuf2, 'b', len);
    memset(readBuf2 + len, 0, 1);
    memset(expectBuf2, 0, len + 1);
    memset(expectBuf2, 'a', 0.5 * 1024 * 1024);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf2);
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
    uint64_t offset = 0 * 1024 * 1024;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    uint64_t chunkId = 25;
    uint64_t chunkId1 = 26;
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId1), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
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
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    uint64_t len1 = inode->GetLength();
    auto ino = inode->GetInode();
    inode->SetLength(len1);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    offset = 1 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    s3ClientAdaptor_->ReleaseCache(inode->GetInodeId());
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 'c', len);
    memset(readBuf + len, 0, 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'b', len);
    memset(expectBuf + len, 0, 1);
    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', 512 * 1024);
    memset(buf + 512 * 1024, 'b', 512 * 1024);

    uint64_t chunkId = 25;
    uint64_t chunkId1 = 26;
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId1), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
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
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    uint64_t len1 = inode->GetLength();
    auto ino = inode->GetInode();
    inode->SetLength(len1);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    offset = 0;
    len = 512 * 1024;
    memset(buf, 'c', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(1024 * 1024);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    s3ClientAdaptor_->ReleaseCache(inode->GetInodeId());
    ino = inode->GetInode();
    inode->SetLength(1024 * 1024);
    auto s3ChunkInfoMap = ino.mutable_s3chunkinfomap();
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

    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t offset = 1024;
    uint64_t len = 1 * 1024 * 1024 - 2048;
    char *buf = new char[len];
    memset(buf, 'a', len);

    uint64_t chunkId = 25;
    uint64_t chunkId1 = 26;
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId1), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
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
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    auto ino = inode->GetInode();
    inode->SetLength(offset + len);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    offset = 2048;
    len = 512 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    s3ClientAdaptor_->ReleaseCache(inode->GetInodeId());
    ino = inode->GetInode();
    inode->SetLength(1024 * 1024);
    auto s3ChunkInfoMap = ino.mutable_s3chunkinfomap();
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

    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    uint64_t chunkId = 25;
    uint64_t chunkId1 = 26;
    uint64_t chunkId2 = 27;
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId1), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId2), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
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
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    auto ino = inode->GetInode();
    inode->SetLength(offset + len);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    offset = 1024;
    len = 512;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    offset = 2048;
    len = 512;
    memset(buf, 'c', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());

    s3ClientAdaptor_->ReleaseCache(inode->GetInodeId());
    ino = inode->GetInode();
    inode->SetLength(1024 * 1024);
    auto s3ChunkInfoMap = ino.mutable_s3chunkinfomap();
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

    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t offset = 20971520;
    uint64_t len = 4194304;
    char *buf = new char[len];
    memset(buf, 'a', len);

    uint64_t chunkId = 25;
    uint64_t chunkId1 = 26;
    uint64_t chunkId2 = 27;
    inode->SetLength(offset + len);
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId1), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId2), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
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
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    auto ino = inode->GetInode();

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    len = 524288;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    len = 1048576;
    memset(buf, 'c', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());

    s3ClientAdaptor_->ReleaseCache(inode->GetInodeId());
    ino = inode->GetInode();

    auto s3ChunkInfoMap = ino.mutable_s3chunkinfomap();
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

    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t offset = 92274688;
    uint64_t len = 4194304;
    char *buf = new char[len];
    memset(buf, 'a', len);
    memset(buf + (93210624 - 92274688), 'b', 1024);

    uint64_t chunkId = 25;
    uint64_t chunkId1 = 26;
    uint64_t chunkId2 = 27;
    inode->SetLength(offset + len);
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId1), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId2), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
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
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);


    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    auto ino = inode->GetInode();

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    offset = 92405760;
    len = 1024;
    memset(buf, 'c', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    offset = 93547520;
    len = 1024;
    memset(buf, 'd', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());

    s3ClientAdaptor_->ReleaseCache(inode->GetInodeId());
    ino = inode->GetInode();

    auto s3ChunkInfoMap = ino.mutable_s3chunkinfomap();
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

    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    inode->SetLength(offset + len);
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId1), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId2), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId3), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId4), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId5), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId6), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
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
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);


    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    auto ino = inode->GetInode();

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    offset = 8818688;
    len = 4096;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    offset = 9961472;
    len = 1048576;
    memset(buf, 'c', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());

    offset = 8523776;
    len = 4096;
    memset(buf, 'd', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());

    offset = 8732672;
    len = 512;
    memset(buf, 'e', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());

    offset = 9184768;
    len = 512;
    memset(buf, 'f', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());

    offset = 9437184;
    len = 1048576;
    memset(buf, 'g', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());


    s3ClientAdaptor_->ReleaseCache(inode->GetInodeId());
    ino = inode->GetInode();

    offset = 9437184;
    len = 131072;
    char *readBuf = new char[len + 1];
    memset(readBuf, 'h', len);
    memset(readBuf + len, 0, 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'g', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t offset = 196608;
    uint64_t len = 131072;
    char *buf = new char[len];
    memset(buf, 'a', len);

    uint64_t chunkId = 1;
    uint64_t chunkId1 = 2;

    inode->SetLength(offset + len);
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId1), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
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
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);


    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    auto ino = inode->GetInode();

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    offset = 323584;
    len = 4096;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    s3ClientAdaptor_->ReleaseCache(inode->GetInodeId());
    ino = inode->GetInode();

    offset = 196608;
    len = 131072;
    char *readBuf = new char[len + 1];
    memset(readBuf, 'c', len);
    memset(readBuf + len, 0, 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'a', len);
    memset(expectBuf + 126976, 'b', 4096);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t offset = 196608;
    uint64_t len = 131072;
    char *buf = new char[len];
    memset(buf, 'a', len);

    uint64_t chunkId = 1;
    uint64_t chunkId1 = 2;

    inode->SetLength(offset + len);
    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId1), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillRepeatedly(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
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
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);


    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    CURVEFS_ERROR ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    auto ino = inode->GetInode();

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    offset = 323584;
    len = 4096;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);

    fsCacheManager = s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ret = s3ClientAdaptor_->Flush(inode->GetInodeId());
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(1, ino.s3chunkinfomap_size());

    s3ClientAdaptor_->ReleaseCache(inode->GetInodeId());
    ino = inode->GetInode();

    offset = 196608;
    len = 131072;
    char *readBuf = new char[len + 1];
    memset(readBuf, 'c', len);
    memset(readBuf + len, 0, 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'a', len);
    memset(expectBuf + 126976, 'b', 4096);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(inode->GetInodeId(), offset, len, readBuf);
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
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)));

    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgReferee<1>(inode),
                        Return(CURVEFS_ERROR::NOTEXIST)))
        .WillOnce(DoAll(SetArgReferee<1>(inode),
                        Return(CURVEFS_ERROR::NOTEXIST)));
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

    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    CURVEFS_ERROR ret = s3ClientAdaptor_->FsSync();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, fsCacheManager->GetDataCacheNum());

    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);
    ret = s3ClientAdaptor_->FsSync();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

    ret = s3ClientAdaptor_->FsSync();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

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
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
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
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);
    offset = 1 * 1024 * 1024;
    s3ClientAdaptor_->Write(inode->GetInodeId(), offset, len, buf);
    inode->SetLength(offset + len);

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

TEST_F(ClientS3IntegrationTest, test_write_read_remotekvcache) {
    curvefs::client::common::FLAGS_enableCto = true;

    uint64_t offset_0 = 0, offset_4M = (4 << 20), chunkId = 10;
    uint64_t inodeId = inode->GetInodeId();
    uint64_t len = 128 * 1024;  // 128K
    char *buf = new char[len];
    memset(buf, 'b', len);

    // write data and prepare for flush
    {
        EXPECT_CALL(mockS3Client_, UploadAsync(_))
            .Times(2)
            .WillRepeatedly(Invoke(
                [&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                    context->retCode = 0;
                    context->cb(context);
                }));
        EXPECT_CALL(mockInodeManager_, GetInode(_, _))
            .Times(2)
            .WillRepeatedly(
                DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));

        int ret =
            s3ClientAdaptor_->Write(inodeId, offset_0, len, buf);
        ASSERT_EQ(len, ret);
        ret = s3ClientAdaptor_->Write(inodeId, offset_4M, len, buf);
        ASSERT_EQ(len, ret);
    }

    // flush data
    {
        EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _, _))
            .Times(2)
            .WillOnce(
                DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)))
            .WillOnce(
                DoAll(SetArgPointee<2>(chunkId + 1), Return(FSStatusCode::OK)));
        EXPECT_CALL(mockKVClient_, Set(_, _, _, _))
            .Times(2)
            .WillOnce(Return(true))
            .WillOnce(Return(false));
        EXPECT_CALL(mockInodeManager_, ShipToFlush(_)).Times(2);

        CURVEFS_ERROR res = s3ClientAdaptor_->Flush(inodeId);
        ASSERT_EQ(CURVEFS_ERROR::OK, res);
    }

    // read (offset_0, len)
    {
        char *readBuf = new char[len];
        memset(readBuf, 0, len);
        EXPECT_CALL(mockInodeManager_, GetInode(_, _))
            .WillOnce(
                DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
        EXPECT_CALL(mockKVClient_, Get(_, _, 0, len, _))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + len), Return(true)));
        int readLen = s3ClientAdaptor_->Read(inodeId, offset_0, len, readBuf);
        EXPECT_EQ(readLen, len);
        ASSERT_EQ(0, memcmp(buf, readBuf, len));
    }

    // read (offset_4M, len)
    {
        char *readBuf = new char[len];
        memset(readBuf, 0, len);
        EXPECT_CALL(mockInodeManager_, GetInode(_, _))
            .WillOnce(
                DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
        EXPECT_CALL(mockKVClient_, Get(_, _, 0, len, _))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + len), Return(true)));
        int readLen = s3ClientAdaptor_->Read(inodeId, offset_4M, len, readBuf);
        EXPECT_EQ(readLen, len);
        ASSERT_EQ(0, memcmp(buf, readBuf, len));
    }

    // read (offset_0, len), remote cache fail
    {
        char *readBuf = new char[len];
        memset(readBuf, 0, len);
        EXPECT_CALL(mockInodeManager_, GetInode(_, _))
            .WillOnce(
                DoAll(SetArgReferee<1>(inode), Return(CURVEFS_ERROR::OK)));
        EXPECT_CALL(mockKVClient_, Get(_, _, 0, len, _))
            .WillOnce(Return(false));
        EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + len), Return(true)));
        int readLen = s3ClientAdaptor_->Read(inodeId, offset_0, len, readBuf);
        EXPECT_EQ(readLen, len);
        ASSERT_EQ(0, memcmp(buf, readBuf, len));
    }
}

}  // namespace client
}  // namespace curvefs

