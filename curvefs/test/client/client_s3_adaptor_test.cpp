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
#include "curvefs/test/client/mock_mds_client.h"
#include "curvefs/test/client/mock_metaserver_service.h"
#include "curvefs/test/client/mock_spacealloc_service.h"

namespace curvefs {
namespace client {
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
void S3RpcService(google::protobuf::RpcController* cntl_base,
                  const RpcRequestType* request, RpcResponseType* response,
                  google::protobuf::Closure* done) {
    if (RpcFailed) {
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        cntl->SetFailed(112, "Not connected to");
    }
    LOG(INFO) << "run s3 prc service";
    done->Run();
}

struct S3Data {
    char* buf;
    uint64_t len;
};

static std::map<std::string, S3Data> gObjectDataMaps;

int S3Upload(std::string name, const char* buf, uint64_t len) {
    S3Data& tmp = gObjectDataMaps[name];

    tmp.len = len;
    tmp.buf = new char[len];
    strncpy(tmp.buf, buf, len);
    LOG(INFO) << "S3Upload len:" << len << ",name" << name << "buf:" << buf[0];
    return len;
}

int S3Download(std::string name, char* buf, uint64_t offset, uint64_t len) {
    S3Data& tmp = gObjectDataMaps[name];

    assert((offset + len) <= tmp.len);
    strncpy(buf, tmp.buf + offset, len);
    LOG(INFO) << "S3Download offset:" << offset << ",len:" << len
              << ",name:" << name << "buf:" << buf[0];
    return len;
}

class ClientS3AdaptorTest : public testing::Test {
 protected:
    ClientS3AdaptorTest() {}
    ~ClientS3AdaptorTest() {}
    void SetUp() override {
        ASSERT_EQ(0, server_.AddService(&mockMetaServerService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        /*ASSERT_EQ(0, server_.AddService(&mockSpaceAllocService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));*/
        ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));
        S3ClientAdaptorOption option;
        option.blockSize = 1 * 1024 * 1024;
        option.chunkSize = 4 * 1024 * 1024;
        option.intervalSec = 5000;
        option.flushIntervalSec = 5000;
        option.readCacheMaxByte = 104857600;
        option.diskCacheOpt.enableDiskCache = false;
        // auto metaClient = std::make_shared<MetaServerClientImpl>();
        std::shared_ptr<MockInodeCacheManager> mockInodeManager(
            &mockInodeManager_);
        std::shared_ptr<MockMdsClient> mockMdsClient(&mockMdsClient_);
        s3ClientAdaptor_ = new S3ClientAdaptorImpl();
        s3ClientAdaptor_->Init(option, &mockS3Client_, mockInodeManager,
                               mockMdsClient);
        S3ClientAdaptorOption option2;
        option2.nearfullRatio = 100;
        option2.writeCacheMaxByte = 104857600;
        option2.blockSize = 1 * 1024 * 1024;
        option2.chunkSize = 4 * 1024 * 1024;
        s3ClientAdaptor2_ = new S3ClientAdaptorImpl();
        s3ClientAdaptor2_->Init(option2, &mockS3Client_, mockInodeManager,
                               mockMdsClient);
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

 protected:
    S3ClientAdaptorImpl* s3ClientAdaptor_;
    S3ClientAdaptorImpl* s3ClientAdaptor2_;
    MockMetaServerService mockMetaServerService_;
    MockS3Client mockS3Client_;
    MockInodeCacheManager mockInodeManager_;
    MockMdsClient mockMdsClient_;
    std::string addr_ = "127.0.0.1:5628";
    brpc::Server server_;
};
uint64_t gInodeId = 1;
void InitInode(Inode* inode) {
    inode->set_inodeid(gInodeId);
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
    gInodeId++;
}

TEST_F(ClientS3AdaptorTest, test_first_write) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char* buf = new char[len];
    memset(buf, 'a', len);

    int ret = s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(len, ret);

    delete buf;
    buf = NULL;
}

TEST_F(ClientS3AdaptorTest, test_first_write_2) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 2;
    char* buf = new char[len];
    memset(buf, 'a', len);

    int ret = s3ClientAdaptor2_->Write(inode.inodeid(), offset, len, buf);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor2_->GetFsCacheManager();
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());
    ASSERT_EQ(len, ret);

    delete buf;
    buf = NULL;
}

TEST_F(ClientS3AdaptorTest, test_overlap_write) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 2 * 1024 * 1024;
    char* buf = new char[len];
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
    // ASSERT_EQ(0, s3ChunkInfoList.s3chunks(0).version());
    // ASSERT_EQ(1, s3ChunkInfoList.s3chunks(1).version());
    // ASSERT_EQ(2, s3ChunkInfoList.s3chunks(2).version());
    // ASSERT_EQ(3, s3ChunkInfoList.s3chunks(3).version());

    delete buf;
    buf = NULL;
}

TEST_F(ClientS3AdaptorTest, test_hole_write) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char* buf = new char[len];
    memset(buf, 'a', len);
    /*
    ::curvefs::space::AllocateS3ChunkResponse response;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);

    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));*/
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

TEST_F(ClientS3AdaptorTest, test_append_write) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char* buf = new char[len];
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

TEST_F(ClientS3AdaptorTest, test_write_more_chunk) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 5 * 1024 * 1024;
    char* buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    std::shared_ptr<FsCacheManager> fsCacheManager =
        s3ClientAdaptor_->GetFsCacheManager();
    ASSERT_EQ(2, fsCacheManager->GetDataCacheNum());

    delete buf;
    buf = NULL;
}

TEST_F(ClientS3AdaptorTest, test_write_merge_data1) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char* buf = new char[len];
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
    char* buf1 = new char[len];
    memset(buf1, 'b', len);

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf1);
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    delete buf;
    delete buf1;
}

TEST_F(ClientS3AdaptorTest, test_write_merge_data2) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char* buf = new char[len];
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
    char* buf1 = new char[len];
    memset(buf1, 'b', len);

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf1);
    ASSERT_EQ(1, fsCacheManager->GetDataCacheNum());

    delete buf;
    delete buf1;
}

TEST_F(ClientS3AdaptorTest, test_read_one_chunk) {
    uint64_t readFileLen = 2 * 1024 * 1024;
    curvefs::metaserver::Inode inode;
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    uint64_t chunkIndex = offset / chunkSize;
    InitInode(&inode);
    inode.set_length(readFileLen);

    auto s3ChunkInfoMap = inode.mutable_s3chunkinfomap();
    S3ChunkInfoList* s3ChunkInfoList = new S3ChunkInfoList();
    S3ChunkInfo* s3ChunkInfo = s3ChunkInfoList->add_s3chunks();
    s3ChunkInfo->set_chunkid(25);
    s3ChunkInfo->set_compaction(0);
    s3ChunkInfo->set_offset(0);
    s3ChunkInfo->set_len(readFileLen);
    s3ChunkInfo->set_size(readFileLen);
    s3ChunkInfo->set_zero(false);
    s3ChunkInfoMap->insert({chunkIndex, *s3ChunkInfoList});

    char* buf = new char[len];
    char* tmpbuf = new char[len];
    memset(tmpbuf, 'a', len);
    curve::common::S3Adapter* s3Adapter = new curve::common::S3Adapter();

    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<1>(*tmpbuf), Return(1 * 1024 * 1024)))
        .WillOnce(Return(-1));

    int ret = s3ClientAdaptor_->Read(&inode, offset, len, buf);
    ASSERT_EQ(len, ret);
    ASSERT_EQ('a', buf[0]);
    offset = offset + len;
    ret = s3ClientAdaptor_->Read(&inode, offset, len, buf);
    ASSERT_EQ(-1, ret);

    delete buf;
    buf = NULL;
}

TEST_F(ClientS3AdaptorTest, test_read_overlap_block1) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char* buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    len = 1 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);

    char* readBuf = new char[len + 1];
    char* expectBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    memset(expectBuf, 'b', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'a', len);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
    // EXPECT_STREQ(expectBuf, readBuf);

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

TEST_F(ClientS3AdaptorTest, test_read_overlap_block2) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char* buf = new char[len];
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
    char* readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char* expectBuf = new char[len + 1];
    memset(expectBuf, 'a', len);
    memset(expectBuf + len, 0, 1);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
    // EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
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

TEST_F(ClientS3AdaptorTest, test_read_overlap_block3) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 3 * 1024 * 1024;
    char* buf = new char[len];
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
    char* readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char* expectBuf = new char[len + 1];
    memset(expectBuf, 'a', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'a', len);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
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

TEST_F(ClientS3AdaptorTest, test_read_overlap_block4) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t max_len = 3 * 1024 * 1024;
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char* buf = new char[max_len];
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
    char* readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char* expectBuf = new char[len + 1];
    memset(expectBuf, 'b', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
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

TEST_F(ClientS3AdaptorTest, test_read_overlap_block5) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t max_len = 2 * 1024 * 1024;
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 2 * 1024 * 1024;
    char* buf = new char[max_len];
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
    char* readBuf = new char[len + 1];
    char* expectBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    memset(expectBuf, 0, len + 1);

    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'b', len);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = offset + len;
    memset(expectBuf, 'a', len);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
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

TEST_F(ClientS3AdaptorTest, test_read_hole1) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char* buf = new char[len];
    memset(buf, 'a', len);
    /*
    ::curvefs::space::AllocateS3ChunkResponse response;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);

    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));*/
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    // ASSERT_EQ(2, gObjectDataMaps.size());
    // ASSERT_EQ('a', gObjectDataMaps.begin()->second.buf[0]);

    offset = 0;
    len = 1 * 1024 * 1024;
    char* readBuf = new char[len + 1];
    memset(readBuf, 'a', len);
    memset(readBuf + len, 0, 1);
    char* expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = 2 * 1024 * 1024;
    inode.set_length(offset + len);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
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

TEST_F(ClientS3AdaptorTest, test_read_hole2) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 2 * 1024 * 1024;
    char* buf = new char[len];
    memset(buf, 'a', len);
    /*
    ::curvefs::space::AllocateS3ChunkResponse response;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);

    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));*/
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    // ASSERT_EQ(2, gObjectDataMaps.size());
    // ASSERT_EQ('a', gObjectDataMaps.begin()->second.buf[0]);

    offset = 0;
    len = 2 * 1024 * 1024;
    char* readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char* expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf + 1024 * 1024, 'a', 1024 * 1024);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);

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

TEST_F(ClientS3AdaptorTest, test_read_hole3) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char* buf = new char[len];
    memset(buf, 'a', len);
    /*
    ::curvefs::space::AllocateS3ChunkResponse response;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);
    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));*/
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    // ASSERT_EQ(1, gObjectDataMaps.size());
    // ASSERT_EQ('a', gObjectDataMaps.begin()->second.buf[0]);

    offset = 0;
    len = 2 * 1024 * 1024;
    char* readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char* expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf + 1024 * 1024, 'a', 1024 * 1024);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
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

TEST_F(ClientS3AdaptorTest, test_read_hole4) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char* buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    offset = 2 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    offset = 0;
    len = 3 * 1024 * 1024;
    char* readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char* expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf, 'a', 1024 * 1024);
    memset(expectBuf + 2 * 1024 * 1024, 'b', 1024 * 1024);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);

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

TEST_F(ClientS3AdaptorTest, test_read_more_write) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 100;
    char* buf = new char[len];
    memset(buf, 'a', len);

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    offset = 120;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);

    offset = 0;
    len = 1024;
    inode.set_length(1024);
    char* readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char* expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf, 'a', 100);
    memset(expectBuf + 120, 'b', 100);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);

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

TEST_F(ClientS3AdaptorTest, test_read_more_chunks) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 4 * 1024 * 1024 - 1024;
    uint64_t len = 1024;
    char* buf = new char[len];
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
    char* readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char* expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf, 'a', 1024);
    memset(expectBuf + 1024, 'b', 1024);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);

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

TEST_F(ClientS3AdaptorTest, test_truncate_small) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char* buf = new char[len];
    memset(buf, 'a', len);

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    len = 1 * 1024 * 1024;
    CURVEFS_ERROR ret = s3ClientAdaptor_->Truncate(&inode, len);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    // cleanup
    delete buf;
    std::map<std::string, S3Data>::iterator iter = gObjectDataMaps.begin();
    for (; iter != gObjectDataMaps.end(); iter++) {
        delete iter->second.buf;
        iter->second.buf = NULL;
    }
    gObjectDataMaps.clear();
}

TEST_F(ClientS3AdaptorTest, test_truncate_big1) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    CURVEFS_ERROR ret;
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();
    ::curvefs::space::AllocateS3ChunkResponse response;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);
    //  mock
    /*EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke(S3RpcService<AllocateS3ChunkRequest,
                                            AllocateS3ChunkResponse>)));*/

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

    char* readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char* expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
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

TEST_F(ClientS3AdaptorTest, test_truncate_big2) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char* buf = new char[len];
    memset(buf, 'a', len);
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();

    ::curvefs::space::AllocateS3ChunkResponse response;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);

    s3ClientAdaptor_->Write(inode.inodeid(), offset, len, buf);
    inode.set_length(offset + len);
    len = 5 * 1024 * 1024;
    s3ClientAdaptor_->Truncate(&inode, len);
    inode.set_length(len);
    auto s3InfoListIter = inode.s3chunkinfomap().find(chunkIndex);
    ASSERT_NE(s3InfoListIter, inode.s3chunkinfomap().end());
    ASSERT_EQ(1, s3InfoListIter->second.s3chunks_size());

    char* readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char* expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    memset(expectBuf, 'a', 1 * 1024 * 1024);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);

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

TEST_F(ClientS3AdaptorTest, test_truncate_big3) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    CURVEFS_ERROR ret;
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();
    ::curvefs::space::AllocateS3ChunkResponse response;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);
    //  mock
    /*
    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(
                S3RpcService<AllocateS3ChunkRequest, AllocateS3ChunkResponse>)))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke(S3RpcService<AllocateS3ChunkRequest,
                                            AllocateS3ChunkResponse>)));*/

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

    char* readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char* expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = 512;
    len = 512;
    char* readBuf1 = new char[len + 1];
    memset(readBuf1, 0, len + 1);
    char* expectBuf1 = new char[len + 1];
    memset(expectBuf1, 0, len + 1);

    s3ClientAdaptor_->Read(&inode, offset, len, readBuf1);
    EXPECT_STREQ(expectBuf1, readBuf1);

    len = 1 * 1024 * 1024;
    inode.set_length(3 * 1024 * 1024);
    offset = 1.5 * 1024 * 1024;
    char* readBuf2 = new char[len + 1];
    memset(readBuf2, 0, len + 1);
    char* expectBuf2 = new char[len + 1];
    memset(expectBuf2, 0, len + 1);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf2);
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

TEST_F(ClientS3AdaptorTest, test_releaseCache) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char* buf = new char[len];
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

TEST_F(ClientS3AdaptorTest, test_flush_first_write) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char* buf = new char[len];
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

TEST_F(ClientS3AdaptorTest, test_flush_overlap_write) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char* buf = new char[len];
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

TEST_F(ClientS3AdaptorTest, test_flush_hole_write) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char* buf = new char[len];
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

TEST_F(ClientS3AdaptorTest, test_flush_write_more_chunk) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 5 * 1024 * 1024;
    char* buf = new char[len];
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
    auto s3InfoListIter = inode.s3chunkinfomap().begin();
    S3ChunkInfo tmp;
    S3ChunkInfoList tmpList;
    ASSERT_NE(s3InfoListIter, inode.s3chunkinfomap().end());
    tmpList = s3InfoListIter->second;
    ASSERT_EQ(1, tmpList.s3chunks_size());
    tmp = tmpList.s3chunks(0);
    ASSERT_EQ(4 * 1024 * 1024, tmp.len());
    ASSERT_EQ(0, tmp.offset());
    s3InfoListIter++;
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

TEST_F(ClientS3AdaptorTest, test_flush_write_and_read1) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char* buf = new char[len];
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

    char* readBuf = new char[len + 1];
    char* expectBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    memset(expectBuf, 'b', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
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

TEST_F(ClientS3AdaptorTest, test_flush_write_and_read2) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char* buf = new char[len];
    memset(buf, 'a', len);
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();
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
    char* readBuf = new char[len + 1];
    char* expectBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    memset(expectBuf, 'a', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    s3ClientAdaptor_->ReleaseCache(inode.inodeid());
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
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

TEST_F(ClientS3AdaptorTest, test_flush_write_and_read3) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 1 * 1024 * 1024;
    char* buf = new char[len];
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
    char* readBuf = new char[len + 1];
    memset(readBuf, 'a', len);
    memset(readBuf + len, 0, 1);
    char* expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    offset = 2 * 1024 * 1024;
    inode.set_length(offset + len);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
    EXPECT_STREQ(expectBuf, readBuf);

    s3ClientAdaptor_->ReleaseCache(inode.inodeid());

    offset = 0;
    len = 3 * 1024 * 1024;
    char* readBuf1 = new char[len + 1];
    char* expectBuf1 = new char[len + 1];
    memset(readBuf1, 'b', len);
    memset(readBuf1 + len, 0, 1);
    memset(expectBuf1, 0, len + 1);
    memset(expectBuf1 + 1024 * 1024, 'a', 1024 * 1024);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf1);
    EXPECT_STREQ(expectBuf1, readBuf1);

    s3ClientAdaptor_->ReleaseCache(inode.inodeid());
    offset = 1.5 * 1024 * 1024;
    len = 1 * 1024 * 1024;
    char* readBuf2 = new char[len + 1];
    char* expectBuf2 = new char[len + 1];
    memset(readBuf2, 'b', len);
    memset(readBuf2 + len, 0, 1);
    memset(expectBuf2, 0, len + 1);
    memset(expectBuf2, 'a', 0.5 * 1024 * 1024);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf2);
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

TEST_F(ClientS3AdaptorTest, test_flush_write_and_read4) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0 * 1024 * 1024;
    uint64_t len = 2 * 1024 * 1024;
    char* buf = new char[len];
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
    char* readBuf = new char[len + 1];
    memset(readBuf, 'c', len);
    memset(readBuf + len, 0, 1);
    char* expectBuf = new char[len + 1];
    memset(expectBuf, 'b', len);
    memset(expectBuf, 'a', 1024 * 1024);
    memset(expectBuf + len, 0, 1);
    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
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

TEST_F(ClientS3AdaptorTest, test_flush_write_and_read5) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char* buf = new char[len];
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
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

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
    char* readBuf = new char[len + 1];
    memset(readBuf, 'c', len);
    memset(readBuf + len, 0, 1);
    char* expectBuf = new char[len + 1];
    memset(expectBuf, 'b', len);
    memset(expectBuf + len, 0, 1);

    s3ClientAdaptor_->Read(&inode, offset, len, readBuf);
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

TEST_F(ClientS3AdaptorTest, test_fssync_success_and_fail) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char* buf = new char[len];
    memset(buf, 'a', len);
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillOnce(Return(1 * 1024 * 1024))
        .WillOnce(Return(-1))
        .WillOnce(Return(1 * 1024 * 1024));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgReferee<1>(inodeWrapper),
                        Return(CURVEFS_ERROR::INTERNAL)));

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

TEST_F(ClientS3AdaptorTest, test_fssync_overlap_write) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char* buf = new char[len];
    memset(buf, 'a', len);
    uint64_t chunkIndex = offset / s3ClientAdaptor_->GetChunkSize();
    //  mock
    uint64_t chunkId = 25;

    EXPECT_CALL(mockMdsClient_, AllocS3ChunkId(_, _))
        .WillOnce(
            DoAll(SetArgPointee<1>(chunkId), Return(FSStatusCode::OK)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));

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
    /*
    auto size = inode.s3chunkinfomap_size();
    auto s3InfoListIter = inode.s3chunkinfomap().find(chunkIndex);
    S3ChunkInfo tmp;
    ASSERT_NE(s3InfoListIter, inode.s3chunkinfomap().end());
    ASSERT_EQ(1, s3InfoListIter->second.s3chunks_size());
    tmp = s3InfoListIter->second.s3chunks(0);
    ASSERT_EQ(3 * 1024 * 1024, tmp.len());
    ASSERT_EQ(0, tmp.offset());*/

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

