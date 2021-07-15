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

#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/test/client/mock_metaserver_service.h"
#include "curvefs/test/client/mock_spacealloc_service.h"
#include "curvefs/test/client/mock_client_s3.h"

namespace curvefs {
namespace client {
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

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

int S3Upload(std::string name, const char* buf, uint64_t len) {
    S3Data &tmp = gObjectDataMaps[name];

    tmp.len = len;
    tmp.buf = new char[len];
    strncpy(tmp.buf, buf, len);

    return len;
}

int S3Download(std::string name, char* buf, uint64_t offset, uint64_t len) {
    S3Data &tmp = gObjectDataMaps[name];

    assert((offset + len) <= tmp.len);
    strncpy(buf + offset, tmp.buf, len);

    return len;
}


class ClientS3AdaptorTest : public testing::Test {
 protected:
    void SetUp() override {
        ASSERT_EQ(0, server_.AddService(&mockMetaServerService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.AddService(&mockSpaceAllocService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));
        S3ClientAdaptorOption option;
        option.blockSize = 1 * 1024 * 1024;
        option.chunkSize = 4 * 1024 * 1024;
        option.metaServerEps = addr_;
        option.allocateServerEps = addr_;
        s3ClientAdaptor_ = new S3ClientAdaptorImpl();
        s3ClientAdaptor_->Init(option, &mockS3Client_);
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

 protected:
    S3ClientAdaptor* s3ClientAdaptor_;
    MockMetaServerService mockMetaServerService_;
    MockSpaceAllocService mockSpaceAllocService_;
    MockS3Client mockS3Client_;
    std::string addr_ = "127.0.0.1:5628";
    brpc::Server server_;
};

void InitInode(Inode* inode) {
    inode->set_inodeid(1);
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
}

TEST_F(ClientS3AdaptorTest, test_first_write) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    ::curvefs::space::AllocateS3ChunkResponse response;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);
    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));

    int ret = s3ClientAdaptor_->Write(&inode, offset, len, buf);
    const S3ChunkInfoList& s3ChunkInfoList = inode.s3chunkinfolist();
    ASSERT_EQ(1, s3ChunkInfoList.s3chunks_size());
    ASSERT_EQ(25, s3ChunkInfoList.s3chunks(0).chunkid());
    ASSERT_EQ(len, ret);
    ASSERT_EQ(0, inode.version());
    delete buf;
    buf = NULL;
}


TEST_F(ClientS3AdaptorTest, test_overlap_write) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 1 * 1024 * 1024;
    uint64_t len = 2 * 1024 * 1024;

    char *buf = new char[len];
    memset(buf, 'a', len);
    ::curvefs::space::AllocateS3ChunkResponse response;
    ::curvefs::metaserver::UpdateInodeS3VersionResponse response1;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);
    response1.set_statuscode(::curvefs::metaserver::MetaStatusCode::OK);
    response1.set_version(1);
    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));
    EXPECT_CALL(mockMetaServerService_, UpdateInodeS3Version(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response1),
                  Invoke(S3RpcService<UpdateInodeS3VersionRequest,
                  UpdateInodeS3VersionResponse>)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));
    s3ClientAdaptor_->Write(&inode, offset, len, buf);
    inode.set_length(offset+len);
    s3ClientAdaptor_->Write(&inode, offset, len, buf);

    const S3ChunkInfoList& s3ChunkInfoList = inode.s3chunkinfolist();
    ASSERT_EQ(2, s3ChunkInfoList.s3chunks_size());
    ASSERT_EQ(0, s3ChunkInfoList.s3chunks(0).version());
    ASSERT_EQ(1, s3ChunkInfoList.s3chunks(1).version());

    response1.set_version(2);
    EXPECT_CALL(mockMetaServerService_, UpdateInodeS3Version(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response1),
                  Invoke(S3RpcService<UpdateInodeS3VersionRequest,
                  UpdateInodeS3VersionResponse>)));
    offset = 0;
    s3ClientAdaptor_->Write(&inode, offset, len, buf);

    ASSERT_EQ(3, s3ChunkInfoList.s3chunks_size());
    ASSERT_EQ(0, s3ChunkInfoList.s3chunks(0).version());
    ASSERT_EQ(1, s3ChunkInfoList.s3chunks(1).version());
    ASSERT_EQ(2, s3ChunkInfoList.s3chunks(2).version());

    response1.set_version(3);
    EXPECT_CALL(mockMetaServerService_, UpdateInodeS3Version(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response1),
                  Invoke(S3RpcService<UpdateInodeS3VersionRequest,
                  UpdateInodeS3VersionResponse>)));
    offset = 2 * 1024 * 1024;
    s3ClientAdaptor_->Write(&inode, offset, len, buf);

    ASSERT_EQ(4, s3ChunkInfoList.s3chunks_size());
    ASSERT_EQ(0, s3ChunkInfoList.s3chunks(0).version());
    ASSERT_EQ(1, s3ChunkInfoList.s3chunks(1).version());
    ASSERT_EQ(2, s3ChunkInfoList.s3chunks(2).version());
    ASSERT_EQ(3, s3ChunkInfoList.s3chunks(3).version());

    delete buf;
    buf = NULL;
}

TEST_F(ClientS3AdaptorTest, test_discontinuity_write) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 2 * 1024 * 1024;
    uint64_t len = 2 * 1024 * 1024;

    char *buf = new char[len];
    memset(buf, 'a', len);
    ::curvefs::space::AllocateS3ChunkResponse response;
    ::curvefs::metaserver::UpdateInodeS3VersionResponse response1;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);
    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));

    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));

    s3ClientAdaptor_->Write(&inode, offset, len, buf);
    inode.set_length(offset+len);
    offset = 0;
    len = 1 * 1024 * 1024;
    s3ClientAdaptor_->Write(&inode, offset, len, buf);

    const S3ChunkInfoList& s3ChunkInfoList = inode.s3chunkinfolist();
    ASSERT_EQ(2, s3ChunkInfoList.s3chunks_size());
    ASSERT_EQ(0, s3ChunkInfoList.s3chunks(0).version());
    ASSERT_EQ(0, s3ChunkInfoList.s3chunks(1).version());

    delete buf;
    buf = NULL;
}

TEST_F(ClientS3AdaptorTest, test_hole_write) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);

    ::curvefs::space::AllocateS3ChunkResponse response;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);

    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));
    s3ClientAdaptor_->Write(&inode, offset, len, buf);
    inode.set_length(offset+len);
    offset = 2 * 1024 * 1024;
    s3ClientAdaptor_->Write(&inode, offset, len, buf);

    const S3ChunkInfoList& s3ChunkInfoList = inode.s3chunkinfolist();
    ASSERT_EQ(2, s3ChunkInfoList.s3chunks_size());
    ASSERT_EQ(0, s3ChunkInfoList.s3chunks(0).version());
    ASSERT_EQ(0, s3ChunkInfoList.s3chunks(1).version());
    delete buf;
    buf = NULL;
}

TEST_F(ClientS3AdaptorTest, test_append_write) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 1 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    ::curvefs::space::AllocateS3ChunkResponse response;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);
    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));
    s3ClientAdaptor_->Write(&inode, offset, len, buf);
    inode.set_length(offset+len);
    offset = offset+len;
    s3ClientAdaptor_->Write(&inode, offset, len, buf);

    const S3ChunkInfoList& s3ChunkInfoList = inode.s3chunkinfolist();
    ASSERT_EQ(1, s3ChunkInfoList.s3chunks_size());
    ASSERT_EQ(25, s3ChunkInfoList.s3chunks(0).chunkid());
    ASSERT_EQ(offset + len, s3ChunkInfoList.s3chunks(0).len());

    delete buf;
    buf = NULL;
}

TEST_F(ClientS3AdaptorTest, test_write_more_chunk) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 5 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    ::curvefs::space::AllocateS3ChunkResponse response, response1;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);
    response1.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response1.set_chunkid(26);
    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)))
        .WillOnce(
            DoAll(SetArgPointee<2>(response1),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));
    s3ClientAdaptor_->Write(&inode, offset, len, buf);
    inode.set_length(offset+len);

    const S3ChunkInfoList& s3ChunkInfoList = inode.s3chunkinfolist();
    ASSERT_EQ(2, s3ChunkInfoList.s3chunks_size());
    ASSERT_EQ(25, s3ChunkInfoList.s3chunks(0).chunkid());
    ASSERT_EQ(4 * 1024 * 1024, s3ChunkInfoList.s3chunks(0).len());
    ASSERT_EQ(26, s3ChunkInfoList.s3chunks(1).chunkid());
    ASSERT_EQ(1 * 1024 * 1024, s3ChunkInfoList.s3chunks(1).len());
}

TEST_F(ClientS3AdaptorTest, test_read_one_chunk) {
    uint64_t readFileLen = 2 * 1024 * 1024;
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    inode.set_length(readFileLen);
    S3ChunkInfoList *s3ChunkInfoList = new S3ChunkInfoList();
    S3ChunkInfo *s3ChunkInfo = s3ChunkInfoList->add_s3chunks();
    s3ChunkInfo->set_chunkid(25);
    s3ChunkInfo->set_version(0);
    s3ChunkInfo->set_offset(0);
    s3ChunkInfo->set_len(readFileLen);
    s3ChunkInfo->set_size(readFileLen);
    inode.set_allocated_s3chunkinfolist(s3ChunkInfoList);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    char *tmpbuf = new char[len];
    memset(tmpbuf, 'a', len);

    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(
                 DoAll(SetArgPointee<1>(*tmpbuf),
                Return(1 * 1024 * 1024)));
    int ret = s3ClientAdaptor_->Read(&inode, offset, len, buf);
    ASSERT_EQ(len, ret);
    ASSERT_EQ('a', buf[0]);
    delete buf;
    buf = NULL;
}

TEST_F(ClientS3AdaptorTest, test_read_overlap_block1) {
     curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    ::curvefs::space::AllocateS3ChunkResponse response;
    ::curvefs::metaserver::UpdateInodeS3VersionResponse response1;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);
    response1.set_statuscode(::curvefs::metaserver::MetaStatusCode::OK);
    response1.set_version(1);
    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockMetaServerService_, UpdateInodeS3Version(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response1),
                  Invoke(S3RpcService<UpdateInodeS3VersionRequest,
                  UpdateInodeS3VersionResponse>)));
    s3ClientAdaptor_->Write(&inode, offset, len, buf);
    inode.set_length(offset+len);
    len = 1 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(&inode, offset, len, buf);

    char *readBuf = new char[len + 1];
    char *expectBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    memset(expectBuf, 'b', len);
    memset(expectBuf + len, 0, 1);

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

TEST_F(ClientS3AdaptorTest, test_read_overlap_block2) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    ::curvefs::space::AllocateS3ChunkResponse response;
    ::curvefs::metaserver::UpdateInodeS3VersionResponse response1;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);
    response1.set_statuscode(::curvefs::metaserver::MetaStatusCode::OK);
    response1.set_version(1);
    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockMetaServerService_, UpdateInodeS3Version(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response1),
                  Invoke(S3RpcService<UpdateInodeS3VersionRequest,
                  UpdateInodeS3VersionResponse>)));
    s3ClientAdaptor_->Write(&inode, offset, len, buf);
    inode.set_length(offset+len);
    offset = 1 * 1024 * 1024;
    len = 2 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(&inode, offset, len, buf);
    offset = 0;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 'a', len);
    memset(expectBuf + len, 0, 1);
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

TEST_F(ClientS3AdaptorTest, test_read_overlap_block3) {
    curvefs::metaserver::Inode inode;

    InitInode(&inode);
    uint64_t offset = 0;
    uint64_t len = 3 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    ::curvefs::space::AllocateS3ChunkResponse response;
    ::curvefs::metaserver::UpdateInodeS3VersionResponse response1;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);
    response1.set_statuscode(::curvefs::metaserver::MetaStatusCode::OK);
    response1.set_version(1);
    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockMetaServerService_, UpdateInodeS3Version(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response1),
                  Invoke(S3RpcService<UpdateInodeS3VersionRequest,
                  UpdateInodeS3VersionResponse>)));
    s3ClientAdaptor_->Write(&inode, offset, len, buf);
    inode.set_length(offset+len);
    offset = 1 * 1024 * 1024;
    len = 1 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(&inode, offset, len, buf);

    offset = 0;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
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
    char *buf = new char[max_len];
    memset(buf, 'a', len);
    ::curvefs::space::AllocateS3ChunkResponse response;
    ::curvefs::metaserver::UpdateInodeS3VersionResponse response1;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);
    response1.set_statuscode(::curvefs::metaserver::MetaStatusCode::OK);
    response1.set_version(1);
    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockMetaServerService_, UpdateInodeS3Version(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response1),
                  Invoke(S3RpcService<UpdateInodeS3VersionRequest,
                  UpdateInodeS3VersionResponse>)));
    s3ClientAdaptor_->Write(&inode, offset, len, buf);
    inode.set_length(offset+len);
    offset = 0;
    len = 3 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(&inode, offset, len, buf);

    offset = 0;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
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
    char *buf = new char[max_len];
    memset(buf, 'a', len);
    ::curvefs::space::AllocateS3ChunkResponse response;
    ::curvefs::metaserver::UpdateInodeS3VersionResponse response1;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);
    response1.set_statuscode(::curvefs::metaserver::MetaStatusCode::OK);
    response1.set_version(1);
    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    EXPECT_CALL(mockMetaServerService_, UpdateInodeS3Version(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response1),
                  Invoke(S3RpcService<UpdateInodeS3VersionRequest,
                  UpdateInodeS3VersionResponse>)));
    s3ClientAdaptor_->Write(&inode, offset, len, buf);
    inode.set_length(offset+len);
    offset = 0;
    len = 2 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(&inode, offset, len, buf);

    offset = 0;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    char *expectBuf = new char[len + 1];
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
    uint64_t offset = 2 * 1024 * 1024;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    memset(buf, 'a', len);
    ::curvefs::space::AllocateS3ChunkResponse response;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);
    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    s3ClientAdaptor_->Write(&inode, offset, len, buf);
    inode.set_length(offset+len);

    ASSERT_EQ(2, gObjectDataMaps.size());
    ASSERT_EQ('a', gObjectDataMaps.begin()->second.buf[0]);

    offset = 0;
    len = 1 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
    memset(expectBuf, 0, len + 1);
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
    char *buf = new char[len];
    memset(buf, 'a', len);
    ::curvefs::space::AllocateS3ChunkResponse response;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);
    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    s3ClientAdaptor_->Write(&inode, offset, len, buf);
    inode.set_length(offset+len);

    ASSERT_EQ(2, gObjectDataMaps.size());
    ASSERT_EQ('a', gObjectDataMaps.begin()->second.buf[0]);

    offset = 0;
    len = 2 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
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
    char *buf = new char[len];
    memset(buf, 'a', len);
    ::curvefs::space::AllocateS3ChunkResponse response;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);
    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    s3ClientAdaptor_->Write(&inode, offset, len, buf);
    inode.set_length(offset+len);

    ASSERT_EQ(1, gObjectDataMaps.size());
    ASSERT_EQ('a', gObjectDataMaps.begin()->second.buf[0]);

    offset = 0;
    len = 3 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
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
    char *buf = new char[len];
    memset(buf, 'a', len);
    ::curvefs::space::AllocateS3ChunkResponse response;
    response.set_status(::curvefs::space::SpaceStatusCode::SPACE_OK);
    response.set_chunkid(25);
    EXPECT_CALL(mockSpaceAllocService_, AllocateS3Chunk(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(S3RpcService<AllocateS3ChunkRequest,
                  AllocateS3ChunkResponse>)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Invoke(S3Upload));
    EXPECT_CALL(mockS3Client_, Download(_, _, _, _))
        .WillRepeatedly(Invoke(S3Download));
    s3ClientAdaptor_->Write(&inode, offset, len, buf);
    inode.set_length(offset+len);

    offset = 2 * 1024 * 1024;
    memset(buf, 'b', len);
    s3ClientAdaptor_->Write(&inode, offset, len, buf);

    ASSERT_EQ(2, gObjectDataMaps.size());
    std::map<std::string, S3Data>::iterator dataMapsIter
                        = gObjectDataMaps.begin();
    ASSERT_EQ('a', dataMapsIter->second.buf[0]);
    dataMapsIter++;
    ASSERT_EQ('b', dataMapsIter->second.buf[0]);

    offset = 0;
    len = 3 * 1024 * 1024;
    char *readBuf = new char[len + 1];
    memset(readBuf, 0, len + 1);
    char *expectBuf = new char[len + 1];
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

}  // namespace client
}  // namespace curvefs
