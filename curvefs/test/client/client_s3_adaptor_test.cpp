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

#if 0
#include <brpc/server.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/test/client/mock_metaserver_service.h"
#include "curvefs/test/client/mock_spacealloc_service.h"
#include "curvefs/test/client/mock_client_s3.h"

namespace curvefs {
namespace client{
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
    LOG(INFO) << "run s3 prc service, response.chunkid" << response->chunkid();
    done->Run();
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
        // write to s3 option
        /*
        curve::common::S3AdapterOption s3Option;
        s3Option.ak = "9bfe26f650dc4499a36b0452d9ed365a";
        s3Option.sk = "a6f4144691e9421288090356059adcd2";
        s3Option.s3Address = "nos-eastchina1.126.net";
        s3Option.bucketName = "curvefs";
        s3Option.loglevel = 4;
        s3Option.scheme = 0;
        s3Option.verifySsl = false;
        s3Option.maxConnections = 32;
        s3Option.connectTimeout = 60000;
        s3Option.requestTimeout = 10000;
        s3Option.asyncThreadNum = 64;
        S3Client *client = new S3ClientImpl();
        client->Init(s3Option);
        s3ClientAdaptor_.Init(option, client);
        */
        s3ClientAdaptor_.Init(option, &mockS3Client_);  
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }
 protected:
    S3ClientAdaptor s3ClientAdaptor_;
    MockMetaServerService mockMetaServerService_;
    MockSpaceAllocService mockSpaceAllocService_;
    MockS3Client mockS3Client_;
    std::string addr_ = "127.0.0.1:5603";
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
                  Invoke(S3RpcService<AllocateS3ChunkRequest, AllocateS3ChunkResponse>)));
    EXPECT_CALL(mockS3Client_, Upload(_, _, _))
        .WillRepeatedly(Return(1 * 1024 * 1024));

    int ret = s3ClientAdaptor_.Write(&inode, offset, len, buf);
    const S3ChunkInfoList& s3ChunkInfoList = inode.s3chunkinfolist();
    ASSERT_EQ(1, s3ChunkInfoList.s3chunks_size());
    ASSERT_EQ(25, s3ChunkInfoList.s3chunks(0).chunkid());
    ASSERT_EQ(len, ret);
    ASSERT_EQ(0, inode.version());
    delete buf;
    buf = NULL;
}
/*
TEST_F(ClientS3AdaptorTest, test_over_write) {
    curvefs::metaserver::Inode inode;
    uint64_t fileLen = 4096;
    InitInode(&inode);
    inode.set_length(fileLen);
    S3ChunkInfoList *s3ChunkInfoList = new S3ChunkInfoList();
    S3ChunkInfo *s3ChunkInfo = s3ChunkInfoList->add_s3chunks();
    s3ChunkInfo->set_chunkid(25);
    s3ChunkInfo->set_version(0);
    s3ChunkInfo->set_offset(0);
    s3ChunkInfo->set_len(fileLen);
    s3ChunkInfo->set_size(fileLen);
    inode.set_allocated_s3chunkinfolist(s3ChunkInfoList);

    uint64_t offset = 30;
    uint64_t len = 30;
    char *buf = new char[len];
    memset(buf, 'a', len);

    int ret = s3ClientAdaptor_.Write(&inode, offset, len, buf);
}
*/
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
    int ret = s3ClientAdaptor_.Read(&inode, offset, len, buf);
    ASSERT_EQ(len, ret);
    ASSERT_EQ('a', buf[0]);
    delete buf;
    buf = NULL;
}

}
}
#endif
