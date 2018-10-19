/*
 * Project: curve
 * Created Date: Wednesday September 26th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */
#include <unistd.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include "src/mds/nameserver2/namespace_service.h"
#include "src/mds/nameserver2/curvefs.h"
#include "src/mds/nameserver2/chunk_allocator.h"
#include "test/mds/nameserver2/fakes.h"


namespace curve {
namespace mds {

class NameSpaceServiceTest : public ::testing::Test {
 protected:
    void SetUp() override {
        // init the kcurvefs, use the fake element
        storage_ =  new FakeNameServerStorage();
        inodeGenerator_ = new FakeInodeIDGenerator(0);
        std::shared_ptr<FackTopologyAdmin> topologyAdmin =
                                std::make_shared<FackTopologyAdmin>();
        std::shared_ptr<FackChunkIDGenerator> chunkIdGenerator =
                            std::make_shared<FackChunkIDGenerator>();
        chunkSegmentAllocate_ =
                new ChunkSegmentAllocatorImpl(topologyAdmin, chunkIdGenerator);
        kCurveFS.Init(storage_, inodeGenerator_, chunkSegmentAllocate_);
    }

    void TearDown() override {
        if (storage_ != nullptr) delete storage_;
        if (inodeGenerator_ != nullptr) delete inodeGenerator_;
        if (chunkSegmentAllocate_ != nullptr) delete chunkSegmentAllocate_;
    }

 public:
    NameServerStorage *storage_;
    InodeIDGenerator *inodeGenerator_;
    ChunkSegmentAllocator *chunkSegmentAllocate_;
};

TEST_F(NameSpaceServiceTest, test1) {
    brpc::Server server;
    std::string listenAddr = "0.0.0.0:9000";

    // start server
    NameSpaceService namespaceService;
    ASSERT_EQ(server.AddService(&namespaceService,
            brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(server.Start(listenAddr.c_str(), &option), 0);

    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(listenAddr.c_str(), nullptr), 0);

    CurveFSService_Stub stub(&channel);


    // test CreateFile
    CreateFileRequest request;
    CreateFileResponse response;
    brpc::Controller cntl;
    uint64_t fileLength = kMiniFileLength;

    request.set_filename("/file1");
    request.set_filetype(INODE_PAGEFILE);
    request.set_filelength(fileLength);

    cntl.set_log_id(1);  // set by user
    stub.CreateFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request.set_filename("/file2");
    request.set_filetype(INODE_PAGEFILE);
    request.set_filelength(fileLength);

    cntl.set_log_id(2);  // set by user
    stub.CreateFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);
    } else {
        FAIL();
    }

    // test GetFileInfo
    cntl.Reset();
    GetFileInfoRequest request1;
    GetFileInfoResponse response1;
    request1.set_filename("/file1");
    stub.GetFileInfo(&cntl, &request1, &response1, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response1.statuscode(), StatusCode::kOK);
        ASSERT_EQ(response1.fileinfo().id(), 1);
        ASSERT_EQ(response1.fileinfo().filename(), "file1");
        ASSERT_EQ(response1.fileinfo().parentid(), 0);
        ASSERT_EQ(response1.fileinfo().filetype(), INODE_PAGEFILE);
        ASSERT_EQ(response1.fileinfo().chunksize(), DefaultChunkSize);
        ASSERT_EQ(response1.fileinfo().segmentsize(), DefaultSegmentSize);
        ASSERT_EQ(response1.fileinfo().length(), fileLength);
    } else {
        ASSERT_TRUE(false);
    }

    // test GetOrAllocateSegment
    cntl.Reset();
    GetOrAllocateSegmentRequest request2;
    GetOrAllocateSegmentResponse response2;
    request2.set_filename("/file1");
    request2.set_offset(DefaultSegmentSize);
    request2.set_allocateifnotexist(false);
    stub.GetOrAllocateSegment(&cntl, &request2, &response2, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response2.statuscode(), StatusCode::kSegmentNotAllocated);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request2.set_filename("/file1");
    request2.set_offset(DefaultSegmentSize);
    request2.set_allocateifnotexist(true);
    stub.GetOrAllocateSegment(&cntl, &request2, &response2, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response2.statuscode(), StatusCode::kOK);
        ASSERT_EQ(response2.pagefilesegment().segmentsize(),
            response1.fileinfo().segmentsize());
        ASSERT_EQ(response2.pagefilesegment().chunksize(),
            response1.fileinfo().chunksize());
        ASSERT_EQ(response2.pagefilesegment().startoffset(), request2.offset());

        int chunkNumber = response2.pagefilesegment().segmentsize()/
                            response2.pagefilesegment().chunksize();

        ASSERT_EQ(response2.pagefilesegment().chunks().size(), chunkNumber);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    GetOrAllocateSegmentRequest request3;
    GetOrAllocateSegmentResponse response3;
    request3.set_filename("/file1");
    request3.set_offset(DefaultSegmentSize);
    request3.set_allocateifnotexist(false);
    stub.GetOrAllocateSegment(&cntl, &request3, &response3, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response3.statuscode(), StatusCode::kOK);
        ASSERT_EQ(response3.pagefilesegment().SerializeAsString(),
            response2.pagefilesegment().SerializeAsString());
    } else {
        ASSERT_TRUE(false);
    }
    server.Stop(10);
    server.Join();
    return;
}
}  // namespace mds
}  // namespace curve


