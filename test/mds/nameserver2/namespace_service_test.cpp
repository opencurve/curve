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
    // create /file1 , /file2
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

    cntl.Reset();
    request.set_filename("/file2");
    request.set_filetype(INODE_PAGEFILE);
    request.set_filelength(fileLength);

    cntl.set_log_id(2);  // set by user
    stub.CreateFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kFileExists);
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
    // 为file1分配空间
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

    // test RenameFile
    // file1 重命名为file3，第一次重命名成功，第二次file1不存在，重命名失败
    cntl.Reset();
    RenameFileRequest request4;
    RenameFileResponse response4;
    request4.set_oldfilename("/file1");
    request4.set_newfilename("/file3");
    stub.RenameFile(&cntl, &request4, &response4, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response4.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request4.set_oldfilename("/file1");
    request4.set_newfilename("/file3");
    stub.RenameFile(&cntl, &request4, &response4, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response4.statuscode(), StatusCode::kFileNotExists);
    } else {
        ASSERT_TRUE(false);
    }

    // test ExtendFile
    // 扩容file2,第一次扩大，成功；第二次缩小，失败
    uint64_t newsize = kMiniFileLength * 2;
    cntl.Reset();
    ExtendFileRequest request5;
    ExtendFileResponse response5;
    request5.set_filename("/file2");
    request5.set_newsize(newsize);
    stub.ExtendFile(&cntl, &request5, &response5, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response5.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request5.set_filename("/file2");
    request5.set_newsize(kMiniFileLength);
    stub.ExtendFile(&cntl, &request5, &response5, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response5.statuscode(), StatusCode::kShrinkBiggerFile);
    } else {
        ASSERT_TRUE(false);
    }

    // test DeleteSegment
    // 回收空间，第一次回收成功，第二次回收失败
    cntl.Reset();
    DeleteSegmentRequest request6;
    DeleteSegmentResponse response6;
    request6.set_filename("/file3");
    request6.set_offset(DefaultSegmentSize);

    stub.DeleteSegment(&cntl, &request6, &response6, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response6.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    DeleteSegmentRequest request7;
    DeleteSegmentResponse response7;
    request7.set_filename("/file3");
    request7.set_offset(DefaultSegmentSize);

    stub.DeleteSegment(&cntl, &request6, &response6, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response6.statuscode(), StatusCode::kSegmentNotAllocated);
    } else {
        ASSERT_TRUE(false);
    }

    server.Stop(10);
    server.Join();
    return;
}
}  // namespace mds
}  // namespace curve


