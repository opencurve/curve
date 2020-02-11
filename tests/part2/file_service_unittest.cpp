/*
 * Project: nebd
 * Created Date: Tuesday February 4th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */


#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <brpc/controller.h>
#include <memory>

#include "src/part2/file_service.h"
#include "tests/part2/mock_file_manager.h"

namespace nebd {
namespace server {

const char testFile1[] = "test:/cinder/111";

using ::testing::_;
using ::testing::Return;
using ::testing::NotNull;
using ::testing::DoAll;
using ::testing::ReturnArg;
using ::testing::ElementsAre;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;

using google::protobuf::RpcController;
using google::protobuf::Closure;

using nebd::client::RetCode;

class FileServiceTestClosure : public Closure {
 public:
    FileServiceTestClosure() : runned_(false) {}
    ~FileServiceTestClosure() {}
    void Run() {
        runned_ = true;
    }
    bool IsRunned() {
        return runned_;
    }
    void Reset() {
        runned_ = false;
    }

 private:
    bool runned_;
};

class FileServiceTest : public ::testing::Test {
 public:
    void SetUp() {
        fileManager_ = std::make_shared<MockFileManager>();
        fileService_ = std::make_shared<NebdFileServiceImpl>(fileManager_);
    }
    void TearDown() {}
 protected:
    std::shared_ptr<MockFileManager> fileManager_;
    std::shared_ptr<NebdFileServiceImpl> fileService_;
};

TEST_F(FileServiceTest, OpenTest) {
    brpc::Controller cntl;
    nebd::client::OpenFileRequest request;
    request.set_filename(testFile1);
    nebd::client::OpenFileResponse response;
    FileServiceTestClosure done;

    // open success
    EXPECT_CALL(*fileManager_, Open(testFile1))
    .WillOnce(Return(1));
    fileService_->OpenFile(&cntl, &request, &response, &done);
    ASSERT_EQ(response.retcode(), RetCode::kOK);
    ASSERT_EQ(response.fd(), 1);
    ASSERT_TRUE(done.IsRunned());

    // open failed
    done.Reset();
    EXPECT_CALL(*fileManager_, Open(testFile1))
    .WillOnce(Return(-1));
    fileService_->OpenFile(&cntl, &request, &response, &done);
    ASSERT_EQ(response.retcode(), RetCode::kNoOK);
    ASSERT_TRUE(done.IsRunned());
}

TEST_F(FileServiceTest, WriteTest) {
    int fd = 1;
    uint64_t offset = 0;
    uint64_t size = 4096;
    brpc::Controller cntl;
    nebd::client::WriteRequest request;
    request.set_fd(fd);
    request.set_offset(offset);
    request.set_size(size);
    nebd::client::WriteResponse response;
    FileServiceTestClosure done;

    // write success
    EXPECT_CALL(*fileManager_, AioWrite(fd, NotNull()))
    .WillOnce(Return(0));
    fileService_->Write(&cntl, &request, &response, &done);
    ASSERT_FALSE(done.IsRunned());

    // write failed
    done.Reset();
    EXPECT_CALL(*fileManager_, AioWrite(fd, NotNull()))
    .WillOnce(Return(-1));
    fileService_->Write(&cntl, &request, &response, &done);
    ASSERT_EQ(response.retcode(), RetCode::kNoOK);
    ASSERT_TRUE(done.IsRunned());
}

TEST_F(FileServiceTest, ReadTest) {
    int fd = 1;
    uint64_t offset = 0;
    uint64_t size = 4096;
    brpc::Controller cntl;
    nebd::client::ReadRequest request;
    request.set_fd(fd);
    request.set_offset(offset);
    request.set_size(size);
    nebd::client::ReadResponse response;
    FileServiceTestClosure done;

    // read success
    EXPECT_CALL(*fileManager_, AioRead(fd, NotNull()))
    .WillOnce(Return(0));
    fileService_->Read(&cntl, &request, &response, &done);
    ASSERT_FALSE(done.IsRunned());

    // read failed
    done.Reset();
    EXPECT_CALL(*fileManager_, AioRead(fd, NotNull()))
    .WillOnce(Return(-1));
    fileService_->Read(&cntl, &request, &response, &done);
    ASSERT_EQ(response.retcode(), RetCode::kNoOK);
    ASSERT_TRUE(done.IsRunned());
}

TEST_F(FileServiceTest, FlushTest) {
    int fd = 1;
    brpc::Controller cntl;
    nebd::client::FlushRequest request;
    request.set_fd(fd);
    nebd::client::FlushResponse response;
    FileServiceTestClosure done;

    // flush success
    EXPECT_CALL(*fileManager_, Flush(fd, NotNull()))
    .WillOnce(Return(0));
    fileService_->Flush(&cntl, &request, &response, &done);
    ASSERT_FALSE(done.IsRunned());

    // flush failed
    done.Reset();
    EXPECT_CALL(*fileManager_, Flush(fd, NotNull()))
    .WillOnce(Return(-1));
    fileService_->Flush(&cntl, &request, &response, &done);
    ASSERT_EQ(response.retcode(), RetCode::kNoOK);
    ASSERT_TRUE(done.IsRunned());
}

TEST_F(FileServiceTest, DiscardTest) {
    int fd = 1;
    uint64_t offset = 0;
    uint64_t size = 4096;
    brpc::Controller cntl;
    nebd::client::DiscardRequest request;
    request.set_fd(fd);
    request.set_offset(offset);
    request.set_size(size);
    nebd::client::DiscardResponse response;
    FileServiceTestClosure done;

    // discard success
    EXPECT_CALL(*fileManager_, Discard(fd, NotNull()))
    .WillOnce(Return(0));
    fileService_->Discard(&cntl, &request, &response, &done);
    ASSERT_FALSE(done.IsRunned());

    // discard failed
    done.Reset();
    EXPECT_CALL(*fileManager_, Discard(fd, NotNull()))
    .WillOnce(Return(-1));
    fileService_->Discard(&cntl, &request, &response, &done);
    ASSERT_EQ(response.retcode(), RetCode::kNoOK);
    ASSERT_TRUE(done.IsRunned());
}

TEST_F(FileServiceTest, StatFileTest) {
    int fd = 1;
    brpc::Controller cntl;
    nebd::client::StatFileRequest request;
    request.set_fd(fd);
    nebd::client::StatFileResponse response;
    FileServiceTestClosure done;

    // stat file success
    NebdFileInfo fileInfo;
    fileInfo.size = 4096;
    EXPECT_CALL(*fileManager_, GetInfo(fd, NotNull()))
    .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                    Return(0)));
    fileService_->StatFile(&cntl, &request, &response, &done);
    ASSERT_EQ(response.retcode(), RetCode::kOK);
    ASSERT_EQ(response.size(), fileInfo.size);
    ASSERT_TRUE(done.IsRunned());

    // stat file failed
    done.Reset();
    EXPECT_CALL(*fileManager_, GetInfo(fd, NotNull()))
    .WillOnce(Return(-1));
    fileService_->StatFile(&cntl, &request, &response, &done);
    ASSERT_EQ(response.retcode(), RetCode::kNoOK);
    ASSERT_TRUE(done.IsRunned());
}

TEST_F(FileServiceTest, GetInfoTest) {
    int fd = 1;
    brpc::Controller cntl;
    nebd::client::GetInfoRequest request;
    request.set_fd(fd);
    nebd::client::GetInfoResponse response;
    FileServiceTestClosure done;

    // stat file success
    NebdFileInfo fileInfo;
    fileInfo.obj_size = 4096;
    EXPECT_CALL(*fileManager_, GetInfo(fd, NotNull()))
    .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                    Return(0)));
    fileService_->GetInfo(&cntl, &request, &response, &done);
    ASSERT_EQ(response.retcode(), RetCode::kOK);
    ASSERT_EQ(response.objsize(), fileInfo.obj_size);
    ASSERT_TRUE(done.IsRunned());

    // stat file failed
    done.Reset();
    EXPECT_CALL(*fileManager_, GetInfo(fd, NotNull()))
    .WillOnce(Return(-1));
    fileService_->GetInfo(&cntl, &request, &response, &done);
    ASSERT_EQ(response.retcode(), RetCode::kNoOK);
    ASSERT_TRUE(done.IsRunned());
}

TEST_F(FileServiceTest, CloseTest) {
    int fd = 1;
    brpc::Controller cntl;
    nebd::client::CloseFileRequest request;
    request.set_fd(fd);
    nebd::client::CloseFileResponse response;
    FileServiceTestClosure done;

    // close success
    EXPECT_CALL(*fileManager_, Close(fd))
    .WillOnce(Return(0));
    fileService_->CloseFile(&cntl, &request, &response, &done);
    ASSERT_EQ(response.retcode(), RetCode::kOK);
    ASSERT_TRUE(done.IsRunned());

    // close failed
    done.Reset();
    EXPECT_CALL(*fileManager_, Close(fd))
    .WillOnce(Return(-1));
    fileService_->CloseFile(&cntl, &request, &response, &done);
    ASSERT_EQ(response.retcode(), RetCode::kNoOK);
    ASSERT_TRUE(done.IsRunned());
}

TEST_F(FileServiceTest, ResizeTest) {
    int fd = 1;
    uint64_t size = 4096;
    brpc::Controller cntl;
    nebd::client::ResizeRequest request;
    request.set_fd(fd);
    request.set_newsize(size);
    nebd::client::ResizeResponse response;
    FileServiceTestClosure done;

    // resize success
    EXPECT_CALL(*fileManager_, Extend(fd, size))
    .WillOnce(Return(0));
    fileService_->ResizeFile(&cntl, &request, &response, &done);
    ASSERT_EQ(response.retcode(), RetCode::kOK);
    ASSERT_TRUE(done.IsRunned());

    // resize failed
    done.Reset();
    EXPECT_CALL(*fileManager_, Extend(fd, size))
    .WillOnce(Return(-1));
    fileService_->ResizeFile(&cntl, &request, &response, &done);
    ASSERT_EQ(response.retcode(), RetCode::kNoOK);
    ASSERT_TRUE(done.IsRunned());
}

TEST_F(FileServiceTest, InvalidCacheTest) {
    int fd = 1;
    brpc::Controller cntl;
    nebd::client::InvalidateCacheRequest request;
    request.set_fd(fd);
    nebd::client::InvalidateCacheResponse response;
    FileServiceTestClosure done;

    // invalid cache success
    EXPECT_CALL(*fileManager_, InvalidCache(fd))
    .WillOnce(Return(0));
    fileService_->InvalidateCache(&cntl, &request, &response, &done);
    ASSERT_EQ(response.retcode(), RetCode::kOK);
    ASSERT_TRUE(done.IsRunned());

    // invalid cache failed
    done.Reset();
    EXPECT_CALL(*fileManager_, InvalidCache(fd))
    .WillOnce(Return(-1));
    fileService_->InvalidateCache(&cntl, &request, &response, &done);
    ASSERT_EQ(response.retcode(), RetCode::kNoOK);
    ASSERT_TRUE(done.IsRunned());
}

TEST_F(FileServiceTest, CallbackTest) {
    // read success
    {
        brpc::Controller cntl;
        nebd::client::ReadResponse response;
        FileServiceTestClosure done;
        NebdServerAioContext* context = new NebdServerAioContext;
        context->op = LIBAIO_OP::LIBAIO_OP_READ;
        context->cntl = &cntl;
        context->response = &response;
        context->offset = 0;
        context->size = 4096;
        context->done = &done;
        context->buf = new char[4096];
        context->ret = 0;
        NebdFileServiceCallback(context);
        ASSERT_TRUE(done.IsRunned());
        ASSERT_EQ(response.retcode(), RetCode::kOK);
    }
    // read failed
    {
        brpc::Controller cntl;
        nebd::client::ReadResponse response;
        FileServiceTestClosure done;
        NebdServerAioContext* context = new NebdServerAioContext;
        context->op = LIBAIO_OP::LIBAIO_OP_READ;
        context->cntl = &cntl;
        context->response = &response;
        context->offset = 0;
        context->size = 4096;
        context->done = &done;
        context->buf = new char[4096];
        context->ret = -1;
        NebdFileServiceCallback(context);
        ASSERT_TRUE(done.IsRunned());
        ASSERT_EQ(response.retcode(), RetCode::kNoOK);
    }
    // write success
    {
        brpc::Controller cntl;
        nebd::client::WriteResponse response;
        FileServiceTestClosure done;
        NebdServerAioContext* context = new NebdServerAioContext;
        context->op = LIBAIO_OP::LIBAIO_OP_WRITE;
        context->cntl = &cntl;
        context->response = &response;
        context->offset = 0;
        context->size = 4096;
        context->done = &done;
        context->buf = new char[4096];
        context->ret = 0;
        NebdFileServiceCallback(context);
        ASSERT_TRUE(done.IsRunned());
        ASSERT_EQ(response.retcode(), RetCode::kOK);
    }
    // write failed
    {
        brpc::Controller cntl;
        nebd::client::WriteResponse response;
        FileServiceTestClosure done;
        NebdServerAioContext* context = new NebdServerAioContext;
        context->op = LIBAIO_OP::LIBAIO_OP_WRITE;
        context->cntl = &cntl;
        context->response = &response;
        context->offset = 0;
        context->size = 4096;
        context->done = &done;
        context->buf = new char[4096];
        context->ret = -1;
        NebdFileServiceCallback(context);
        ASSERT_TRUE(done.IsRunned());
        ASSERT_EQ(response.retcode(), RetCode::kNoOK);
    }
    // flush success
    {
        brpc::Controller cntl;
        nebd::client::FlushResponse response;
        FileServiceTestClosure done;
        NebdServerAioContext* context = new NebdServerAioContext;
        context->op = LIBAIO_OP::LIBAIO_OP_FLUSH;
        context->cntl = &cntl;
        context->response = &response;
        context->done = &done;
        context->ret = 0;
        NebdFileServiceCallback(context);
        ASSERT_TRUE(done.IsRunned());
        ASSERT_EQ(response.retcode(), RetCode::kOK);
    }
    // flush failed
    {
        brpc::Controller cntl;
        nebd::client::FlushResponse response;
        FileServiceTestClosure done;
        NebdServerAioContext* context = new NebdServerAioContext;
        context->op = LIBAIO_OP::LIBAIO_OP_FLUSH;
        context->cntl = &cntl;
        context->response = &response;
        context->done = &done;
        context->ret = -1;
        NebdFileServiceCallback(context);
        ASSERT_TRUE(done.IsRunned());
        ASSERT_EQ(response.retcode(), RetCode::kNoOK);
    }
    // discard success
    {
        brpc::Controller cntl;
        nebd::client::DiscardResponse response;
        FileServiceTestClosure done;
        NebdServerAioContext* context = new NebdServerAioContext;
        context->op = LIBAIO_OP::LIBAIO_OP_DISCARD;
        context->cntl = &cntl;
        context->response = &response;
        context->done = &done;
        context->ret = 0;
        NebdFileServiceCallback(context);
        ASSERT_TRUE(done.IsRunned());
        ASSERT_EQ(response.retcode(), RetCode::kOK);
    }
    // discard failed
    {
        brpc::Controller cntl;
        nebd::client::DiscardResponse response;
        FileServiceTestClosure done;
        NebdServerAioContext* context = new NebdServerAioContext;
        context->op = LIBAIO_OP::LIBAIO_OP_DISCARD;
        context->cntl = &cntl;
        context->response = &response;
        context->done = &done;
        context->ret = -1;
        NebdFileServiceCallback(context);
        ASSERT_TRUE(done.IsRunned());
        ASSERT_EQ(response.retcode(), RetCode::kNoOK);
    }
}

}  // namespace server
}  // namespace nebd

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}
