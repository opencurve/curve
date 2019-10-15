/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#include <brpc/server.h>
#include <butil/logging.h>
#include <cmock.h>
#include <gflags/gflags.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <rados/librados.h>
#include <rbd/librbd.h>
#include <string>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/typeof/typeof.hpp>
#include "src/common/client.pb.h"
#include "src/part2/rados_interface.h"
#include "src/part2/rpc_server.h"
#include "tests/part2/test_rpcceph.h"

class RpcServiceTestClosure : public ::google::protobuf::Closure {
 public:
    explicit RpcServiceTestClosure(int sleepUs = 0) : sleep_(sleepUs) {}
    virtual ~RpcServiceTestClosure() = default;

    void Run() override {
        if (0 != sleep_) {
            ::usleep(sleep_);
            LOG(INFO) << "return rpc";
        }
    }

 private:
    int sleep_;
};

TEST(RpcRequestCephTest, RbdFinishAioWrite_1) {
    nebd::client::WriteResponse response;
    brpc::Controller cntl;
    nebd::client::WriteRequest request;
    request.set_fd(4);
    request.set_offset(4);
    request.set_size(4);
    RpcServiceTestClosure done;
    AsyncWrite* writejob = NULL;
    writejob = new AsyncWrite;
    writejob->done = &done;
    writejob->response = &response;
    writejob->request = &request;
    writejob->cntl = &cntl;

    rbd_completion_t c;
    rbd_aio_create_completion(NULL, NULL, &c);
    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_get_return_value(::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rbd_aio_release(::testing::_))
        .WillOnce(testing::Return());
    response.set_retcode(nebd::client::kNoOK);
    RbdFinishAioWrite(c, writejob);
    EXPECT_EQ(nebd::client::kOK, response.retcode());
}

TEST(RpcRequestCephTest, RbdFinishAioWrite_2) {
    nebd::client::WriteResponse response;
    brpc::Controller cntl;
    nebd::client::WriteRequest request;
    request.set_fd(4);
    request.set_offset(4);
    request.set_size(4);
    RpcServiceTestClosure done;
    AsyncWrite* writejob = NULL;
    writejob = new AsyncWrite;
    writejob->done = &done;
    writejob->response = &response;
    writejob->request = &request;
    writejob->cntl = &cntl;

    rbd_completion_t c;
    rbd_aio_create_completion(NULL, NULL, &c);
    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_get_return_value(::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_CALL(mock, rbd_aio_release(::testing::_))
        .WillOnce(testing::Return());
    response.set_retcode(nebd::client::kNoOK);
    RbdFinishAioWrite(c, writejob);
    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(RpcRequestCephTest, RbdFinishAioRead_1) {
    nebd::client::ReadResponse response;
    brpc::Controller cntl;
    nebd::client::ReadRequest request;
    request.set_fd(4);
    request.set_offset(4);
    request.set_size(4);
    RpcServiceTestClosure done;
    AsyncRead* readjob = NULL;
    readjob = new AsyncRead;
    readjob->done = &done;
    readjob->response = &response;
    readjob->request = &request;
    readjob->cntl = &cntl;
    readjob->buf = new char[5]();
    rbd_completion_t c;
    rbd_aio_create_completion(NULL, NULL, &c);
    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_get_return_value(::testing::_))
        .WillOnce(testing::Return(1));
    EXPECT_CALL(mock, rbd_aio_release(::testing::_))
        .WillOnce(testing::Return());
    response.set_retcode(nebd::client::kNoOK);
    RbdFinishAioRead(c, readjob);
    EXPECT_EQ(nebd::client::kOK, response.retcode());
}

TEST(RpcRequestCephTest, RbdFinishAioRead_2) {
    nebd::client::ReadResponse response;
    brpc::Controller cntl;
    nebd::client::ReadRequest request;
    request.set_fd(4);
    request.set_offset(4);
    request.set_size(4);
    RpcServiceTestClosure done;
    AsyncRead* readjob = NULL;
    readjob = new AsyncRead;
    readjob->done = &done;
    readjob->response = &response;
    readjob->request = &request;
    readjob->cntl = &cntl;
    readjob->buf = new char[5]();
    rbd_completion_t c;
    rbd_aio_create_completion(NULL, NULL, &c);
    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_get_return_value(::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rbd_aio_release(::testing::_))
        .WillOnce(testing::Return());
    response.set_retcode(nebd::client::kNoOK);
    RbdFinishAioRead(c, readjob);
    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(RpcRequestCephTest, RbdFinishAioDiscard_1) {
    nebd::client::DiscardResponse response;
    brpc::Controller cntl;
    nebd::client::DiscardRequest request;
    request.set_fd(4);
    request.set_offset(1);
    request.set_size(1);
    RpcServiceTestClosure done;
    AsyncDiscard* discardjob = NULL;
    discardjob = new AsyncDiscard;
    discardjob->done = &done;
    discardjob->response = &response;
    discardjob->request = &request;

    rbd_completion_t c;
    rbd_aio_create_completion(NULL, NULL, &c);
    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_get_return_value(::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rbd_aio_release(::testing::_))
        .WillOnce(testing::Return());
    response.set_retcode(nebd::client::kNoOK);
    RbdFinishAioDiscard(c, discardjob);
    EXPECT_EQ(nebd::client::kOK, response.retcode());
}

TEST(RpcRequestCephTest, RbdFinishAioDiscard_2) {
    nebd::client::DiscardResponse response;
    brpc::Controller cntl;
    nebd::client::DiscardRequest request;
    request.set_fd(4);
    request.set_offset(1);
    request.set_size(1);
    RpcServiceTestClosure done;
    AsyncDiscard* discardjob = NULL;
    discardjob = new AsyncDiscard;
    discardjob->done = &done;
    discardjob->response = &response;
    discardjob->request = &request;

    rbd_completion_t c;
    rbd_aio_create_completion(NULL, NULL, &c);
    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_get_return_value(::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_CALL(mock, rbd_aio_release(::testing::_))
        .WillOnce(testing::Return());
    response.set_retcode(nebd::client::kNoOK);
    RbdFinishAioDiscard(c, discardjob);
    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(RpcRequestCephTest, RbdFinishAioFlush_1) {
    nebd::client::FlushResponse response;
    nebd::client::FlushRequest request;
    request.set_fd(3);
    RpcServiceTestClosure done;
    AsyncFlush* flushjob = NULL;
    flushjob = new AsyncFlush;
    flushjob->done = &done;
    flushjob->response = &response;
    flushjob->request = &request;
    rbd_completion_t c;
    rbd_aio_create_completion(NULL, NULL, &c);
    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_get_return_value(::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rbd_aio_release(::testing::_))
        .WillOnce(testing::Return());
    response.set_retcode(nebd::client::kNoOK);
    RbdFinishAioFlush(c, flushjob);
    EXPECT_EQ(nebd::client::kOK, response.retcode());
}

TEST(RpcRequestCephTest, RbdFinishAioFlush_2) {
    nebd::client::FlushResponse response;
    nebd::client::FlushRequest request;
    request.set_fd(3);
    RpcServiceTestClosure done;
    AsyncFlush* flushjob = NULL;
    flushjob = new AsyncFlush;
    flushjob->done = &done;
    flushjob->response = &response;
    flushjob->request = &request;
    rbd_completion_t c;
    rbd_aio_create_completion(NULL, NULL, &c);
    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_get_return_value(::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_CALL(mock, rbd_aio_release(::testing::_))
        .WillOnce(testing::Return());
    response.set_retcode(nebd::client::kNoOK);
    RbdFinishAioFlush(c, flushjob);
    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(RpcRequestCephTest, InvalidateCache_1) {
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    RpcRequestCeph cephImpl;

    EXPECT_EQ(-1, cephImpl.InvalidateCache(3));
}

TEST(RpcRequestCephTest, InvalidateCache_2) {
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_invalidate_cache(::testing::_))
        .WillOnce(testing::Return(-1));

    RpcRequestCeph cephImpl;
    EXPECT_EQ(-1, cephImpl.InvalidateCache(4));
}

TEST(RpcRequestCephTest, InvalidateCache_3) {
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_invalidate_cache(::testing::_))
        .WillOnce(testing::Return(0));

    RpcRequestCeph cephImpl;
    EXPECT_EQ(0, cephImpl.InvalidateCache(4));
}

TEST(RpcRequestCephTest, Resize_1) {
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    RpcRequestCeph cephImpl;

    EXPECT_EQ(-1, cephImpl.Resize(3, 1024));
}

TEST(RpcRequestCephTest, Resize_2) {
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_resize(::testing::_, ::testing::_))
        .WillOnce(testing::Return(-1));

    RpcRequestCeph cephImpl;
    EXPECT_EQ(-1, cephImpl.Resize(4, 1024));
}

TEST(RpcRequestCephTest, Resize_3) {
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_resize(::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));

    RpcRequestCeph cephImpl;
    EXPECT_EQ(0, cephImpl.Resize(4, 1024));
}

TEST(RpcRequestCephTest, GetInfo_1) {
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    RpcRequestCeph cephImpl;
    nebd::client::GetInfoResponse response;
    EXPECT_EQ(-1, cephImpl.GetInfo(3, &response));
}

TEST(RpcRequestCephTest, GetInfo_2) {
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_stat(::testing::_, ::testing::_, ::testing::_))
        .WillOnce(testing::Return(-1));

    RpcRequestCeph cephImpl;
    nebd::client::GetInfoResponse response;
    EXPECT_EQ(-1, cephImpl.GetInfo(4, &response));
}

TEST(RpcRequestCephTest, GetInfo_3) {
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_stat(::testing::_, ::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));

    RpcRequestCeph cephImpl;
    nebd::client::GetInfoResponse response;
    EXPECT_EQ(0, cephImpl.GetInfo(4, &response));
}

TEST(RpcRequestCephTest, StatFile_1) {
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    RpcRequestCeph cephImpl;

    nebd::client::StatFileResponse response;
    EXPECT_EQ(-1, cephImpl.StatFile(3, &response));
}

TEST(RpcRequestCephTest, StatFile_2) {
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_stat(::testing::_, ::testing::_, ::testing::_))
        .WillOnce(testing::Return(-1));
    RpcRequestCeph cephImpl;
    nebd::client::StatFileResponse response;
    EXPECT_EQ(-1, cephImpl.StatFile(4, &response));
}

TEST(RpcRequestCephTest, StatFile_3) {
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_stat(::testing::_, ::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));
    RpcRequestCeph cephImpl;
    nebd::client::StatFileResponse response;
    EXPECT_EQ(0, cephImpl.StatFile(4, &response));
}

TEST(RpcRequestCephTest, CloseFile_1) {
    NebdServerMocker mock;
    std::string lockfile = "/tmp/unit_test";
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lockfile));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(-1));

    RpcRequestCeph cephImpl;
    nebd::client::StatFileResponse response;
    EXPECT_EQ(-1, cephImpl.CloseFile(4));
}

TEST(RpcRequestCephTest, CloseFile_2) {
    NebdServerMocker mock;
    std::string lockfile = "/tmp/unit_test";
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lockfile));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, CloseImage(::testing::_)).WillOnce(testing::Return(-1));

    RpcRequestCeph cephImpl;
    nebd::client::StatFileResponse response;
    EXPECT_EQ(-1, cephImpl.CloseFile(4));
}

TEST(RpcRequestCephTest, CloseFile_3) {
    NebdServerMocker mock;
    std::string lockfile = "/tmp/unit_test";
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lockfile));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, CloseImage(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, RmFd(::testing::_)).WillOnce(testing::Return(-1));

    RpcRequestCeph cephImpl;
    nebd::client::StatFileResponse response;
    EXPECT_EQ(-1, cephImpl.CloseFile(4));
}

TEST(RpcRequestCephTest, CloseFile_4) {
    NebdServerMocker mock;
    std::string lockfile = "/tmp/unit_test";
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lockfile));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, CloseImage(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, RmFd(::testing::_)).WillOnce(testing::Return(0));

    RpcRequestCeph cephImpl;
    nebd::client::StatFileResponse response;
    EXPECT_EQ(0, cephImpl.CloseFile(4));
}

TEST(RpcRequestCephTest, Write_1) {
    nebd::client::WriteResponse response;
    brpc::Controller cntl;
    nebd::client::WriteRequest request;
    request.set_fd(3);
    request.set_offset(1);
    request.set_size(1);
    RpcServiceTestClosure done;
    AsyncWrite* writejob = NULL;
    writejob = new AsyncWrite;
    writejob->done = &done;
    writejob->response = &response;
    writejob->request = &request;
    writejob->cntl = &cntl;

    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    FdImage_t fd_image;
    fd_image.filename = filename;
    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, &fd_image));

    RpcRequestCeph cephImpl;

    NebdServerMocker mock;
    // EXPECT_CALL(mock, rbd_aio_create_completion(::testing::_, ::testing::_,
    // ::testing::_)).WillOnce(testing::Return(-1));
    // EXPECT_CALL(mock, rbd_aio_write(::testing::_, 1, 1, ::testing::_,
    // ::testing::_)).WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, cephImpl.Write(writejob));
}

TEST(RpcRequestCephTest, Write_2) {
    nebd::client::WriteResponse response;
    brpc::Controller cntl;
    nebd::client::WriteRequest request;
    request.set_fd(4);
    request.set_offset(4);
    request.set_size(4);
    RpcServiceTestClosure done;
    AsyncWrite* writejob = NULL;
    writejob = new AsyncWrite;
    writejob->done = &done;
    writejob->response = &response;
    writejob->request = &request;
    writejob->cntl = &cntl;
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;
    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    RpcRequestCeph cephImpl;
    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_create_completion(::testing::_, ::testing::_,
                                                ::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, cephImpl.Write(writejob));
}

TEST(RpcRequestCephTest, Write_3) {
    nebd::client::WriteResponse response;
    brpc::Controller cntl;
    nebd::client::WriteRequest request;
    request.set_fd(4);
    request.set_offset(4);
    request.set_size(4);
    RpcServiceTestClosure done;
    AsyncWrite* writejob = NULL;
    writejob = new AsyncWrite;
    writejob->done = &done;
    writejob->response = &response;
    writejob->request = &request;
    writejob->cntl = &cntl;
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;
    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    RpcRequestCeph cephImpl;
    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_create_completion(::testing::_, ::testing::_,
                                                ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rbd_aio_write(::testing::_, ::testing::_, ::testing::_,
                                    ::testing::_, ::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, cephImpl.Write(writejob));
}

TEST(RpcRequestCephTest, Write_4) {
    nebd::client::WriteResponse response;
    brpc::Controller cntl;
    nebd::client::WriteRequest request;
    request.set_fd(4);
    request.set_offset(4);
    request.set_size(4);
    RpcServiceTestClosure done;
    AsyncWrite* writejob = NULL;
    writejob = new AsyncWrite;
    writejob->done = &done;
    writejob->response = &response;
    writejob->request = &request;
    writejob->cntl = &cntl;
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;
    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    RpcRequestCeph cephImpl;
    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_create_completion(::testing::_, ::testing::_,
                                                ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rbd_aio_write(::testing::_, ::testing::_, ::testing::_,
                                    ::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_EQ(0, cephImpl.Write(writejob));
}

TEST(RpcRequestCephTest, Read_1) {
    nebd::client::ReadResponse response;
    brpc::Controller cntl;
    nebd::client::ReadRequest request;
    request.set_fd(3);
    request.set_offset(1);
    request.set_size(1);
    RpcServiceTestClosure done;
    AsyncRead* readjob = NULL;
    readjob = new AsyncRead;
    readjob->done = &done;
    readjob->response = &response;
    readjob->request = &request;
    readjob->cntl = &cntl;

    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    FdImage_t fd_image;
    fd_image.filename = filename;
    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, &fd_image));

    RpcRequestCeph cephImpl;

    NebdServerMocker mock;
    // EXPECT_CALL(mock, rbd_aio_create_completion(::testing::_, ::testing::_,
    // ::testing::_)).WillOnce(testing::Return(-1));
    // EXPECT_CALL(mock, rbd_aio_write(::testing::_, 1, 1, ::testing::_,
    // ::testing::_)).WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, cephImpl.Read(readjob));
}

TEST(RpcRequestCephTest, Read_2) {
    nebd::client::ReadResponse response;
    brpc::Controller cntl;
    nebd::client::ReadRequest request;
    request.set_fd(4);
    request.set_offset(4);
    request.set_size(1);
    RpcServiceTestClosure done;
    AsyncRead* readjob = NULL;
    readjob = new AsyncRead;
    readjob->done = &done;
    readjob->response = &response;
    readjob->request = &request;
    readjob->cntl = &cntl;
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;
    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    RpcRequestCeph cephImpl;
    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_create_completion(::testing::_, ::testing::_,
                                                ::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, cephImpl.Read(readjob));
}

TEST(RpcRequestCephTest, Read_3) {
    nebd::client::ReadResponse response;
    brpc::Controller cntl;
    nebd::client::ReadRequest request;
    request.set_fd(4);
    request.set_offset(4);
    request.set_size(1);
    RpcServiceTestClosure done;
    AsyncRead* readjob = NULL;
    readjob = new AsyncRead;
    readjob->done = &done;
    readjob->response = &response;
    readjob->request = &request;
    readjob->cntl = &cntl;
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;
    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    RpcRequestCeph cephImpl;
    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_create_completion(::testing::_, ::testing::_,
                                                ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rbd_aio_read(::testing::_, ::testing::_, ::testing::_,
                                   ::testing::_, ::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, cephImpl.Read(readjob));
}

TEST(RpcRequestCephTest, Read_4) {
    nebd::client::ReadResponse response;
    brpc::Controller cntl;
    nebd::client::ReadRequest request;
    request.set_fd(4);
    request.set_offset(4);
    request.set_size(1);
    RpcServiceTestClosure done;
    AsyncRead* readjob = NULL;
    readjob = new AsyncRead;
    readjob->done = &done;
    readjob->response = &response;
    readjob->request = &request;
    readjob->cntl = &cntl;
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;
    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    RpcRequestCeph cephImpl;
    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_create_completion(::testing::_, ::testing::_,
                                                ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rbd_aio_read(::testing::_, ::testing::_, ::testing::_,
                                   ::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_EQ(0, cephImpl.Read(readjob));
}

TEST(RpcRequestCephTest, Discard_1) {
    nebd::client::DiscardResponse response;
    brpc::Controller cntl;
    nebd::client::DiscardRequest request;
    request.set_fd(3);
    request.set_offset(1);
    request.set_size(1);
    RpcServiceTestClosure done;
    AsyncDiscard* discardjob = NULL;
    discardjob = new AsyncDiscard;
    discardjob->done = &done;
    discardjob->response = &response;
    discardjob->request = &request;

    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    FdImage_t fd_image;
    fd_image.filename = filename;
    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, &fd_image));

    RpcRequestCeph cephImpl;

    NebdServerMocker mock;
    EXPECT_EQ(-1, cephImpl.Discard(discardjob));
}

TEST(RpcRequestCephTest, Discard_2) {
    nebd::client::DiscardResponse response;
    brpc::Controller cntl;
    nebd::client::DiscardRequest request;
    request.set_fd(4);
    request.set_offset(1);
    request.set_size(1);
    RpcServiceTestClosure done;
    AsyncDiscard* discardjob = NULL;
    discardjob = new AsyncDiscard;
    discardjob->done = &done;
    discardjob->response = &response;
    discardjob->request = &request;

    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    FdImage_t fd_image;
    fd_image.filename = filename;
    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, &fd_image));

    RpcRequestCeph cephImpl;

    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_create_completion(::testing::_, ::testing::_,
                                                ::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, cephImpl.Discard(discardjob));
}

TEST(RpcRequestCephTest, Discard_3) {
    nebd::client::DiscardResponse response;
    brpc::Controller cntl;
    nebd::client::DiscardRequest request;
    request.set_fd(4);
    request.set_offset(1);
    request.set_size(1);
    RpcServiceTestClosure done;
    AsyncDiscard* discardjob = NULL;
    discardjob = new AsyncDiscard;
    discardjob->done = &done;
    discardjob->response = &response;
    discardjob->request = &request;

    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    FdImage_t fd_image;
    fd_image.filename = filename;
    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, &fd_image));

    RpcRequestCeph cephImpl;

    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_create_completion(::testing::_, ::testing::_,
                                                ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rbd_aio_discard(::testing::_, ::testing::_, ::testing::_,
                                      ::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, cephImpl.Discard(discardjob));
}

TEST(RpcRequestCephTest, Discard_4) {
    nebd::client::DiscardResponse response;
    brpc::Controller cntl;
    nebd::client::DiscardRequest request;
    request.set_fd(4);
    request.set_offset(1);
    request.set_size(1);
    RpcServiceTestClosure done;
    AsyncDiscard* discardjob = NULL;
    discardjob = new AsyncDiscard;
    discardjob->done = &done;
    discardjob->response = &response;
    discardjob->request = &request;

    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    FdImage_t fd_image;
    fd_image.filename = filename;
    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, &fd_image));

    RpcRequestCeph cephImpl;

    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_create_completion(::testing::_, ::testing::_,
                                                ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rbd_aio_discard(::testing::_, ::testing::_, ::testing::_,
                                      ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_EQ(0, cephImpl.Discard(discardjob));
}

TEST(RpcRequestCephTest, Flush_1) {
    nebd::client::FlushResponse response;
    nebd::client::FlushRequest request;
    request.set_fd(3);
    RpcServiceTestClosure done;
    AsyncFlush* flushjob = NULL;
    flushjob = new AsyncFlush;
    flushjob->done = &done;
    flushjob->response = &response;
    flushjob->request = &request;

    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    FdImage_t fd_image;
    fd_image.filename = filename;
    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, &fd_image));

    RpcRequestCeph cephImpl;

    NebdServerMocker mock;
    EXPECT_EQ(-1, cephImpl.Flush(flushjob));
}

TEST(RpcRequestCephTest, Flush_2) {
    nebd::client::FlushResponse response;
    nebd::client::FlushRequest request;
    request.set_fd(4);
    RpcServiceTestClosure done;
    AsyncFlush* flushjob = NULL;
    flushjob = new AsyncFlush;
    flushjob->done = &done;
    flushjob->response = &response;
    flushjob->request = &request;

    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    FdImage_t fd_image;
    fd_image.filename = filename;
    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, &fd_image));

    RpcRequestCeph cephImpl;

    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_create_completion(::testing::_, ::testing::_,
                                                ::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, cephImpl.Flush(flushjob));
}

TEST(RpcRequestCephTest, Flush_3) {
    nebd::client::FlushResponse response;
    nebd::client::FlushRequest request;
    request.set_fd(4);
    RpcServiceTestClosure done;
    AsyncFlush* flushjob = NULL;
    flushjob = new AsyncFlush;
    flushjob->done = &done;
    flushjob->response = &response;
    flushjob->request = &request;

    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    FdImage_t fd_image;
    fd_image.filename = filename;
    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, &fd_image));

    RpcRequestCeph cephImpl;

    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_create_completion(::testing::_, ::testing::_,
                                                ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rbd_aio_flush(::testing::_, ::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, cephImpl.Flush(flushjob));
}

TEST(RpcRequestCephTest, Flush_4) {
    nebd::client::FlushResponse response;
    nebd::client::FlushRequest request;
    request.set_fd(4);
    RpcServiceTestClosure done;
    AsyncFlush* flushjob = NULL;
    flushjob = new AsyncFlush;
    flushjob->done = &done;
    flushjob->response = &response;
    flushjob->request = &request;

    int fd = 4;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    FdImage_t fd_image;
    fd_image.filename = filename;
    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, &fd_image));

    RpcRequestCeph cephImpl;

    NebdServerMocker mock;
    EXPECT_CALL(mock, rbd_aio_create_completion(::testing::_, ::testing::_,
                                                ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rbd_aio_flush(::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_EQ(0, cephImpl.Flush(flushjob));
}

TEST(OpenFile, OpenFile_1) {
    const char* tmp = "rbdrbdvolume03auth_supportednonemon_host";
    char* filename = new char[strlen(tmp) + 1]();
    snprintf(filename, strlen(tmp) + 1, "%s", tmp);
    RpcRequestCeph cephImpl;
    EXPECT_EQ(-1, cephImpl.OpenFile(filename));
}

TEST(OpenFile, OpenFile_2) {
    const char* tmp = "rbdrbdvolume03auth_supported=nonemon_host=10.182.30.27";
    char* filename = new char[strlen(tmp) + 1]();
    snprintf(filename, strlen(tmp) + 1, "%s", tmp);
    RpcRequestCeph cephImpl;
    EXPECT_EQ(-1, cephImpl.OpenFile(filename));
}

TEST(OpenFile, OpenFile_3) {
    const char* tmp =
        "rbd:rbdvolume03:auth_supported=none:mon_host=10.182.30.27:6789";
    char* filename = new char[strlen(tmp) + 1]();
    snprintf(filename, strlen(tmp) + 1, "%s", tmp);
    RpcRequestCeph cephImpl;
    EXPECT_EQ(-1, cephImpl.OpenFile(filename));
}

TEST(OpenFile, OpenFile_4) {
    const char* tmp = "rbd:rbd/volume03:auth_supported=none:mon_host";
    char* filename = new char[strlen(tmp) + 1]();
    snprintf(filename, strlen(tmp) + 1, "%s", tmp);
    RpcRequestCeph cephImpl;
    EXPECT_EQ(-1, cephImpl.OpenFile(filename));
}

TEST(OpenFile, OpenFile_5) {
    const char* tmp =
        "rbd:rbd/volume03:auth_supported=none:mon_host=10.182.30.27\\:6789";
    char* filename = new char[strlen(tmp) + 1]();
    snprintf(filename, strlen(tmp) + 1, "%s", tmp);
    RpcRequestCeph cephImpl;
    NebdServerMocker mock;
    EXPECT_CALL(mock, FilenameFdExist(::testing::_))
        .WillOnce(testing::Return(3));
    EXPECT_EQ(3, cephImpl.OpenFile(filename));
}

TEST(OpenFile, OpenFile_6) {
    const char* filename =
        "rbd:rbd/volume03:auth_supported=none:mon_host=10.182.30.27\\:6789";
    RpcRequestCeph cephImpl;
    NebdServerMocker mock;
    EXPECT_CALL(mock, FilenameFdExist(::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_CALL(mock, ConnectRados(::testing::_))
        .WillOnce(testing::ReturnNull());
    EXPECT_EQ(-1, cephImpl.OpenFile(const_cast<char*>(filename)));
}

TEST(OpenFile, OpenFile_7) {
    const char* filename =
        "rbd:rbd/volume03:auth_supported=none:mon_host=10.182.30.27\\:6789";
    RpcRequestCeph cephImpl;
    rados_t cluster;
    rados_create(&cluster, "admin");
    NebdServerMocker mock;
    EXPECT_CALL(mock, FilenameFdExist(::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_CALL(mock, ConnectRados(::testing::_))
        .WillOnce(testing::Return(&cluster));
    EXPECT_CALL(mock, CloseRados(::testing::_)).WillOnce(testing::Return());
    EXPECT_CALL(mock, OpenImage(::testing::_, ::testing::_, ::testing::_,
                                ::testing::_, ::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, cephImpl.OpenFile(const_cast<char*>(filename)));
}

TEST(OpenFile, OpenFile_8) {
    const char* filename =
        "rbd:rbd/volume03:auth_supported=none:mon_host=10.182.30.27\\:6789";
    RpcRequestCeph cephImpl;
    rados_t cluster;
    rados_create(&cluster, "admin");
    NebdServerMocker mock;
    EXPECT_CALL(mock, FilenameFdExist(::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_CALL(mock, ConnectRados(::testing::_))
        .WillOnce(testing::Return(&cluster));
    EXPECT_CALL(mock, OpenImage(::testing::_, ::testing::_, ::testing::_,
                                ::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));
    std::string lockfile = "/tmp/unittest";
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lockfile));
    EXPECT_CALL(mock, CloseImage(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, cephImpl.OpenFile(const_cast<char*>(filename)));
}

TEST(OpenFile, OpenFile_9) {
    const char* filename =
        "rbd:rbd/volume03:auth_supported=none:mon_host=10.182.30.27\\:6789";
    RpcRequestCeph cephImpl;
    rados_t cluster;
    rados_create(&cluster, "admin");
    NebdServerMocker mock;
    EXPECT_CALL(mock, FilenameFdExist(::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_CALL(mock, ConnectRados(::testing::_))
        .WillOnce(testing::Return(&cluster));
    EXPECT_CALL(mock, OpenImage(::testing::_, ::testing::_, ::testing::_,
                                ::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));
    std::string lockfile = "/tmp/unittest";
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lockfile));
    EXPECT_CALL(mock, CloseImage(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, GenerateFd(::testing::_, ::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, cephImpl.OpenFile(const_cast<char*>(filename)));
}

TEST(OpenFile, OpenFile_10) {
    const char* filename =
        "rbd:rbd/volume03:auth_supported=none:mon_host=10.182.30.27\\:6789";
    RpcRequestCeph cephImpl;
    rados_t cluster;
    rados_create(&cluster, "admin");
    NebdServerMocker mock;
    EXPECT_CALL(mock, FilenameFdExist(::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_CALL(mock, ConnectRados(::testing::_))
        .WillOnce(testing::Return(&cluster));
    EXPECT_CALL(mock, OpenImage(::testing::_, ::testing::_, ::testing::_,
                                ::testing::_, ::testing::_))
        .WillOnce(testing::Return(5));
    std::string lockfile = "/tmp/unittest";
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lockfile));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, GenerateFd(::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_EQ(5, cephImpl.OpenFile(const_cast<char*>(filename)));
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
