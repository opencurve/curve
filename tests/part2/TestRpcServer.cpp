/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#include <brpc/server.h>
#include <butil/logging.h>
#include <gflags/gflags.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <rados/librados.h>
#include <rbd/librbd.h>
#include <cmock.h>
#include <string>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/typeof/typeof.hpp>
#include "src/common/client.pb.h"
#include "src/part2/rados_interface.h"
#include "src/part2/rpc_server.h"

class RpcRequestTestOk : public RpcRequestCeph {
 public:
    RpcRequestTestOk() {}
    virtual ~RpcRequestTestOk() {}
    int OpenFile(char* filename) { return 100; }
    int StatFile(int fd, nebd::client::StatFileResponse* response) { return 0; }
    int Write(AsyncWrite* writejob) { return 0; }
    int Read(AsyncRead* writejob) { return 0; }
    int GetInfo(int fd, nebd::client::GetInfoResponse* response) { return 0; }
    int CloseFile(int fd) { return 0; }
    int Flush(AsyncFlush* flushjob) { return 0; }
    int Discard(AsyncDiscard* discardjob) { return 0; }
    int Resize(int fd, uint64_t size) { return 0; }
    int InvalidateCache(int fd) { return 0; }
};

class RpcRequestTestNoOk : public RpcRequestCeph {
 public:
    RpcRequestTestNoOk() {}
    virtual ~RpcRequestTestNoOk() {}
    int OpenFile(char* filename) { return -1; }
    int StatFile(int fd, nebd::client::StatFileResponse* response) {
        return -1;
    }
    int Write(AsyncWrite* writejob) { return -1; }
    int Read(AsyncRead* writejob) { return -1; }
    int GetInfo(int fd, nebd::client::GetInfoResponse* response) { return -1; }
    int CloseFile(int fd) { return -1; }
    int Flush(AsyncFlush* flushjob) { return -1; }
    int Discard(AsyncDiscard* discardjob) { return -1; }
    int Resize(int fd, uint64_t size) { return -1; }
    int InvalidateCache(int fd) { return -1; }
};

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

TEST(QemuClientServiceImplTest, OpenFile_1) {
    nebd::client::OpenFileResponse response;
    brpc::Controller cntl;
    nebd::client::OpenFileRequest request;
    request.set_filename(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    RpcServiceTestClosure done;

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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.OpenFile(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kOK, response.retcode());
}

TEST(QemuClientServiceImplTest, OpenFile_2) {
    nebd::client::OpenFileResponse response;
    brpc::Controller cntl;
    nebd::client::OpenFileRequest request;
    request.set_filename(
        "nbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    RpcServiceTestClosure done;

    g_imageMap.clear();

    QemuClientServiceImpl qemuImpl;
    qemuImpl.OpenFile(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, OpenFile_3) {
    nebd::client::OpenFileResponse response;
    brpc::Controller cntl;
    nebd::client::OpenFileRequest request;
    request.set_filename(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    RpcServiceTestClosure done;

    g_imageMap.clear();
    RpcRequestTestNoOk* req = new RpcRequestTestNoOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.OpenFile(&cntl, &request, &response, &done);
    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, OpenFile_4) {
    nebd::client::OpenFileResponse response;
    brpc::Controller cntl;
    nebd::client::OpenFileRequest request;
    request.set_filename(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    RpcServiceTestClosure done;

    g_imageMap.clear();
    RpcRequestTestOk* req = new RpcRequestTestOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.OpenFile(&cntl, &request, &response, &done);
    EXPECT_EQ(nebd::client::kOK, response.retcode());
}

TEST(QemuClientServiceImplTest, Write_1) {
    nebd::client::WriteResponse response;
    brpc::Controller cntl;
    nebd::client::WriteRequest request;
    request.set_fd(3);
    RpcServiceTestClosure done;

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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.Write(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, Write_2) {
    nebd::client::WriteResponse response;
    brpc::Controller cntl;
    nebd::client::WriteRequest request;
    request.set_fd(10000);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 10000;
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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.Write(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, Write_3) {
    nebd::client::WriteResponse response;
    brpc::Controller cntl;
    nebd::client::WriteRequest request;
    request.set_fd(300);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 300;
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

    RpcRequestTestOk* req = new RpcRequestTestOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.Write(&cntl, &request, &response, &done);
    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, Write_4) {
    nebd::client::WriteResponse response;
    brpc::Controller cntl;
    nebd::client::WriteRequest request;
    request.set_fd(300);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 300;
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

    RpcRequestTestNoOk* req = new RpcRequestTestNoOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.Write(&cntl, &request, &response, &done);
    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, read_1) {
    nebd::client::ReadResponse response;
    brpc::Controller cntl;
    nebd::client::ReadRequest request;
    request.set_fd(3);
    RpcServiceTestClosure done;

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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.Read(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, Read_2) {
    nebd::client::ReadResponse response;
    brpc::Controller cntl;
    nebd::client::ReadRequest request;
    request.set_fd(10000);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 10000;
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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.Read(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, Read_3) {
    nebd::client::ReadResponse response;
    brpc::Controller cntl;
    nebd::client::ReadRequest request;
    request.set_fd(300);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 300;
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

    RpcRequestTestNoOk* req = new RpcRequestTestNoOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.Read(&cntl, &request, &response, &done);
    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, Read_4) {
    nebd::client::ReadResponse response;
    brpc::Controller cntl;
    nebd::client::ReadRequest request;
    request.set_fd(300);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 300;
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

    RpcRequestTestOk* req = new RpcRequestTestOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.Read(&cntl, &request, &response, &done);
    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, StatFile_1) {
    nebd::client::StatFileResponse response;
    brpc::Controller cntl;
    nebd::client::StatFileRequest request;
    request.set_fd(3);
    RpcServiceTestClosure done;

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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.StatFile(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, StatFile_2) {
    nebd::client::StatFileResponse response;
    brpc::Controller cntl;
    nebd::client::StatFileRequest request;
    request.set_fd(10000);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 10000;
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

    RpcRequestTestOk* req = new RpcRequestTestOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.StatFile(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, StatFile_3) {
    nebd::client::StatFileResponse response;
    brpc::Controller cntl;
    nebd::client::StatFileRequest request;
    request.set_fd(100);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 100;
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

    RpcRequestTestOk* req = new RpcRequestTestOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.StatFile(&cntl, &request, &response, &done);
    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, StatFile_4) {
    nebd::client::StatFileResponse response;
    brpc::Controller cntl;
    nebd::client::StatFileRequest request;
    request.set_fd(100);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 100;
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

    RpcRequestTestNoOk* req = new RpcRequestTestNoOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.StatFile(&cntl, &request, &response, &done);
    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, GetInfo_1) {
    nebd::client::GetInfoResponse response;
    brpc::Controller cntl;
    nebd::client::GetInfoRequest request;
    request.set_fd(3);
    RpcServiceTestClosure done;

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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.GetInfo(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, GetInfo_2) {
    nebd::client::GetInfoResponse response;
    brpc::Controller cntl;
    nebd::client::GetInfoRequest request;
    request.set_fd(10000);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 10000;
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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.GetInfo(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, GetInfo_3) {
    nebd::client::GetInfoResponse response;
    brpc::Controller cntl;
    nebd::client::GetInfoRequest request;
    request.set_fd(300);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 300;
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

    RpcRequestTestNoOk* req = new RpcRequestTestNoOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.GetInfo(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, GetInfo_4) {
    nebd::client::GetInfoResponse response;
    brpc::Controller cntl;
    nebd::client::GetInfoRequest request;
    request.set_fd(300);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 300;
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

    RpcRequestTestOk* req = new RpcRequestTestOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.GetInfo(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, Flush_1) {
    nebd::client::FlushResponse response;
    brpc::Controller cntl;
    nebd::client::FlushRequest request;
    request.set_fd(3);
    RpcServiceTestClosure done;

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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.Flush(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, Flush_2) {
    nebd::client::FlushResponse response;
    brpc::Controller cntl;
    nebd::client::FlushRequest request;
    request.set_fd(10000);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 10000;
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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.Flush(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, Flush_3) {
    nebd::client::FlushResponse response;
    brpc::Controller cntl;
    nebd::client::FlushRequest request;
    request.set_fd(300);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 300;
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

    RpcRequestTestOk* req = new RpcRequestTestOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.Flush(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, Flush_4) {
    nebd::client::FlushResponse response;
    brpc::Controller cntl;
    nebd::client::FlushRequest request;
    request.set_fd(300);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 300;
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
    RpcRequestTestNoOk* req = new RpcRequestTestNoOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.Flush(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, CloseFile_1) {
    nebd::client::CloseFileResponse response;
    brpc::Controller cntl;
    nebd::client::CloseFileRequest request;
    request.set_fd(3);
    RpcServiceTestClosure done;

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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.CloseFile(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, CloseFile_2) {
    nebd::client::CloseFileResponse response;
    brpc::Controller cntl;
    nebd::client::CloseFileRequest request;
    request.set_fd(10000);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 10000;
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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.CloseFile(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, CloseFile_3) {
    nebd::client::CloseFileResponse response;
    brpc::Controller cntl;
    nebd::client::CloseFileRequest request;
    request.set_fd(300);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 300;
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

    RpcRequestTestNoOk* req = new RpcRequestTestNoOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.CloseFile(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, CloseFile_4) {
    nebd::client::CloseFileResponse response;
    brpc::Controller cntl;
    nebd::client::CloseFileRequest request;
    request.set_fd(300);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 300;
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

    RpcRequestTestOk* req = new RpcRequestTestOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.CloseFile(&cntl, &request, &response, &done);
    EXPECT_EQ(nebd::client::kOK, response.retcode());
}

TEST(QemuClientServiceImplTest, Discard_1) {
    nebd::client::DiscardResponse response;
    brpc::Controller cntl;
    nebd::client::DiscardRequest request;
    request.set_fd(3);
    RpcServiceTestClosure done;

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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.Discard(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, Discard_2) {
    nebd::client::DiscardResponse response;
    brpc::Controller cntl;
    nebd::client::DiscardRequest request;
    request.set_fd(10000);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 10000;
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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.Discard(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, Discard_3) {
    nebd::client::DiscardResponse response;
    brpc::Controller cntl;
    nebd::client::DiscardRequest request;
    request.set_fd(300);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 300;
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
    RpcRequestTestOk* req = new RpcRequestTestOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.Discard(&cntl, &request, &response, &done);
    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, Discard_4) {
    nebd::client::DiscardResponse response;
    brpc::Controller cntl;
    nebd::client::DiscardRequest request;
    request.set_fd(300);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 300;
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
    RpcRequestTestNoOk* req = new RpcRequestTestNoOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.Discard(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, ResizeFile_1) {
    nebd::client::ResizeResponse response;
    brpc::Controller cntl;
    nebd::client::ResizeRequest request;
    request.set_fd(3);
    RpcServiceTestClosure done;

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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.ResizeFile(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, ResizeFile_2) {
    nebd::client::ResizeResponse response;
    brpc::Controller cntl;
    nebd::client::ResizeRequest request;
    request.set_fd(10000);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 10000;
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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.ResizeFile(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, ResizeFile_3) {
    nebd::client::ResizeResponse response;
    brpc::Controller cntl;
    nebd::client::ResizeRequest request;
    request.set_fd(300);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 300;
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

    RpcRequestTestNoOk* req = new RpcRequestTestNoOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.ResizeFile(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, ResizeFile_4) {
    nebd::client::ResizeResponse response;
    brpc::Controller cntl;
    nebd::client::ResizeRequest request;
    request.set_fd(300);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 300;
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

    RpcRequestTestOk* req = new RpcRequestTestOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.ResizeFile(&cntl, &request, &response, &done);
    EXPECT_EQ(nebd::client::kOK, response.retcode());
}

TEST(QemuClientServiceImplTest, InvalidateCache_1) {
    nebd::client::InvalidateCacheResponse response;
    brpc::Controller cntl;
    nebd::client::InvalidateCacheRequest request;
    request.set_fd(3);
    RpcServiceTestClosure done;

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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.InvalidateCache(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, InvalidateCache_2) {
    nebd::client::InvalidateCacheResponse response;
    brpc::Controller cntl;
    nebd::client::InvalidateCacheRequest request;
    request.set_fd(10000);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 10000;
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

    QemuClientServiceImpl qemuImpl;
    qemuImpl.InvalidateCache(&cntl, &request, &response, &done);

    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, InvalidateCache_3) {
    nebd::client::InvalidateCacheResponse response;
    brpc::Controller cntl;
    nebd::client::InvalidateCacheRequest request;
    request.set_fd(300);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 300;
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

    RpcRequestTestNoOk* req = new RpcRequestTestNoOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.InvalidateCache(&cntl, &request, &response, &done);
    EXPECT_EQ(nebd::client::kNoOK, response.retcode());
}

TEST(QemuClientServiceImplTest, InvalidateCache_4) {
    nebd::client::InvalidateCacheResponse response;
    brpc::Controller cntl;
    nebd::client::InvalidateCacheRequest request;
    request.set_fd(300);
    RpcServiceTestClosure done;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 300;
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

    RpcRequestTestOk* req = new RpcRequestTestOk;
    QemuClientServiceImpl qemuImpl;
    qemuImpl.SetRpcRequest(req);
    qemuImpl.InvalidateCache(&cntl, &request, &response, &done);
    EXPECT_EQ(nebd::client::kOK, response.retcode());
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
