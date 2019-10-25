/*
 * Project: nebd
 * Created Date: 2019-08-12
 * Author: hzchenwei7
 * Copyright (c) 2018 netease
 */
#include <brpc/server.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gflags/gflags.h>
#include <butil/logging.h>
#include <condition_variable>  // NOLINT
#include <mutex>  // NOLINT
#include "tests/part1/mock_client_service.h"
#include "src/part1/libnebd.h"
#include "src/part1/libnebd_file.h"
#include "src/part1/nebd_client.h"
#include "src/part1/nebd_lifecycle.h"

DEFINE_string(uuid, "12345678-1234-1234-1234-123456789012", "uuid");

#define confFilename "tests/part1/nebd.conf"
#define filename  "test_file_name"
#define DEFAULT_FD  1
#define BUFSIZE    512
#define DEFAULT_FILEZIZE (50*1024*1024)
std::mutex mtx;
std::condition_variable condition;
bool callback;

void LibAioCallBackFunc(struct ClientAioContext* context) {
    ASSERT_EQ(context->ret, 0);
    callback = true;
    condition.notify_one();
    ASSERT_EQ(context->retryCount, 0);
}

void LibAioFailCallBackFunc(struct ClientAioContext* context) {
    ASSERT_EQ(context->ret, -1);
    callback = true;
    condition.notify_one();
    ASSERT_EQ(context->retryCount, 0);
}


class nebdClientTest: public ::testing::Test {
 protected:
    void SetUp() override {
        callback = false;
        system("sudo killall client_server");
        system("sudo mkdir -p /etc/nebd");
        system("sudo cp tests/part1/nebd.conf /etc/nebd/nebd-client.conf");
        ASSERT_EQ(0, nebd_lib_init());
    }
    void TearDown() override {
        nebd_lib_uninit();
    }
};

TEST_F(nebdClientTest, nebdCommonTest) {
    // nebd_lib_open_test
    int ret;
    ret = nebd_lib_open(filename);
    ASSERT_EQ(DEFAULT_FD, ret);

    // nebd_lib_pread_test
    char buf[BUFSIZE] = {};
    ret = nebd_lib_pread(DEFAULT_FD, buf, 0, BUFSIZE);
    ASSERT_EQ(-1, ret);

    // nebd_lib_pwrite_test
    memset(buf, 'a', BUFSIZE);
    ret = nebd_lib_pwrite(DEFAULT_FD, buf, 0, BUFSIZE);
    ASSERT_EQ(-1, ret);

    // nebd_lib_aio_pread_pwrite_test
    char writeBuf[BUFSIZE] = "test";
    char readBuf[BUFSIZE];
    ClientAioContext context;
    context.buf = writeBuf;
    context.offset = 0;
    context.length = BUFSIZE;
    context.ret = 0;
    context.op = LIBAIO_OP_WRITE;
    context.cb = LibAioCallBackFunc;
    context.retryCount = 0;

    ret = nebd_lib_aio_pwrite(DEFAULT_FD, &context);
    ASSERT_EQ(ret, 0);

    std::unique_lock<std::mutex> lock{mtx};
    condition.wait(lock);
    ASSERT_TRUE(callback);

    callback = false;
    ret = nebd_lib_aio_pread(DEFAULT_FD, &context);
    ASSERT_EQ(ret, 0);

    condition.wait(lock);
    ASSERT_TRUE(callback);

    // nebd_lib_sync_test
    ret = nebd_lib_sync(DEFAULT_FD);
    ASSERT_EQ(0, ret);

    // nebd_lib_discard_test
    context.buf = nullptr;
    context.offset = 0;
    context.length = BUFSIZE;
    context.ret = 0;
    context.op = LIBAIO_OP_DISCARD;
    context.cb = LibAioCallBackFunc;
    context.retryCount = 0;

    ret = nebd_lib_discard(DEFAULT_FD, &context);
    ASSERT_EQ(ret, 0);

    condition.wait(lock);
    ASSERT_TRUE(callback);

    // nebd_lib_filesize_test
    int64_t filesize;
    filesize = nebd_lib_filesize(DEFAULT_FD);
    ASSERT_EQ(DEFAULT_FILEZIZE, filesize);

    // nebd_lib_resize_test
    ret = nebd_lib_resize(DEFAULT_FD, DEFAULT_FILEZIZE);
    ASSERT_EQ(0, ret);

    // nebd_lib_getinfo_test
    ret = nebd_lib_getinfo(DEFAULT_FD);
    ASSERT_EQ(DEFAULT_FILEZIZE, ret);

    // nebd_lib_flush_test
    context.buf = nullptr;
    context.offset = 0;
    context.length = 0;
    context.ret = 0;
    context.op = LIBAIO_OP_FLUSH;
    context.cb = LibAioCallBackFunc;
    context.retryCount = 0;

    ret = nebd_lib_flush(DEFAULT_FD, &context);
    ASSERT_EQ(ret, 0);

    condition.wait(lock);
    ASSERT_TRUE(callback);

    // nebd_lib_getinfo_test
    filesize = nebd_lib_getinfo(DEFAULT_FD);
    ASSERT_EQ(DEFAULT_FILEZIZE, filesize);

    // nebd_lib_invalidcache_test
    ret = nebd_lib_invalidcache(DEFAULT_FD);
    ASSERT_EQ(0, ret);

    // nebd_lib_close_test
    ret = nebd_lib_close(DEFAULT_FD);
    ASSERT_EQ(0, ret);
}

class nebdFileClientTest: public ::testing::Test {
 protected:
    void SetUp() override {
        system("sudo killall client_server");
        system("sudo mkdir -p /etc/nebd");
        system("sudo cp tests/part1/nebd.conf /etc/nebd/nebd-client.conf");
    }
    void TearDown() override {
    }
};

TEST_F(nebdFileClientTest, common_test) {
    nebd::client::FileClient fileClientTest;
    ASSERT_EQ(fileClientTest.Init(confFilename), 0);
    ASSERT_EQ(fileClientTest.Open(filename), DEFAULT_FD);
    ASSERT_EQ(fileClientTest.Extend(DEFAULT_FD, DEFAULT_FILEZIZE), 0);
    ASSERT_EQ(fileClientTest.StatFile(DEFAULT_FD), DEFAULT_FILEZIZE);
    ASSERT_EQ(fileClientTest.GetInfo(DEFAULT_FD), DEFAULT_FILEZIZE);
    ASSERT_EQ(fileClientTest.InvalidCache(DEFAULT_FD), 0);

    char writeBuf[BUFSIZE] = "test";
    char readBuf[BUFSIZE];
    ClientAioContext context;
    context.buf = writeBuf;
    context.offset = 0;
    context.length = BUFSIZE;
    context.ret = 0;
    context.op = LIBAIO_OP_WRITE;
    context.cb = LibAioCallBackFunc;
    context.retryCount = 0;

    ASSERT_EQ(fileClientTest.AioRead(DEFAULT_FD, &context), 0);
    std::unique_lock<std::mutex> lock{mtx};
    condition.wait(lock);

    ASSERT_EQ(fileClientTest.AioWrite(DEFAULT_FD, &context), 0);
    condition.wait(lock);
    ASSERT_TRUE(callback);

    context.buf = nullptr;
    context.offset = 0;
    context.length = BUFSIZE;
    context.ret = 0;
    context.op = LIBAIO_OP_DISCARD;
    context.cb = LibAioCallBackFunc;
    context.retryCount = 0;
    ASSERT_EQ(fileClientTest.Discard(DEFAULT_FD, &context), 0);
    condition.wait(lock);
    ASSERT_TRUE(callback);

    context.buf = nullptr;
    context.offset = 0;
    context.length = 0;
    context.ret = 0;
    context.op = LIBAIO_OP_FLUSH;
    context.cb = LibAioCallBackFunc;
    context.retryCount = 0;
    ASSERT_EQ(fileClientTest.Flush(DEFAULT_FD, &context), 0);
    condition.wait(lock);
    ASSERT_TRUE(callback);

    ASSERT_EQ(fileClientTest.Close(DEFAULT_FD), 0);
    fileClientTest.Uninit();
    return;
}

TEST_F(nebdFileClientTest, no_rpc_server_test) {
    nebd::client::FileClient fileClientTest;
    ASSERT_EQ(fileClientTest.Init("wrongConfPath"), -1);

    nebd::common::Configuration conf;
    conf.SetConfigPath(confFilename);
    ASSERT_TRUE(conf.LoadConfig());

    ASSERT_EQ(fileClientTest.LoadConf(&conf), 0);

    ASSERT_EQ(fileClientTest.InitChannel("127.0.0.1:6667"), 0);
    ASSERT_EQ(fileClientTest.Open(filename), -1);
    ASSERT_EQ(fileClientTest.Extend(DEFAULT_FD, DEFAULT_FILEZIZE), -1);
    ASSERT_EQ(fileClientTest.StatFile(DEFAULT_FD), -1);
    ASSERT_EQ(fileClientTest.GetInfo(DEFAULT_FD), -1);
    ASSERT_EQ(fileClientTest.InvalidCache(DEFAULT_FD), -1);

    // char writeBuf[BUFSIZE] = "test";
    // char readBuf[BUFSIZE];
    // ClientAioContext context;
    // context.buf = writeBuf;
    // context.offset = 0;
    // context.length = BUFSIZE;
    // context.ret = 0;
    // context.op = LIBAIO_OP_WRITE;
    // context.cb = LibAioCallBackFunc;
    // context.retryCount = 0;

    // ASSERT_EQ(fileClientTest.AioRead(DEFAULT_FD, &context), 0);
    // std::unique_lock<std::mutex> lock{mtx};
    // condition.wait(lock);

    // ASSERT_EQ(fileClientTest.AioWrite(DEFAULT_FD, &context), 0);
    // condition.wait(lock);
    // ASSERT_TRUE(callback);

    // context.buf = nullptr;
    // context.offset = 0;
    // context.length = BUFSIZE;
    // context.ret = 0;
    // context.op = LIBAIO_OP_DISCARD;
    // context.cb = LibAioCallBackFunc;
    // context.retryCount = 0;
    // ASSERT_EQ(fileClientTest.Discard(DEFAULT_FD, &context), 0);
    // condition.wait(lock);
    // ASSERT_TRUE(callback);

    // context.buf = nullptr;
    // context.offset = 0;
    // context.length = 0;
    // context.ret = 0;
    // context.op = LIBAIO_OP_FLUSH;
    // context.cb = LibAioCallBackFunc;
    // context.retryCount = 0;
    // ASSERT_EQ(fileClientTest.Flush(DEFAULT_FD, &context), 0);
    // condition.wait(lock);
    // ASSERT_TRUE(callback);

    ASSERT_EQ(fileClientTest.Close(DEFAULT_FD), -1);
    fileClientTest.Uninit();
    return;
}

static void OpenFileFunc(::google::protobuf::RpcController* controller,
                       const ::nebd::client::OpenFileRequest* request,
                       ::nebd::client::OpenFileResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    // if (0 != gReadCntlFailedCode) {
    //    brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
    //     cntl->SetFailed(-1, "open file controller error");
    // }
}

static void CloseFileFunc(::google::protobuf::RpcController* controller,
                       const ::nebd::client::CloseFileRequest* request,
                       ::nebd::client::CloseFileResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
}

static void ReadFunc(::google::protobuf::RpcController* controller,
                       const ::nebd::client::ReadRequest* request,
                       ::nebd::client::ReadResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
}

static void WriteFunc(::google::protobuf::RpcController* controller,
                       const ::nebd::client::WriteRequest* request,
                       ::nebd::client::WriteResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
}

static void DiscardFunc(::google::protobuf::RpcController* controller,
                       const ::nebd::client::DiscardRequest* request,
                       ::nebd::client::DiscardResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
}

static void StatFileFunc(::google::protobuf::RpcController* controller,
                       const ::nebd::client::StatFileRequest* request,
                       ::nebd::client::StatFileResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
}

static void ResizeFileFunc(::google::protobuf::RpcController* controller,
                       const ::nebd::client::ResizeRequest* request,
                       ::nebd::client::ResizeResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
}

static void FlushFunc(::google::protobuf::RpcController* controller,
                       const ::nebd::client::FlushRequest* request,
                       ::nebd::client::FlushResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
}

static void GetInfoFunc(::google::protobuf::RpcController* controller,
                       const ::nebd::client::GetInfoRequest* request,
                       ::nebd::client::GetInfoResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
}

static void InvalidateCacheFunc(::google::protobuf::RpcController* controller,
                       const ::nebd::client::InvalidateCacheRequest* request,
                       ::nebd::client::InvalidateCacheResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
}

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::AnyNumber;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;
using ::testing::InSequence;
using ::testing::AtLeast;
using ::testing::SaveArgPointee;

TEST_F(nebdFileClientTest, rpc_fail_test) {
    nebd::client::FileClient fileClientTest;
    nebd::common::Configuration conf;
    conf.SetConfigPath(confFilename);
    ASSERT_TRUE(conf.LoadConfig());
    ASSERT_EQ(fileClientTest.LoadConf(&conf), 0);

    // add service
    brpc::Server server;
    nebd::client::MockQemuClientService clientService;
    ASSERT_EQ(server.AddService(&clientService,
                          brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    // start rpc server
    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    std::string listenAddr = "127.0.0.1:6667";
    ASSERT_EQ(server.Start(listenAddr.c_str(), &option), 0);

    ASSERT_EQ(fileClientTest.InitChannel("127.0.0.1:6667"), 0);

    ::nebd::client::OpenFileResponse openFileResponse;
    openFileResponse.set_retcode(nebd::client::RetCode::kNoOK);
    EXPECT_CALL(clientService, OpenFile(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(openFileResponse),
                        Invoke(OpenFileFunc)));
    ASSERT_EQ(fileClientTest.Open(filename), -1);

    ::nebd::client::ResizeResponse resizeResponse;
    resizeResponse.set_retcode(nebd::client::RetCode::kNoOK);
    EXPECT_CALL(clientService, ResizeFile(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(resizeResponse),
                        Invoke(ResizeFileFunc)));
    ASSERT_EQ(fileClientTest.Extend(DEFAULT_FD, DEFAULT_FILEZIZE), -1);

    ::nebd::client::StatFileResponse statFileResponse;
    statFileResponse.set_retcode(nebd::client::RetCode::kNoOK);
    EXPECT_CALL(clientService, StatFile(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(statFileResponse),
                        Invoke(StatFileFunc)));
    ASSERT_EQ(fileClientTest.StatFile(DEFAULT_FD), -1);

    ::nebd::client::GetInfoResponse getInfoResponse;
    getInfoResponse.set_retcode(nebd::client::RetCode::kNoOK);
    EXPECT_CALL(clientService, GetInfo(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(getInfoResponse),
                        Invoke(GetInfoFunc)));
    ASSERT_EQ(fileClientTest.GetInfo(DEFAULT_FD), -1);

    ::nebd::client::InvalidateCacheResponse invalidateCacheResponse;
    invalidateCacheResponse.set_retcode(nebd::client::RetCode::kNoOK);
    EXPECT_CALL(clientService, InvalidateCache(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(invalidateCacheResponse),
                        Invoke(InvalidateCacheFunc)));
    ASSERT_EQ(fileClientTest.InvalidCache(DEFAULT_FD), -1);

    char writeBuf[BUFSIZE] = "test";
    char readBuf[BUFSIZE];
    ClientAioContext context;
    context.buf = writeBuf;
    context.offset = 0;
    context.length = BUFSIZE;
    context.ret = 0;
    context.op = LIBAIO_OP_WRITE;
    context.cb = LibAioFailCallBackFunc;
    context.retryCount = 0;

    ::nebd::client::ReadResponse readResponse;
    readResponse.set_retcode(nebd::client::RetCode::kNoOK);
    EXPECT_CALL(clientService, Read(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(readResponse),
                        Invoke(ReadFunc)));
    ASSERT_EQ(fileClientTest.AioRead(DEFAULT_FD, &context), 0);
    std::unique_lock<std::mutex> lock{mtx};
    condition.wait(lock);
    ASSERT_TRUE(callback);


    context.buf = readBuf;
    context.offset = 0;
    context.length = BUFSIZE;
    context.ret = 0;
    context.op = LIBAIO_OP_READ;
    context.cb = LibAioFailCallBackFunc;
    context.retryCount = 0;
    ::nebd::client::WriteResponse writeResponse;
    writeResponse.set_retcode(nebd::client::RetCode::kNoOK);
    EXPECT_CALL(clientService, Write(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(writeResponse),
                        Invoke(WriteFunc)));
    ASSERT_EQ(fileClientTest.AioWrite(DEFAULT_FD, &context), 0);
    condition.wait(lock);
    ASSERT_TRUE(callback);

    context.buf = nullptr;
    context.offset = 0;
    context.length = BUFSIZE;
    context.ret = 0;
    context.op = LIBAIO_OP_DISCARD;
    context.cb = LibAioFailCallBackFunc;
    context.retryCount = 0;
    ::nebd::client::DiscardResponse discardResponse;
    discardResponse.set_retcode(nebd::client::RetCode::kNoOK);
    EXPECT_CALL(clientService, Discard(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(discardResponse),
                        Invoke(DiscardFunc)));
    ASSERT_EQ(fileClientTest.Discard(DEFAULT_FD, &context), 0);
    condition.wait(lock);
    ASSERT_TRUE(callback);

    context.buf = nullptr;
    context.offset = 0;
    context.length = 0;
    context.ret = 0;
    context.op = LIBAIO_OP_FLUSH;
    context.cb = LibAioFailCallBackFunc;
    context.retryCount = 0;
    ::nebd::client::FlushResponse flushResponse;
    flushResponse.set_retcode(nebd::client::RetCode::kNoOK);
    EXPECT_CALL(clientService, Flush(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(flushResponse),
                        Invoke(FlushFunc)));
    ASSERT_EQ(fileClientTest.Flush(DEFAULT_FD, &context), 0);
    condition.wait(lock);
    ASSERT_TRUE(callback);

    ::nebd::client::CloseFileResponse closeFileResponse;
    closeFileResponse.set_retcode(nebd::client::RetCode::kNoOK);
    EXPECT_CALL(clientService, CloseFile(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(closeFileResponse),
                        Invoke(CloseFileFunc)));
    ASSERT_EQ(fileClientTest.Close(DEFAULT_FD), -1);
    fileClientTest.Uninit();
    return;
}

class nebdClientLifeCycleTest: public ::testing::Test {
 protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(nebdClientLifeCycleTest, init_when_part2_not_alive) {
    ASSERT_EQ(Init4Qemu(confFilename), 0);
    Uninit4Qemu();
}

TEST_F(nebdClientLifeCycleTest, init_when_part2_alive) {
    ASSERT_EQ(Init4Qemu(confFilename), 0);
    ASSERT_EQ(Init4Qemu(confFilename), 0);
    Uninit4Qemu();
}

TEST_F(nebdClientLifeCycleTest, lifecycle_manager_start_stop_without_hb_test) {
    using ::nebd::common::Configuration;
    Configuration conf;
    conf.SetConfigPath(confFilename);
    ASSERT_EQ(conf.LoadConfig(), true);

    nebd::client::LifeCycleManager manager;
    ASSERT_EQ(manager.Start(&conf), 0);
    ASSERT_EQ(manager.IsPart2Alive(), true);
    ASSERT_EQ(manager.IsHeartbeatThreadStart(), false);
    ASSERT_EQ(manager.Start(&conf), 0);
    ASSERT_EQ(manager.IsPart2Alive(), true);
    ASSERT_EQ(manager.IsHeartbeatThreadStart(), false);
    manager.Stop();
    ASSERT_EQ(manager.IsPart2Alive(), false);
}

TEST_F(nebdClientLifeCycleTest, lifecycle_manager_start_stop_with_hb_test) {
    using ::nebd::common::Configuration;
    Configuration conf;
    conf.SetConfigPath(confFilename);
    ASSERT_EQ(conf.LoadConfig(), true);

    nebd::client::LifeCycleManager manager;
    ASSERT_EQ(manager.Start(&conf), 0);
    ASSERT_EQ(manager.IsPart2Alive(), true);
    ASSERT_EQ(manager.Start(&conf), 0);
    ASSERT_EQ(manager.IsPart2Alive(), true);
    ASSERT_EQ(manager.IsHeartbeatThreadStart(), false);
    manager.StartHeartbeat();
    ASSERT_EQ(manager.IsHeartbeatThreadStart(), true);
    ASSERT_EQ(manager.IsPart2Alive(), true);
    manager.Stop();
    ASSERT_EQ(manager.IsPart2Alive(), false);
}

TEST_F(nebdClientLifeCycleTest, lifecycle_kill_part2_test) {
    using ::nebd::common::Configuration;
    Configuration conf;
    conf.SetConfigPath(confFilename);
    ASSERT_EQ(conf.LoadConfig(), true);

    nebd::client::LifeCycleManager manager;
    ASSERT_EQ(manager.Start(&conf), 0);
    ASSERT_EQ(manager.IsPart2Alive(), true);
    ASSERT_EQ(manager.IsHeartbeatThreadStart(), false);
    manager.StartHeartbeat();
    ASSERT_EQ(manager.IsHeartbeatThreadStart(), true);
    manager.KillPart2();
    sleep(2);
    ASSERT_EQ(manager.IsPart2Alive(), true);
    manager.Stop();
    ASSERT_EQ(manager.IsPart2Alive(), false);
}

TEST_F(nebdClientLifeCycleTest, lifecycle_manager_start_load_conf_fail_test) {
    ::nebd::common::Configuration conf;
    nebd::client::LifeCycleManager manager;
    ASSERT_EQ(manager.Start(&conf), -1);
}

TEST_F(nebdClientLifeCycleTest, lifecycle_manager_start_get_uuid_fail_test) {
    ::nebd::common::Configuration conf;
    conf.SetConfigPath(confFilename);
    ASSERT_EQ(conf.LoadConfig(), true);
    conf.SetStringValue("qemuProcName", "wrong_qemuProcName");

    nebd::client::LifeCycleManager manager;
    ASSERT_EQ(manager.Start(&conf), -1);
}

TEST_F(nebdClientLifeCycleTest, lifecycle_manager_start_start_part2_fail) {
    ::nebd::common::Configuration conf;
    conf.SetConfigPath(confFilename);
    ASSERT_EQ(conf.LoadConfig(), true);
    conf.SetStringValue("part2ProcPath", "wrong_part2ProcPath");

    nebd::client::LifeCycleManager manager;
    ASSERT_EQ(manager.Start(&conf), -1);
}

TEST_F(nebdClientLifeCycleTest, lifecycle_manager_start_read_port_fail) {
    ::nebd::common::Configuration conf;
    conf.SetConfigPath(confFilename);
    ASSERT_EQ(conf.LoadConfig(), true);
    conf.SetStringValue("metadataPrefix", "wrong_metadataPrefix");

    nebd::client::LifeCycleManager manager;
    ASSERT_EQ(manager.Start(&conf), -1);
}

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, false);
    int ret = RUN_ALL_TESTS();

    return ret;
}
