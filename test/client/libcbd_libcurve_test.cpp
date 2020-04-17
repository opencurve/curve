/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/11/23  Wenyu Zhou   Initial version
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <braft/configuration.h>

#include <string>

// #define CBD_BACKEND_FAKE

#include "src/client/libcbd.h"

#include "src/client/libcurve_file.h"
#include "include/client/libcurve.h"
#include "src/client/file_instance.h"
#include "test/client/fake/mock_schedule.h"
#include "test/client/fake/fakeMDS.h"
#include "src/client/client_common.h"
#include "test/integration/cluster_common/cluster.h"
#include "test/util/config_generator.h"

using curve::client::EndPoint;

#define BUFSIZE     4 * 1024
#define FILESIZE    10uL * 1024 * 1024 * 1024
#define NEWSIZE     20uL * 1024 * 1024 * 1024

#define filename    "1_userinfo_test.img"

DECLARE_string(chunkserver_list);

extern std::string configpath;
void LibcbdLibcurveTestCallback(CurveAioContext* context) {
    context->op = LIBCURVE_OP_MAX;
}

class TestLibcbdLibcurve : public ::testing::Test {
 public:
    void SetUp() {
        FLAGS_chunkserver_list =
         "127.0.0.1:9110:0,127.0.0.1:9111:0,127.0.0.1:9112:0";

        mds_ = new FakeMDS(filename);

        // 设置leaderid
        EndPoint ep;
        butil::str2endpoint("127.0.0.1", 9110, &ep);
        braft::PeerId pd(ep);

        /*** init mds service ***/
        mds_->Initialize();
        mds_->StartCliService(pd);
        mds_->StartService();
        mds_->CreateCopysetNode(true);

        if (Init(configpath.c_str()) != 0) {
            LOG(FATAL) << "Fail to init config";
            return;
        }

        int64_t t0 = butil::monotonic_time_ms();
        int ret = -1;
        for (;;) {
            ret = Open4Qemu(filename);
            if (ret == 0) {
                LOG(INFO) << "Created file for test.";
                break;
            }

            int64_t t1 = butil::monotonic_time_ms();
            // Set timeout to 10 seconds
            if (t1 - t0 > 10 * 1000) {
                LOG(ERROR) << "Timed out retrying of creating file.";
                break;
            }

            LOG(ERROR) << "Failed to create file, retrying again.";
            usleep(100 * 1000);
        }
        ASSERT_EQ(ret, 0);
    }

    void TearDown() {
        mds_->UnInitialize();

        UnInit();
        delete mds_;
    }

 protected:
    FakeMDS* mds_;
};

extern bool globalclientinited_;
extern curve::client::FileClient* globalclient;
TEST_F(TestLibcbdLibcurve, InitTest) {
    int ret;
    CurveOptions opt;

    globalclient->UnInit();
    globalclient = nullptr;
    globalclientinited_ = false;
    memset(&opt, 0, sizeof(opt));
    // testing with no conf specified
    opt.conf = "";
    ret = cbd_lib_init(&opt);
    ASSERT_NE(ret, 0);
    ret = cbd_lib_fini();
    ASSERT_EQ(ret, 0);

    // testing with conf specified
    opt.conf = const_cast<char*>(configpath.c_str());
    ret = cbd_lib_init(&opt);
    ASSERT_EQ(ret, 0);
    ret = cbd_lib_fini();
    ASSERT_EQ(ret, 0);
}

TEST_F(TestLibcbdLibcurve, ExtendTest) {
    int ret;
    CurveOptions opt;

    memset(&opt, 0, sizeof(opt));

    // testing with conf specified
    opt.conf = const_cast<char*>(configpath.c_str());
    ret = cbd_lib_init(&opt);
    ASSERT_EQ(ret, 0);
    ret = cbd_lib_resize(filename, NEWSIZE);
    ASSERT_EQ(ret, 0);
    ret = cbd_lib_resize(filename, -1);
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, ret);

    ret = cbd_lib_fini();
    ASSERT_EQ(ret, LIBCURVE_ERROR::OK);
}

TEST_F(TestLibcbdLibcurve, ReadWriteTest) {
    int ret;
    int fd;
    int i;
    char buf[BUFSIZE];
    CurveOptions opt;

    memset(&opt, 0, sizeof(opt));
    memset(buf, 'a', BUFSIZE);

    opt.conf = const_cast<char*>(configpath.c_str());
    ret = cbd_lib_init(&opt);
    ASSERT_EQ(ret, LIBCURVE_ERROR::OK);

    fd = cbd_lib_open(filename);
    ASSERT_GE(fd, 0);

    uint64_t size = cbd_lib_filesize(filename);
    ASSERT_EQ(size, FILESIZE);

    ret = cbd_lib_pwrite(fd, buf, 0, BUFSIZE);
    ASSERT_EQ(ret, BUFSIZE);

    ret = cbd_lib_sync(fd);
    ASSERT_EQ(ret, 0);

    ret = cbd_lib_pread(fd, buf, 0, BUFSIZE);
    ASSERT_EQ(ret, BUFSIZE);

    for (i = 0; i < BUFSIZE; i++) {
        if (buf[i] != 'a') {
            break;
        }
    }
    ASSERT_EQ(i, BUFSIZE);

    ret = cbd_lib_close(fd);
    ASSERT_EQ(ret, LIBCURVE_ERROR::OK);

    ret = cbd_lib_fini();
    ASSERT_EQ(ret, LIBCURVE_ERROR::OK);
}

TEST_F(TestLibcbdLibcurve, AioReadWriteTest) {
    int ret;
    int fd;
    int i;
    char buf[BUFSIZE];
    CurveOptions opt;
    CurveAioContext aioCtx;

    aioCtx.buf = buf;
    aioCtx.offset = 0;
    aioCtx.length = BUFSIZE;
    aioCtx.cb = LibcbdLibcurveTestCallback;

    memset(&opt, 0, sizeof(opt));
    memset(buf, 'a', BUFSIZE);

    opt.conf = const_cast<char*>(configpath.c_str());
    ret = cbd_lib_init(&opt);
    ASSERT_EQ(ret, LIBCURVE_ERROR::OK);

    fd = cbd_lib_open(filename);
    ASSERT_GE(fd, 0);

    uint64_t size = cbd_lib_filesize(filename);
    ASSERT_EQ(size, FILESIZE);

    aioCtx.op = LIBCURVE_OP_WRITE;
    ret = cbd_lib_aio_pwrite(fd, &aioCtx);
    ASSERT_EQ(ret, 0);

    while (aioCtx.op == LIBCURVE_OP_WRITE) {
        usleep(10 * 1000);
    }

    ret = cbd_lib_sync(fd);
    ASSERT_EQ(ret, 0);

    aioCtx.op = LIBCURVE_OP_READ;
    ret = cbd_lib_aio_pread(fd, &aioCtx);
    ASSERT_EQ(ret, 0);

    while (aioCtx.op == LIBCURVE_OP_READ) {
        usleep(10 * 1000);
    }

    for (i = 0; i < BUFSIZE; i++) {
        if (buf[i] != 'a') {
            break;
        }
    }
    ASSERT_EQ(i, BUFSIZE);

    ret = cbd_lib_close(fd);
    ASSERT_EQ(ret, LIBCURVE_ERROR::OK);

    ret = cbd_lib_fini();
    ASSERT_EQ(ret, LIBCURVE_ERROR::OK);
}

TEST_F(TestLibcbdLibcurve, StatFileTest) {
    int64_t ret;
    CurveOptions opt;

    memset(&opt, 0, sizeof(opt));

    // testing with conf specified
    opt.conf = const_cast<char*>(configpath.c_str());
    ret = cbd_lib_init(&opt);
    ASSERT_EQ(ret, 0);

    ret = cbd_lib_filesize(filename);
    ASSERT_EQ(ret, FILESIZE);

    ret = cbd_lib_fini();
    ASSERT_EQ(ret, LIBCURVE_ERROR::OK);
}

TEST_F(TestLibcbdLibcurve, ReadAndCloseConcurrencyTest) {
    char buffer[BUFSIZE];
    CurveOptions opts;

    const int closeFileSleepS = 20;

    memset(&opts, 0, sizeof(opts));
    memset(buffer, 0, sizeof(buffer));

    opts.conf = const_cast<char*>(configpath.c_str());
    ASSERT_EQ(LIBCURVE_ERROR::OK, cbd_lib_init(&opts));

    int fd1 = cbd_lib_open("/ReadWithCloseTest1_test_");
    int fd2 = cbd_lib_open("/ReadWithCloseTest2_test_");
    ASSERT_GE(fd1, 0);
    ASSERT_GE(fd2, 0);

    auto curvefsService = mds_->GetMDSService();
    curvefsService->SetCloseFileTask([]() {
        std::this_thread::sleep_for(std::chrono::seconds(closeFileSleepS));
    });

    auto closeThread = [](int fd) {
        ASSERT_EQ(0, cbd_lib_close(fd));
        LOG(INFO) << "here";
    };

    auto readThread = [buffer](int fd) {
        auto start = curve::common::TimeUtility::GetTimeofDayMs();
        ASSERT_EQ(BUFSIZE, cbd_lib_pread(fd, (void*)buffer, 0, BUFSIZE));  // NOLINT
        auto end = curve::common::TimeUtility::GetTimeofDayMs();

        ASSERT_LE(end - start, 1000);
    };

    std::thread t1(closeThread, fd1);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::thread t2(readThread, fd2);

    t1.join();
    t2.join();

    curvefsService->SetCloseFileTask(nullptr);

    ASSERT_EQ(LIBCURVE_ERROR::OK, cbd_lib_close(fd1));
    ASSERT_EQ(LIBCURVE_ERROR::OK, cbd_lib_close(fd2));

    ASSERT_EQ(LIBCURVE_ERROR::OK, cbd_lib_fini());
}

std::string mdsMetaServerAddr = "127.0.0.1:9951";     // NOLINT
uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 4 * 1024 * 1024;   // NOLINT
std::string configpath = "./test/client/testConfig/client_libcbd.conf";   // NOLINT

const std::vector<std::string> clientConf {
    std::string("mds.listen.addr=127.0.0.1:9951"),
    std::string("global.logPath=./runlog/"),
    std::string("chunkserver.rpcTimeoutMS=1000"),
    std::string("chunkserver.opMaxRetry=3"),
    std::string("metacache.getLeaderRetry=3"),
    std::string("metacache.getLeaderTimeOutMS=1000"),
    std::string("global.fileMaxInFlightRPCNum=2048"),
    std::string("metacache.rpcRetryIntervalUS=500"),
    std::string("mds.rpcRetryIntervalUS=500"),
    std::string("schedule.threadpoolSize=2"),
};

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, false);

    curve::CurveCluster* cluster = new curve::CurveCluster();

    cluster->PrepareConfig<curve::ClientConfigGenerator>(
        configpath, clientConf);

    int ret = RUN_ALL_TESTS();
    return ret;
}
