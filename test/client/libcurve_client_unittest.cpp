/**
 * Project: curve
 * File Created: 2020-02-04 15:37
 * Author: wuhanqing
 * Copyright (c) 2020 netease
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <braft/configuration.h>

#include <string>

#include "include/client/libcurve.h"
#include "test/integration/cluster_common/cluster.h"
#include "test/util/config_generator.h"
#include "test/client/fake/mock_schedule.h"
#include "test/client/fake/fakeMDS.h"
#include "test/client/mock_file_client.h"

using curve::client::EndPoint;
using ::testing::Return;
using ::testing::_;

const uint32_t kBufSize = 4 * 1024;
const uint64_t kFileSize = 10ul * 1024 * 1024 * 1024;
const uint64_t kNewSize = 20ul * 1024 * 1024 * 1024;
const char* kFileName = "1_userinfo_test.img";
const char* kWrongFileName = "xxxxx";

DECLARE_string(chunkserver_list);

extern std::string configpath;

void LibcbdLibcurveTestCallback(CurveAioContext* context) {
    context->op = LIBCURVE_OP_MAX;
}

namespace curve {
namespace client {

class CurveClientTest : public ::testing::Test {
 public:
    void SetUp() {
        FLAGS_chunkserver_list =
            "127.0.0.1:19110:0,127.0.0.1:19111:0,127.0.0.1:19112:0";

        mds_ = new FakeMDS(kFileName);

        // 设置leaderid
        EndPoint ep;
        butil::str2endpoint("127.0.0.1", 19110, &ep);
        braft::PeerId pd(ep);

        // init mds service
        mds_->Initialize();
        mds_->StartCliService(pd);
        mds_->StartService();
        mds_->CreateCopysetNode(true);

        if (client_.Init(configpath.c_str()) != 0) {
            ASSERT_TRUE(false);
            return;
        }
    }

    void TearDown() {
        mds_->UnInitialize();
        delete mds_;
        mds_ = nullptr;

        client_.UnInit();
    }

    FakeMDS* mds_;
    CurveClient client_;
};

TEST_F(CurveClientTest, OpenTest) {
    // filename invalid
    int fd = client_.Open(kWrongFileName, nullptr);
    ASSERT_LT(fd, 0);

    // 第一次open
    std::string sessionId;
    fd = client_.Open(kFileName, &sessionId);
    ASSERT_GE(fd, 0);

    // 第二次open
    int fd2 = client_.Open(kFileName, &sessionId);
    ASSERT_GT(fd2, fd);

    ASSERT_EQ(0, client_.Close(fd));
    ASSERT_EQ(0, client_.Close(fd2));
}

TEST_F(CurveClientTest, StatFileTest) {
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, client_.StatFile(kWrongFileName));
    ASSERT_EQ(kFileSize, client_.StatFile(kFileName));
}

TEST_F(CurveClientTest, StatFileFailedTest) {
    MockFileClient* mockFileClient = new MockFileClient();
    client_.SetFileClient(mockFileClient);
    EXPECT_CALL(*mockFileClient, StatFile(_, _, _))
        .Times(1)
        .WillOnce(Return(-LIBCURVE_ERROR::FAILED));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, client_.StatFile(kFileName));
}

TEST_F(CurveClientTest, ExtendTest) {
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED,
              client_.Extend(kWrongFileName, kNewSize));
    ASSERT_EQ(0, client_.Extend(kFileName, kNewSize));
}

TEST_F(CurveClientTest, AioReadWriteTest) {
    int fd = client_.Open(kFileName, nullptr);
    ASSERT_NE(-1, fd);

    char buffer[kBufSize];

    CurveAioContext aioctx;
    aioctx.buf = buffer;
    aioctx.offset = 0;
    aioctx.length = BUFSIZ;
    aioctx.cb = LibcbdLibcurveTestCallback;

    memset(buffer, 'a', kBufSize);

    aioctx.op = LIBCURVE_OP_WRITE;
    ASSERT_EQ(0, client_.AioWrite(fd, &aioctx));

    while (aioctx.op != LIBCURVE_OP_MAX) {
        usleep(10 * 1000);
    }

    aioctx.op = LIBCURVE_OP_READ;
    ASSERT_EQ(0, client_.AioRead(fd, &aioctx));

    while (aioctx.op != LIBCURVE_OP_MAX) {
        usleep(10  * 1000);
    }

    for (int i = 0; i < kBufSize; ++i) {
        ASSERT_EQ(buffer[i], 'a');
    }

    ASSERT_EQ(0, client_.Close(fd));
}

}  // namespace client
}  // namespace curve

std::string mdsMetaServerAddr = "127.0.0.1:19151";     // NOLINT
uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 4 * 1024 * 1024;   // NOLINT
std::string configpath = "./test/client/testConfig/libcurve_client_test.conf";   // NOLINT

const std::vector<std::string> clientConf {
    std::string("mds.listen.addr=127.0.0.1:19151"),
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

