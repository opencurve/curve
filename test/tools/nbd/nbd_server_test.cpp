/**
 * Project: curve
 * Date: Fri Apr 24 12:35:49 CST 2020
 * Author: wuhanqing
 * Copyright (c) 2020 Netease
 */

#include <arpa/inet.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <memory>
#include "src/tools/nbd/NBDServer.h"
#include "test/tools/nbd/fake_safe_io.h"
#include "test/tools/nbd/mock_image_instance.h"
#include "test/tools/nbd/mock_safe_io.h"

namespace curve {
namespace nbd {

using ::testing::_;
using ::testing::DoAll;
using ::testing::ElementsAre;
using ::testing::Invoke;
using ::testing::NotNull;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;

const int64_t kSleepTime = 500;  // ms

class NBDServerTest : public ::testing::Test {
 public:
    void SetUp() override {
        ASSERT_NE(-1, socketpair(AF_UNIX, SOCK_STREAM, 0, fd_));

        image_ = std::make_shared<MockImageInstance>();
        safeIO_ = std::make_shared<MockSafeIO>();
        server_.reset(new NBDServer(fd_[1], nullptr, image_));
    }

    void TearDown() override {
        ::shutdown(fd_[0], SHUT_RDWR);
    }

 protected:
    int fd_[2];
    std::shared_ptr<MockImageInstance> image_;
    std::shared_ptr<MockSafeIO> safeIO_;

    std::unique_ptr<NBDServer> server_;
    char handle_[8] = {0};

    struct nbd_request request_;
    struct nbd_reply reply_;

    const uint32_t NBDRequestSize = sizeof(request_);
    const uint32_t NBDReplySize = sizeof(reply_);
};

TEST_F(NBDServerTest, InvalidCommandTypeTest) {
    ASSERT_NO_THROW(server_->Start());

    request_.from = 0;
    request_.len = htonl(8);
    request_.type = htonl(100);  // 设置一个无效的请求类型
    request_.magic = htonl(NBD_REQUEST_MAGIC);
    memcpy(&request_.handle, &handle_, sizeof(request_.handle));

    EXPECT_CALL(*image_, AioRead(_))
        .Times(0);

    ASSERT_EQ(NBDRequestSize, write(fd_[0], &request_, NBDRequestSize));

    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepTime));

    ASSERT_TRUE(server_->IsTerminated());
}

TEST_F(NBDServerTest, InvalidRequestMagicTest) {
    ASSERT_NO_THROW(server_->Start());

    request_.from = 0;
    request_.len = htonl(8);
    request_.type = htonl(NBD_CMD_READ);
    request_.magic = htonl(0);  // 设置一个错误的magic
    memcpy(&request_.handle, &handle_, sizeof(request_.handle));

    EXPECT_CALL(*image_, AioRead(_))
        .Times(0);

    ASSERT_EQ(NBDRequestSize, write(fd_[0], &request_, NBDRequestSize));

    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepTime));

    ASSERT_TRUE(server_->IsTerminated());
}

TEST_F(NBDServerTest, ReadRequestErrorTest) {
    auto fakeSafeIO = std::make_shared<FakeSafeIO>();
    server_.reset(new NBDServer(fd_[1], nullptr, image_, fakeSafeIO));

    fakeSafeIO->SetReadExactTask(
        [](int fd, void* buf, size_t count) { return -1; });

    ASSERT_NO_THROW(server_->Start());

    EXPECT_CALL(*image_, AioRead(_))
        .Times(0);

    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepTime));

    ASSERT_TRUE(server_->IsTerminated());
}

TEST_F(NBDServerTest, AioReadTest) {
    ASSERT_NO_THROW(server_->Start());

    request_.from = 0;
    request_.len = htonl(8);
    request_.type = htonl(NBD_CMD_READ);
    request_.magic = htonl(NBD_REQUEST_MAGIC);
    memcpy(&request_.handle, &handle_, sizeof(request_.handle));

    NebdClientAioContext* nebdContext;
    EXPECT_CALL(*image_, AioRead(_))
        .Times(1)
        .WillOnce(SaveArg<0>(&nebdContext));

    ASSERT_EQ(NBDRequestSize, write(fd_[0], &request_, NBDRequestSize));

    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepTime));

    ASSERT_EQ(nebdContext->offset, request_.from);
    ASSERT_EQ(nebdContext->length, ntohl(request_.len));
    ASSERT_EQ(nebdContext->op, LIBAIO_OP::LIBAIO_OP_READ);

    memcpy(nebdContext->buf, handle_, sizeof(handle_));
    nebdContext->cb(nebdContext);

    char readbuf[8];
    ASSERT_EQ(NBDReplySize, read(fd_[0], &reply_, NBDReplySize));
    ASSERT_EQ(sizeof(readbuf), read(fd_[0], readbuf, sizeof(readbuf)));

    ASSERT_EQ(0, memcmp(readbuf, handle_, sizeof(handle_)));
}

TEST_F(NBDServerTest, AioWriteTest) {
    ASSERT_NO_THROW(server_->Start());

    request_.from = 0;
    request_.len = htonl(8);
    request_.type = htonl(NBD_CMD_WRITE);
    request_.magic = htonl(NBD_REQUEST_MAGIC);
    memcpy(&request_.handle, &handle_, sizeof(request_.handle));

    NebdClientAioContext* nebdContext;
    EXPECT_CALL(*image_, AioWrite(_))
        .Times(1)
        .WillOnce(SaveArg<0>(&nebdContext));

    ASSERT_EQ(NBDRequestSize, write(fd_[0], &request_, NBDRequestSize));
    ASSERT_EQ(8, write(fd_[0], "hello, world", 8));

    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepTime));

    ASSERT_EQ(nebdContext->offset, request_.from);
    ASSERT_EQ(nebdContext->length, ntohl(request_.len));
    ASSERT_EQ(nebdContext->op, LIBAIO_OP::LIBAIO_OP_WRITE);

    memcpy(nebdContext->buf, handle_, sizeof(handle_));
    nebdContext->cb(nebdContext);

    ASSERT_EQ(NBDReplySize, read(fd_[0], &reply_, NBDReplySize));
    ASSERT_EQ(0, reply_.error);
}

TEST_F(NBDServerTest, FlushTest) {
    ASSERT_NO_THROW(server_->Start());

    request_.from = 0;
    request_.len = htonl(8);
    request_.type = htonl(NBD_CMD_FLUSH);
    request_.magic = htonl(NBD_REQUEST_MAGIC);
    memcpy(&request_.handle, &handle_, sizeof(request_.handle));

    NebdClientAioContext* nebdContext;
    EXPECT_CALL(*image_, Flush(_))
        .Times(1)
        .WillOnce(SaveArg<0>(&nebdContext));

    ASSERT_EQ(NBDRequestSize, write(fd_[0], &request_, NBDRequestSize));

    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepTime));

    ASSERT_EQ(nebdContext->op, LIBAIO_OP::LIBAIO_OP_FLUSH);
    nebdContext->cb(nebdContext);

    ASSERT_EQ(NBDReplySize, read(fd_[0], &reply_, NBDReplySize));
    ASSERT_EQ(0, reply_.error);
}

TEST_F(NBDServerTest, TrimTest) {
    ASSERT_NO_THROW(server_->Start());

    request_.from = 0;
    request_.len = htonl(8);
    request_.type = htonl(NBD_CMD_TRIM);
    request_.magic = htonl(NBD_REQUEST_MAGIC);
    memcpy(&request_.handle, &handle_, sizeof(request_.handle));

    NebdClientAioContext* nebdContext;
    EXPECT_CALL(*image_, Trim(_))
        .Times(1)
        .WillOnce(SaveArg<0>(&nebdContext));

    ASSERT_EQ(NBDRequestSize, write(fd_[0], &request_, NBDRequestSize));

    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepTime));

    ASSERT_EQ(nebdContext->op, LIBAIO_OP::LIBAIO_OP_DISCARD);
    nebdContext->cb(nebdContext);

    struct nbd_reply reply;
    ASSERT_EQ(sizeof(reply), read(fd_[0], &reply, sizeof(reply)));
    ASSERT_EQ(0, reply.error);
}

TEST_F(NBDServerTest, DisconnectTest) {
    ASSERT_NO_THROW(server_->Start());

    request_.from = 0;
    request_.len = htonl(8);
    request_.type = htonl(NBD_CMD_DISC);
    request_.magic = htonl(NBD_REQUEST_MAGIC);
    memcpy(&request_.handle, &handle_, sizeof(request_.handle));

    ASSERT_EQ(NBDRequestSize, write(fd_[0], &request_, NBDRequestSize));

    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepTime));

    ASSERT_TRUE(server_->IsTerminated());
}

TEST_F(NBDServerTest, ReadWriteDataErrorTest) {
    auto fakeSafeIO = std::make_shared<FakeSafeIO>();
    server_.reset(new NBDServer(fd_[1], nullptr, image_, fakeSafeIO));

    request_.from = 0;
    request_.len = htonl(8);
    request_.type = htonl(NBD_CMD_WRITE);
    request_.magic = htonl(NBD_REQUEST_MAGIC);
    memcpy(&request_.handle, &handle_, sizeof(request_.handle));

    EXPECT_CALL(*image_, AioWrite(_))
        .Times(0);

    auto task = [this](int fd, void* buf, size_t count) {
        static int callTime = 1;
        if (callTime++ == 1) {
            *reinterpret_cast<struct nbd_request*>(buf) = request_;
            return 0;
        } else {
            return -1;
        }
    };

    fakeSafeIO->SetReadExactTask(task);

    ASSERT_NO_THROW(server_->Start());

    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepTime));

    ASSERT_TRUE(server_->IsTerminated());
}

}  // namespace nbd
}  // namespace curve
