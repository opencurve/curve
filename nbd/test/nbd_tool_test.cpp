/*
 *     Copyright (c) 2020 NetEase Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

/*
 * Project: curve
 * Created Date: Sunday April 26th 2020
 * Author: yangyaokai
 */

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <memory>
#include "nbd/test/mock_image_instance.h"
#include "nbd/src/NBDTool.h"
#include "nbd/src/util.h"

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

const std::string kTestImage = "test:/test";  // NOLINT
const int64_t kGB = 1024 * 1024 * 1024;

class NBDToolTest : public ::testing::Test {
 public:
    void SetUp() override {
        image_ = std::make_shared<MockImageInstance>();
        g_test_image = image_;
    }

    void TearDown() override {
        nbdThread_.join();
    }

    void StartInAnotherThread(NBDConfig* config) {
        auto task = [](NBDConfig* config, bool* isRunning, NBDTool* tool) {
            *isRunning = true;
            if (0 == (*tool).Connect(config)) {
                (*tool).RunServerUntilQuit();
            }
            *isRunning = false;
        };
        EXPECT_CALL(*image_, Open())
            .Times(1)
            .WillRepeatedly(Return(true));
        EXPECT_CALL(*image_, GetImageSize())
            .WillRepeatedly(Return(1 * kGB));
        NebdClientAioContext aioCtx;
        EXPECT_CALL(*image_, AioRead(_))
            .WillRepeatedly(Invoke([](NebdClientAioContext* context){
                context->cb(context);
            }));
        nbdThread_ = std::thread(task, config, &isRunning_, &tool_);

        // 确保nbd device加载起来
        sleep(3);
    }

    void AssertWriteSuccess(const std::string& devpath) {
        char buf[4096];
        NebdClientAioContext tempContext;
        EXPECT_CALL(*image_, AioWrite(_))
            .WillOnce(Invoke([&](NebdClientAioContext* context){
                tempContext = *context;
                context->cb(context);
            }));
        EXPECT_CALL(*image_, Flush(_))
            .WillOnce(Invoke([](NebdClientAioContext* context){
                context->cb(context);
            }));

        int fd = open(devpath.c_str(), O_RDWR | O_SYNC);
        ASSERT_GT(fd, 0);
        ASSERT_EQ(4096, pwrite(fd, buf, 4096, 4096));
        ASSERT_EQ(tempContext.offset, 4096);
        ASSERT_EQ(tempContext.length, 4096);
        ASSERT_EQ(tempContext.op, LIBAIO_OP::LIBAIO_OP_WRITE);
        close(fd);
    }

    void AssertWriteTimeout(const std::string& devpath, int timeout) {
        std::thread timeoutThread;
        char buf[4096];
        EXPECT_CALL(*image_, AioWrite(_))
            .WillOnce(Invoke([&](NebdClientAioContext* context){
                auto callback = [&](NebdClientAioContext* ctx) {
                    sleep(timeout);
                    ctx->cb(ctx);
                };
                timeoutThread = std::thread(callback, context);
            }));

        int fd = open(devpath.c_str(), O_RDWR | O_SYNC);
        ASSERT_GT(fd, 0);
        ASSERT_EQ(-1, pwrite(fd, buf, 4096, 4096));
        close(fd);
        timeoutThread.join();
    }

    void AssertWriteFailed(const std::string& devpath) {
        char buf[4096];
        int fd = open(devpath.c_str(), O_RDWR | O_SYNC);
        ASSERT_GT(fd, 0);
        ASSERT_EQ(-1, pwrite(fd, buf, 4096, 4096));
        close(fd);
    }

 protected:
    NBDTool tool_;
    bool isRunning_ = false;
    std::thread nbdThread_;
    std::shared_ptr<MockImageInstance> image_;
};

TEST_F(NBDToolTest, ioctl_connect_test) {
    NBDConfig config;
    config.imgname = kTestImage;
    StartInAnotherThread(&config);
    ASSERT_TRUE(isRunning_);
    AssertWriteSuccess(config.devpath);
    ASSERT_EQ(0, tool_.Disconnect(&config));
    sleep(1);
    ASSERT_FALSE(isRunning_);
}

TEST_F(NBDToolTest, netlink_connect_test) {
    NBDConfig config;
    config.imgname = kTestImage;
    config.try_netlink = true;
    StartInAnotherThread(&config);
    ASSERT_TRUE(isRunning_);
    ASSERT_EQ(0, tool_.Disconnect(&config));
    sleep(1);
    ASSERT_FALSE(isRunning_);
}

TEST_F(NBDToolTest, readonly_test) {
    NBDConfig config;
    config.imgname = kTestImage;
    config.readonly = true;
    StartInAnotherThread(&config);
    ASSERT_TRUE(isRunning_);
    AssertWriteFailed(config.devpath);
    ASSERT_EQ(0, tool_.Disconnect(&config));
    sleep(1);
    ASSERT_FALSE(isRunning_);
}

TEST_F(NBDToolTest, timeout_test) {
    NBDConfig config;
    config.imgname = kTestImage;
    config.timeout = 3;
    StartInAnotherThread(&config);
    ASSERT_TRUE(isRunning_);
    AssertWriteTimeout(config.devpath, 5);
    // io timeout 情况下，nbd_do_it这边会退出，所以这里disconnect会失败
    ASSERT_EQ(-1, tool_.Disconnect(&config));
}

}  // namespace nbd
}  // namespace curve
