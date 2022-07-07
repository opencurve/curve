/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Date: Thursday Jul 07 15:18:38 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/volume/block_device_aio.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>

#include "absl/memory/memory.h"
#include "include/client/libcurve.h"
#include "include/client/libcurve_define.h"
#include "test/client/mock/mock_file_client.h"

namespace curvefs {
namespace volume {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using ::curve::client::MockFileClient;
using ::curve::client::UserDataType;

class AioTest : public ::testing::Test {
 public:
    AioTest() : dev_(absl::make_unique<MockFileClient>()) {}

 protected:
    std::unique_ptr<MockFileClient> dev_;
    char data_[4096];
};

TEST_F(AioTest, IssueAlignedError) {
    {
        AioRead read(0, 4096, data_, dev_.get(), 0);

        EXPECT_CALL(*dev_, AioRead(_, _, _)).WillOnce(Return(-1));

        read.Issue();
        EXPECT_GT(0, read.Wait());
    }

    {
        AioWrite write(0, 4096, data_, dev_.get(), 0);

        EXPECT_CALL(*dev_, AioWrite(_, _, _)).WillOnce(Return(-1));

        write.Issue();
        EXPECT_GT(0, write.Wait());
    }
}

TEST_F(AioTest, AioRead_IssueUnAligned) {
    AioRead read(1, 4096, data_, dev_.get(), 0);
    char buffer[8192];
    unsigned int seed = time(nullptr);

    for (auto& c : buffer) {
        c = rand_r(&seed) % 26 + 'a';
    }

    EXPECT_CALL(*dev_, AioRead(_, _, _))
        .WillOnce(Invoke([buffer](int, CurveAioContext* aio, UserDataType) {
            EXPECT_EQ(aio->length, 8192);
            std::memcpy(aio->buf, buffer, aio->length);
            aio->ret = aio->length;
            aio->cb(aio);
            return 0;
        }));

    read.Issue();
    EXPECT_EQ(4096, read.Wait());
    EXPECT_EQ(std::string(data_, 4096), std::string(buffer + 1, 4096));
}

TEST_F(AioTest, AioRead_IssueUnAlignedError) {
    AioRead read(1, 4096, data_, dev_.get(), 0);

    EXPECT_CALL(*dev_, AioRead(_, _, _)).WillOnce(Return(-1));

    read.Issue();
    EXPECT_GT(0, read.Wait());
}

TEST_F(AioTest, AioWrite_IssueUnAlignedError) {
    // padding leading
    {
        AioWrite write(1, 4095, data_, dev_.get(), 0);

        EXPECT_CALL(*dev_, AioRead(_, _, _)).WillOnce(Return(-1));

        write.Issue();
        EXPECT_GT(0, write.Wait());
    }

    // padding both leading and trailing
    // issue trailing padding read error
    {
        auto* write = new AioWrite(1, 12 * 1024, data_, dev_.get(), 0);

        EXPECT_CALL(*dev_, AioRead(_, _, _))
            .WillOnce(Invoke([&](int, CurveAioContext* aio, UserDataType) {
                std::thread th([aio]() {
                    std::this_thread::sleep_for(std::chrono::seconds(3));
                    aio->ret = aio->length;
                    aio->cb(aio);
                });

                th.detach();

                return 0;
            }))
            .WillOnce(Invoke(
                [&](int, CurveAioContext* aio, UserDataType) { return -1; }));

        write->Issue();
        EXPECT_GT(0, write->Wait());
        delete write;

        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
}

}  // namespace volume
}  // namespace curvefs
