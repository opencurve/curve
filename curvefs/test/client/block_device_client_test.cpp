/*
 *  Copyright (c) 2021 NetEase Inc.
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

/**
 * Project: Curve
 * Created Date: 2021-06-15
 * Author: Jingli Chen (Wine93)
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "curvefs/src/client/block_device_client.h"
#include "test/client/mock/mock_file_client.h"

namespace curvefs {
namespace client {

using ::testing::_;
using ::testing::Return;
using ::testing::Invoke;
using ::curve::client::UserInfo;
using ::curve::client::MockFileClient;
using AlignRead = std::pair<off_t, size_t>;
using AlignReads = std::vector<AlignRead>;

class BlockDeviceClientTest : public ::testing::Test {
 protected:
    void SetUp() override {
        options_.configPath = "/etc/curvefs/client.conf";
        options_.volumeName = "/filename_user_";

        fileClient_ = std::make_shared<MockFileClient>();
        client_ = BlockDeviceClientImpl(fileClient_);
    }

    void TearDown() override {}

    static int ReadCallback(int fd, char* buf, off_t offset, size_t length) {
        for (auto i = 0; i < length; i++) {
            buf[i] = '1';
        }
        return LIBCURVE_ERROR::OK;
    }

 protected:
    BlockDeviceClientOptions options_;
    BlockDeviceClientImpl client_;
    std::shared_ptr<MockFileClient> fileClient_;
};

TEST_F(BlockDeviceClientTest, TestInit) {
    // CASE 1: init success
    EXPECT_CALL(*fileClient_, Init(options_.configPath))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
    // TODO(Wine93): check userinfo
    EXPECT_CALL(*fileClient_, Open("/filename", _, _))
        .WillOnce(Return(1));
    ASSERT_EQ(client_.Init(options_), CURVEFS_ERROR::OK);

    // CASE 2: init failed
    EXPECT_CALL(*fileClient_, Init(options_.configPath))
        .WillOnce(Return(LIBCURVE_ERROR::FAILED));
    ASSERT_EQ(client_.Init(options_), CURVEFS_ERROR::FAILED);

    // CASE 3: init failed with open return invalid fd
    EXPECT_CALL(*fileClient_, Init(options_.configPath))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
    EXPECT_CALL(*fileClient_, Open("/filename", _, _))
        .WillOnce(Return(0));
    ASSERT_EQ(client_.Init(options_), CURVEFS_ERROR::FAILED);

    // CASE 4: init failed with invalid volume name
    options_.volumeName = "abcdef012345";
    EXPECT_CALL(*fileClient_, Init(options_.configPath))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
    EXPECT_CALL(*fileClient_, Open(_, _, _))
        .Times(0);
    ASSERT_EQ(client_.Init(options_), CURVEFS_ERROR::FAILED);
}

TEST_F(BlockDeviceClientTest, TestUnInit) {
    // CASE 1: uninit failed with file not open
    ASSERT_EQ(client_.UnInit(), CURVEFS_ERROR::BAD_FD);

    // CASE 2: uninit failed with close file failed
    EXPECT_CALL(*fileClient_, Init(options_.configPath))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
    EXPECT_CALL(*fileClient_, Open("/filename", _, _))
        .WillOnce(Return(10));
    ASSERT_EQ(client_.Init(options_), CURVEFS_ERROR::OK);

    EXPECT_CALL(*fileClient_, Close(10))
        .WillOnce(Return(LIBCURVE_ERROR::FAILED));
    EXPECT_CALL(*fileClient_, UnInit())
        .Times(0);
    ASSERT_EQ(client_.UnInit(), CURVEFS_ERROR::FAILED);

    // CASE 3: uninit success
    EXPECT_CALL(*fileClient_, Close(10))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
    EXPECT_CALL(*fileClient_, UnInit())
        .Times(1);
    ASSERT_EQ(client_.UnInit(), CURVEFS_ERROR::OK);
}

TEST_F(BlockDeviceClientTest, TestReadBasic) {
    char buf[4096];

    // CASE 1: read failed with file not open
    ASSERT_EQ(client_.Read(buf, 0, 4096), CURVEFS_ERROR::BAD_FD);

    // CASE 2: read failed
    EXPECT_CALL(*fileClient_, Init(options_.configPath))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
    EXPECT_CALL(*fileClient_, Open("/filename", _, _))
        .WillOnce(Return(10));
    ASSERT_EQ(client_.Init(options_), CURVEFS_ERROR::OK);

    EXPECT_CALL(*fileClient_, Read(10, buf, 0, 4096))
        .WillOnce(Return(LIBCURVE_ERROR::FAILED));
    ASSERT_EQ(client_.Read(buf, 0, 4096), CURVEFS_ERROR::FAILED);

    // CASE 3: read success with length is zero
    EXPECT_CALL(*fileClient_, Read(_, _, _, _))
        .Times(0);
    ASSERT_EQ(client_.Read(buf, 0, 0), CURVEFS_ERROR::OK);

    // CASE 4: read success with aligned offset and length
    EXPECT_CALL(*fileClient_, Read(10, buf, 0, 4096))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
    ASSERT_EQ(client_.Read(buf, 0, 4096), CURVEFS_ERROR::OK);
}

TEST_F(BlockDeviceClientTest, TestReadWithUnAligned) {
    auto TEST_READ = [this](off_t offset, size_t length,
                            off_t alignOffset, size_t alignLength) {
        char buf[40960];
        memset(buf, '0', sizeof(buf));

        EXPECT_CALL(*fileClient_, Read(10, _, alignOffset, alignLength))
            .WillOnce(Invoke(ReadCallback));

        ASSERT_EQ(client_.Read(buf, offset, length), CURVEFS_ERROR::OK);
        for (auto i = 0; i < 40960; i++) {
            ASSERT_EQ(buf[i], i < length ? '1' : '0');
        }
    };

    // Prepare: open file
    EXPECT_CALL(*fileClient_, Init(options_.configPath))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
    EXPECT_CALL(*fileClient_, Open("/filename", _, _))
        .WillOnce(Return(10));
    ASSERT_EQ(client_.Init(options_), CURVEFS_ERROR::OK);

    // Test Cases: read success
    {
        TEST_READ(0, 1, 0, 4096);              // offset = 0,     length = 1
        TEST_READ(1, 4095, 0, 4096);           // offset = 1,     length = 4095
        TEST_READ(1, 4096, 0, 8192);           // offset = 1,     length = 4096
        TEST_READ(1000, 5000, 0, 8192);        // offset = 1000,  length = 5000
        TEST_READ(4096, 5000, 4096, 8192);     // offset = 4096,  length = 5000
        TEST_READ(10000, 10000, 8192, 12288);  // offset = 10000, length = 10000
    }

    // Test Cases: read failed
    {
        char buf[4096];
        memset(buf, '0', sizeof(buf));

        EXPECT_CALL(*fileClient_, Read(10, _, 0, 4096))
            .WillOnce(Return(LIBCURVE_ERROR::FAILED));
        ASSERT_EQ(client_.Read(buf, 0, 1), CURVEFS_ERROR::FAILED);
        for (auto i = 0; i < 4096; i++) {
            ASSERT_EQ(buf[i], '0');
        }
    }
}

TEST_F(BlockDeviceClientTest, TestWriteBasic) {
    char buf[4096];

    // CASE 1: write failed with file not open
    ASSERT_EQ(client_.Write(buf, 0, 4096), CURVEFS_ERROR::BAD_FD);

    // CASE 2: write failed
    EXPECT_CALL(*fileClient_, Init(options_.configPath))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
    EXPECT_CALL(*fileClient_, Open("/filename", _, _))
        .WillOnce(Return(10));
    ASSERT_EQ(client_.Init(options_), CURVEFS_ERROR::OK);

    EXPECT_CALL(*fileClient_, Write(10, buf, 0, 4096))
        .WillOnce(Return(LIBCURVE_ERROR::FAILED));
    ASSERT_EQ(client_.Write(buf, 0, 4096), CURVEFS_ERROR::FAILED);

    // CASE 3: write success with length is zero
    EXPECT_CALL(*fileClient_, Write(10, buf, 0, 4096))
        .Times(0);
    ASSERT_EQ(client_.Write(buf, 0, 0), CURVEFS_ERROR::OK);

    // CASE 4: write success with aligned offset and length
    EXPECT_CALL(*fileClient_, Write(10, buf, 0, 4096))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
    ASSERT_EQ(client_.Write(buf, 0, 4096), CURVEFS_ERROR::OK);
}

TEST_F(BlockDeviceClientTest, TestWriteWithUnAligned) {
    auto TEST_WRITE = [this](off_t offset, size_t length,
                             off_t alignOffset, size_t alignLength,
                             AlignReads&& alignReads) {
        // Prepare write buffer
        char buf[40960], writeBuffer[40960];
        memset(buf, '0', sizeof(buf));
        memset(writeBuffer, '0', sizeof(writeBuffer));
        for (auto i = 0; i < length; i++) {
            buf[i] = '2';
        }

        // Align read
        for (auto& alignRead : alignReads) {
            auto readOffset = alignRead.first;
            auto readLength = alignRead.second;
            EXPECT_CALL(*fileClient_, Read(10, _, readOffset, readLength))
                .WillOnce(Invoke(ReadCallback));
        }

        // Align write
        EXPECT_CALL(*fileClient_, Write(10, _, alignOffset, alignLength))
            .WillOnce(Invoke([&](int fd, const char* buf,
                                 off_t offset, size_t length) {
                memcpy(writeBuffer, buf, length);
                return LIBCURVE_ERROR::OK;
            }));

        ASSERT_EQ(client_.Write(buf, offset, length), CURVEFS_ERROR::OK);

        // Check write buffer
        auto count = 0;
        for (auto i = 0; i < alignLength; i++) {
            auto pos = i + alignOffset;
            if (pos >= offset && pos < offset + length) {
                count++;
                ASSERT_EQ(writeBuffer[i], '2');
            } else {
                ASSERT_EQ(writeBuffer[i], '1');
            }
        }

        ASSERT_EQ(count, length);
    };

    // Prepare: open file
    EXPECT_CALL(*fileClient_, Init(options_.configPath))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
    EXPECT_CALL(*fileClient_, Open("/filename", _, _))
        .WillOnce(Return(10));
    ASSERT_EQ(client_.Init(options_), CURVEFS_ERROR::OK);

    // Test Cases: write success
    {
        TEST_WRITE(0, 1, 0, 4096,
                   AlignReads{ AlignRead(0, 4096) });
        TEST_WRITE(1, 4095, 0, 4096,
                   AlignReads{ AlignRead(0, 4096) });
        TEST_WRITE(1, 4096, 0, 8192,
                   AlignReads{ AlignRead(0, 4096), AlignRead(4096, 4096)});
        TEST_WRITE(1000, 5000, 0, 8192,
                   AlignReads{ AlignRead(0, 4096), AlignRead(4096, 4096)});
        TEST_WRITE(4096, 5000, 4096, 8192,
                   AlignReads{ AlignRead(8192, 4096)});
        TEST_WRITE(10000, 10000, 8192, 12288,
                   AlignReads{ AlignRead(8192, 4096), AlignRead(16384, 4096)});
    }

    // Test Cases: write failed
    {
        char buf[4096];
        memset(buf, '0', sizeof(buf));

        // CASE 1: read failed -> write failed
        EXPECT_CALL(*fileClient_, Read(10, _, 0, 4096))
            .WillOnce(Return(LIBCURVE_ERROR::FAILED));
        EXPECT_CALL(*fileClient_, Write(_, _, _, _))
            .Times(0);
        ASSERT_EQ(client_.Write(buf, 0, 1), CURVEFS_ERROR::FAILED);

        // CASE 2: read failed once -> write failed
        EXPECT_CALL(*fileClient_, Read(10, _, 0, 4096))
            .WillOnce(Return(LIBCURVE_ERROR::OK));
        EXPECT_CALL(*fileClient_, Read(10, _, 4096, 4096))
            .WillOnce(Return(LIBCURVE_ERROR::FAILED));
        EXPECT_CALL(*fileClient_, Write(_, _, _, _))
            .Times(0);
        ASSERT_EQ(client_.Write(buf, 1000, 5000), CURVEFS_ERROR::FAILED);

        // CASE 3: write failed
        EXPECT_CALL(*fileClient_, Read(10, _, 0, 4096))
            .WillOnce(Return(LIBCURVE_ERROR::OK));
        EXPECT_CALL(*fileClient_, Write(_, _, _, _))
            .WillOnce(Return(LIBCURVE_ERROR::FAILED));
        ASSERT_EQ(client_.Write(buf, 0, 1), CURVEFS_ERROR::FAILED);
    }
}

}  // namespace client
}  // namespace curvefs

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, false);

    return RUN_ALL_TESTS();
}
