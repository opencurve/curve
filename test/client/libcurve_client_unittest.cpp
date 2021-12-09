/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: curve
 * File Created: 2020-02-04 15:37
 * Author: wuhanqing
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>

#include "include/client/libcurve.h"
#include "src/client/client_common.h"
#include "test/client/mock/mock_file_client.h"

namespace curve {
namespace client {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArgPointee;

const char* kWrongFileName = "xxxxx";
const char* kValidFileName = "/filename_user_";

constexpr uint64_t kTiB = 1ull * 1024 * 1024 * 1024 * 1024;

class CurveClientTest : public ::testing::Test {
 protected:
    void SetUp() override {
        mockFileClient_ = new MockFileClient();
        client_.SetFileClient(mockFileClient_);
    }

    void TearDown() override {}

    CurveClient client_;
    MockFileClient* mockFileClient_;
};

TEST_F(CurveClientTest, TestInit) {
    {
        EXPECT_CALL(*mockFileClient_, Init(_))
            .WillOnce(Return(LIBCURVE_ERROR::OK));

        ASSERT_EQ(LIBCURVE_ERROR::OK, client_.Init("/etc/curve/client.conf"));
    }

    {
        EXPECT_CALL(*mockFileClient_, Init(_))
            .WillOnce(Return(-LIBCURVE_ERROR::FAILED));

        ASSERT_EQ(-LIBCURVE_ERROR::FAILED,
                  client_.Init("/etc/curve/client.conf"));
    }
}

TEST_F(CurveClientTest, TestUnInit) {
    EXPECT_CALL(*mockFileClient_, UnInit())
        .Times(1);

    ASSERT_NO_FATAL_FAILURE(client_.UnInit());
}

TEST_F(CurveClientTest, TestOpen) {
    // parse filename and user info failed
    {
        EXPECT_CALL(*mockFileClient_, Open(_, _, _))
            .Times(0);

        ASSERT_EQ(-LIBCURVE_ERROR::FAILED,
                  client_.Open(kWrongFileName, {}));
    }

    // open success
    {
        EXPECT_CALL(*mockFileClient_, Open(_, _, _))
            .WillOnce(Return(1));

        ASSERT_EQ(1, client_.Open(kValidFileName, {}));
    }
}

TEST_F(CurveClientTest, TestClose) {
    EXPECT_CALL(*mockFileClient_, Close(_))
        .WillOnce(Return(LIBCURVE_ERROR::OK));

    ASSERT_EQ(LIBCURVE_ERROR::OK, client_.Close(0));
}

TEST_F(CurveClientTest, TestExtend) {
    // parse filename and user info failed
    {
        EXPECT_CALL(*mockFileClient_, Extend(_, _, _))
            .Times(0);

        ASSERT_EQ(-LIBCURVE_ERROR::FAILED,
                  client_.Extend(kWrongFileName, 1 * kTiB));
    }

    // extend success
    {
        EXPECT_CALL(*mockFileClient_, Extend(_, _, _))
            .Times(1);

        ASSERT_EQ(LIBCURVE_ERROR::OK, client_.Extend(kValidFileName, 1 * kTiB));
    }
}

TEST_F(CurveClientTest, TestStatFile) {
    // parse filename and user info failed
    {
        EXPECT_CALL(*mockFileClient_, StatFile(_, _, _))
            .Times(0);

        ASSERT_EQ(-LIBCURVE_ERROR::FAILED, client_.StatFile(kWrongFileName));
    }

    // statfile return failed
    {
        FileStatInfo info;
        info.length = 1 * kTiB;

        EXPECT_CALL(*mockFileClient_, StatFile(_, _, _))
            .WillOnce(
                DoAll(SetArgPointee<2>(info), Return(-LIBCURVE_ERROR::FAILED)));

        ASSERT_EQ(-LIBCURVE_ERROR::FAILED, client_.StatFile(kValidFileName));
    }

    // statfile success
    {
        FileStatInfo info;
        info.length = 1 * kTiB;
        EXPECT_CALL(*mockFileClient_, StatFile(_, _, _))
            .WillOnce(
                DoAll(SetArgPointee<2>(info), Return(LIBCURVE_ERROR::OK)));

        ASSERT_EQ(1 * kTiB, client_.StatFile(kValidFileName));
    }
}

TEST_F(CurveClientTest, TestAioRead) {
    // aio read call failed
    {
        EXPECT_CALL(*mockFileClient_, AioRead(_, _, _))
            .WillOnce(Return(-LIBCURVE_ERROR::FAILED));

        CurveAioContext aioctx;
        ASSERT_EQ(-LIBCURVE_ERROR::FAILED,
                  client_.AioRead(0, &aioctx, UserDataType::RawBuffer));
    }

    // aio read call success
    {
        EXPECT_CALL(*mockFileClient_, AioRead(_, _, _))
            .WillOnce(Return(LIBCURVE_ERROR::OK));

        CurveAioContext aioctx;
        ASSERT_EQ(-LIBCURVE_ERROR::OK,
                  client_.AioRead(0, &aioctx, UserDataType::RawBuffer));
    }
}

TEST_F(CurveClientTest, TestAioWrite) {
    // aio write call failed
    {
        EXPECT_CALL(*mockFileClient_, AioWrite(_, _, _))
            .WillOnce(Return(-LIBCURVE_ERROR::FAILED));

        CurveAioContext aioctx;
        ASSERT_EQ(-LIBCURVE_ERROR::FAILED,
                  client_.AioWrite(0, &aioctx, UserDataType::RawBuffer));
    }

    // aio write call success
    {
        EXPECT_CALL(*mockFileClient_, AioWrite(_, _, _))
            .WillOnce(Return(LIBCURVE_ERROR::OK));

        CurveAioContext aioctx;
        ASSERT_EQ(-LIBCURVE_ERROR::OK,
                  client_.AioWrite(0, &aioctx, UserDataType::RawBuffer));
    }
}

}  // namespace client
}  // namespace curve

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, false);

    return RUN_ALL_TESTS();
}
