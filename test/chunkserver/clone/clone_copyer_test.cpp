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

/*
 * Project: curve
 * Created Date: Wednesday April 10th 2019
 * Author: yangyaokai
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <glog/logging.h>

#include "include/client/libcurve.h"
#include "src/chunkserver/clone_copyer.h"
#include "src/chunkserver/clone_core.h"
#include "test/chunkserver/clone/clone_test_util.h"
#include "test/client/mock/mock_file_client.h"
#include "test/common/mock_s3_adapter.h"

namespace curve {
namespace chunkserver {

using curve::client::MockFileClient;
using curve::common::MockS3Adapter;

const char CURVE_CONF[] = "client.conf";
const char S3_CONF[] = "s3.conf";
const char ROOT_OWNER[] = "root";
const char ROOT_PWD[] = "pwd";
const uint64_t EXPIRED_USE = 5;

class MockDownloadClosure : public DownloadClosure {
 public:
    explicit MockDownloadClosure(AsyncDownloadContext* context)
        : DownloadClosure(nullptr, nullptr, context, nullptr)
        , isRun_(false) {}

    void Run() {
        CHECK(!isRun_) << "closure has been invoked.";
        isRun_ = true;
    }

    bool IsFailed() {
        return isFailed_;
    }

    bool IsRun() {
        return isRun_;
    }

    void Reset() {
        isFailed_ = false;
        isRun_ = false;
    }

 private:
    bool isRun_;
};

class CloneCopyerTest : public testing::Test  {
 public:
    void SetUp() {
        curveClient_ = std::make_shared<MockFileClient>();
        s3Client_ = std::make_shared<MockS3Adapter>();
        Aws::InitAPI(awsOptions_);
    }
    void TearDown() {
        Aws::ShutdownAPI(awsOptions_);
    }

 protected:
    std::shared_ptr<MockFileClient> curveClient_;
    std::shared_ptr<MockS3Adapter> s3Client_;
    Aws::SDKOptions awsOptions_;
};

TEST_F(CloneCopyerTest, BasicTest) {
    OriginCopyer copyer;
    CopyerOptions options;
    options.curveConf = CURVE_CONF;
    options.s3Conf = S3_CONF;
    options.curveUser.owner = ROOT_OWNER;
    options.curveUser.password = ROOT_PWD;
    options.curveClient = curveClient_;
    options.s3Client = s3Client_;
    options.curveFileTimeoutSec = EXPIRED_USE;
    // init test
    {
        // curvefs init failed
        EXPECT_CALL(*curveClient_, Init(StrEq(CURVE_CONF)))
            .WillOnce(Return(LIBCURVE_ERROR::FAILED));
        ASSERT_EQ(-1, copyer.Init(options));

        // curvefs init success
        EXPECT_CALL(*curveClient_, Init(StrEq(CURVE_CONF)))
            .WillOnce(Return(LIBCURVE_ERROR::OK));
        ASSERT_EQ(0, copyer.Init(options));
    }
    // Download test
    {
        char* buf = new char[4096];
        AsyncDownloadContext context;
        context.offset = 0;
        context.size = 4096;
        context.buf = buf;
        MockDownloadClosure closure(&context);

        // invalid location
        context.location = "aaaaa";
        copyer.DownloadAsync(&closure);
        ASSERT_TRUE(closure.IsRun());
        ASSERT_TRUE(closure.IsFailed());
        closure.Reset();

        // invalid location
        context.location = "aaaaa@cs";
        copyer.DownloadAsync(&closure);
        ASSERT_TRUE(closure.IsRun());
        ASSERT_TRUE(closure.IsFailed());
        closure.Reset();

        /* 用例:读curve上的数据，读取成功
         * 预期:调用Open和Read读取数据
         */
        context.location = "test:0@cs";
        EXPECT_CALL(*curveClient_, Open4ReadOnly("test", _, true))
            .WillOnce(Return(1));
        EXPECT_CALL(*curveClient_, AioRead(_, _, _))
            .WillOnce(Invoke([](int fd, CurveAioContext* context,
                                curve::client::UserDataType dataType) {
                context->ret = 1024;
                context->cb(context);
                return LIBCURVE_ERROR::OK;
            }));
        copyer.DownloadAsync(&closure);
        ASSERT_TRUE(closure.IsRun());
        ASSERT_FALSE(closure.IsFailed());
        closure.Reset();

        /* 用例:再次读前面的文件,但是ret值为-1
         * 预期:直接Read，返回失败
         */
        context.location = "test:0@cs";
        EXPECT_CALL(*curveClient_, Open4ReadOnly(_, _, true))
            .Times(0);
        EXPECT_CALL(*curveClient_, AioRead(_, _, _))
            .WillOnce(Invoke([](int fd, CurveAioContext* context,
                                curve::client::UserDataType dataType) {
                context->ret = -1;
                context->cb(context);
                return LIBCURVE_ERROR::OK;
            }));
        copyer.DownloadAsync(&closure);
        ASSERT_TRUE(closure.IsRun());
        ASSERT_TRUE(closure.IsFailed());
        closure.Reset();

        /* 用例:读curve上的数据，Open的时候失败
         * 预期:返回-1
         */
        context.location = "test2:0@cs";
        EXPECT_CALL(*curveClient_, Open4ReadOnly("test2", _, true))
            .WillOnce(Return(-1));
        EXPECT_CALL(*curveClient_, AioRead(_, _, _))
            .Times(0);
        copyer.DownloadAsync(&closure);
        ASSERT_TRUE(closure.IsRun());
        ASSERT_TRUE(closure.IsFailed());
        closure.Reset();

        /* 用例:读curve上的数据，Read的时候失败
         * 预期:返回-1
         */
        context.location = "test2:0@cs";
        EXPECT_CALL(*curveClient_, Open4ReadOnly("test2", _, true))
            .WillOnce(Return(2));
        EXPECT_CALL(*curveClient_, AioRead(_, _, _))
            .WillOnce(Return(-1 * LIBCURVE_ERROR::FAILED));
        copyer.DownloadAsync(&closure);
        ASSERT_TRUE(closure.IsRun());
        ASSERT_TRUE(closure.IsFailed());
        closure.Reset();


        /* 用例:读s3上的数据，读取成功
         * 预期:返回0
         */
        context.location = "test@s3";
        EXPECT_CALL(*s3Client_, GetObjectAsync(_))
            .WillOnce(Invoke(
                [&] (const std::shared_ptr<GetObjectAsyncContext>& context) {
                    context->retCode = 0;
                    context->cb(s3Client_.get(), context);
                }));
        copyer.DownloadAsync(&closure);
        ASSERT_TRUE(closure.IsRun());
        ASSERT_FALSE(closure.IsFailed());
        closure.Reset();

        /* 用例:读s3上的数据，读取失败
         * 预期:返回-1
         */
        context.location = "test@s3";
        EXPECT_CALL(*s3Client_, GetObjectAsync(_))
            .WillOnce(Invoke(
                [&] (const std::shared_ptr<GetObjectAsyncContext>& context) {
                    context->retCode = -1;
                    context->cb(s3Client_.get(), context);
                }));
        copyer.DownloadAsync(&closure);
        ASSERT_TRUE(closure.IsRun());
        ASSERT_TRUE(closure.IsFailed());
        closure.Reset();

        delete [] buf;
    }
    // fini test
    {
        EXPECT_CALL(*curveClient_, Close(1))
            .Times(1);
        EXPECT_CALL(*curveClient_, Close(2))
            .Times(1);
        EXPECT_CALL(*curveClient_, UnInit())
            .Times(1);
        EXPECT_CALL(*s3Client_, Deinit())
            .Times(1);
        ASSERT_EQ(0, copyer.Fini());
    }
}

TEST_F(CloneCopyerTest, DisableTest) {
    OriginCopyer copyer;
    CopyerOptions options;
    options.curveConf = CURVE_CONF;
    options.s3Conf = S3_CONF;
    options.curveUser.owner = ROOT_OWNER;
    options.curveUser.password = ROOT_PWD;
    options.curveFileTimeoutSec = EXPIRED_USE;
    // 禁用curveclient和s3adapter
    options.curveClient = nullptr;
    options.s3Client = nullptr;

    // curvefs init success
    EXPECT_CALL(*curveClient_, Init(_))
        .Times(0);
    ASSERT_EQ(0, copyer.Init(options));

    // 从上s3或者curve请求下载数据会返回失败
    {
        char* buf = new char[4096];
        AsyncDownloadContext context;
        context.offset = 0;
        context.size = 4096;
        context.buf = buf;
        MockDownloadClosure closure(&context);

        /* 用例:读curve上的数据，读取失败
         */
        context.location = "test:0@cs";
        EXPECT_CALL(*curveClient_, Open4ReadOnly(_, _, true))
            .Times(0);
        EXPECT_CALL(*curveClient_, AioRead(_, _, _))
            .Times(0);
        copyer.DownloadAsync(&closure);
        ASSERT_TRUE(closure.IsRun());
        ASSERT_TRUE(closure.IsFailed());
        closure.Reset();

        /* 用例:读s3上的数据，读取失败
         */
        context.location = "test@s3";
        EXPECT_CALL(*s3Client_, GetObjectAsync(_))
            .Times(0);
        copyer.DownloadAsync(&closure);
        ASSERT_TRUE(closure.IsRun());
        ASSERT_TRUE(closure.IsFailed());
        closure.Reset();
        delete [] buf;
    }
    // fini 可以成功
    ASSERT_EQ(0, copyer.Fini());
}

TEST_F(CloneCopyerTest, ExpiredTest) {
    OriginCopyer copyer;
    CopyerOptions options;
    options.curveConf = CURVE_CONF;
    options.s3Conf = S3_CONF;
    options.curveUser.owner = ROOT_OWNER;
    options.curveUser.password = ROOT_PWD;
    options.curveClient = curveClient_;
    options.s3Client = s3Client_;
    options.curveFileTimeoutSec = EXPIRED_USE;

    // curvefs init success
    EXPECT_CALL(*curveClient_, Init(StrEq(CURVE_CONF)))
            .WillOnce(Return(LIBCURVE_ERROR::OK));
    ASSERT_EQ(0, copyer.Init(options));

    {
        char* buf = new char[4096];
        AsyncDownloadContext context;
        context.offset = 0;
        context.size = 4096;
        context.buf = buf;
        MockDownloadClosure closure(&context);

        /* Case: Read the same chunk after it expired
        * Expect: Re-Open the curve file
        */
        context.location = "test:0@cs";
        EXPECT_CALL(*curveClient_, Open4ReadOnly("test", _, true))
                .WillOnce(Return(1));
        EXPECT_CALL(*curveClient_, AioRead(_, _, _))
                .WillOnce(Invoke([](int fd, CurveAioContext* context,
                                    curve::client::UserDataType dataType) {
                    context->ret = 1024;
                    context->cb(context);
                    return LIBCURVE_ERROR::OK;
                }));
        copyer.DownloadAsync(&closure);
        ASSERT_TRUE(closure.IsRun());
        ASSERT_FALSE(closure.IsFailed());
        closure.Reset();

        std::this_thread::sleep_for(std::chrono::seconds(5));
        context.location = "test:0@cs";
        std::this_thread::sleep_for(std::chrono::seconds(1));
        EXPECT_CALL(*curveClient_, Open4ReadOnly(_, _, true))
                .WillOnce(Return(2));
        EXPECT_CALL(*curveClient_, AioRead(_, _, _))
                .WillOnce(Invoke([](int fd, CurveAioContext* context,
                                    curve::client::UserDataType dataType) {
                    context->ret = 1024;
                    context->cb(context);
                    return LIBCURVE_ERROR::OK;
                }));
        copyer.DownloadAsync(&closure);
        ASSERT_TRUE(closure.IsRun());
        closure.Reset();
        delete [] buf;
    }
    // fini
    EXPECT_CALL(*curveClient_, Close(2))
            .Times(1);
    EXPECT_CALL(*curveClient_, UnInit())
            .Times(1);
    EXPECT_CALL(*s3Client_, Deinit())
            .Times(1);
    ASSERT_EQ(0, copyer.Fini());
}

}  // namespace chunkserver
}  // namespace curve
