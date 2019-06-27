/*
 * Project: curve
 * Created Date: Wednesday April 10th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <glog/logging.h>

#include "src/client/libcurve_define.h"
#include "src/chunkserver/clone_copyer.h"
#include "test/chunkserver/clone/clone_test_util.h"
#include "test/client/mock_file_client.h"
#include "test/common/mock_s3_adapter.h"

namespace curve {
namespace chunkserver {

using curve::client::MockFileClient;
using curve::common::MockS3Adapter;

const char CURVE_CONF[] = "client.conf";
const char S3_CONF[] = "s3.conf";
const char ROOT_OWNER[] = "root";
const char ROOT_PWD[] = "pwd";

class CloneCopyerTest : public testing::Test  {
 public:
    void SetUp() {
        curveClient_ = std::make_shared<MockFileClient>();
        s3Client_ = std::make_shared<MockS3Adapter>();
    }
    void TearDown() {}

 protected:
    std::shared_ptr<MockFileClient> curveClient_;
    std::shared_ptr<MockS3Adapter> s3Client_;
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
        string location;
        off_t off = 0;
        size_t size = 4096;
        char* buf = new char[4096];

        // invalid location
        location = "aaaaa";
        ASSERT_EQ(-1, copyer.Download(location, off, size, buf));

        /* 用例:读curve上的数据，读取成功
         * 预期:调用Open和Read读取数据
         */
        location = "test:0@cs";
        EXPECT_CALL(*curveClient_, Open("test", _))
            .WillOnce(Return(1));
        EXPECT_CALL(*curveClient_, Read(1, _, off, size))
            .WillOnce(Return(LIBCURVE_ERROR::OK));
        ASSERT_EQ(0, copyer.Download(location, off, size, buf));

        /* 用例:再次读前面的文件
         * 预期:直接Read
         */
        location = "test:0@cs";
        EXPECT_CALL(*curveClient_, Open(_, _))
            .Times(0);
        EXPECT_CALL(*curveClient_, Read(1, _, off, size))
            .WillOnce(Return(LIBCURVE_ERROR::OK));
        ASSERT_EQ(0, copyer.Download(location, off, size, buf));

        /* 用例:读curve上的数据，Open的时候失败
         * 预期:返回-1
         */
        location = "test2:0@cs";
        EXPECT_CALL(*curveClient_, Open("test2", _))
            .WillOnce(Return(-1));
        EXPECT_CALL(*curveClient_, Read(_, _, _, _))
            .Times(0);
        ASSERT_EQ(-1, copyer.Download(location, off, size, buf));

        /* 用例:读curve上的数据，Read的时候失败
         * 预期:返回-1
         */
        location = "test2:0@cs";
        EXPECT_CALL(*curveClient_, Open("test2", _))
            .WillOnce(Return(2));
        EXPECT_CALL(*curveClient_, Read(2, _, off, size))
            .WillOnce(Return(-1 * LIBCURVE_ERROR::FAILED));
        ASSERT_EQ(-1, copyer.Download(location, off, size, buf));

        /* 用例:读s3上的数据，读取成功
         * 预期:返回0
         */
        location = "test@s3";
        EXPECT_CALL(*s3Client_, GetObject("test", _, off, size))
            .WillOnce(Return(0));
        ASSERT_EQ(0, copyer.Download(location, off, size, buf));

        /* 用例:读s3上的数据，读取失败
         * 预期:返回-1
         */
        location = "test@s3";
        EXPECT_CALL(*s3Client_, GetObject("test", _, off, size))
            .WillOnce(Return(-1));
        ASSERT_EQ(-1, copyer.Download(location, off, size, buf));

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
    // 禁用curveclient和s3adapter
    options.curveClient = nullptr;
    options.s3Client = nullptr;

    // curvefs init success
    EXPECT_CALL(*curveClient_, Init(_))
        .Times(0);
    ASSERT_EQ(0, copyer.Init(options));

    // 从上s3或者curve请求下载数据会返回失败
    {
        string location;
        off_t off = 0;
        size_t size = 4096;
        char* buf = new char[4096];

        /* 用例:读curve上的数据，读取失败
         */
        location = "test:0@cs";
        EXPECT_CALL(*curveClient_, Open(_, _))
            .Times(0);
        EXPECT_CALL(*curveClient_, Read(_, _, _, _))
            .Times(0);
        ASSERT_EQ(-1, copyer.Download(location, off, size, buf));

        /* 用例:读s3上的数据，读取失败
         */
        location = "test@s3";
        EXPECT_CALL(*s3Client_, GetObject(_, _, _, _))
            .Times(0);
        ASSERT_EQ(-1, copyer.Download(location, off, size, buf));
        delete [] buf;
    }
    // fini 可以成功
    ASSERT_EQ(0, copyer.Fini());
}

}  // namespace chunkserver
}  // namespace curve
