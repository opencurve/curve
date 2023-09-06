/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: Fri Apr 21 2023
 * Author: Xinlong-Chen
 */

#include "curvefs/src/client/metric/client_metric.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <memory>

#include "src/common/s3_adapter.h"

using ::curvefs::client::metric::MDSClientMetric;
using ::curvefs::client::metric::MetaServerClientMetric;
using ::curvefs::client::metric::ClientOpMetric;
using ::curvefs::client::metric::S3MultiManagerMetric;
using ::curvefs::client::metric::FSMetric;
using ::curvefs::client::metric::S3Metric;
using ::curvefs::client::metric::DiskCacheMetric;
using ::curvefs::client::metric::KVClientMetric;
using ::curvefs::client::metric::S3ChunkInfoMetric;
using ::curvefs::client::metric::WarmupManagerS3Metric;


namespace curvefs {
namespace client {

class ClientMetricTest : public ::testing::Test {
 protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(ClientMetricTest, test_prefix) {
    {
        const char* prefix = "curvefs_mds_client";
        ASSERT_EQ(0, ::strcmp(MDSClientMetric::prefix.c_str(), prefix));
    }

    {
        const char* prefix = "curvefs_metaserver_client";
        ASSERT_EQ(0, ::strcmp(MetaServerClientMetric::prefix.c_str(), prefix));
    }

    {
        const char* prefix = "curvefs_client";
        ASSERT_EQ(0, ::strcmp(ClientOpMetric::prefix.c_str(), prefix));
    }

    {
        const char* prefix = "curvefs_client_manager";
        ASSERT_EQ(0, ::strcmp(S3MultiManagerMetric::prefix.c_str(), prefix));
    }

    {
        const char* prefix = "curvefs_client";
        ASSERT_EQ(0, ::strcmp(FSMetric::prefix.c_str(), prefix));
    }

    {
        const char* prefix = "curvefs_s3";
        ASSERT_EQ(0, ::strcmp(S3Metric::prefix.c_str(), prefix));
    }

    {
        const char* prefix = "curvefs_disk_cache";
        ASSERT_EQ(0, ::strcmp(DiskCacheMetric::prefix.c_str(), prefix));
    }

    {
        const char* prefix = "curvefs_kvclient";
        ASSERT_EQ(0, ::strcmp(KVClientMetric::prefix.c_str(), prefix));
    }

    {
        const char* prefix = "inode_s3_chunk_info";
        ASSERT_EQ(0, ::strcmp(S3ChunkInfoMetric::prefix.c_str(), prefix));
    }

    {
        const char* prefix = "curvefs_warmup";
        ASSERT_EQ(0, ::strcmp(WarmupManagerS3Metric::prefix.c_str(), prefix));
    }
}

TEST_F(ClientMetricTest, AsyncContextCollectMetrics_disk_read) {
    std::shared_ptr<S3Metric> s3Metric = std::make_shared<S3Metric>("test");
    curve::common::GetObjectAsyncCallBack cb =
        [&](const curve::common::S3Adapter*,
            const std::shared_ptr<curve::common::GetObjectAsyncContext>&
                context) {
            sleep(1);
            context->timer.stop();
            metric::AsyncContextCollectMetrics(s3Metric, context);
        };
    Aws::SDKOptions option;
    Aws::InitAPI(option);
    char* buf = new char[10];
    auto context = std::make_shared<curve::common::GetObjectAsyncContext>(
        "test", buf, 1, 10, cb, curve::common::ContextType::Disk);
    constexpr int times = 2;
    for (int i = 0; i < times; ++i) {
        context->timer.start();
        cb(nullptr, context);
    }
    ASSERT_EQ(s3Metric->readFromDiskCache.latency.count(), times);
    ASSERT_GE(s3Metric->readFromDiskCache.latency.latency(), 1000000);
    Aws::ShutdownAPI(option);
}

TEST_F(ClientMetricTest, AsyncContextCollectMetrics_s3_read) {
    std::shared_ptr<S3Metric> s3Metric = std::make_shared<S3Metric>("test");
    curve::common::GetObjectAsyncCallBack cb =
        [&](const curve::common::S3Adapter*,
            const std::shared_ptr<curve::common::GetObjectAsyncContext>&
                context) {
            sleep(1);
            context->timer.stop();
            metric::AsyncContextCollectMetrics(s3Metric, context);
        };
    Aws::SDKOptions option;
    Aws::InitAPI(option);
    char* buf = new char[10];
    auto context = std::make_shared<curve::common::GetObjectAsyncContext>(
        "test", buf, 1, 10, cb, curve::common::ContextType::S3);
    constexpr int times = 2;
    for (int i = 0; i < times; ++i) {
        context->timer.start();
        cb(nullptr, context);
    }
    ASSERT_EQ(s3Metric->readFromS3.latency.count(), times);
    ASSERT_GE(s3Metric->readFromS3.latency.latency(), 1000000);
    Aws::ShutdownAPI(option);
}

TEST_F(ClientMetricTest, AsyncContextCollectMetrics_unkown_read) {
    std::shared_ptr<S3Metric> s3Metric = std::make_shared<S3Metric>("test");
    curve::common::GetObjectAsyncCallBack cb =
        [&](const curve::common::S3Adapter*,
            const std::shared_ptr<curve::common::GetObjectAsyncContext>&
                context) {
            sleep(1);
            context->timer.stop();
            metric::AsyncContextCollectMetrics(s3Metric, context);
        };
    Aws::SDKOptions option;
    Aws::InitAPI(option);
    char* buf = new char[10];
    auto context = std::make_shared<curve::common::GetObjectAsyncContext>(
        "test", buf, 1, 10, cb, curve::common::ContextType::Unkown);
    constexpr int times = 1;
    for (int i = 0; i < times; ++i) {
        context->timer.start();
        cb(nullptr, context);
    }
    Aws::ShutdownAPI(option);
}

TEST_F(ClientMetricTest, AsyncContextCollectMetrics_disk_write) {
    std::shared_ptr<S3Metric> s3Metric = std::make_shared<S3Metric>("test");
    curve::common::PutObjectAsyncCallBack cb =
        [&](const std::shared_ptr<curve::common::PutObjectAsyncContext>&
                context) {
            sleep(1);
            context->timer.stop();
            metric::AsyncContextCollectMetrics(s3Metric, context);
        };
    Aws::SDKOptions option;
    Aws::InitAPI(option);
    char* buf = new char[10];
    auto context = std::make_shared<curve::common::PutObjectAsyncContext>(
        "test", buf, 10, cb, curve::common::ContextType::Disk);
    constexpr int times = 2;
    for (int i = 0; i < times; ++i) {
        context->timer.start();
        cb(context);
    }
    ASSERT_EQ(s3Metric->writeToDiskCache.latency.count(), times);
    ASSERT_GE(s3Metric->writeToDiskCache.latency.latency(), 1000000);
    Aws::ShutdownAPI(option);
}

TEST_F(ClientMetricTest, AsyncContextCollectMetrics_s3_write) {
    std::shared_ptr<S3Metric> s3Metric = std::make_shared<S3Metric>("test");
    curve::common::PutObjectAsyncCallBack cb =
        [&](const std::shared_ptr<curve::common::PutObjectAsyncContext>&
                context) {
            sleep(1);
            context->timer.stop();
            metric::AsyncContextCollectMetrics(s3Metric, context);
        };
    Aws::SDKOptions option;
    Aws::InitAPI(option);
    char* buf = new char[10];
    auto context = std::make_shared<curve::common::PutObjectAsyncContext>(
        "test", buf, 10, cb, curve::common::ContextType::S3);
    constexpr int times = 2;
    for (int i = 0; i < times; ++i) {
        context->timer.start();
        cb(context);
    }
    ASSERT_EQ(s3Metric->writeToS3.latency.count(), times);
    ASSERT_GE(s3Metric->writeToS3.latency.latency(), 1000000);
    Aws::ShutdownAPI(option);
}

TEST_F(ClientMetricTest, AsyncContextCollectMetrics_unkown_write) {
    std::shared_ptr<S3Metric> s3Metric = std::make_shared<S3Metric>("test");
    curve::common::PutObjectAsyncCallBack cb =
        [&](const std::shared_ptr<curve::common::PutObjectAsyncContext>&
                context) {
            sleep(1);
            context->timer.stop();
            metric::AsyncContextCollectMetrics(s3Metric, context);
        };
    Aws::SDKOptions option;
    Aws::InitAPI(option);
    char* buf = new char[10];
    auto context = std::make_shared<curve::common::PutObjectAsyncContext>(
        "test", buf, 10, cb, curve::common::ContextType::Unkown);
    constexpr int times = 1;
    for (int i = 0; i < times; ++i) {
        context->timer.start();
        cb(context);
    }
    Aws::ShutdownAPI(option);
}

TEST_F(ClientMetricTest, AsyncContextCollectMetrics_nullptr_write) {
    curve::common::PutObjectAsyncCallBack cb =
        [&](const std::shared_ptr<curve::common::PutObjectAsyncContext>&
                context) {
            sleep(1);
            context->timer.stop();
            metric::AsyncContextCollectMetrics(nullptr, context);
        };
    Aws::SDKOptions option;
    Aws::InitAPI(option);
    char* buf = new char[10];
    auto context = std::make_shared<curve::common::PutObjectAsyncContext>(
        "test", buf, 10, cb, curve::common::ContextType::Unkown);
    context->timer.start();
    cb(context);
    Aws::ShutdownAPI(option);
}

TEST_F(ClientMetricTest, AsyncContextCollectMetrics_nullptr_read) {
    curve::common::GetObjectAsyncCallBack cb =
        [&](const curve::common::S3Adapter*,
            const std::shared_ptr<curve::common::GetObjectAsyncContext>&
                context) {
            sleep(1);
            context->timer.stop();
            metric::AsyncContextCollectMetrics(nullptr, context);
        };
    Aws::SDKOptions option;
    Aws::InitAPI(option);
    char* buf = new char[10];
    auto context = std::make_shared<curve::common::GetObjectAsyncContext>(
        "test", buf, 1, 10, cb, curve::common::ContextType::Unkown);
    context->timer.start();
    cb(nullptr, context);
    Aws::ShutdownAPI(option);
}

}  // namespace client
}  // namespace curvefs
