/*
 * Project: curve
 * Created Date: 2020-02-06
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <brpc/server.h>
#include <string>
#include "src/tools/metric_client.h"

namespace curve {
namespace tool {

const char serverAddr[] = "127.0.0.1:9193";

class MetricClientTest : public ::testing::Test {
 protected:
    MetricClientTest() {}
    void SetUp() {
        server = new brpc::Server();
        ASSERT_EQ(0, server->Start(serverAddr, nullptr));
    }
    void TearDown() {
        server->Stop(0);
        server->Join();
        delete server;
        server = nullptr;
    }
    brpc::Server *server;
};

TEST_F(MetricClientTest, GetMetric) {
    MetricClient client;
    // 正常情况
    std::string metricName = "string_metric";
    bvar::Status<std::string> metric(metricName, "value");
    std::string value;
    ASSERT_EQ(MetricRet::kOK, client.GetMetric(serverAddr,
                                      metricName,
                                      &value));
    ASSERT_EQ("value", value);
    // bvar不存在
    ASSERT_EQ(MetricRet::kNotFound, client.GetMetric(serverAddr,
                                      "not-exist-metric",
                                      &value));
    // 其他错误
    ASSERT_EQ(MetricRet::kOtherErr, client.GetMetric("127.0.0.1:9191",
                                      "not-exist-metric",
                                      &value));
}

TEST_F(MetricClientTest, GetMetricUint) {
    MetricClient client;
    // 正常情况
    std::string metricName = "uint_metric";
    bvar::Status<uint64_t> metric(metricName, 10);
    uint64_t value;
    ASSERT_EQ(MetricRet::kOK, client.GetMetricUint(serverAddr,
                                      metricName,
                                      &value));
    ASSERT_EQ(10, value);
    // bvar不存在
    ASSERT_EQ(MetricRet::kNotFound, client.GetMetricUint(serverAddr,
                                      "not-exist-metric",
                                      &value));
    // 其他错误
    ASSERT_EQ(MetricRet::kOtherErr, client.GetMetricUint("127.0.0.1:9191",
                                      "not-exist-metric",
                                      &value));
    // 解析失败
    bvar::Status<std::string> metric2("string_metric", "value");
    ASSERT_EQ(MetricRet::kOtherErr, client.GetMetricUint(serverAddr,
                                      "string_metric",
                                      &value));
}

TEST_F(MetricClientTest, GetConfValue) {
    MetricClient client;
    // 正常情况
    std::string metricName = "conf_metric";
    bvar::Status<std::string> conf_metric(metricName, "");
    conf_metric.set_value("{\"conf_name\":\"key\","
                          "\"conf_value\":\"value\"}");
    std::string value;
    ASSERT_EQ(MetricRet::kOK, client.GetConfValueFromMetric(serverAddr,
                                                            metricName,
                                                            &value));
    ASSERT_EQ("value", value);
    // bvar不存在
    ASSERT_EQ(MetricRet::kNotFound, client.GetConfValueFromMetric(
                                                        serverAddr,
                                                        "not-exist-metric",
                                                        &value));
    // 其他错误
    ASSERT_EQ(MetricRet::kOtherErr, client.GetConfValueFromMetric(
                                                        "127.0.0.1:9191",
                                                        "not-exist-metric",
                                                        &value));
    // 解析失败
    conf_metric.set_value("string");
    ASSERT_EQ(MetricRet::kOtherErr, client.GetConfValueFromMetric(
                                                        serverAddr,
                                                        metricName,
                                                        &value));
}

}  // namespace tool
}  // namespace curve
