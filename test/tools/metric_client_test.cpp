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
    curve::tool::MetricClient client;
    // 正常情况
    std::string metricName = "string_metric";
    bvar::Status<std::string> metric(metricName, "value");
    std::string value;
    ASSERT_EQ(0, client.GetMetric(serverAddr,
                                      metricName,
                                      &value));
    ASSERT_EQ("value", value);
}
