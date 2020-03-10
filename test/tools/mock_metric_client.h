/*
 * Project: curve
 * File Created: 2020-02-20
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */


#ifndef TEST_TOOLS_MOCK_METRIC_CLIENT_H_
#define TEST_TOOLS_MOCK_METRIC_CLIENT_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <vector>
#include <map>
#include "src/tools/metric_client.h"

using ::testing::Return;
namespace curve {
namespace tool {
class MockMetricClient : public MetricClient {
 public:
    MOCK_METHOD3(GetMetric, int(const std::string&, const std::string&,
                                std::string*));
    MOCK_METHOD3(GetMetricUint, int(const std::string&, const std::string&,
                                uint64_t*));
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_METRIC_CLIENT_H_
