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
 * File Created: 2020-02-20
 * Author: charisu
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
    MOCK_METHOD3(GetMetric, MetricRet(const std::string&, const std::string&,
                                std::string*));
    MOCK_METHOD3(GetMetricUint, MetricRet(const std::string&,
                                const std::string&,
                                uint64_t*));
    MOCK_METHOD3(GetConfValueFromMetric, MetricRet(const std::string&,
                                         const std::string&,
                                         std::string*));
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_METRIC_CLIENT_H_
