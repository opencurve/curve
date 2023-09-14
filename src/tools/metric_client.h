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
 * Created Date: 2020-02-06
 * Author: charisu
 */

#ifndef SRC_TOOLS_METRIC_CLIENT_H_
#define SRC_TOOLS_METRIC_CLIENT_H_

#include <brpc/channel.h>
#include <json/json.h>
#include <iostream>
#include <string>
#include "src/tools/common.h"
#include "src/common/string_util.h"
#include "src/tools/curve_tool_define.h"

namespace curve {
namespace tool {

enum class MetricRet {
    // Success
    kOK = 0,
    // Metric not found
    kNotFound = -1,
    // Other errors
    kOtherErr  = -2,
};

const int kHttpCodeNotFound = 404;

class MetricClient {
 public:
     virtual ~MetricClient() {}

	/**
     * @brief Get metric from specified address
     * @param addr Address to access
     * @param metricName The metric name to obtain
     * @param[out] value The value of metric
     * @return error code
     */
    virtual MetricRet GetMetric(const std::string& addr,
                                const std::string& metricName,
                                std::string* value);

     /**
     * @brief retrieves metric from the specified address and converts it to uint
     * @param addr Address to access
     * @param metricName The metric name to obtain
     * @param[out] value The value of metric
     * @return error code
     */
    virtual MetricRet GetMetricUint(const std::string& addr,
                                    const std::string& metricName,
                                    uint64_t* value);

    /**
     * @brief Get the configured value from metric
     * @param addr Address to access
     * @param metricName The metric name to obtain
     * @param[out] confValue The value configured in metric
     * @return error code
     */
    virtual MetricRet GetConfValueFromMetric(const std::string& addr,
                                             const std::string& metricName,
                                             std::string* confValue);

 private:
    // Parse the metric value from the response attachment
    int GetValueFromAttachment(const std::string& attachment,
                               std::string* value);
};

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_METRIC_CLIENT_H_
