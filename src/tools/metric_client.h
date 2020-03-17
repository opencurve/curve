/*
 * Project: curve
 * Created Date: 2020-02-06
 * Author: charisu
 * Copyright (c) 2018 netease
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
    // 成功
    kOK = 0,
    // metric未找到
    kNotFound = -1,
    // 其他错误
    kOtherErr  = -2,
};

const int kHttpCodeNotFound = 404;

class MetricClient {
 public:
     virtual ~MetricClient() {}

	/**
     *  @brief 从指定地址获取metric
     *  @param addr 要访问的地址
     *  @param metricName 要获取的metric name
     *  @param[out] value metric的值
     *  @return 错误码
     */
    virtual MetricRet GetMetric(const std::string& addr,
                                const std::string& metricName,
                                std::string* value);

     /**
     *  @brief 从指定地址获取metric,并转换成uint
     *  @param addr 要访问的地址
     *  @param metricName 要获取的metric name
     *  @param[out] value metric的值
     *  @return 错误码
     */
    virtual MetricRet GetMetricUint(const std::string& addr,
                                    const std::string& metricName,
                                    uint64_t* value);

    /**
     *  @brief 从metric获取配置的值
     *  @param addr 要访问的地址
     *  @param metricName 要获取的metric name
     *  @param[out] confValue metric中配置的值
     *  @return 错误码
     */
    virtual MetricRet GetConfValueFromMetric(const std::string& addr,
                                             const std::string& metricName,
                                             std::string* confValue);

 private:
    // 从response attachment解析出metric值
    int GetValueFromAttachment(const std::string& attachment,
                               std::string* value);
};

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_METRIC_CLIENT_H_
