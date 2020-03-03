/*
 * Project: curve
 * Created Date: 2020-02-06
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef SRC_TOOLS_METRIC_CLIENT_H_
#define SRC_TOOLS_METRIC_CLIENT_H_

#include <brpc/channel.h>
#include <iostream>
#include <string>
#include "src/tools/common.h"
#include "src/common/string_util.h"

namespace curve {
namespace tool {

class MetricClient {
 public:
     virtual ~MetricClient() {}

	/**
     *  @brief 从指定地址获取metric
     *  @param addr 要访问的地址
     *  @param metricName 要获取的metric name
     *  @param[out] value metric的值
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetMetric(const std::string& addr,
                  const std::string& metricName,
                  std::string* value);

     /**
     *  @brief 从指定地址获取metric,并转换成uint
     *  @param addr 要访问的地址
     *  @param metricName 要获取的metric name
     *  @param[out] value metric的值
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetMetricUint(const std::string& addr,
                  const std::string& metricName,
                  uint64_t* value);

 private:
    // 从response attachment解析出metric值
    int GetValueFromAttachment(const std::string& attachment,
                               std::string* value);
};

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_METRIC_CLIENT_H_
