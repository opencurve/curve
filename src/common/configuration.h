/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include <glog/logging.h>
#include <string>
#include <map>
#include <memory>
#include <unordered_map>

#include "src/common/stringstatus.h"

#ifndef SRC_COMMON_CONFIGURATION_H_
#define SRC_COMMON_CONFIGURATION_H_

namespace curve {
namespace common {

using ConfigItemPtr = std::shared_ptr<StringStatus>;
using ConfigMetricMap =  std::unordered_map<std::string, ConfigItemPtr>;

class Configuration {
 public:
    Configuration() {}
    ~Configuration() {}

    bool LoadConfig();
    bool SaveConfig();
    void PrintConfig();
    std::map<std::string, std::string> ListConfig() const;
    /**
     * 暴露config的metric供采集
     * 如果metric已经暴露，则直接返回
     * @param exposeName: 对外暴露的metric的名字
     */
    void ExposeMetric(const std::string& exposeName);

    /**
     * 更新新的配置到metric
     */
    void UpdateMetric();

    void SetConfigPath(const std::string &path);
    std::string GetConfigPath();

    std::string GetStringValue(const std::string &key);
    /*
    * @brief GetStringValue 获取指定配置项的值
    *
    * @param[in] key 配置项名称
    * @param[out] out 获取的值
    *
    * @return false-未获取到 true-获取成功
    */
    bool GetStringValue(const std::string &key, std::string *out);
    void SetStringValue(const std::string &key, const std::string &value);

    int GetIntValue(const std::string &key, uint64_t defaultvalue = 0);
    /*
    * @brief GetIntValue/GetUInt32Value/GetUInt64Value 获取指定配置项的值 //NOLINT
    *
    * @param[in] key 配置项名称
    * @param[out] out 获取的值
    *
    * @return false-未获取到 true-获取成功
    */
    bool GetIntValue(const std::string &key, int *out);
    bool GetUInt32Value(const std::string &key, uint32_t *out);
    bool GetUInt64Value(const std::string &key, uint64_t *out);
    void SetIntValue(const std::string &key, const int value);

    double GetDoubleValue(const std::string &key, double defaultvalue = 0.0);
    /*
    * @brief GetDoubleValue 获取指定配置项的值
    *
    * @param[in] key 配置项名称
    * @param[out] out 获取的值
    *
    * @return false-未获取到 true-获取成功
    */
    bool GetDoubleValue(const std::string &key, double *out);
    void SetDoubleValue(const std::string &key, const double value);

    double GetFloatValue(const std::string &key, float defaultvalue = 0.0);
    /*
    * @brief GetFloatValue 获取指定配置项的值
    *
    * @param[in] key 配置项名称
    * @param[out] out 获取的值
    *
    * @return false-未获取到 true-获取成功
    */
    bool GetFloatValue(const std::string &key, float *out);
    void SetFloatValue(const std::string &key, const float value);

    bool GetBoolValue(const std::string &key, bool defaultvalue = false);
    /*
    * @brief GetBoolValue 获取指定配置项的值
    *
    * @param[in] key 配置项名称
    * @param[out] out 获取的值
    *
    * @return false-未获取到 true-获取成功
    */
    bool GetBoolValue(const std::string &key, bool *out);
    void SetBoolValue(const std::string &key, const bool value);

    std::string GetValue(const std::string &key);
    bool GetValue(const std::string &key, std::string *out);
    void SetValue(const std::string &key, const std::string &value);

 private:
    std::string                         confFile_;
    std::map<std::string, std::string>  config_;
    // metric对外暴露的名字
    std::string                         exposeName_;
    // 每一个配置项使用单独的一个metric，用map管理
    ConfigMetricMap                     configMetric_;
};

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_CONFIGURATION_H_
