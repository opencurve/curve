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
    bool LoadConfig();
    bool SaveConfig();
    void PrintConfig();
    std::map<std::string, std::string> ListConfig() const;
    /**
     * Expose the metric of config for collection
     * If the metric has already been exposed, return it directly
     * @param exposeName: The name of the exposed metric
     */
    void ExposeMetric(const std::string& exposeName);

    void SetConfigPath(const std::string &path);
    std::string GetConfigPath();

    std::string GetStringValue(const std::string &key);
    /*
    * @brief GetStringValue Get the value of the specified configuration item
    *
    * @param[in] key configuration item name
    * @param[out] out The value obtained
    *
    * @return false-did not obtain, true-obtained successfully
    */
    bool GetStringValue(const std::string &key, std::string *out);
    void SetStringValue(const std::string &key, const std::string &value);

    int GetIntValue(const std::string &key, uint64_t defaultvalue = 0);
    /*
    * @brief GetIntValue/GetUInt32Value/GetUInt64Value Get the value of the specified configuration item//NOLINT
    *
    * @param[in] key  configuration item name
    * @param[out] out The value obtained
    *
    * @return false-did not obtain, true-obtained successfully
    */
    bool GetIntValue(const std::string &key, int *out);
    bool GetUInt32Value(const std::string &key, uint32_t *out);
    bool GetUInt64Value(const std::string &key, uint64_t *out);
    void SetIntValue(const std::string &key, const int value);
    void SetUInt32Value(const std::string &key, const uint32_t value);
    void SetUInt64Value(const std::string &key, const uint64_t value);

    bool GetInt64Value(const std::string& key, int64_t* out);
    void SetInt64Value(const std::string& key, const int64_t value);

    double GetDoubleValue(const std::string &key, double defaultvalue = 0.0);
    /*
    * @brief GetDoubleValue Get the value of the specified configuration item
    *
    * @param[in] key  configuration item name
    * @param[out] out The value obtained
    *
    * @return false-did not obtain, true-obtained successfully
    */
    bool GetDoubleValue(const std::string &key, double *out);
    void SetDoubleValue(const std::string &key, const double value);

    double GetFloatValue(const std::string &key, float defaultvalue = 0.0);
    /*
    * @brief GetFloatValue Get the value of the specified configuration item
    *
    * @param[in] key  configuration item name
    * @param[out] out The value obtained
    *
    * @return false-did not obtain, true-obtained successfully
    */
    bool GetFloatValue(const std::string &key, float *out);
    void SetFloatValue(const std::string &key, const float value);

    bool GetBoolValue(const std::string &key, bool defaultvalue = false);
    /*
    * @brief GetBoolValue Get the value of the specified configuration item
    *
    * @param[in] key  configuration item name
    * @param[out] out The value obtained
    *
    * @return false-did not obtain, true-obtained successfully
    */
    bool GetBoolValue(const std::string &key, bool *out);
    void SetBoolValue(const std::string &key, const bool value);

    std::string GetValue(const std::string &key);
    bool GetValue(const std::string &key, std::string *out);
    void SetValue(const std::string &key, const std::string &value);

    /*
    * @brief GetValueFatalIfFail to obtain the value of the specified configuration item, failed to log FATAL
    *
    * @param[in] key    configuration item name
    * @param[out] value The value obtained
    *
    * @return None
    */
    void GetValueFatalIfFail(const std::string& key, int* value);
    void GetValueFatalIfFail(const std::string& key, std::string* value);
    void GetValueFatalIfFail(const std::string& key, bool* value);
    void GetValueFatalIfFail(const std::string& key, uint32_t* value);
    void GetValueFatalIfFail(const std::string& key, uint64_t* value);
    void GetValueFatalIfFail(const std::string& key, float* value);
    void GetValueFatalIfFail(const std::string& key, double* value);

    bool GetValue(const std::string &key, int *value) {
        return GetIntValue(key, value);
    }

    bool GetValue(const std::string &key, uint32_t *value) {
        return GetUInt32Value(key, value);
    }

    bool GetValue(const std::string& key, int64_t* value) {
        return GetInt64Value(key, value);
    }

    bool GetValue(const std::string& key, uint64_t* value) {
        return GetUInt64Value(key, value);
    }

    bool GetValue(const std::string& key, double* value) {
        return GetDoubleValue(key, value);
    }

    bool GetValue(const std::string& key, float* value) {
        return GetFloatValue(key, value);
    }

    bool GetValue(const std::string& key, bool* value) {
        return GetBoolValue(key, value);
    }

 private:
    /**
     *Update new configuration to metric
     * @param The metric to update
     */
    void UpdateMetricIfExposed(const std::string &key,
                               const std::string &value);

 private:
    std::string                         confFile_;
    std::map<std::string, std::string>  config_;
    //Metric's exposed name
    std::string                         exposeName_;
    //Each configuration item uses a separate metric and is managed using a map
    ConfigMetricMap                     configMetric_;
};

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_CONFIGURATION_H_
