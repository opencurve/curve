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
 * Project: nebd
 * File Created: 2019-08-07
 * Author: hzchenwei7
 */

#include <string>
#include <map>

#ifndef NEBD_SRC_COMMON_CONFIGURATION_H_
#define NEBD_SRC_COMMON_CONFIGURATION_H_

namespace nebd {
namespace common {

class Configuration {
 public:
    Configuration() {}
    ~Configuration() {}

    bool LoadConfig();
    bool SaveConfig();
    std::string DumpConfig();
    std::map<std::string, std::string> ListConfig() const;

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
    * @param[in] key configuration item name
    * @param[out] outThe value obtained
    *
    * @return false-did not obtain, true-obtained successfully
    */
    bool GetIntValue(const std::string &key, int *out);
    bool GetUInt32Value(const std::string &key, uint32_t *out);
    bool GetUInt64Value(const std::string &key, uint64_t *out);
    bool GetInt64Value(const std::string& key, int64_t* out);
    void SetIntValue(const std::string &key, const int value);

    double GetDoubleValue(const std::string &key, double defaultvalue = 0.0);
    /*
    * @brief GetDoubleValue Get the value of the specified configuration item
    *
    * @param[in] key configuration item name
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
    * @param[in] key configuration item name
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
    * @param[in] key configuration item name
    * @param[out] out The value obtained
    *
    * @return false-did not obtain, true-obtained successfully
    */
    bool GetBoolValue(const std::string &key, bool *out);
    void SetBoolValue(const std::string &key, const bool value);

    std::string GetValue(const std::string &key);
    bool GetValue(const std::string &key, std::string *out);
    void SetValue(const std::string &key, const std::string &value);

 private:
    std::string                         confFile_;
    std::map<std::string, std::string>  config_;
};

}  // namespace common
}  // namespace nebd

#endif  // NEBD_SRC_COMMON_CONFIGURATION_H_
