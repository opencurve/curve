/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include <string>
#include <map>

#ifndef SRC_COMMON_CONFIGURATION_H_
#define SRC_COMMON_CONFIGURATION_H_

namespace curve {
namespace common {

class Configuration {
 public:
    Configuration() {}
    ~Configuration() {}

    bool LoadConfig();
    bool SaveConfig();
    std::string DumpConfig();

    void SetConfigPath(const std::string &path);
    std::string GetConfigPath();

    std::string GetStringValue(const std::string &key);
    void SetStringValue(const std::string &key, const std::string &value);

    int GetIntValue(const std::string &key, uint64_t defaultvalue = 0);
    void SetIntValue(const std::string &key, const int value);

    double GetDoubleValue(const std::string &key, double defaultvalue = 0.0);
    void SetDoubleValue(const std::string &key, const double value);

    bool GetBoolValue(const std::string &key, bool defaultvalue = false);
    void SetBoolValue(const std::string &key, const bool value);

    std::string GetValue(const std::string &key);
    void SetValue(const std::string &key, const std::string &value);

 private:
    std::string                         confFile_;
    std::map<std::string, std::string>  config_;
};

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_CONFIGURATION_H_
