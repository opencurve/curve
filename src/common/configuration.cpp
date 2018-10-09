/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 * 
 * History: 
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include "src/common/configuration.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>

namespace curve {
namespace common {

bool Configuration::LoadConfig() {
    std::ifstream cFile(confFile_);

    if (cFile.is_open()) {
        std::string line;
        while (getline(cFile, line)) {
            // FIXME: may not remove middle spaces
            line.erase(std::remove_if(line.begin(), line.end(), isspace), line.end());
            if (line[0] == '#' || line.empty())
                continue;

            int delimiterPos = line.find("=");
            std::string key = line.substr(0, delimiterPos);
            std::string value = line.substr(delimiterPos + 1);
            config_[key] = value;
        }
    } else {
        return false;
    }

    return true;
}

bool Configuration::SaveConfig() {
    // TODO(wenyu): to implement
    return false;
}

std::string Configuration::DumpConfig() {
    // TODO(wenyu): to implement
    return NULL;
}

void Configuration::SetConfigPath(const std::string &path) {
    confFile_ = path;
}

std::string Configuration::GetConfigPath() {
    return confFile_;
}

std::string Configuration::GetStringValue(const std::string &key) {
    return GetValue(key);
}

void Configuration::SetStringValue(const std::string &key, const std::string &value) {
    SetValue(key, value);
}

int Configuration::GetIntValue(const std::string &key) {
    return std::stoi(config_[key]);
}

void Configuration::SetIntValue(const std::string &key, const int value) {
    SetValue(key, std::to_string(value));
}

bool Configuration::GetBoolValue(const std::string &key) {
    std::string svalue = config_[key];
    transform(svalue.begin(), svalue.end(), svalue.begin(), ::tolower);

    return (svalue == "true") || (svalue == "yes") || (svalue == "1");
}

void Configuration::SetBoolValue(const std::string &key, const bool value) {
    SetValue(key, std::to_string(value));
}

std::string Configuration::GetValue(const std::string &key) {
    return config_[key];
}

void Configuration::SetValue(const std::string &key, const std::string &value) {
    config_[key] = value;
}

}  // namespace common
}  // namespace curve
