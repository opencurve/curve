/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include "src/common/configuration.h"

#include <glog/logging.h>
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
            line.erase(std::remove_if(line.begin(), line.end(), isspace),
                       line.end());
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
    // 当前先只保存配置，原文件的注释等内容先忽略
    // TODO(yyk): 后续考虑改成原文件格式不变，只修改配置值
    std::ofstream wStream(confFile_);
    if (wStream.is_open()) {
        for (auto& pair : config_) {
            wStream << pair.first << "=" << pair.second << std::endl;
        }
        wStream.close();
    } else {
        return false;
    }
    return true;
}

void Configuration::PrintConfig() {
    LOG(INFO) << std::string(30, '=') << "BEGIN" << std::string(30, '=');
    for (auto &item : config_) {
        LOG(INFO) << item.first << std::string(60 - item.first.size(), ' ')
                  << ": " << item.second;
    }
    LOG(INFO) << std::string(31, '=') << "END" << std::string(31, '=');
}


void Configuration::ExposeMetric(const std::string& exposeName) {
    if (exposeName_.empty()) {
        exposeName_ = exposeName;
    } else {
        LOG(WARNING) << "Config metric has been exposed.";
    }
}

void Configuration::UpdateMetric() {
    if (exposeName_.empty()) {
        LOG(WARNING) << "Config metric is not been exposed, update failed.";
        return;
    }

    for (auto& config : config_) {
        std::string configKey = config.first;
        std::string configValue = config.second;
        auto it = configMetric_.find(configKey);
        // 如果配置项不存在，则新建配置项
        if (it == configMetric_.end()) {
            ConfigItemPtr configItem = std::make_shared<StringStatus>();
            configItem->ExposeAs(exposeName_, configKey);
            configMetric_[configKey] = configItem;
        }
        // 更新配置项
        configMetric_[configKey]->Set("conf_name", configKey);
        configMetric_[configKey]->Set("conf_value", configValue);
        configMetric_[configKey]->Update();
    }
}

std::map<std::string, std::string> Configuration::ListConfig() const {
    return config_;
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

bool Configuration::GetStringValue(const std::string &key, std::string *out) {
    return GetValue(key, out);
}

void Configuration::SetStringValue(const std::string &key,
                                   const std::string &value) {
    SetValue(key, value);
}

int Configuration::GetIntValue(const std::string &key, uint64_t defaultvalue) {
    std::string value = GetValue(key);
    return (value == "") ? defaultvalue : std::stoi(value);
}

bool Configuration::GetIntValue(const std::string &key, int *out) {
    std::string res;
    if (GetValue(key, &res)) {
        *out = std::stoi(res);
        return true;
    }
    return false;
}

bool Configuration::GetUInt32Value(const std::string &key, uint32_t *out) {
    std::string res;
    if (GetValue(key, &res)) {
        *out = std::stoul(res);
        return true;
    }
    return false;
}

bool Configuration::GetUInt64Value(const std::string &key, uint64_t *out) {
    std::string res;
    if (GetValue(key, &res)) {
        *out = std::stoull(res);
        return true;
    }
    return false;
}


void Configuration::SetIntValue(const std::string &key, const int value) {
    SetValue(key, std::to_string(value));
}

double Configuration::GetDoubleValue(
    const std::string &key,
    double defaultvalue) {
    std::string value = GetValue(key);
    return (value == "") ? defaultvalue : std::stod(value);
}

bool Configuration::GetDoubleValue(const std::string &key, double *out) {
    std::string res;
    if (GetValue(key, &res)) {
        *out = std::stod(res);
        return true;
    }
    return false;
}

void Configuration::SetDoubleValue(const std::string &key, const double value) {
    SetValue(key, std::to_string(value));
}


double Configuration::GetFloatValue(
    const std::string &key, float defaultvalue) {
    std::string value = GetValue(key);
    return (value == "") ? defaultvalue : std::stof(value);
}

bool Configuration::GetFloatValue(const std::string &key, float *out) {
    std::string res;
    if (GetValue(key, &res)) {
        *out = std::stof(res);
        return true;
    }
    return false;
}

void Configuration::SetFloatValue(const std::string &key, const float value) {
    SetValue(key, std::to_string(value));
}

bool Configuration::GetBoolValue(const std::string &key, bool defaultvalue) {
    std::string svalue = config_[key];
    transform(svalue.begin(), svalue.end(), svalue.begin(), ::tolower);

    bool istrue = (svalue == "true") || (svalue == "yes") || (svalue == "1");
    bool isfalse = (svalue == "false") || (svalue == "no") || (svalue == "0");
    bool ret = istrue ? true : isfalse ? false : defaultvalue;
    return ret;
}

bool Configuration::GetBoolValue(const std::string &key, bool *out) {
    std::string res;
    if (GetValue(key, &res)) {
        transform(res.begin(), res.end(), res.begin(), ::tolower);
        bool istrue = (res == "true") || (res == "yes") || (res == "1");
        bool isfalse = (res == "false") || (res == "no") || (res == "0");
        if (istrue) {
            *out = true;
            return true;
        }
        if (isfalse) {
            *out = false;
            return true;
        }
        return false;
    }

    return false;
}


void Configuration::SetBoolValue(const std::string &key, const bool value) {
    SetValue(key, std::to_string(value));
}

std::string Configuration::GetValue(const std::string &key) {
    return config_[key];
}

bool Configuration::GetValue(const std::string &key, std::string *out) {
    if (config_.find(key) != config_.end()) {
        *out = config_[key];
        return true;
    }

    return false;
}

void Configuration::SetValue(const std::string &key, const std::string &value) {
    config_[key] = value;
}

}  // namespace common
}  // namespace curve
