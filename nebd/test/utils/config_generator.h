/**
 * Project: nebd
 * Create Date: 2020/03/04
 * Author: wuhanqing
 * Copyright (c) 2020 netease
 */

#ifndef TESTS_UTILS_CONFIG_GENERATOR_H_
#define TESTS_UTILS_CONFIG_GENERATOR_H_

#include <string>
#include <vector>

#include "src/common/configuration.h"

namespace nebd {
namespace common {

static const char* kNebdClientConfigPath = "nebd/etc/nebd/nebd-client.conf";
static const char* kNebdServerConfigPath = "nebd/etc/nebd/nebd-server.conf";

class NebdClientConfigGenerator {
 public:
    NebdClientConfigGenerator() {
        LoadDefaultConfig();
    }

    void SetConfigPath(const std::string& configPath) {
        configPath_ = configPath;
    }

    void SetConfigOptions(const std::vector<std::string>& options) {
        for (const auto& opt : options) {
            auto pos = opt.find("=");
            std::string key = opt.substr(0, pos);
            std::string value = opt.substr(pos + 1);
            SetKV(key, value);
        }
    }

    bool Generate() {
        if (configPath_.empty()) {
            return false;
        }

        conf_.SetConfigPath(configPath_);
        return conf_.SaveConfig();
    }

 private:
    void LoadDefaultConfig() {
        conf_.SetConfigPath(kNebdClientConfigPath);
        conf_.LoadConfig();
    }

    void SetKV(const std::string& key, const std::string& value) {
        conf_.SetValue(key, value);
    }

    std::string configPath_;
    curve::common::Configuration conf_;
};

}  // namespace common
}  // namespace nebd

#endif  // TESTS_UTILS_CONFIG_GENERATOR_H_
