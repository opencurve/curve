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

/**
 * Project: nebd
 * Create Date: 2020/03/04
 * Author: wuhanqing
 * Copyright (c) 2020 netease
 */

#ifndef NEBD_TEST_UTILS_CONFIG_GENERATOR_H_
#define NEBD_TEST_UTILS_CONFIG_GENERATOR_H_

#include <string>
#include <vector>

#include "nebd/src/common/configuration.h"

namespace nebd {
namespace common {

static const char* kNebdClientConfigPath = "nebd/etc/nebd/nebd-client.conf";

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
    nebd::common::Configuration conf_;
};

}  // namespace common
}  // namespace nebd

#endif  // NEBD_TEST_UTILS_CONFIG_GENERATOR_H_
