/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Project: curve
 * Date: Friday Oct 22 17:20:07 CST 2021
 * Author: wuhanqing
 */

#ifndef SRC_COMMON_GFLAGS_HELPER_H_
#define SRC_COMMON_GFLAGS_HELPER_H_

#include <gflags/gflags.h>

#include <memory>
#include <string>

#include "src/common/configuration.h"

namespace curve {
namespace common {

struct GflagsLoadValueFromConfIfCmdNotSet {
    template <typename T>
    void Load(const std::shared_ptr<Configuration>& conf,
              const std::string& cmdName, const std::string& confName,
              T* value) {
        Load(conf.get(), cmdName, confName, value);
    }

    template <typename T>
    void Load(Configuration* conf, const std::string& cmdName,
              const std::string& confName, T* value) {
        using ::google::CommandLineFlagInfo;
        using ::google::GetCommandLineFlagInfo;

        CommandLineFlagInfo info;
        if (GetCommandLineFlagInfo(cmdName.c_str(), &info) && info.is_default) {
            conf->GetValueFatalIfFail(confName, value);
        }
    }
};

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_GFLAGS_HELPER_H_
