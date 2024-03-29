/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-07-03
 * Author: Jingli Chen (Wine93)
 */

// clang-format off

#include <glog/logging.h>

#include <iostream>
#include <string>
#include <thread>

#include "curvefs/src/common/dynamic_vlog.h"

#ifndef CURVEFS_SRC_CLIENT_LOGGER_ERROR_LOG_H_
#define CURVEFS_SRC_CLIENT_LOGGER_ERROR_LOG_H_

namespace curvefs {
namespace client {
namespace logger {

using ::curvefs::common::FLAGS_vlog_level;

inline bool InitErrorLog(const std::string& prefix,
                         const std::string& name,
                         int32_t loglevel) {
    FLAGS_log_dir = prefix;
    FLAGS_v = loglevel;
    FLAGS_vlog_level = loglevel;
    FLAGS_minloglevel = 0;
    FLAGS_logtostderr = 0;
    FLAGS_alsologtostderr = 0;
    google::InitGoogleLogging(name.c_str());
    return true;
}

inline void ShutdownErrorLog() { google::ShutdownGoogleLogging(); }

}  // namespace logger
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_LOGGER_ERROR_LOG_H_
