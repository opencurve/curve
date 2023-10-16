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

#include <signal.h>
#include <glog/logging.h>

#include <string>
#include <thread>
#include <iostream>

#include "curvefs/src/common/dynamic_vlog.h"

#ifndef CURVEFS_SRC_CLIENT_LOGGER_ERROR_LOG_H_
#define CURVEFS_SRC_CLIENT_LOGGER_ERROR_LOG_H_

namespace curvefs {
namespace client {
namespace logger {

DECLARE_int32(vlog_level);

inline void myhandle(int mysignal, siginfo_t *si, void* arg) {}

inline bool InitErrorLog(const std::string& prefix,
                         const std::string& name,
                         int32_t loglevel) {
    static bool inited = false;
    FLAGS_log_dir = prefix;  // glog built-in flag
    FLAGS_v = loglevel;  // glog built-in flag
    FLAGS_vlog_level = 6;  // glog built-int flag
    FLAGS_logtostderr = 0;
    FLAGS_minloglevel = 3;
    FLAGS_log_async = false;
    FLAGS_stderrthreshold = 0;
    FLAGS_alsologtostderr = 0;
    google::InitGoogleLogging("/client");
    return true;
}

inline void ShutdownErrLog() {
    google::ShutdownGoogleLogging();
}

}  // namespace logger
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_LOGGER_ERROR_LOG_H_
