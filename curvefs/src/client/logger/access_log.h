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
 * Created Date: 2023-03-17
 * Author: Jingli Chen (Wine93)
 */

#include <unistd.h>
#include <butil/time.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/daily_file_sink.h>

#include <string>
#include <memory>

#include "absl/strings/str_format.h"
#include "curvefs/src/client/common/config.h"

#ifndef CURVEFS_SRC_CLIENT_LOGGER_ACCESS_LOG_H_
#define CURVEFS_SRC_CLIENT_LOGGER_ACCESS_LOG_H_

namespace curvefs {
namespace client {
namespace common {

DECLARE_bool(access_logging);

}
namespace logger {

using ::absl::StrFormat;
using ::curvefs::client::common::FLAGS_access_logging;
using MessageHandler = std::function<std::string()>;

extern std::shared_ptr<spdlog::logger> Logger;

bool InitAccessLog(const std::string& prefix);

struct AccessLogGuard {
    explicit AccessLogGuard(MessageHandler handler)
        : enable(FLAGS_access_logging),
          handler(handler) {
        if (!enable) {
            return;
        }

        timer.start();
    }

    ~AccessLogGuard() {
        if (!enable) {
            return;
        }

        timer.stop();
        Logger->info("{0} <{1:.6f}>", handler(), timer.u_elapsed() / 1e6);
    }

    bool enable;
    MessageHandler handler;
    butil::Timer timer;
};

}  // namespace logger
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_LOGGER_ACCESS_LOG_H_
