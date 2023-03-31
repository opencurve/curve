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
 * Project: curve
 * Created Date: 2023-3-29
 * Author: Ziy1-Tan
 */

#include "src/common/telemetry/brpc_carrier.h"

opentelemetry::nostd::string_view curve::telemetry::RpcServiceCarrier::Get(
    opentelemetry::nostd::string_view key) const noexcept {
    const auto *it = cntl_->http_request().GetHeader(key.data());
    if (it != nullptr) {
        return it->data();
    }
    return "";
}

void curve::telemetry::RpcServiceCarrier::Set(
    opentelemetry::nostd::string_view key,
    opentelemetry::nostd::string_view value) noexcept {
    cntl_->http_request().SetHeader(key.data(), value.data());
}
