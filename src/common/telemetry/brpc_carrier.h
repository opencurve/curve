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

#pragma once

#include <brpc/controller.h>

#include "opentelemetry/context/propagation/global_propagator.h"
#include "opentelemetry/context/propagation/text_map_propagator.h"
#include "opentelemetry/context/runtime_context.h"
#include "opentelemetry/trace/context.h"
#include "opentelemetry/trace/span_metadata.h"
#include "opentelemetry/trace/span_startoptions.h"
#include "src/common/telemetry/telemetry.h"

namespace curve {
namespace telemetry {

class RpcServiceCarrier : public context::propagation::TextMapCarrier {
 public:
    explicit RpcServiceCarrier(brpc::Controller *cntl) : cntl_(cntl) {}

    RpcServiceCarrier() = default;

    nostd::string_view Get(nostd::string_view key) const noexcept override;

    void Set(nostd::string_view key,
             nostd::string_view value) noexcept override;

 private:
    brpc::Controller *cntl_{};
};

inline nostd::shared_ptr<trace::Span>
StartRpcServerSpan(const std::string &tracerName, const std::string &spanName,
                   google::protobuf::RpcController *cntl_base) {
    RpcServiceCarrier carrier(static_cast<brpc::Controller *>(cntl_base));
    auto prop =
        context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
    auto currCtx = context::RuntimeContext::GetCurrent();
    auto newCtx = prop->Extract(carrier, currCtx);

    trace::StartSpanOptions options;
    options.kind = trace::SpanKind::kServer;
    options.parent = trace::GetSpan(newCtx)->GetContext();
    return GetTracer(tracerName)->StartSpan(spanName, options);
}
}  // namespace telemetry
}  // namespace curve
