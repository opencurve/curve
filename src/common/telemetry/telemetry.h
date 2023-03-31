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

#include "opentelemetry/exporters/ostream/span_exporter_factory.h"
#include "opentelemetry/sdk/trace/simple_processor_factory.h"
#include "opentelemetry/sdk/trace/tracer_context_factory.h"
#include "opentelemetry/sdk/trace/tracer_provider_factory.h"
#include "opentelemetry/trace/provider.h"

#include "opentelemetry/context/propagation/global_propagator.h"
#include "opentelemetry/context/propagation/text_map_propagator.h"
#include "opentelemetry/trace/propagation/http_trace_context.h"


namespace trace = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace trace_exporter = opentelemetry::exporter::trace;
namespace nostd = opentelemetry::nostd;
namespace resource = opentelemetry::sdk::resource;
namespace propagation = opentelemetry::context::propagation;
namespace context = opentelemetry::context;

namespace curve {
namespace telemetry {

void InitTracer();

inline nostd::shared_ptr<trace::Tracer>
GetTracer(const std::string &tracer_name) {
    auto provider = opentelemetry::trace::Provider::GetTracerProvider();
    return provider->GetTracer(tracer_name);
}

inline nostd::shared_ptr<trace::Tracer> &GetNoopTracer() {
    static nostd::shared_ptr<trace::Tracer> noopTracer =
        opentelemetry::nostd::shared_ptr<opentelemetry::trace::NoopTracer>(
            new opentelemetry::trace::NoopTracer);
    return noopTracer;
}

inline void CleanupTracer() {
    std::shared_ptr<opentelemetry::trace::TracerProvider> none;
    trace::Provider::SetTracerProvider(none);
}

}  // namespace telemetry
}  // namespace curve
