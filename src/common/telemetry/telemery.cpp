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

#include "src/common/telemetry/telemetry.h"
#include <gflags/gflags.h>

DEFINE_bool(enable_tracing, true, "enable tracing");

namespace curve {
namespace telemetry {
void InitTracer() {
    if (!FLAGS_enable_tracing) {
        return;
    }

    auto exporter = trace_exporter::OStreamSpanExporterFactory::Create();
    auto processor =
        trace_sdk::SimpleSpanProcessorFactory::Create(std::move(exporter));
    std::vector<std::unique_ptr<trace_sdk::SpanProcessor>> processors;
    processors.push_back(std::move(processor));
    // Default is an always-on sampler.
    std::shared_ptr<trace_sdk::TracerContext> context =
        trace_sdk::TracerContextFactory::Create(std::move(processors));
    std::shared_ptr<trace::TracerProvider> provider =
        trace_sdk::TracerProviderFactory::Create(context);
    // Set the global trace provider
    trace::Provider::SetTracerProvider(provider);

    // set global propagator
    propagation::GlobalTextMapPropagator::SetGlobalPropagator(
        opentelemetry::nostd::shared_ptr<propagation::TextMapPropagator>(
            new trace::propagation::HttpTraceContext()));
}
}  // namespace telemetry
}  // namespace curve
