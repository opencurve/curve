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
 * Created Date: 2023-03-26
 * Author: Ziy1-Tan
 */


#include <gtest/gtest.h>
#include <glog/logging.h>

#include <memory>
#include <chrono>
#include "opentelemetry/sdk/trace/simple_processor.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"
#include "opentelemetry/exporters/ostream/span_exporter.h"

#include "src/common/telemetry/telemetry.h"


namespace curve {
namespace telemetry {
class OpenTelemetryTest : public testing::Test {
    void SetUp() override {}

    void TearDown() override {}

 protected:
};

TEST_F(OpenTelemetryTest, basic_test) {
    // init tracer, trace will be output to the console
    auto exporter = std::unique_ptr<trace_sdk::SpanExporter>(
        new opentelemetry::exporter::trace::OStreamSpanExporter);
    auto processor = std::unique_ptr<trace_sdk::SpanProcessor>(
        new trace_sdk::SimpleSpanProcessor(std::move(exporter)));
    auto provider = std::shared_ptr<trace::TracerProvider>(
        new trace_sdk::TracerProvider(std::move(processor)));
    auto tracer = provider->GetTracer("basic_test");

    // Start a new span
    auto span1 = tracer->StartSpan("span1");
    span1->AddEvent("event1");

    // span2 is child of span
    trace::StartSpanOptions opts;
    opts.parent = span1->GetContext();
    auto span2 = tracer->StartSpan("span2", opts);
    span2->AddEvent("event2");
}
}  // namespace telemetry
}  // namespace curve
