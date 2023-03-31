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

#include <memory>
#include <iostream>
#include "opentelemetry/sdk/trace/simple_processor.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"
#include "opentelemetry/exporters/ostream/span_exporter.h"

namespace trace = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace nostd = opentelemetry::nostd;
class OpenTelemetryTest : public testing::Test {};

TEST_F(OpenTelemetryTest, basic_test) {
    auto exporter = std::unique_ptr<trace_sdk::SpanExporter>(
        new opentelemetry::exporter::trace::OStreamSpanExporter);
    auto processor = std::unique_ptr<trace_sdk::SpanProcessor>(
        new trace_sdk::SimpleSpanProcessor(std::move(exporter)));
    auto provider = nostd::shared_ptr<trace::TracerProvider>(
        new trace_sdk::TracerProvider(std::move(processor)));

    auto tracer = provider->GetTracer("example");

    // Start a new span and sleep for 1 second
    auto span = tracer->StartSpan("test");
    span->AddEvent("event1");
    span->AddEvent("event2");
    span->AddEvent("event3");
    std::cout << "Span1 ended" << std::endl;
    // End the span and print a message
    span->End();

    auto span2 = tracer->StartSpan("test2");
    span2->AddEvent("event4");
    span2->AddEvent("event5");
    span2->AddEvent("event6");
    // End the span and print a message
    span2->End();
    std::cout << "Span2 ended" << std::endl;
}
