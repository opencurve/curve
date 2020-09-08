#
#  Copyright (c) 2020 NetEase Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

DEPS = [
    "@com_google_googletest//:gtest",
    "//external:braft",
    "//external:gflags",
    "//include:include-common",
    "//include/chunkserver:include-chunkserver",
    "//src/common:curve_common",
    "//src/chunkserver/datastore:chunkserver_datastore",
    "//test/chunkserver/datastore:filepool_helper",
]

cc_test(
    name = "datastore_clone_case_test",
    srcs = glob([
        "datastore_integration_base.h",
        "datastore_clone_case_test.cpp",
        "datastore_integration_main.cpp",
    ]),
    includes = ([]),
    copts = ["-std=c++11"],
    deps = DEPS,
)

cc_test(
    name = "datastore_concurrency_test",
    srcs = glob([
        "datastore_integration_base.h",
        "datastore_concurrency_test.cpp",
        "datastore_integration_main.cpp",
    ]),
    includes = ([]),
    copts = ["-std=c++11"],
    deps = DEPS,
)

cc_test(
    name = "datastore_snapshot_case_test",
    srcs = glob([
        "datastore_integration_base.h",
        "datastore_snapshot_case_test.cpp",
        "datastore_integration_main.cpp",
    ]),
    includes = ([]),
    copts = ["-std=c++11"],
    deps = DEPS,
)

cc_test(
    name = "datastore_stress_test",
    srcs = glob([
        "datastore_integration_base.h",
        "datastore_stress_test.cpp",
        "datastore_integration_main.cpp",
    ]),
    includes = ([]),
    copts = ["-std=c++11"],
    deps = DEPS,
)

cc_test(
    name = "datastore_basic_test",
    srcs = glob([
        "datastore_integration_base.h",
        "datastore_basic_test.cpp",
        "datastore_integration_main.cpp",
    ]),
    includes = ([]),
    copts = ["-std=c++11"],
    deps = DEPS,
)

cc_test(
    name = "datastore_exception_test",
    srcs = glob([
        "datastore_integration_base.h",
        "datastore_exception_test.cpp",
        "datastore_integration_main.cpp",
    ]),
    includes = ([]),
    copts = ["-std=c++11"],
    deps = DEPS,
)

cc_test(
    name = "datastore_restart_test",
    srcs = glob([
        "datastore_integration_base.h",
        "datastore_restart_test.cpp",
        "datastore_integration_main.cpp",
    ]),
    includes = ([]),
    copts = ["-std=c++11"],
    deps = DEPS,
)
