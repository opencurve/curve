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

load("//:copts.bzl", "CURVE_TEST_COPTS")

cc_binary(
    name = "filemanager_test",
    srcs = glob([
        "file_manager_unittest.cpp",
    ]),
    copts = CURVE_TEST_COPTS,
    deps = [
        "//external:gflags",
        "//nebd/src/part2:nebdserver",
        "//nebd/test/part2:mock_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "fileservice_test",
    srcs = glob([
        "file_service_unittest.cpp",
    ]),
    copts = CURVE_TEST_COPTS,
    deps = [
        "//external:gflags",
        "//nebd/src/part2:nebdserver",
        "//nebd/test/part2:mock_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "heartbeat_service_test",
    srcs = glob([
        "heartbeat_service_test.cpp",
    ]),
    copts = CURVE_TEST_COPTS,
    deps = [
        "//external:gflags",
        "//nebd/src/part2:nebdserver",
        "//nebd/test/part2:mock_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "heartbeat_manager_test",
    srcs = glob([
        "heartbeat_manager_unittest.cpp",
    ]),
    copts = CURVE_TEST_COPTS,
    deps = [
        "//external:gflags",
        "//nebd/src/part2:nebdserver",
        "//nebd/test/part2:mock_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "metafile_manager_test",
    srcs = glob([
        "metafile_manager_test.cpp",
    ]),
    copts = CURVE_TEST_COPTS,
    deps = [
        "//external:gflags",
        "//nebd/src/part2:nebdserver",
        "//nebd/test/part2:mock_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "test_nebd_server",
    srcs = glob([
        "test_nebd_server.cpp",
    ]),
    copts = CURVE_TEST_COPTS,
    deps = [
        "//external:gflags",
        "//nebd/src/part2:nebdserver",
        "//nebd/test/part2:mock_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "test_request_executor_curve",
    srcs = glob([
        "test_request_executor_curve.cpp",
    ]),
    copts = CURVE_TEST_COPTS,
    deps = [
        "//external:gflags",
        "//nebd/src/part2:nebdserver",
        "//nebd/test/part2:mock_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "mock_lib",
    srcs = glob([
        "mock_*",
    ]),
    hdrs = glob(["*.h"]),
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//nebd/src/part2:nebdserver",
        "@com_google_googletest//:gtest",
    ],
)
