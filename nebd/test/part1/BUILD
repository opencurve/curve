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
    name = "nebd_metacache_unittest",
    srcs = glob([
        "nebd_metacache_unittest.cpp",
    ]),
    copts = CURVE_TEST_COPTS,
    deps = [
        "//external:gflags",
        "//nebd/src/part1:nebdclient",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "heartbeat_manager_unittest",
    srcs = glob([
        "heartbeat_manager_unittest.cpp",
    ]),
    copts = CURVE_TEST_COPTS,
    deps = [
        "//external:gflags",
        "//nebd/src/part1:nebdclient",
        "//nebd/test/part1:fake_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "nebd_client_unittest",
    srcs = glob([
        "nebd_client_unittest.cpp",
    ]),
    copts = CURVE_TEST_COPTS,
    linkstatic = False,
    deps = [
        "//external:gflags",
        "//nebd/src/part1:nebdclient",
        "//nebd/test/part1:fake_lib",
        "//nebd/test/utils:test_utils_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "nebd_lib_unittest",
    srcs = glob([
        "nebd_lib_unittest.cpp",
    ]),
    copts = CURVE_TEST_COPTS,
    linkstatic = False,
    deps = [
        "//external:gflags",
        "//nebd/src/part1:nebdclient",
        "//nebd/test/part1:fake_lib",
        "//nebd/test/utils:test_utils_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "fake_lib",
    srcs = glob([
        "fake_*",
        "mock_*",
    ]),
    hdrs = glob(["*.h"]),
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//nebd/src/part1:nebdclient",
        "@com_google_googletest//:gtest",
    ],
)
