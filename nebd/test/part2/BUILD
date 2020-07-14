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

cc_binary(
    name = "filemanager_test",
    srcs = glob([
        "file_manager_unittest.cpp",
    ]),
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
    deps = [
        "//external:gflags",
        "//nebd/src/part2:nebdserver",
        "//nebd/test/part2:mock_lib",
        "@com_google_googletest//:gtest_main",
    ],
    linkopts = ["-lpthread"],
)

cc_library(
    name = "mock_lib",
    srcs = glob([
	 "mock_*",
    ]),
    hdrs = glob(["*.h"]),
    deps = [
        "@com_google_googletest//:gtest",
        "//nebd/src/part2:nebdserver",
    ],
    visibility = ["//visibility:public"],
)
