#
#  Copyright (c) 2021 NetEase Inc.
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

# umountfs
cc_test(
    name = "curvefs_umount_tool_test",
    srcs = glob([
        "curvefs_umount_fs_tool_test.cpp",
        "mock_mds_service.h",
    ]),
    copts = CURVE_TEST_COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/src/tools:curvefs_tools_lib",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

# version
cc_test(
    name = "curvefs_version_tool_test",
    srcs = glob(["curvefs_version_tool_test.cpp"]),
    copts = CURVE_TEST_COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/src/tools:curvefs_tools_lib",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

# factory
cc_test(
    name = "curvefs_tool_factory_test",
    srcs = glob(["curvefs_tool_factory_test.cpp"]),
    copts = CURVE_TEST_COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/src/tools:curvefs_tools_lib",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

# create-topology
cc_test(
    name = "curvefs_create_topology_tool_test",
    srcs = glob([
        "curvefs_create_topology_tool_test.cpp",
        "mock_topology_service.h",
    ]),
    copts = CURVE_TEST_COPTS,
    deps = [
        "//curvefs/src/tools:curvefs_tools_lib",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

# space-base-tool
cc_test(
    name = "curvefs_space_base_tool_test",
    srcs = glob([
        "curvefs_space_base_tool_test.cpp",
    ]),
    copts = CURVE_TEST_COPTS,
    deps = [
        "//curvefs/src/tools:curvefs_tools_lib",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

# space-status
cc_test(
    name = "curvefs_space_status_tool_test",
    srcs = glob([
        "curvefs_space_status_tool_test.cpp",
    ]),
    copts = CURVE_TEST_COPTS,
    deps = [
        "//curvefs/src/tools:curvefs_tools_lib",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)
