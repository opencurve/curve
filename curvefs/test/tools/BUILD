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

COPTS = [
    "-DGFLAGS=gflags",
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DHAVE_SSE42",
    "-DNDEBUG",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
    "-msse4.2",
    "-pthread",
    "-Wsign-compare",
    "-Wno-unused-parameter",
    "-Wno-unused-variable",
    "-Woverloaded-virtual",
    "-Wnon-virtual-dtor",
    "-Wno-missing-field-initializers",
    "-std=c++11",
    "-Wall",
    '-Werror',
]

# umountfs
cc_test(
    name = "curvefs_umount_tool_test",
    srcs = glob([
        "curvefs_umountfs_tool_test.cpp",
        "mock_mds_service.h",
    ]),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//external:brpc",
        "//external:glog",
        "//external:gflags",
        "//curvefs/src/tools:curvefs_tools_lib",
    ]
)

# version
cc_test(
    name = "curvefs_version_tool_test",
    srcs = glob(["curvefs_version_tool_test.cpp"]),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//external:brpc",
        "//external:glog",
        "//external:gflags",
        "//curvefs/src/tools:curvefs_tools_lib",
    ]
)

# factory
cc_test(
    name = "curvefs_tool_factory_test",
    srcs = glob(["curvefs_tool_factory_test.cpp"]),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//external:brpc",
        "//external:glog",
        "//external:gflags",
        "//curvefs/src/tools:curvefs_tools_lib",
    ]
)

# build-topology
cc_test(
    name = "curvefs_build_topology_tool_test",
    srcs = glob([
        "curvefs_build_topology_tool_test.cpp",
        "mock_topology_service.h",
    ]),
    copts = ["-g","-DHAVE_ZLIB=1"],
    deps = [
        "@com_google_googletest//:gtest_main",
        "@com_google_googletest//:gtest",
        "//curvefs/src/tools:curvefs_tools_lib",
    ],
)
