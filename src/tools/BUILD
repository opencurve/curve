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

# tools

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
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
    "-std=c++14",
]

cc_test(
    name = "createFile",
    srcs = ["createtool.cpp"],
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//include/client:include_client",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "curve_format",
    srcs = [
        "curve_format_main.cpp",
    ],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:bvar",
        "//external:gflags",
        "//external:glog",
        "//external:protobuf",
        "//proto:chunkserver-cc-protos",
        "//src/chunkserver:chunkserver-lib",
        "//src/common:curve_common",
        "//src/fs:lfs",
        "//external:json"
    ],
)

cc_binary(
    name = "curve_tool",
    srcs = ["curve_tool_main.cpp"],

    copts = COPTS,
    linkopts = [
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:gflags",
        "//src/tools:curve_tool_lib",
    ],
)

cc_binary(
    name = "curve_chunkserver_tool",
    srcs = ["chunkserver_tool_main.cpp"],

    copts = COPTS,
    linkopts = [
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:gflags",
        "//external:braft",
        "//src/tools:curve_tool_lib",
    ],
)

cc_library(
    name = "curve_tool_lib",
    srcs = glob(
        ["*.cpp"],
        exclude = [
            "curve_tool_main.cpp",
            "curve_format_main.cpp",
            "createtool.cpp",
            "chunkserver_tool_main.cpp",
        ],
    ),
    hdrs = glob([
        "*.h",
    ]),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:gflags",
        "//external:protobuf",
        "//external:brpc",
        "//external:braft",
        "//external:bthread",
        "//external:butil",
        "//external:bvar",
        "//external:glog",
        "//external:leveldb",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//proto:schedule_cc_proto",
        "//proto:chunkserver-cc-protos",
        "//src/common:curve_common",
        "//src/mds/common:mds_common",
        "//src/mds/nameserver2:nameserver2",
        "//src/chunkserver:chunkserver-lib",
        "//src/client:curve_client",
        "//include/client:include_client",
    ],
)
