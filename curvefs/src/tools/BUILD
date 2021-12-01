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

load("//:copts.bzl", "CURVE_DEFAULT_COPTS")

cc_binary(
    name = "curvefs_tool",
    srcs = glob(["curvefs_tool_main.cpp"]),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/src/tools:curvefs_tools_lib",
        "//external:gflags",
    ],
)

# tools lib
cc_library(
    name = "curvefs_tools_lib",
    srcs = glob([
        "*.cpp",
        "buildtopo/*.cpp",
        "version/*.cpp",
        "usage/*.cpp",
        "status/*.cpp",
        "query/*.cpp",
        "list/*.cpp",
        "check/*.cpp",
        "copyset/*.cpp",
        "delete/*.cpp",
        "umount/*.cpp",
    ]),
    hdrs = glob([
        "*.h",
        "buildtopo/*.h",
        "version/*.h",
        "usage/*.h",
        "status/*.h",
        "query/*.h",
        "list/*.h",
        "check/*.h",
        "copyset/*.h",
        "delete/*.h",
        "umount/*.h",
    ]),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/proto:copyset_cc_proto",
        "//curvefs/proto:curvefs_topology_cc_proto",
        "//curvefs/proto:mds_cc_proto",
        "//curvefs/proto:metaserver_cc_proto",
        "//curvefs/proto:space_cc_proto",
        "//curvefs/src/mds/common:fs_mds_common",
        "//curvefs/src/mds/topology:curvefs_deal_peerid",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:json",
        "//src/common:curve_common",
        "//src/mds/topology",
    ],
)
