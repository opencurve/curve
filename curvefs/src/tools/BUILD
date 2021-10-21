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
    "-Werror",
]

cc_binary(
    name = "curvefs_tool",
    srcs = glob(["curvefs_tool_main.cpp"]),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:gflags",
        "//curvefs/src/tools:curvefs_tools_lib",
    ],
)


# tools lib
cc_library(
    name = "curvefs_tools_lib",
    srcs = glob([
        "curvefs_tool_factory.cpp",
        "umountfs/curvefs_umountfs_tool.cpp",
        "curvefs_tool_define.cpp",
        "topology/curvefs_build_topology_tool.cpp",
        "version/curvefs_version_tool.cpp",
        "space/curvefs_metadata_usage_tool.cpp"
    ]),
    hdrs = glob([
        "curvefs_tool.h",
        "curvefs_tool_factory.h",
        "curvefs_tool_define.h",
        "version/curvefs_version_tool.h",
        "curvefs_tool_abstract_creator.h",
        "umountfs/curvefs_umountfs_tool.h",
        "topology/curvefs_build_topology_tool.h",
        "space/curvefs_metadata_usage_tool.h",
        "space/curvefs_space_base_tool.h",
    ]),
    copts = COPTS,
    linkopts = [
        "-lpthread"
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:gflags",
        "//external:brpc",
        "//external:glog",
        "//external:json",
        "//curvefs/src/mds/common:fs_mds_common",
        "//src/common:curve_common",
        "//curvefs/proto:mds_cc_proto",
        "//curvefs/proto:curvefs_topology_cc_proto",
        "//curvefs/proto:space_cc_proto",
    ],
)
