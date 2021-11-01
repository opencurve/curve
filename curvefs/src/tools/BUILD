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
        "curvefs_tool_metric.cpp",
        "topology/curvefs_build_topology_tool.cpp",
        "version/curvefs_version_tool.cpp",
        "space/curvefs_metadata_usage_tool.cpp",
        "space/curvefs_space_base_tool.cpp",
        "status/curvefs_status_base_tool.cpp",
        "status/curvefs_mds_status.cpp",
        "status/curvefs_metaserver_status.cpp",
        "status/curvefs_etcd_status.cpp",
        "status/curvefs_copyset_status.cpp",
        "query/curvefs_copyset_query.cpp",
        "query/curvefs_partition_list.cpp",
        "query/curvefs_fsinfo_list.cpp",
        "status/curvefs_cluster_copyset_status.cpp",
        "status/curvefs_metaserver_status.cpp",

    ]),
    hdrs = glob([
        "curvefs_tool.h",
        "curvefs_tool_factory.h",
        "curvefs_tool_define.h",
        "curvefs_tool_metric.h",
        "version/curvefs_version_tool.h",
        "curvefs_tool_abstract_creator.h",
        "umountfs/curvefs_umountfs_tool.h",
        "topology/curvefs_build_topology_tool.h",
        "space/curvefs_metadata_usage_tool.h",
        "space/curvefs_space_base_tool.h",
        "status/curvefs_status_base_tool.h",
        "status/curvefs_mds_status.h",
        "status/curvefs_copyset_status.h",
        "query/curvefs_copyset_query.h",
        "query/curvefs_partition_list.h",
        "query/curvefs_fsinfo_list.h",
        "status/curvefs_cluster_copyset_status.h",
        "status/curvefs_metaserver_status.h",
        "status/curvefs_etcd_status.h",
    ]),
    copts = COPTS,
    linkopts = [
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
        "//curvefs/proto:copyset_cc_proto",
    ],
)
