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
        "curvefs_tool.cpp",
        "curvefs_tool_factory.cpp",
        "curvefs_tool_define.cpp",
        "curvefs_tool_metric.cpp",
        "buildtopo/curvefs_build_topology_tool.cpp",
        "version/curvefs_version_tool.cpp",
        "usage/curvefs_metadata_usage_tool.cpp",
        "usage/curvefs_space_base_tool.cpp",
        "status/curvefs_status_base_tool.cpp",
        "status/curvefs_mds_status.cpp",
        "status/curvefs_metaserver_status.cpp",
        "status/curvefs_etcd_status.cpp",
        "status/curvefs_copyset_status.cpp",
        "query/curvefs_copyset_query.cpp",
        "query/curvefs_partition_query.cpp",
        "query/curvefs_metaserver_query.cpp",
        "query/curvefs_fs_query.cpp",
        "list/curvefs_fsinfo_list.cpp",
        "list/curvefs_copysetid_list.cpp",
        "list/curvefs_copysetinfo_list.cpp",
        "check/curvefs_copyset_check.cpp",
        "copyset/curvefs_copyset_base_tool.cpp",
        "delete/curvefs_delete_fs_tool.cpp",
        "umount/curvefs_umount_fs_tool.cpp",
    ]),
    hdrs = glob([
        "curvefs_tool.h",
        "curvefs_tool_factory.h",
        "curvefs_tool_define.h",
        "curvefs_tool_metric.h",
        "version/curvefs_version_tool.h",
        "curvefs_tool_abstract_creator.h",
        "buildtopo/curvefs_build_topology_tool.h",
        "usage/curvefs_metadata_usage_tool.h",
        "usage/curvefs_space_base_tool.h",
        "status/curvefs_status_base_tool.h",
        "status/curvefs_mds_status.h",
        "status/curvefs_copyset_status.h",
        "status/curvefs_metaserver_status.h",
        "status/curvefs_etcd_status.h",
        "query/curvefs_copyset_query.h",
        "query/curvefs_partition_query.h",
        "query/curvefs_metaserver_query.h",
        "query/curvefs_fs_query.h",
        "list/curvefs_fsinfo_list.h",
        "list/curvefs_copysetid_list.h",
        "list/curvefs_copysetinfo_list.h",
        "check/curvefs_copyset_check.h",
        "copyset/curvefs_copyset_base_tool.h",
        "delete/curvefs_delete_fs_tool.h",
        "umount/curvefs_umount_fs_tool.h",
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
