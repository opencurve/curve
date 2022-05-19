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

cc_library(
    name = "curvefs_metaserver",
    srcs = glob(
        ["*.cpp"],
        exclude = [
            "main.cpp",
        ],
    ) + glob(
        ["copyset/*.cpp"],
    ) + glob(
        ["storage/*.cpp"],
    ) + glob(
        ["streaming/*.cpp"],
    ),
    hdrs = glob(
        ["*.h"],
    ) + glob(
        ["copyset/*.h"],
    ) + glob(
        ["storage/*.h"],
    ) + glob(
        ["streaming/*.h"],
    ),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        ":metaserver_s3_lib",
        "//curvefs/proto:cli2_cc_proto",
        "//curvefs/proto:copyset_cc_proto",
        "//curvefs/proto:curvefs_heartbeat_cc_proto",
        "//curvefs/proto:curvefs_topology_cc_proto",
        "//curvefs/proto:metaserver_cc_proto",
        "//curvefs/proto:mds_cc_proto",
        "//curvefs/src/common:curvefs_common",
        "//curvefs/src/metaserver/common:fs_metaserver_common",
        "//external:braft",
        "//src/common:curve_common",
        "//src/fs:lfs",
        "//curvefs/src/common:dynamic_vlog",
        "@rocksdb//:rocksdb_lib",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/utility",
    ],
)

cc_library(
    name = "metaserver_s3_lib",
    srcs = glob(["s3/*.cpp", "mdsclient/*.cpp"]),
    hdrs = glob(["s3/*.h", "mdsclient/*.h"]),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/proto:metaserver_cc_proto",
        "//curvefs/src/common:curvefs_common",
        "//curvefs/proto:mds_cc_proto",
        "//external:gflags",
        "//external:glog",
        "//src/common:curve_s3_adapter",
        "//curvefs/src/client/common",
        "//curvefs/src/client/metric:client_metric",
        "//curvefs/src/client/rpcclient",
    ],
)

cc_binary(
    name = "curvefs-metaserver",
    srcs = ["main.cpp"],
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        ":curvefs_metaserver",
    ],
)
