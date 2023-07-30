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
    name = "curvefs_mds_fs_storage",
    srcs = [
        "fs_info_wrapper.cpp",
        "fs_storage.cpp",
    ],
    hdrs = [
        "fs_info_wrapper.h",
        "fs_storage.h",
    ],
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/proto:mds_cc_proto",
        "//curvefs/src/common:curvefs_common",
        "//curvefs/src/mds/codec:fs_mds_codec",
        "//curvefs/src/mds/idgenerator:fs_mds_idgenerator",
        "//src/common:curve_common",
        "//src/common/concurrent:curve_concurrent",
        "//src/idgenerator",
        "//src/kvstorageclient:kvstorage_client",
    ],
)

cc_library(
    name = "curvefs_mds_lib",
    srcs = glob(
        ["*.cpp"],
        exclude = [
            "main.cpp",
            "fs_storage.cpp",
            "fs_info_wrapper.cpp",
        ],
    ),
    hdrs = glob(["*.h"]),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        ":curvefs_mds_fs_storage",
        "//curvefs/src/common:curvefs_common",
        "//curvefs/src/common:dynamic_vlog",
        "//curvefs/src/mds/codec:fs_mds_codec",
        "//curvefs/src/mds/common:fs_mds_common",
        "//curvefs/src/mds/dlock:fs_mds_dlock",
        "//curvefs/src/mds/heartbeat:curvefs_heartbeat_lib",
        "//curvefs/src/mds/idgenerator:fs_mds_idgenerator",
        "//curvefs/src/mds/metaserverclient:curvefs_mds_metaserverclient",
        "//curvefs/src/mds/metric:fs_mds_metric",
        "//curvefs/src/mds/schedule:curvefs_schedule",
        "//curvefs/src/mds/schedule/scheduleService:curvefs_schedule_service",
        "//curvefs/src/mds/space:curvefs_mds_space",
        "//curvefs/src/mds/topology:curvefs_topology",
        "//curvefs/src/client:filesystem_xattr",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//src/common:curve_common",
        "//src/common:curve_s3_adapter",
        "//src/common/concurrent:curve_concurrent",
        "//src/idgenerator",
        "//src/kvstorageclient:kvstorage_client",
        "//src/leader_election",
    ],
)

cc_binary(
    name = "curvefs-mds",
    srcs = ["main.cpp"],
    copts = CURVE_DEFAULT_COPTS,
    deps = [
        "//curvefs/src/mds:curvefs_mds_lib",
    ],
)
