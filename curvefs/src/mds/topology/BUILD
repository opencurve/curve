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
    name = "curvefs_topology",
    srcs = glob([
        "*.cpp",
        "*.h",
    ]),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/proto:copyset_cc_proto",
        "//curvefs/proto:curvefs_topology_cc_proto",
        "//curvefs/src/mds/common:fs_mds_common",
        "//curvefs/src/mds/metaserverclient:curvefs_mds_metaserverclient",
        "//curvefs/src/mds/topology:curvefs_deal_peerid",
        "//external:brpc",
        "//external:glog",
        "//external:json",
        "//src/common:curve_common",
        "//src/common/concurrent:curve_concurrent",
        "//src/kvstorageclient:kvstorage_client",
    ],
)

cc_library(
    name = "curvefs_deal_peerid",
    srcs = glob([
        "deal_peerid.h",
    ]),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//src/common:curve_common",
    ],
)
