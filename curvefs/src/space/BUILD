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

load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library")
load("//:copts.bzl", "CURVE_DEFAULT_COPTS")

config_setting(
    name = "absl_btree",
    values = {
        "define": "use_absl_btree=true",
    },
    visibility = ["//visibility:public"],
)

config_setting(
    name = "absl_vector",
    values = {
        "define": "use_absl_vector=true",
    },
    visibility = ["//visibility:public"],
)

ABSL_COPTS = select({
    ":absl_btree": ["-DUSE_ABSL_BTREE"],
    "//conditions:default": [],
}) + select({
    ":absl_vector": ["-DUSE_ABSL_VECTOR"],
    "//conditions:default": [],
})

ABSL_DEPS = select({
    ":absl_btree": ["@com_google_absl//absl/container:btree"],
    "//conditions:default": [],
}) + select({
    ":absl_vector": ["@com_google_absl//absl/container:inlined_vector"],
    "//conditions:default": [],
})

cc_library(
    name = "curve_fs_space",
    srcs = glob(
        ["*.cpp"],
        exclude = ["main.cpp"],
    ),
    hdrs = glob(["*.h"]),
    copts = CURVE_DEFAULT_COPTS + ABSL_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/proto:space_cc_proto",
        "//curvefs/proto:mds_cc_proto",
        "//curvefs/proto:metaserver_cc_proto",
        "//curvefs/src/common:curvefs_common",
        "//src/common:bitmap",
        "//src/common/concurrent:rw_lock",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:bvar",
    ] + ABSL_DEPS,
)

cc_binary(
    name = "curve_fs_space_main",
    srcs = [
        "main.cpp",
    ],
    copts = CURVE_DEFAULT_COPTS + ABSL_COPTS,
    deps = [
        ":curve_fs_space",
        "//src/common:curve_common",
    ] + ABSL_DEPS,
)
