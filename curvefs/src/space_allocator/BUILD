# Copyright 2018 Google Inc.  All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library")

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
    "-std=c++11",
    "-Wall",
    "-Werror",
]

cc_library(
    name = "curve_fs_space_allocator",
    srcs = glob(
        ["*.cpp"],
        exclude = ["space_allocator_main.cpp"],
    ),
    hdrs = glob(["*.h"]),
    copts = COPTS + select({
        "//curvefs/src/space_allocator:absl_btree": ["-DUSE_BTREE"],
        "//conditions:default": [],
    }),
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/proto:space_cc_proto",
        "//curvefs/proto:mds_cc_proto",
        "//curvefs/proto:metaserver_cc_proto",
        "@com_github_apache_brpc//:brpc",
        "@com_github_apache_brpc//:bthread",
        "@com_github_apache_brpc//:butil",
        "@com_github_apache_brpc//:bvar",
    ] + select({
        ":absl_btree": ["@com_google_absl//absl/container:btree"],
        "//conditions:default": [],
    }),
)

cc_binary(
    name = "curve_fs_space_allocator_main",
    srcs = [
        "space_allocator_main.cpp",
    ],
    copts = COPTS,
    linkopts = [
        "-lpthread",
        "-lrt",
        "-lz",
        "-ldl",
        "-lssl",
        "-lcrypto",
    ],
    deps = [
        ":curve_fs_space_allocator",
    ],
)

config_setting(
    name = "absl_btree",
    values = {
        "define": "use_btree=true",
    },
    visibility = ["//visibility:public"],
)
