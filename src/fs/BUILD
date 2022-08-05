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

load("//:copts.bzl", "CURVE_DEFAULT_COPTS")

cc_library(
    name = "lfs",
    srcs = ["ext4_filesystem_impl.cpp",
            "local_filesystem.cpp",
            "wrap_posix.cpp",
    ] + select({
        "//:with_spdk": ["pfs_filesystem_impl.cpp"],
        "//conditions:default": [],
    }),
    hdrs = ["ext4_filesystem_impl.h",
            "fs_common.h",
            "local_filesystem.h",
            "wrap_posix.h",
    ] + select({
        "//:with_spdk": ["pfs_filesystem_impl.h"],
        "//conditions:default": [],
    }),
    copts = CURVE_DEFAULT_COPTS + select({
        "//:with_spdk": ["-DWITH_SPDK", "-Iexternal/pfs"],
        "//conditions:default": [],
    }),
    linkopts = select({
        "//:with_spdk": [ "-L/usr/local/polarstore/pfsd/lib", "-lpfsd" ],
        "//conditions:default": [],
    }) + [
        "-std=c++11",
    ],
    deps = [
        "//src/common:curve_common",
        "//external:glog",
        "//external:butil",
    ] + select({
        "//:with_spdk": ["@pfs//:headers"],
        "//conditions:default": [],
    }),
    visibility = ["//visibility:public"],
)


