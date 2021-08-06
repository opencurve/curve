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
    "-std=c++11",
    "-Wall",
    "-Werror",
]

cc_library(
    name = "curvefs_copyset_snapshot_closure",
    hdrs = ["snapshot_closure.h"],
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/proto:metaserver_cc_proto",
    ],
)

cc_library(
    name = "curvefs_copyset",
    srcs = glob(["*.cpp"]),
    hdrs = glob(
        ["*.h"],
        exclude = ["snapshot_closure.h"],
    ),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        ":curvefs_copyset_snapshot_closure",
        "//curvefs/proto:cli2_cc_proto",
        "//curvefs/proto:copyset_cc_proto",
        "//curvefs/proto:metaserver_cc_proto",
        "//curvefs/src/metaserver:curvefs_metastore",
        "//curvefs/src/metaserver/common:fs_metaserver_common",
        "//external:braft",
        "//src/common:curve_common",
        "//src/fs:lfs",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/memory",
    ],
)
