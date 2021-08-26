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
]

cc_library(
    name="curvefs_metaserver_lib",
    hdrs=glob(["*.h"]),
    srcs=glob(["*.cpp"],
        exclude = ["main.cpp"],
    ),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//curvefs/proto:metaserver_cc_proto",
        "//curvefs/proto:curvefs_common_cc_proto",
        "//src/fs:lfs",
        "//src/common/concurrent:curve_concurrent",
        "//src/common:curve_common",
        "//curvefs/src/common:curvefs_common",
        "@com_google_absl//absl/cleanup:cleanup",
        "@com_google_absl//absl/container:btree",
    ],
)

cc_binary(
    name = "curvefs_metaserver",
    srcs = glob([
        "main.cpp",
    ]),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/src/metaserver:curvefs_metaserver_lib",
    ],
)

