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
    name = "curvefs_metastore",
    srcs = glob(
        ["*.cpp"],
        exclude = [
            "main.cpp",
            "metaserver.cpp",
            "metaserver_service.cpp",
        ],
    ),
    hdrs = glob(
        ["*.h"],
        exclude = [
            "inflight_throttle.h",
            "metaservice_closure.h",
            "metaserver_service.h",
            "metaserver.h",
        ],
    ),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        ":metaserver_s3_lib",
        "//curvefs/src/common:curvefs_common",
        "//curvefs/src/metaserver/copyset:curvefs_copyset_snapshot_closure",
        "//src/common:curve_common",
        "//src/fs:lfs",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:btree",
    ],
)

cc_library(
    name = "curvefs_metaserver_lib",
    srcs = [
        "metaserver.cpp",
        "metaserver_service.cpp",
    ],
    hdrs = [
        "inflight_throttle.h",
        "metaserver.h",
        "metaserver_service.h",
        "metaservice_closure.h",
    ],
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/proto:curvefs_common_cc_proto",
        "//curvefs/proto:metaserver_cc_proto",
        "//curvefs/src/metaserver/copyset:curvefs_copyset",
        "//external:brpc",
        "//src/common/concurrent:curve_concurrent",
        "//src/common:curve_common",
        "//curvefs/src/common:curvefs_common",
        "@com_google_absl//absl/cleanup:cleanup",
        "//curvefs/src/metaserver:metaserver_s3_lib",
        "@com_google_absl//absl/container:btree",
    ],
)

cc_library(
    name="metaserver_s3_lib",
        hdrs=glob(["s3/*.h"]),
        srcs=glob(["s3/*.cpp"]),
        copts = COPTS,
        visibility = ["//visibility:public"],
        deps = [
            "//external:glog",
            "//external:gflags",
            "//curvefs/proto:metaserver_cc_proto",
            "//src/common:curve_s3_adapter",
        ],
        linkopts = ["-lpthread", "-L/usr/local/lib/x86_64-linux-gnu"],
)

cc_binary(
    name = "curvefs_metaserver",
    srcs = glob([
        "main.cpp",
    ]),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        ":curvefs_metaserver_lib",
        ":curvefs_metastore",
    ],
)
