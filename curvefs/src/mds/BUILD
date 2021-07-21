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
    "-DHAVE_ZLIB",
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
    "-Wall",
    "-Werror",
]

cc_library(
    name="curvefs_mds_lib",
    hdrs=glob(["*.h"]),
    srcs=glob(["*.cpp"],
        exclude = ["main.cpp"]
    ),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//curvefs/proto:metaserver_cc_proto",
        "//curvefs/proto:mds_cc_proto",
        "//curvefs/proto:space_cc_proto",
        "//curvefs/src/common:curvefs_common",
        "//src/common/concurrent:curve_concurrent",
        "//src/common:curve_common",
        "//src/kvstorageclient:kvstorage_client",
        "//src/leader_election:leader_election",
        "//src/idgenerator:idgenerator",
        "//curvefs/src/mds/codec:fs_mds_codec",
        "//curvefs/src/mds/common:fs_mds_common",
        "//curvefs/src/mds/idgenerator:fs_mds_idgenerator",
        "//curvefs/src/mds/metric:fs_mds_metric",
    ],
)

cc_binary(
    name = "curvefs_mds",
    srcs = glob(["main.cpp"]),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/src/mds:curvefs_mds_lib",
    ],
)
