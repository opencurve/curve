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
    "-std=c++11",
    "-Wall",
    "-Werror",
]

cc_library(
    name = "fs_mds_codec",
    srcs = ["codec.cpp"],
    hdrs = ["codec.h"],
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/src/mds/common:fs_mds_common",
        "//external:protobuf",
    ],
)
