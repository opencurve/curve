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

COPTS = [
    "-DGFLAGS=gflags",
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DHAVE_ZLIB",
    "-DHAVE_SSE42",
    "-DNDEBUG",
    "-std=c++11",
    "-pipe",
    "-W",
    "-Wall",
    "-Wno-unused-parameter",
    "-fPIC",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
    "-msse4.2",
    "-pthread",
    "-Wsign-compare",
    "-Wno-unused-variable",
    "-Wno-missing-field-initializers",
    "-Woverloaded-virtual",
    "-Wnon-virtual-dtor",
]

ARM64_COPTS = [
    "-DGFLAGS=gflags",
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DHAVE_ZLIB",
    "-DNDEBUG",
    "-std=c++11",
    "-pipe",
    "-W",
    "-Wall",
    "-Wno-unused-parameter",
    "-fPIC",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
    "-pthread",
    "-Wsign-compare",
    "-Wno-unused-variable",
    "-Wno-missing-field-initializers",
    "-Woverloaded-virtual",
    "-Wnon-virtual-dtor",
]

cc_library(
    name = "nebdclient",
    srcs = glob([
        "*.h",
        "*.cpp",
        ]
    ),
    visibility = ["//visibility:public"],
    deps = [
        "//external:glog",
        "//external:brpc",
        "//nebd/src/common:nebd_common",
        "//nebd/proto:client_cc_proto",
        "//include:include-common"
    ],
    copts = select({
        ":arm64_build": ARM64_COPTS,
        ":x86_build": COPTS,
        "//conditions:default": COPTS,
    }),
    linkopts = ["-Wl,-rpath=/usr/lib/nebd"],
)

config_setting(
    name = "arm64_build",
    values = {"cpu": "aarch64"},
)

config_setting(
    name = "x86_build",
    values = {"cpu": "x86_64"},
)
