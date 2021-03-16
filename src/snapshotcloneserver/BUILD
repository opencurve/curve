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

ARM64_COPTS = [
    "-DGFLAGS=gflags",
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DNDEBUG",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
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
    name = "snapshot_server_lib",
    srcs = glob([
        "clone/*.cpp",
        "clone/*.h",
        "snapshot/*.cpp",
        "snapshot/*.h",
        "common/*.cpp",
        "common/*.h",
        "*.cpp",
        "*.h"
    ],
    exclude = [
        "main.cpp",
    ]),
    copts = select({
        ":arm64_build": ARM64_COPTS,
        ":x86_build": COPTS,
        "//conditions:default": COPTS,
    }),
    linkopts = [
        "-lrt",
        "-lssl",
        "-lcrypto",
        "-ldl",
        "-lz",
        "-lpthread",
        "-luuid",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:leveldb",
        "//external:protobuf",
        "//external:json",
        "//proto:snapshotcloneserver_cc_proto",
        "//src/common:curve_common",
        "//src/common:curve_s3_adapter",
        "//src/common/snapshotclone:curve_snapshotclone",
        "//proto:nameserver2_cc_proto",
        "//proto:chunkserver-cc-protos",
        "//src/client:curve_client",
        "//src/leader_election:leader_election"
    ],
)

cc_binary(
    name = "snapshotcloneserver",
    srcs = glob([
        "clone/*.cpp",
        "clone/*.h",
        "snapshot/*.cpp",
        "snapshot/*.h",
        "common/*.cpp",
        "common/*.h",
        "*.cpp",
        "*.h"
    ]),
    copts = select({
        ":arm64_build": ARM64_COPTS,
        ":x86_build": COPTS,
        "//conditions:default": COPTS,
    }),
    linkopts = [
        "-lrt",
        "-lssl",
        "-lcrypto",
        "-ldl",
        "-lz",
        "-lpthread",
        "-luuid",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:leveldb",
        "//external:protobuf",
        "//external:json",
        "//proto:snapshotcloneserver_cc_proto",
        "//src/common:curve_common",
        "//src/common:curve_s3_adapter",
        "//src/common/snapshotclone:curve_snapshotclone",
        "//proto:nameserver2_cc_proto",
        "//proto:chunkserver-cc-protos",
        "//src/client:curve_client",
        "//src/leader_election:leader_election"
    ],
)

config_setting(
    name = "arm64_build",
    values = {"cpu": "aarch64"},
)

config_setting(
    name = "x86_build",
    values = {"cpu": "x86_64"},
)
