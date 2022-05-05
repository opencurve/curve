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

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library

load("//:copts.bzl", "CURVE_DEFAULT_COPTS")

cc_library(
    name = "curve_client",
    srcs = glob(
        ["*.cpp"],
        exclude = ["libcurve.cpp"],
    ),
    hdrs = glob(
        ["*h"],
        exclude = ["libcurve.h"],
    ),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:brpc",
        "//external:protobuf",
        "//src/common:curve_common",
        "//src/common:curve_auth",
        "//include/client:include_client",
        "//include:include-common",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//proto:chunkserver-cc-protos",
    ],
)

cc_library(
    name = "cbd",
    srcs = glob([
        "libcbd_ext4.cpp",
        "libcbd_libcurve.cpp",
    ]),
    hdrs = glob([
        "libcbd.h",
    ]),
    copts = CURVE_DEFAULT_COPTS,
    linkopts = [
        "-lrt",
        "-lssl",
        "-lcrypto",
        "-ldl",
        "-lz",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:bvar",
        "//external:gflags",
        "//external:protobuf",
        "//proto:chunkserver-cc-protos",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "//include/client:include_client",
    ],
)

cc_library(
    name = "curve",
    srcs = glob([
        "libcurve_client.cpp",
    ]),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//include:include-common",
        "//include/client:include_client",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/client:curve_client",
        "//src/common:curve_auth",
        "//src/common:curve_common",
    ],
)
