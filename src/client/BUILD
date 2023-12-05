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
    name = "curve_client",
    srcs = glob(["*.cpp"],
        exclude = ["libcurve.cpp"]),
    hdrs = glob(["*h"],
        exclude = ["libcurve.h"]),
    defines = ["UNIT_TEST"],
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
        # "//src/chunkserver:chunkserver-lib"
        "@com_google_googletest//:gtest_prod",
    ],
    linkopts = [
        "-luuid"
    ],
    copts = COPTS,
    visibility = ["//visibility:public"],
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
    copts = [
        "-DGFLAGS=gflags",
        "-DOS_LINUX",
        "-DSNAPPY",
        "-DHAVE_ZLIB",
        "-DHAVE_SSE42",
        "-DNDEBUG",
        "-fno-omit-frame-pointer",
        "-momit-leaf-frame-pointer",
        "-msse4.2",
        "-pthread",
        "-Wsign-compare",
        "-Wno-unused-parameter",
        "-Wno-unused-variable",
        "-Wno-missing-field-initializers",
    ],
    linkopts = [
        "-lrt",
        "-lssl",
        "-lcrypto",
        "-ldl",
        "-lz",
        "-lpthread",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//proto:chunkserver-cc-protos",
        "//src/common:curve_common",
        "//src/client:curve_client",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:bvar",
        "//external:gflags",
        "//external:protobuf",
        "//include/client:include_client",
    ],
)

cc_library(
    name = "curve",
    srcs = glob([
        "libcurve_client.cpp",
    ]),
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
        "//src/client:curve_client"
    ],
    copts = COPTS,
    visibility = ["//visibility:public"],
)
