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

load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

cc_binary(
    name = "curve-fuse",
    srcs = glob([
        "*.cpp",
        "*.c",
        "*.h",
        "s3/*.cpp",
        "s3/*.h",
    ]),
    copts = GCC_FLAGS + ["-I/usr/local/include/fuse3"],
    deps = [
        "//external:brpc",
        "//external:glog",
        "//external:gflags",
        "//curvefs/proto:metaserver_cc_proto",
        "//curvefs/proto:mds_cc_proto",
        "//curvefs/proto:space_cc_proto",
        "//curvefs/src/common:curvefs_common",
        "//src/common:curve_common",
        "//src/client:curve_client",
        "//src/common:curve_s3_adapter",
        "//curvefs/src/client/rpcclient:rpcclient",
    ],
    linkopts = ["-lfuse3",
                "-lpthread",
                "-L/usr/local/lib/x86_64-linux-gnu"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "fuse_client_lib",
    srcs = glob([
        "*.cpp",
        "*.c",
        "*.h",
        "s3/*.cpp",
        "s3/*.h",
    ],
    exclude = [ "main.c"]),
    copts = GCC_TEST_FLAGS + ["-I/usr/local/include/fuse3"],
    deps = [
        "//external:brpc",
        "//external:glog",
        "//external:gflags",
        "//curvefs/proto:metaserver_cc_proto",
        "//curvefs/proto:mds_cc_proto",
        "//curvefs/proto:space_cc_proto",
        "//curvefs/src/common:curvefs_common",
        "//src/common:curve_common",
        "//src/client:curve_client",
        "//src/common:curve_s3_adapter",
        "//curvefs/src/client/rpcclient:rpcclient",
    ],
    linkopts = ["-lfuse3",
                "-lpthread",
                "-L/usr/local/lib/x86_64-linux-gnu"],
    visibility = ["//visibility:public"],
)
