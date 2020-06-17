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

load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

cc_library(
    name = "topology",
    srcs = glob([
        "*.cpp",
        "*.h",
    ]),
    copts = GCC_FLAGS,
    linkopts = [
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//external:glog",
        "//external:json",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/mds/common:mds_common",
        "//src/mds/copyset:copyset",
        "//src/common:curve_common",
        "//src/mds/nameserver2/allocstatistic:alloc_statistic",
    ],
)
