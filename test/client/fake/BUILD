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

load("//:copts.bzl", "CURVE_TEST_COPTS")

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_library(
    name = "fake_lib",
    srcs = glob(
        ["*.cpp"],
        exclude = ["client_workflow_test.cpp",
                    "mds_workflow_test.cpp",
                    "client_workflow_test4snap.cpp"
                    ],
    ),
    hdrs = glob(["*.h"]),
    linkopts = ["-lfiu"],
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:brpc",
        "//external:braft",
        "//external:protobuf",
        "//src/common:curve_common",
        "//include/client:include_client",
        "//proto:nameserver2_cc_proto",
        "//proto:chunkserver-cc-protos",
        "//proto:schedule_cc_proto",
        "//proto:topology_cc_proto",
        "//src/client:curve_client"
    ],
    visibility = ["//visibility:public"],
    copts = CURVE_TEST_COPTS,
)
