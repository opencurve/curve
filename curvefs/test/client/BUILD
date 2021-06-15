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

cc_test(
    name = "block_device_client_test",
    srcs = glob([
        "block_device_client_test.cpp"
    ]),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "//include/client:include_client",
        "//curvefs/src/client:fuse_client_lib",
        "//test/client/mock:client_mock_lib",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "fuse_client_test",
    srcs = glob([
        "*.cpp",
        "*.c",
        "*.h"],
        exclude = [ "block_device_client_test.cpp" ]
    ),
    copts = ["-I/usr/local/include/fuse3",
             "-L/usr/local/lib/x86_64-linux-gnu"],
    deps = [
        "//external:gtest",
        "//curvefs/src/client:fuse_client_lib",
    ],
    linkopts = ["-lfuse3",
                "-lpthread",
                "-L/usr/local/lib/x86_64-linux-gnu"],
    visibility = ["//visibility:public"],
)
