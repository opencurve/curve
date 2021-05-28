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
    name = "fuse_client_test",
    srcs = glob([
        "*.cpp",
        "*.c",
        "*.h",
    ]),
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
