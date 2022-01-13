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
    name = "fs_mock",
    srcs = [
            "mock_local_filesystem.h",
            "mock_posix_wrapper.h"
            ],
    deps = [
            "//src/fs:lfs",
            ],
    visibility = ["//visibility:public"],
    copts = CURVE_TEST_COPTS,
)

cc_test(
    name = "lfs_unittest",
    srcs = glob([
            "*.cpp",
        ]),
    copts = CURVE_TEST_COPTS,
    deps = [
            "//src/fs:lfs",
            "//test/fs:fs_mock",
            "@com_google_googletest//:gtest_main",
            ],
    visibility = ["//visibility:public"],
)
