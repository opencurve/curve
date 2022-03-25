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

cc_library(
    name = "datastore_mock",
    srcs = [
            "mock_file_pool.h",
            "mock_datastore.h"
            ],
    deps = [
            "//src/chunkserver/datastore:chunkserver_datastore",
            ],
    visibility = ["//visibility:public"],
    copts = CURVE_TEST_COPTS,
)

cc_library(
    name = "filepool_helper",
    srcs = [
            "filepool_helper.cpp",
            "filepool_helper.h",
            ],
    deps = [
            "@com_google_googletest//:gtest",
            "//src/chunkserver/datastore:chunkserver_datastore",
            ],
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
)

cc_test(
    name = "curve_datastore_unittest",
    srcs = [
        "filepool_unittest.cpp",
        "filepool_mock_unittest.cpp",
        "datastore_mock_unittest.cpp",
        "datastore_unittest_main.cpp",
        "file_helper_unittest.cpp",
    ],
    copts = CURVE_TEST_COPTS,
    deps = [
        "@com_google_googletest//:gtest",
        "//external:gflags",
        "//include:include-common",
        "//include/chunkserver:include-chunkserver",
        "//src/common:curve_common",
        "//src/chunkserver/datastore:chunkserver_datastore",
        "//test/fs:fs_mock",
        "//test/chunkserver/datastore:datastore_mock",
        "//external:json",
        "//test/chunkserver/datastore:filepool_helper",
    ],
)
