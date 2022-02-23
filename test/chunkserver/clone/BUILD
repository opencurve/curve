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

cc_test(
    name = "clone_test",
    srcs = glob([
        "*.cpp",
    ]) + [
        "clone_test_util.h",
    ],
    copts = ["-std=c++11"],
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//external:gflags",
        "//external:glog",
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//include:include-common",
        "//include/chunkserver:include-chunkserver",
        "//src/common:curve_common",
        "//src/chunkserver:chunkserver-test-lib",
        "//test/chunkserver/datastore:datastore_mock",
        "//test/chunkserver:chunkserver_mock",
        "//test/chunkserver/clone:clone_mock",
        "//test/client/mock:curve_client_mock",
        "//test/common:common_mock",
    ],
)

cc_library(
    name = "clone_mock",
    srcs = [
            "mock_clone_copyer.h",
            "mock_clone_manager.h"
            ],
    deps = [
            "//src/chunkserver:chunkserver-test-lib",
            ],
    visibility = ["//visibility:public"],
)
