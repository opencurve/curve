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

cc_test(
        name = "snapshot_server_utest",
        srcs = glob([
            "*.cpp","*.h"
        ],
        ),
        deps = ["//src/common/concurrent:curve_concurrent", 
                "//external:gtest",
                "//src/snapshotcloneserver:snapshot_server_lib",
                "//test/util:test_util",
                "//test/common:common_mock",
                ],
        copts = CURVE_TEST_COPTS
        )

cc_library(
    name = "mock_repo",
    srcs = [
            "mock_repo.h",
            ],
    deps = [],
    visibility = ["//visibility:public"],
    copts = CURVE_TEST_COPTS,
)
