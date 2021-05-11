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
    name = "common-test",
    srcs = glob([
        "*.cpp",
    ]),
    linkopts = [
        "-luuid"
    ],
    deps = [
        "//src/common:curve_common",
        "//src/common:curve_auth",
        "//src/common:curve_s3_adapter",
        "//src/common/concurrent:curve_concurrent",
        "//src/kvstorageclient:kvstorage_client",
        "//src/common/concurrent:curve_dlock",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "common_mock",
    srcs = [
            "mock_s3_adapter.h",
            ],
    linkopts = [
        "-luuid"
    ],
    deps = [
            "//src/common:curve_common",
            "//src/common:curve_s3_adapter",
            ],
    visibility = ["//visibility:public"],
)
