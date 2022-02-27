#
#  Copyright (c) 2022 NetEase Inc.
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
    name = "curvefs_mds_space_unittest",
    srcs = glob([
        "*.cpp",
        "*.h",
    ]),
    copts = CURVE_TEST_COPTS,
    deps = [
        "//curvefs/proto:curvefs_common_cc_proto",
        "//curvefs/src/mds/space:curvefs_mds_space",
        "//curvefs/test/mds/mock:curvefs_mds_mock",
        "//curvefs/test/utils:curvefs_test_utils",
        "//external:brpc",
        "//external:butil",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)
