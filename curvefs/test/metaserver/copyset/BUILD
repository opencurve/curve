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

load("//:copts.bzl", "CURVE_TEST_COPTS")

cc_test(
    name = "metaserver_copyset_test",
    srcs = glob(
        ["*.cpp"],
        exclude = ["raft_cli_servic2_test.cpp"],
    ),
    copts = CURVE_TEST_COPTS + ["-DUNIT_TEST"],
    deps = [
        "//curvefs/src/metaserver:curvefs_metaserver",
        "//curvefs/test/metaserver/copyset/mock:metaserver_copyset_test_mock",
        "//curvefs/test/metaserver/mock:metaserver_test_mock",
        "//curvefs/test/utils:curvefs_test_utils",
        "//src/common:curve_common",
        "//test/fs:fs_mock",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "metaserver_copyset_cli_service2_test",
    srcs = [
        "raft_cli_servic2_test.cpp",
    ],
    copts = CURVE_TEST_COPTS + ["-DUNIT_TEST"],
    deps = [
        "//curvefs/src/metaserver:curvefs_metaserver",
        "//curvefs/test/utils:curvefs_test_utils",
        "//src/common:curve_common",
        "//test/fs:fs_mock",
        "@com_google_googletest//:gtest",
    ],
)
