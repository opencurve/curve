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

load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

cc_test(
    name = "schedule-test",
    srcs = glob([
        "*.cpp",
        "*.h",
    ]),
    copts = GCC_TEST_FLAGS,
    deps = [
        "//external:gflags",
        "//external:glog",
        "//src/mds/schedule",
        "//src/mds/topology",
        "//test/mds/mock:common_mock",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)
