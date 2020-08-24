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
    name = "mds_heartbeat_basic_integration",
    srcs = glob([
        "common.h",
        "common.cpp",
        "heartbeat_basic_test.cpp"]),
    deps = [
        "//src/mds/heartbeat:heartbeat",
        "//src/mds/nameserver2:nameserver2",
        "//src/common:curve_common",
        "//src/mds/topology:topology",
        "//src/mds/schedule",
        "//test/mds/mock:common_mock",
        "@com_google_googletest//:gtest_main",
        "@com_google_googletest//:gtest",
        "//external:brpc"
    ]
)

cc_test(
    name = "mds_heartbeat_exception_integration",
    srcs = glob([
        "common.h",
        "common.cpp",
        "heartbeat_exception_test.cpp"]),
    deps = [
        "//src/mds/heartbeat:heartbeat",
        "//src/mds/nameserver2:nameserver2",
        "//src/common:curve_common",
        "//src/mds/topology:topology",
        "//src/mds/schedule",
        "//test/mds/mock:common_mock",
        "@com_google_googletest//:gtest_main",
        "@com_google_googletest//:gtest",
        "//external:brpc"
    ]
)
