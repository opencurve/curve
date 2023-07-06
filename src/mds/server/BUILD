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

load("//:copts.bzl", "CURVE_DEFAULT_COPTS", "CURVE_TEST_COPTS")

cc_library(
    name = "mds",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//src/common:curve_common",
        "//src/common:curve_auth",
        "//src/leader_election",
        "//src/mds/heartbeat",
        "//src/mds/nameserver2",
        "//src/mds/nameserver2/allocstatistic:alloc_statistic",
        "//src/mds/nameserver2/helper",
        "//src/mds/schedule",
        "//src/mds/schedule/scheduleService:scheduleservice",
        "//src/mds/topology",
        "//src/mds/auth",
        "//src/client:curve_client",
    ],
)

cc_library(
    name = "mds_for_test",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//src/common:curve_common",
        "//src/leader_election:leader_election_for_test",
        "//src/mds/heartbeat",
        "//src/mds/nameserver2",
        "//src/mds/nameserver2/allocstatistic:alloc_statistic",
        "//src/mds/nameserver2/helper",
        "//src/mds/schedule",
        "//src/mds/schedule/scheduleService:scheduleservice",
        "//src/mds/topology",
        "//src/mds/auth",
    ],
)
