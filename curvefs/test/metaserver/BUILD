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
    name = "curvefs_metaserver_test",
    srcs = glob(
        ["*.cpp", "*.h"],
        exclude = [
            "metastore_test.cpp",
            "mock_client_s3.h",
            "mock_metaserver_s3.h",
            "metaserver_s3_adaptor_test.h",
            "metaserver_s3_adaptor_test.cpp",
            "metaserver_s3_test.cpp",
            "mock_s3compactwq_impl.h",
            "s3compactwq_test.cpp",
            "mock_s3_adapter.h",
            "partition_clean_test.cpp",
        ],
    ),
    copts = CURVE_TEST_COPTS,
    deps = [
            "//curvefs/src/metaserver:curvefs_metaserver",
            "@com_google_googletest//:gtest_main",
            "@com_google_googletest//:gtest",
            "//external:brpc",
            "//curvefs/proto:curvefs_topology_cc_proto",
            "//curvefs/test/metaserver/copyset/mock:metaserver_copyset_test_mock",
            "//curvefs/src/metaserver:metaserver_s3_lib",
            "//curvefs/test/metaserver/storage:metaserver_storage_test_utils",
            "//curvefs/test/metaserver/mock:metaserver_test_mock",
            "//curvefs/test/client/rpcclient:rpcclient_test_mock",
    ],
)

cc_test(
    name = "curvefs_partition_clean_test",
    srcs = glob([
        "partition_clean_test.cpp",
        "mock_metaserver_s3_adaptor.h",
        "mock_mds_client.h",
    ],
        exclude = [],
    ),
    copts = CURVE_TEST_COPTS,
    deps = [
            "//curvefs/src/metaserver:curvefs_metaserver",
            "@com_google_googletest//:gtest_main",
            "@com_google_googletest//:gtest",
            # "//external:brpc",
            "//curvefs/proto:curvefs_topology_cc_proto",
            "//curvefs/test/metaserver/copyset/mock:metaserver_copyset_test_mock",
            "//curvefs/src/metaserver:metaserver_s3_lib",
            "//curvefs/test/client/rpcclient:rpcclient_test_mock",
            "//curvefs/test/metaserver/storage:metaserver_storage_test_utils",
    ],
)

# s3 adaptor
cc_test(
    name = "metaserver_s3_adaptor_test",
    srcs = glob([
        "mock_metaserver_s3.h",
        "metaserver_s3_adaptor_test.h",
        "metaserver_s3_adaptor_test.cpp",
        "curvefs/test/client/mock_client_s3.h",
        "mock_metaserver_s3_adaptor.h",
        "metaserver_s3_test.cpp"
    ]),
    copts = CURVE_TEST_COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//external:brpc",
        "//external:glog",
        "//external:gflags",
        "//include/client:include_client",
        "//src/common:curve_s3_adapter",
        "//src/client:curve_client",
        "//curvefs/src/client:fuse_client_lib",
        "//curvefs/src/metaserver:metaserver_s3_lib",
        "//curvefs/test/client:client_s3_test_lib",
        "//test/client/mock:client_mock_lib",
        "//test/common:common_mock"
    ],
    linkopts = [
        "-lfuse3",
        "-L/usr/local/lib/x86_64-linux-gnu",
    ],
)

cc_test(
    name = "curvefs_metaserver_metastore_test",
    srcs = glob(
        ["metastore*"],
    ),
    copts = CURVE_TEST_COPTS,
    deps = [
        "//curvefs/src/metaserver:curvefs_metaserver",
        "@com_google_googletest//:gtest_main",
        "@com_google_googletest//:gtest",
        "//external:brpc",
        "//curvefs/test/metaserver/storage:metaserver_storage_test_utils",
    ],
)

cc_test(
    name = "s3compactwq_test",
    srcs = glob([
        "s3compactwq_test.cpp",
        "mock_s3compactwq_impl.h",
        "mock_s3_adapter.h",
        "mock_s3infocache.h"
    ]),
    copts = CURVE_TEST_COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/src/metaserver:curvefs_metaserver",
        "//curvefs/test/metaserver/copyset/mock:metaserver_copyset_test_mock",
        "//curvefs/test/metaserver/storage:metaserver_storage_test_utils",
        "@com_google_googletest//:gtest_main",
        "@com_google_googletest//:gtest",
    ],
)
