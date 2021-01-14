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

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
COPTS = [
    "-DGFLAGS=gflags",
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DHAVE_SSE42",
    "-DNDEBUG",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
    "-msse4.2",
    "-pthread",
    "-Wsign-compare",
    "-Wno-unused-parameter",
    "-Wno-unused-variable",
    "-Woverloaded-virtual",
    "-Wnon-virtual-dtor",
    "-Wno-missing-field-initializers",
    "-std=c++11",
    #"-faligned-new"
]

cc_test(
    name = "client_test_unittest",
    srcs = glob([
        "*.h",
        "*.cpp"],
        exclude = ["client_session_unittest.cpp",
                "snapshot_service_unittest.cpp",
                "client_metric_test.cpp",
                "libcbd_libcurve_test.cpp",
                "inflight_rpc_control_test.cpp",
                "mds_failover_test.cpp",
                "mds_client_test.cpp",
                "libcurve_client_unittest.cpp",
                "lease_executor_test.cpp",
                "request_sender_test.cpp",
                "mds_client_test.cpp"
                ]
    ),
    copts = COPTS,
    defines = ["UNIT_TEST", "FIU_ENABLE"],
    linkopts = ["-lfiu"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//include/client:include_client",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "//test/client/fake:fake_lib",
        "//src/common/concurrent:curve_concurrent",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//test/integration/cluster_common:integration_cluster_common",
    ],
)

cc_test(
    name = "client_mds_failover_test",
    srcs = [
        "mds_failover_test.cpp"],
    copts = COPTS,
    defines = ["UNIT_TEST", "FIU_ENABLE"],
    linkopts = ["-lfiu"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//include/client:include_client",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "//test/client/fake:fake_lib",
        "//src/common/concurrent:curve_concurrent",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//test/integration/cluster_common:integration_cluster_common",
    ],
)

cc_test(
    name = "client_inflightrpc_test",
    srcs = glob([
        "*.h",
        "inflight_rpc_control_test.cpp"]
    ),
    copts = COPTS,
    defines = ["UNIT_TEST", "FIU_ENABLE"],
    linkopts = ["-lfiu"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//include/client:include_client",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "//src/common/concurrent:curve_concurrent",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "libcurve_client_unittest",
    srcs = glob([
        "libcurve_client_unittest.cpp"
    ]),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    linkopts = ["-lfiu"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//include/client:include_client",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "//test/client/fake:fake_lib",
        "//test/client:client_mock",
        "//src/common/concurrent:curve_concurrent",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//test/integration/cluster_common:integration_cluster_common",
    ],
)

cc_test(
    name = "client_libcbd_unittest",
    srcs = glob([
        "libcbd_libcurve_test.cpp"
    ]),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    linkopts = ["-lfiu"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//include/client:include_client",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "//test/client/fake:fake_lib",
        "//src/common/concurrent:curve_concurrent",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//test/integration/cluster_common:integration_cluster_common",
    ],
)

cc_test(
    name = "client_session_unittest",
    srcs = glob([
        "client_session_unittest.cpp"
    ]),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//include/client:include_client",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "//test/client/fake:fake_lib",
        "//src/common/concurrent:curve_concurrent",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//test/integration/cluster_common:integration_cluster_common",
    ],
)

cc_test(
    name = "client_snapshot_test_unittest",
    srcs = glob([
        "*.h",
        "snapshot_service_unittest.cpp"]
    ),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    linkopts = ["-lfiu"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//include/client:include_client",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "//test/client/fake:fake_lib",
        "//src/common/concurrent:curve_concurrent",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//test/integration/cluster_common:integration_cluster_common",
    ],
)

cc_test(
    name = "client_request_sender_test",
    srcs = glob([
        "*.h",
        "request_sender_test.cpp"]
    ),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:gflags",
        "//external:glog",
        "//external:brpc",
        "//include/client:include_client",
        "//src/client:curve_client",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "client_lease_executor_test",
    srcs = glob([
        "*.h",
        "lease_executor_test.cpp"]
    ),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:gflags",
        "//external:glog",
        "//external:brpc",
        "//include/client:include_client",
        "//src/client:curve_client",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "client_metric_test",
    srcs = glob([
        "*.h",
        "client_metric_test.cpp"]
    ),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    linkopts = ["-lfiu"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//include/client:include_client",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "//test/client/fake:fake_lib",
        "//src/common/concurrent:curve_concurrent",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//test/integration/cluster_common:integration_cluster_common",
    ],
)

cc_library(
    name = "client_mock",
    srcs = [
            "mock_file_client.h",
            ],
    deps = [
            "//src/client:curve_client",
            ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "client_mds_client_test",
    srcs = [
        "mds_client_test.cpp"],
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//include/client:include_client",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "//src/common/concurrent:curve_concurrent",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//test/client/mock:curve_client_mock"
    ],
)
