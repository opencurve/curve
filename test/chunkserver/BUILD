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

DEPS = [
    "//external:braft",
    "//external:brpc",
    "//external:bthread",
    "//external:butil",
    "//external:gflags",
    "//external:glog",
    "//external:leveldb",
    "//external:protobuf",
    "//proto:chunkserver-cc-protos",
    "//src/chunkserver:chunkserver-test-lib",
    "//src/chunkserver/datastore:chunkserver_datastore",
    "//src/common:curve_common",
    "//src/fs:lfs",
    "//test/fs:fs_mock",
    "//test/chunkserver:chunkserver_mock",
    "//test/chunkserver/datastore:datastore_mock",
    "//test/chunkserver/datastore:filepool_helper",
    "//test/common:common_mock",
    "@com_google_googletest//:gtest",
    "@com_google_googletest//:gtest_main",
]

cc_library(
    name = "chunkserver_mock",
    srcs = [
        "fake_datastore.h",
        "mock_copyset_node.h",
        "mock_curve_filesystem_adaptor.h",
        "mock_node.h",
    ],
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//src/chunkserver:chunkserver-test-lib",
        "//src/common:curve_common",
    ],
)

cc_library(
    name = "chunkserver-test-util-lib",
    srcs = glob([
        "chunkserver_test_util.h",
        "chunkserver_test_util.cpp",
    ]),
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    deps = DEPS,
)

cc_test(
    name = "chunk-test1",
    srcs = glob([
        "copyset_service_test.cpp",
        "op_request_test.cpp",
        "copyset_node_test.cpp",
        "conf_epoch_file_test.cpp",
        "inflight_throttle_test.cpp",
        "concurrent_apply_unittest.cpp",
    ]),
    copts = CURVE_TEST_COPTS,
    deps = DEPS,
)

cc_test(
    name = "copyset-node-manager-test",
    srcs = ["copyset_node_manager_test.cpp"],
    copts = CURVE_TEST_COPTS,
    deps = DEPS,
)

cc_test(
    name = "chunk-service-test",
    srcs = ["chunk_service_test.cpp"],
    copts = CURVE_TEST_COPTS,
    deps = DEPS + [
        "//test/chunkserver:chunkserver-test-util-lib",
    ],
)

cc_test(
    name = "chunk-service-test2",
    srcs = ["chunk_service_test2.cpp"],
    copts = CURVE_TEST_COPTS,
    deps = DEPS + [
        "//test/chunkserver:chunkserver-test-util-lib",
    ],
)

cc_test(
    name = "copyset-epoch-test",
    srcs = ["copyset_epoch_test.cpp"],
    copts = CURVE_TEST_COPTS,
    deps = DEPS + [
        "//test/chunkserver:chunkserver-test-util-lib",
    ],
)

cc_test(
    name = "chunk-snapshot-test",
    srcs = ["chunkserver_snapshot_test.cpp"],
    copts = CURVE_TEST_COPTS,
    deps = DEPS + [
        "//test/chunkserver:chunkserver-test-util-lib",
    ],
)

cc_test(
    name = "cli-test",
    srcs = ["cli_test.cpp"],
    copts = CURVE_TEST_COPTS,
    deps = DEPS + [
        "//test/chunkserver:chunkserver-test-util-lib",
    ],
)

cc_test(
    name = "cli2-test",
    srcs = ["cli2_test.cpp"],
    copts = CURVE_TEST_COPTS,
    deps = DEPS + [
        "//test/chunkserver:chunkserver-test-util-lib",
    ],
)

cc_test(
    name = "braftcliservice-test",
    srcs = ["braft_cli_service_test.cpp"],
    copts = CURVE_TEST_COPTS,
    deps = DEPS + [
        "//test/chunkserver:chunkserver-test-util-lib",
    ],
)

cc_test(
    name = "braftcliservice2-test",
    srcs = ["braft_cli_service2_test.cpp"],
    copts = CURVE_TEST_COPTS,
    deps = DEPS + [
        "//test/chunkserver:chunkserver-test-util-lib",
    ],
)

cc_test(
    name = "scan-manager-test",
    srcs = ["scan_manager_test.cpp",
            "mock_copyset_node_manager.h"],
    copts = ["-std=c++11"],
    deps = DEPS,
)

# server exec for unit test
cc_binary(
    name = "server-test",
    srcs = [
        "server.cpp",
    ],
    copts = CURVE_TEST_COPTS,
    deps = DEPS,
)

# client exec
cc_binary(
    name = "client-test",
    srcs = ["client.cpp"],
    copts = CURVE_TEST_COPTS,
    deps = DEPS + [
        "//test/chunkserver:chunkserver-test-util-lib",
    ],
)

# exec for multi-copyset test
cc_binary(
    name = "multi-copyset-io-test",
    srcs = ["multiple_copysets_io_test.cpp"],
    copts = CURVE_TEST_COPTS,
    deps = DEPS,
)

cc_test(
    name = "chunkserver_test",
    srcs = [
        "chunkserver_helper_test.cpp",
        "chunkserver_test.cpp",
        "trash_test.cpp",
    ],
    copts = CURVE_TEST_COPTS,
    deps = DEPS + [
        "//test/client/fake:fake_lib",
        "//test/chunkserver:chunkserver-test-util-lib",
    ],
)

cc_test(
    name = "heartbeat_test",
    srcs = [
        "heartbeat_test.cpp",
        "heartbeat_test_common.cpp",
        "heartbeat_test_common.h",
        "heartbeat_test_main.cpp",
    ],
    copts = CURVE_TEST_COPTS,
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:leveldb",
        "//external:protobuf",
        "//proto:chunkserver-cc-protos",
        "//src/chunkserver:chunkserver-test-lib",
        "//src/common:curve_common",
        "//test/client/fake:fake_lib",
        "//test/integration/common:integration-test-common",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "heartbeat_helper_test",
    srcs = [
        "heartbeat_helper_test.cpp",
        "mock_copyset_node_manager.h",
    ],
    copts = CURVE_TEST_COPTS,
    deps = DEPS,
)

cc_test(
    name = "chunkserver_service_test",
    srcs = [
        "chunkserver_service_test.cpp",
        "mock_copyset_node_manager.h",
    ],
    copts = CURVE_TEST_COPTS,
    deps = DEPS,
)

cc_test(
    name = "metric_test",
    srcs = glob([
        "metrics_test.cpp",
    ]),
    copts = CURVE_TEST_COPTS,
    deps = DEPS,
)
