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
    "//test/chunkserver/datastore:chunkfilepool_helper",
    "@com_google_googletest//:gtest",
    "@com_google_googletest//:gtest_main",
]

cc_library(
    name = "chunkserver_mock",
    srcs = [
        "mock_copyset_node.h",
        "mock_node.h",
        "fake_datastore.h",
        "mock_raftsnapshot_filesystem_adaptor.h"
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/chunkserver:chunkserver-test-lib",
        "//src/common:curve_common",
        "//external:braft",
    ],
)

cc_library(
    name = "chunkserver-test-util-lib",
    srcs = glob([
        "chunkserver_test_util.h",
        "chunkserver_test_util.cpp",
    ]),
    copts = ["-std=c++14"],
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
    copts = ["-std=c++14"],
    deps = DEPS,
)

cc_test(
    name = "copyset-node-manager-test",
    srcs = ["copyset_node_manager_test.cpp"],
    copts = ["-std=c++14"],
    deps = DEPS,
)

cc_test(
    name = "chunk-service-test",
    srcs = ["chunk_service_test.cpp"],
    copts = ["-std=c++14"],
    linkopts = [
        "-luuid",
    ],
    deps = DEPS
         + ["//test/chunkserver:chunkserver-test-util-lib"],
)

cc_test(
    name = "chunk-service-test2",
    srcs = ["chunk_service_test2.cpp"],
    copts = ["-std=c++14"],
    linkopts = [
        "-luuid",
    ],
    deps = DEPS
         + ["//test/chunkserver:chunkserver-test-util-lib"],
)

cc_test(
    name = "copyset-epoch-test",
    srcs = ["copyset_epoch_test.cpp"],
    copts = ["-std=c++14"],
    linkopts = [
        "-luuid",
    ],
    deps = DEPS
         + ["//test/chunkserver:chunkserver-test-util-lib"],
)

cc_test(
    name = "chunk-snapshot-test",
    srcs = ["chunkserver_snapshot_test.cpp"],
    copts = ["-std=c++14"],
    deps = DEPS
         + ["//test/chunkserver:chunkserver-test-util-lib"],
)

cc_test(
    name = "cli-test",
    srcs = ["cli_test.cpp"],
    copts = ["-std=c++14"],
    deps = DEPS
         + ["//test/chunkserver:chunkserver-test-util-lib"],
)

cc_test(
    name = "cli2-test",
    srcs = ["cli2_test.cpp"],
    copts = ["-std=c++14"],
    linkopts = [
        "-luuid",
    ],
    deps = DEPS
         + ["//test/chunkserver:chunkserver-test-util-lib"],
)

cc_test(
    name = "braftcliservice-test",
    srcs = ["braft_cli_service_test.cpp"],
    copts = ["-std=c++14"],
    deps = DEPS
         + ["//test/chunkserver:chunkserver-test-util-lib"],
)

cc_test(
    name = "braftcliservice2-test",
    srcs = ["braft_cli_service2_test.cpp"],
    copts = ["-std=c++14"],
    linkopts = [
        "-luuid",
    ],
    deps = DEPS
         + ["//test/chunkserver:chunkserver-test-util-lib"],
)

# server exec for unit test
cc_binary(
    name = "server-test",
    srcs = [
        "server.cpp",
    ],
    copts = ["-std=c++14"],
    deps = DEPS,
)

# client exec
cc_binary(
    name = "client-test",
    srcs = ["client.cpp"],
    copts = ["-std=c++14"],
    deps = DEPS
         + ["//test/chunkserver:chunkserver-test-util-lib"],
)

# exec for multi-copyset test
cc_binary(
    name = "multi-copyset-io-test",
    srcs = ["multiple_copysets_io_test.cpp"],
    deps = DEPS,
)

cc_test(
    name = "chunkserver_test",
    srcs = [
        "chunkserver_helper_test.cpp",
        "chunkserver_test.cpp",
        "trash_test.cpp",
    ],
    copts = ["-std=c++14"],
    deps = DEPS
         + ["//test/client/fake:fake_lib"]
         + ["//test/chunkserver:chunkserver-test-util-lib"],
)

cc_test(
    name = "heartbeat_test",
    srcs = [
        "heartbeat_test.cpp",
        "heartbeat_test_common.h",
        "heartbeat_test_common.cpp",
        "heartbeat_test_main.cpp",
    ],
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
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "heartbeat_helper_test",
    srcs = [
        "mock_copyset_node_manager.h",
        "heartbeat_helper_test.cpp",
    ],
    deps = DEPS,
)

cc_test(
    name = "chunkserver_service_test",
    srcs = [
        "mock_copyset_node_manager.h",
        "chunkserver_service_test.cpp",
    ],
    deps = DEPS,
)

cc_test(
    name = "raftsnapshot_test",
    srcs = [
        "raftsnapshot_chunkfilepool_integration.cpp",
        "raftsnapshot_filesystem_adaptor_mock_unittest.cpp",
        "raftsnapshot_filesystem_adaptor_unittest.cpp",
        "raftsnapshot_attachment_test.cpp",
    ],
    copts = ["-std=c++14"],
    deps = DEPS
         + ["//test/chunkserver:chunkserver-test-util-lib"],
)

cc_test(
    name = "metric_test",
    srcs = glob([
        "metrics_test.cpp",
    ]),
    copts = ["-std=c++14"],
    deps = DEPS,
)
