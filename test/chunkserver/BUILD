DEPS = [
    "//external:braft",
    "//external:brpc",
    "//external:bthread",
    "//external:butil",
    "//external:gflags",
    "//external:glog",
    "//external:leveldb",
    "//proto:chunkserver-cc-protos",
    "//src/chunkserver:chunkserver-test-lib",
    "//src/chunkserver/datastore:chunkserver_datastore",
    "//src/common:curve_common",
    "//src/fs:lfs",
    "//test/fs:fs_mock",
    "@com_google_googletest//:gtest",
    "@com_google_googletest//:gtest_main",
]

cc_test(
    name = "chunk-test1",
    srcs = glob([
        "copyset_node_manager_test.cpp",
        "copyset_service_test.cpp",
        "op_request_test.cpp",
        "copyset_node_test.cpp",
        "mock_local_file_system.h",
        "conf_epoch_file_test.cpp",
        "concurrent_apply_unittest.cpp",
        "mock_local_file_system.h",
        "fake_datastore.h",
    ]),
    copts = ["-std=c++14"],
    deps = DEPS,
)

cc_test(
    name = "chunk-test",
    srcs = glob([
        "chunk_service_test.cpp",
        "mock_local_file_system.h",
        "chunkserver_test_util.h",
        "chunkserver_test_util.cpp",
        "fake_datastore.h",
    ]),
    copts = ["-std=c++14"],
    deps = DEPS,
)

cc_test(
    name = "chunk-snapshot-test",
    srcs = glob([
        "chunkserver_test_util.h",
        "chunkserver_test_util.cpp",
        "chunkserver_snapshot_test.cpp",
        "fake_datastore.h",
    ]),
    copts = ["-std=c++14"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//proto:chunkserver-cc-protos",
        "//src/chunkserver:chunkserver-test-lib",
        "//src/common:curve_common",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "cli-test",
    srcs = glob([
        "cli_test.cpp",
        "chunkserver_test_util.h",
        "chunkserver_test_util.cpp",
        "fake_datastore.h",
    ]),
    copts = ["-std=c++14"],
    deps = DEPS,
)

cc_test(
    name = "braftcliservice-test",
    srcs = glob([
        "braft_cli_service_test.cpp",
        "chunkserver_test_util.h",
        "chunkserver_test_util.cpp",
        "fake_datastore.h",
    ]),
    copts = ["-std=c++14"],
    deps = DEPS,
)

cc_library(
    name = "chunkserver-test-util-lib",
    srcs = glob([
        "fake_datastore.h",
        "chunkserver_test_util.h",
        "chunkserver_test_util.cpp",
    ]),
    copts = ["-std=c++14"],
    visibility = ["//visibility:public"],
    deps = DEPS,
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
    srcs = [
        "chunkserver_test_util.cpp",
        "chunkserver_test_util.h",
        "client.cpp",
        "fake_datastore.h",
    ],
    copts = ["-std=c++14"],
    deps = [
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
        "//src/common:curve_common",
    ],
)

# exec for multi-copyset test
cc_binary(
    name = "multi-copyset-io-test",
    srcs = [
        "multiple_copysets_io_test.cpp",
    ],
    deps = [
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
        "//src/common:curve_common",
    ],
)

cc_test(
    name = "chunkserver_test",
    srcs = [
        "chunkserver_test.cpp",
        "chunkserver_test_util.cpp",
        "chunkserver_test_util.h",
        "fake_datastore.h",
    ],
    copts = ["-std=c++14"],
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
        "//src/chunkserver/datastore:chunkserver_datastore",
        "//src/common:curve_common",
        "//test/client/fake:fake_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "heartbeat_test",
    srcs = [
        "heartbeat_test_main.cpp",
        "heartbeat_test.h",
        "heartbeat_test.cpp",
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

cc_library(
    name = "chunkserver_mock",
    srcs = [
            "mock_copyset_node.h",
            ],
    deps = [
            "//src/chunkserver:chunkserver-test-lib",
            ],
    visibility = ["//visibility:public"],
)
