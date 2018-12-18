cc_binary(
    name = "chunkserver-test",
    srcs = glob([
        "cli_test.cpp",
        "chunk_service_test.cpp",
        "copyset_node_manager_test.cpp",
        "op_request_test.cpp",
        "copyset_service_test.cpp",
    ]),
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:leveldb",
        "//proto:chunkserver-cc-protos",
        "//src/chunkserver:chunkserver-lib",
        "//src/common:curve_common",
        "@com_google_googletest//:gtest_main",
    ],
)

# server exec for unit test
cc_binary(
    name = "server-test",
    srcs = [
        "server.cpp",
    ],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:leveldb",
        "//proto:chunkserver-cc-protos",
        "//src/chunkserver:chunkserver-lib",
        "//src/common:curve_common",
    ],
)

# client exec
cc_binary(
    name = "client-test",
    srcs = [
        "client.cpp",
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
        "//src/chunkserver:chunkserver-lib",
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
        "//external:leveldb",
        "//external:protobuf",
        "//proto:chunkserver-cc-protos",
        "//src/chunkserver:chunkserver-lib",
        "//src/common:curve_common",
    ],
)

cc_test(
    name = "chunkserver_test",
    srcs = [
        "chunkserver_test.cpp",
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
        "//src/chunkserver:chunkserver-lib",
        "//src/common:curve_common",
        "@com_google_googletest//:gtest_main",
    ],
)
