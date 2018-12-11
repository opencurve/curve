cc_test(
    name = "chunk-test",
    srcs = glob([
        "chunk_service_test.cpp",
        "copyset_node_manager_test.cpp",
        "op_context_test.cpp",
        "copyset_service_test.cpp",
        "mock_cs_data_store.h",
        "copyset_node_test.cpp",
        "chunk_closure_test.cpp",
        "chunkserver_test_util.h",
        "chunkserver_test_util.cpp",
    ]),
    copts = ["-std=c++14"],
    defines = ["UNIT_TEST"],
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
    name = "braftcliservice-test",
    srcs = glob([
        "braft_cli_service_test.cpp",
        "chunkserver_test_util.h",
        "chunkserver_test_util.cpp",
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
        "//external:glog",
        "//external:leveldb",
        "//proto:chunkserver-cc-protos",
        "//src/chunkserver:chunkserver-test-lib",
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
