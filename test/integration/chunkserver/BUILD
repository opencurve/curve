cc_test(
    name = "chunkserver-concurrent-test",
    srcs = glob([
        "peer_cluster.h",
        "peer_cluster.cpp",
        "chunkserver_concurrent_test.cpp",
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
        "//test/chunkserver/datastore:chunkfilepool_helper",
        "//test/integration/common:integration-test-common",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "chunkserver-basic-test",
    srcs = glob([
        "peer_cluster.h",
        "peer_cluster.cpp",
        "chunkserver_basic_test.cpp",
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
        "//test/chunkserver/datastore:chunkfilepool_helper",
        "//test/integration/common:integration-test-common",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "chunkserver-clone-recover-test",
    srcs = glob([
        "peer_cluster.h",
        "peer_cluster.cpp",
        "chunkserver_clone_recover.cpp",
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
        "//test/chunkserver/datastore:chunkfilepool_helper",
        "//test/integration/common:integration-test-common",
        "//test/integration/client/common:client_integration_common_lib",
        "//test/integration/cluster_common:integration_cluster_common",
        "@com_google_googletest//:gtest_main",
    ],
)

