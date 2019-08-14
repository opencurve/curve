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
