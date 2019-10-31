cc_test(
    name = "raft-log-replication-test",
    srcs = glob([
        "raft_log_replication_test.cpp",
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
        "//test/integration/common:integration-test-common",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "raft-config-change-test",
    srcs = glob([
        "raft_config_change_test.cpp",
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
        "//test/integration/common:integration-test-common",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "raft-vote-test",
    srcs = glob([
        "peer_cluster.h",
        "peer_cluster.cpp",
        "raft_vote_test.cpp",
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
        "//test/integration/common:integration-test-common",
        "@com_google_googletest//:gtest_main",
    ],
)
