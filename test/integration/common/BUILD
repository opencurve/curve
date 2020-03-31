cc_library(
    name = "integration-test-common",
    srcs = glob([
        "config_generator.h",
        "peer_cluster.h",
        "peer_cluster.cpp",
        "chunkservice_op.h",
        "chunkservice_op.cpp",
    ]),
    copts = ["-std=c++14"],
    visibility = ["//visibility:public"],
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
        "//test/util:test_util",
        "@com_google_googletest//:gtest",
    ],
)

