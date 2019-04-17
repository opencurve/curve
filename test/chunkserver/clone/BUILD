cc_test(
    name = "clone_test",
    srcs = glob([
        "*.cpp",
        "clone_test_util.h",
    ]),
    copts = ["-std=c++14"],
    deps = [
        "@com_google_googletest//:gtest",
        "//external:gflags",
        "//external:glog",
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//include:include-common",
        "//include/chunkserver:include-chunkserver",
        "//src/common:curve_common",
        "//src/chunkserver:chunkserver-test-lib",
        "//test/chunkserver/datastore:datastore_mock",
        "//test/chunkserver:chunkserver_mock",
        "//test/chunkserver/clone:clone_mock",
        "//test/client:client_mock",
        "//test/common:common_mock",
    ],
)

cc_library(
    name = "clone_mock",
    srcs = [
            "mock_clone_copyer.h",
            "mock_clone_manager.h"
            ],
    deps = [
            "//src/chunkserver:chunkserver-test-lib",
            ],
    visibility = ["//visibility:public"],
)
