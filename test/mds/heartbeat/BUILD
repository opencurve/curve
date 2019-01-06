load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

cc_test(
    name = "heartbeat-test",
    srcs = glob([
        "*.cpp",
        "*.h",
    ]),
    copts = GCC_TEST_FLAGS,
    deps = [
        "//external:gflags",
        "//external:glog",
        "//src/mds/heartbeat",
        "//src/mds/schedule",
        "//src/mds/topology",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)
