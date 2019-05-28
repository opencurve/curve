load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

cc_test(
    name = "schedule-test",
    srcs = glob([
        "*.cpp",
        "*.h",
    ]),
    copts = GCC_TEST_FLAGS,
    deps = [
        "//external:gflags",
        "//external:glog",
        "//src/mds/schedule",
        "//src/mds/topology",
        "//test/mds/mock:common_mock",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)
