load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS"
)

cc_test(
    name = "scheduler_poc",
    srcs = [
        "scheduler_poc.cpp",
        "mock_topology.h"],
    deps = ["//external:gtest",
            "//src/mds/copyset:copyset",
            "//src/mds/topology:topology",
            "//src/mds/schedule:schedule",
            "//test/mds/mock:common_mock",
            "@com_google_googletest//:gtest",
            "@com_google_googletest//:gtest_main"]
)