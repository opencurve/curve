load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

cc_test(
    name = "topology_utest",
    srcs = glob([
        "*.cpp",
        "*.h",
    ]),
    copts = GCC_TEST_FLAGS,
    deps = [
        "//external:gtest",
        "//src/mds/topology",
    ],
)
