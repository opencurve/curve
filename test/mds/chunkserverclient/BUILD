load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

cc_test(
    name = "chunkserverclient_utest",
    srcs = glob(["*.cpp", "*.h"]),
    copts = GCC_TEST_FLAGS,
    deps = [
        "//external:gtest",
        "//src/mds/chunkserverclient:chunkserverclient",
        "@com_google_googletest//:gtest_main",
    ],
)
