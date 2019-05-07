load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS"
    )


cc_test(
        name = "copyset_utest",
        srcs = glob(["*.cpp", "*.h"]),
        deps = ["//external:gtest", "//src/mds/copyset:copyset"],
        copts = GCC_TEST_FLAGS
        )
