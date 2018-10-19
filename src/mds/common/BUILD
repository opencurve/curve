load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS"
    )

cc_library(
        name = "mds_common",
        srcs = glob(["*.cpp", "*.h"]),
        copts = GCC_FLAGS,
        visibility = ["//visibility:public"],
        )
