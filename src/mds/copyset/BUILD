load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS"
    )

cc_library(
        name = "copyset",
        srcs = glob(["*.cpp", "*.h"]),
        deps = ["//external:glog", "//src/mds/common:mds_common"],
        copts = GCC_FLAGS,
        visibility = ["//visibility:public"],
        )
