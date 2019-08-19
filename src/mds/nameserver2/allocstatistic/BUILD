load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

cc_library(
    name = "alloc_statistic",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = GCC_FLAGS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:glog",
        "//external:gflags",
        "//src/mds/kvstorageclient:kvstorage_client",
        "//src/mds/nameserver2/helper:helper",
        "//src/common:curve_common",
    ],
)