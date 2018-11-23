load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

cc_library(
    name = "schedule",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = GCC_FLAGS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//proto:heartbeat_cc_proto",
        "//proto:topology_cc_proto",
        "//src/common:curve_common",
        "//src/mds/topology",
    ],
)
