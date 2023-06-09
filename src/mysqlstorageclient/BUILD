load("//:copts.bzl", "CURVE_DEFAULT_COPTS")

cc_library(
    name = "mysqlstorage_client",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:gflags",
        "//external:glog",
        "//src/common:curve_common",
        "@mysqlclient",
    ],
)