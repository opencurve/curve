load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

cc_library(
    name = "mdsrepo",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = GCC_FLAGS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:glog",
        "//external:json",
        "//src/repo:repo",
    ],
)
