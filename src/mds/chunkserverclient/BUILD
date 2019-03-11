load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

cc_library(
    name = "chunkserverclient",
    srcs = glob([
        "*.cpp",
        "*.h",
    ]),
    copts = GCC_FLAGS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//external:glog",
        "//proto:chunkserver-cc-protos",
        "//src/mds/common:mds_common",
        "//src/mds/topology:topology"
    ],
)
