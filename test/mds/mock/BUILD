load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

cc_library(
    name = "common_mock",
    srcs = glob(["*.cpp", "*.h"]),
    copts = GCC_TEST_FLAGS,
    visibility = ["//visibility:public"],
    deps = [
        "//proto:chunkserver-cc-protos",
        "//src/mds/nameserver2"
    ],
)
