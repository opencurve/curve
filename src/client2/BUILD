load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS"
    )

cc_library(
        name = "curve_client",
        srcs = glob(["*.cpp", "*.h"]),
        copts = GCC_FLAGS,
        visibility = ["//visibility:public"],
        deps = [
            "//src/chunkserver:chunkserver-lib",
            "//proto:chunkserver-cc-protos",
            "//src/common:curve_common",
            "//src/client:curve_client",
            "//external:braft",
            "//external:brpc",
            "//external:bthread",
            "//external:butil",
            "//external:bvar",
            "//external:gflags",
            "//external:protobuf",
        ],
        )
