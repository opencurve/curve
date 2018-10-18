cc_library(
    name = "include-chunkserver",
    srcs = ["chunkserver_common.h"],
    linkopts = ([
        "-pthread",
        "-std=c++11",
    ]),
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:gflags",
    ],
)
