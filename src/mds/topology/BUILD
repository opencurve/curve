load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

cc_library(
    name = "topology",
    srcs = glob([
        "*.cpp",
        "*.h",
    ]),
    copts = GCC_FLAGS,
    linkopts = [
        "-lmysqlcppconn",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//external:glog",
        "//external:json",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/mds/common:mds_common",
        "//src/mds/copyset:copyset",
        "//src/mds/repo:repo",
        "//src/common:curve_common",
    ],
)
