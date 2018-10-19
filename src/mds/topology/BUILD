load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS"
    )

cc_library (
        name = "topology",
        srcs = glob(["*.cpp", "*.h"]),
        deps = ["//external:glog",
                "//external:brpc",
                "//external:json",
                "//proto:topology_cc_proto",
                "//src/mds/copyset:copyset",
                "//src/mds/common:mds_common",
                "//src/repo:repo",
                "//proto:chunkserver-cc-protos",
                "//proto:nameserver2_cc_proto"],
        copts = GCC_FLAGS,
        linkopts = [
        "-lmysqlcppconn",
        ],
        visibility = ["//visibility:public"],
        )

