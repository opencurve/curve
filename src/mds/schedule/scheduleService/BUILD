load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

cc_library(
    name = "scheduleservice",
    srcs = glob([
        "*.cpp",
        "*.h",
    ]),
    copts = GCC_FLAGS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//external:glog",
        "//src/mds/common:mds_common",
        "//src/mds/schedule:schedule",
        "//proto:schedule_cc_proto",
    ],
)