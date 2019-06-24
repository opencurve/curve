load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

cc_library(
    name = "leader_election",
    srcs = ["leader_election.cpp"],
    hdrs = ["leader_election.h"],
    copts = GCC_FLAGS,
    visibility = ["//visibility:public"],
    deps = [
        "//src/mds/nameserver2:nameserver2",
        "//external:glog",
        "//external:gflags"
    ],
)