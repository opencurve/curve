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
        "//src/kvstorageclient:kvstorage_client",
        "//src/mds/nameserver2/helper:helper",
        "//external:glog",
        "//external:gflags"
    ],
)


cc_library(
    name = "leader_election_for_test",
    srcs = ["leader_election.cpp"],
    hdrs = ["leader_election.h"],
    copts = GCC_FLAGS,
    visibility = ["//visibility:public"],
    defines = ["FIU_ENABLE"],
    deps = [
        "//src/kvstorageclient:kvstorage_client",
        "//src/common:curve_common",
        "//external:glog",
        "//external:gflags"
    ],
    linkopts = ["-lfiu"],
)