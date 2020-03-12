load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

cc_library(
    name = "mds",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = GCC_FLAGS,
    linkopts = ["-luuid"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//src/common:curve_common",
        "//src/mds/heartbeat",
        "//src/mds/nameserver2",
        "//src/mds/nameserver2/helper:helper",
        "//src/leader_election",
        "//src/mds/schedule",
        "//src/mds/schedule/scheduleService:scheduleservice",
        "//src/mds/topology",
        "//src/mds/nameserver2/allocstatistic:alloc_statistic",
    ],
)

cc_library(
    name = "mds_for_test",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = GCC_FLAGS,
    linkopts = ["-luuid"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//src/common:curve_common",
        "//src/mds/heartbeat",
        "//src/mds/nameserver2",
        "//src/mds/nameserver2/helper:helper",
        "//src/leader_election:leader_election_for_test",
        "//src/mds/schedule",
        "//src/mds/schedule/scheduleService:scheduleservice",
        "//src/mds/topology",
        "//src/mds/nameserver2/allocstatistic:alloc_statistic",
    ],
)