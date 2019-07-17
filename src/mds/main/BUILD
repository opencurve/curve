# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_binary
cc_binary(
    name = "curvemds",
    srcs = glob(["*.cpp"]),
    copts = [],
    linkopts = ["-luuid"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//src/common:curve_common",
        "//src/mds/heartbeat",
        "//src/mds/nameserver2",
        "//src/mds/leader_election",
        "//src/mds/schedule",
        "//src/mds/topology",
        "//test/mds/nameserver2:fakes",
    ],
)
