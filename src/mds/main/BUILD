# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_binary
cc_binary(
    name = "curvemds",
    srcs=glob(["*.cpp"]),
    visibility = ["//visibility:public"],
    copts = [],
    deps = [
            "//src/mds/nameserver2:nameserver2",
            "@com_google_googletest//:gtest_main",
            "@com_google_googletest//:gtest",
            "//external:brpc",
            "//src/common:curve_common",
            "//src/mds/topology:topology",
            "//test/mds/nameserver2:fakes",
    ],
)