# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_binary
cc_binary(
    name = "mds-test",
    srcs = glob(["*.cpp"]),
    copts = [],
    defines = ["FIU_ENABLE"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//src/mds/server:mds_for_test",
    ],
    linkopts = ["-lfiu"],
)
