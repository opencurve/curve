load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS"
    )

cc_library(
    name = "test_util",
    srcs = glob([
        "*.h",
        "*.cpp",
    ]),
    copts =GCC_TEST_FLAGS ,
    visibility = ["//visibility:public"],
    deps = [
        "//src/common:curve_common",
    ],
)