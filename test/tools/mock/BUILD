load("//:copts.bzl", "CURVE_TEST_COPTS")

cc_library(
    name = "curve_tool_mock",
    srcs = glob([
                "*.h",
                "*.cpp",
                ]),
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//external:gflags",
        "//src/tools:curve_tool_lib",
    ],
    copts = CURVE_TEST_COPTS,
)

