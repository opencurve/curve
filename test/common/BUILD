cc_test(
    name = "common-test",
    srcs = glob([
        "*.cpp",
    ]),
    deps = [
        "//src/common:curve_common",
        "//src/common/concurrent:curve_concurrent",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "common_mock",
    srcs = [
            "mock_s3_adapter.h",
            ],
    linkopts = [
        "-luuid"
    ],
    deps = [
            "//src/common:curve_common",
            ],
    visibility = ["//visibility:public"],
)
