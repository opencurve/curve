cc_test(
    name = "common-test",
    srcs = glob([
        "*.cpp",
        "task_thread_pool_test.cpp",
        "bitmap_test.cpp",
    ]),
    deps = [
        "//src/common:curve_common",
        "//src/common/concurrent:curve_concurrent",
        "@com_google_googletest//:gtest_main",
    ],
)
