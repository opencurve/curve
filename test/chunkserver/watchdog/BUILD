cc_test(
    name = "curve-watchdog-unittest",
    srcs = glob([
        "*.cpp",
        "*.h",
    ]),
    copts = ["-std=c++11"],
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//src/chunkserver/watchdog:chunkserver-watchdog",
        "//test/chunkserver:chunkserver-test-util-lib",
    ],
)