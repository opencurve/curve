cc_test(
    name = "curve-raftsnapshot-unittest",
    srcs = glob([
        "*.cpp",
        "*.h",
    ]),
    copts = ["-std=c++14"],
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//external:braft",
        "//src/chunkserver/raftsnapshot:chunkserver-raft-snapshot",
        "//test/fs:fs_mock",
        "//test/chunkserver:chunkserver-test-util-lib",
    ],
)
