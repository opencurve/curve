# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_library(
    name = "fs_mock",
    srcs = [
            "mock_local_filesystem.h",
            "mock_posix_wrapper.h"
            ],
    deps = [
            "//src/fs:lfs",
            ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "lfs_unittest",
    srcs = glob([
            "*.cpp",
        ]),
    copts = ([
        "-std=c++11",
    ]),
    deps = [
            "//src/fs:lfs",
            "//test/fs:fs_mock",
            "@com_google_googletest//:gtest_main",
            ],
    visibility = ["//visibility:public"],
)
