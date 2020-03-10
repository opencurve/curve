load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS"
    )

cc_test(
        name = "snapshot_server_utest",
        srcs = glob([
            "*.cpp","*.h"
        ],
        ),
        deps = ["//src/common/concurrent:curve_concurrent", 
                "//external:gtest",
                "//src/snapshotcloneserver:snapshot_server_lib",
                "//test/util:test_util", 
                ],
        copts = GCC_TEST_FLAGS
        )

cc_library(
    name = "mock_repo",
    srcs = [
            "mock_repo.h",
            ],
    deps = [
            "//src/snapshotcloneserver/dao:snapshotrepo",
            ],
    visibility = ["//visibility:public"],
)
