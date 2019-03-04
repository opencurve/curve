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
        deps = ["//test/utils:test-utils", "//external:gtest", "//src/snapshot:snapshot_server_lib"],
        copts = GCC_TEST_FLAGS
        )

