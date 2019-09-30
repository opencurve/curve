load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS"
    )

cc_test(
        name = "snapshot_server_common_itest",
        srcs = [
            "snapshot_test_main.cpp",
            "snapshotcloneserver_common_test.cpp",
            "test_snapshotcloneserver_helpler.cpp",
            "test_snapshotcloneserver_helpler.h"
        ],
        deps = ["//src/common/concurrent:curve_concurrent",
                "//external:gtest",
                "//test/integration/cluster_common:integration_cluster_common",
                "//src/snapshotcloneserver:snapshot_server_lib",
                ],
                copts = GCC_TEST_FLAGS
        )


cc_test(
        name = "snapshot_server_concurrent_itest",
        srcs = [
            "snapshot_test_main.cpp",
            "snapshotcloneserver_concurrent_test.cpp",
            "test_snapshotcloneserver_helpler.cpp",
            "test_snapshotcloneserver_helpler.h"
        ],
        deps = ["//src/common/concurrent:curve_concurrent",
                "//external:gtest",
                "//test/integration/cluster_common:integration_cluster_common",
                "//src/snapshotcloneserver:snapshot_server_lib",
                ],
                copts = GCC_TEST_FLAGS
        )
