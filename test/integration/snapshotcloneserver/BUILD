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

cc_test(
        name = "snapshot_server_recover_itest",
        srcs = [
            "snapshot_test_main.cpp",
            "snapshotcloneserver_recover_test.cpp",
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
        name = "snapshot_server_exception_itest",
        srcs = [
            "snapshot_test_main.cpp",
            "snapshotcloneserver_exception_test.cpp",
            "fake_curvefs_client.h",
            "fake_curvefs_client.cpp",
            "fake_snapshotclone_meta_store.h",
            "fake_snapshotclone_meta_store.cpp",
            "fake_snapshot_data_store.h",
            "fake_snapshot_data_store.cpp",
            "test_snapshotcloneserver_helpler.h",
            "test_snapshotcloneserver_helpler.cpp",
            "snapshotcloneserver_module.h",
            "snapshotcloneserver_module.cpp"
        ],
        deps = ["//src/common/concurrent:curve_concurrent",
                "//external:gtest",
                "//test/integration/cluster_common:integration_cluster_common",
                "//src/snapshotcloneserver:snapshot_server_lib",
                ],
        copts = GCC_TEST_FLAGS,
        defines = ["UNIT_TEST", "FIU_ENABLE"],
        linkopts = ["-lfiu"],
        )


