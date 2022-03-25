#
#  Copyright (c) 2020 NetEase Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

load("//:copts.bzl", "CURVE_TEST_COPTS")

cc_test(
        name = "snapshot_server_main_test",
        srcs = [
            "snapshot_test_main.cpp",
            "snapshotcloneserver_test.cpp",
            "test_snapshotcloneserver_helpler.cpp",
            "test_snapshotcloneserver_helpler.h"
        ],
        deps = ["//src/common/concurrent:curve_concurrent",
                "//external:gtest",
                "//test/integration/cluster_common:integration_cluster_common",
                "//src/snapshotcloneserver:snapshot_server_lib",
                ],
                copts = CURVE_TEST_COPTS
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
                copts = CURVE_TEST_COPTS
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
                copts = CURVE_TEST_COPTS
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
                copts = CURVE_TEST_COPTS
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
        copts = CURVE_TEST_COPTS,
        defines = ["UNIT_TEST", "FIU_ENABLE"],
        linkopts = ["-lfiu"],
        )


