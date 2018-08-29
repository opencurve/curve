BUILD_SRCS = [
            "src/qos/simulate/sim/test_dmclock_main.cc",
            "src/qos/simulate/sim/test_dmclock.cc",
            "src/qos/simulate/sim/test_dmclock.h",
            "src/qos/simulate/sim/str_list.cc",
            "src/qos/simulate/sim/str_list.h",
            "src/qos/simulate/sim/config.cc",
            "src/qos/simulate/sim/config.h",
            "src/qos/simulate/sim/ConfUtils.cc",
            "src/qos/simulate/sim/ConfUtils.h",
            "src/qos/simulate/sim/sim_recs.h",
            "src/qos/simulate/sim/sim_client.h",
            "src/qos/simulate/sim/simulate.h",
            "src/qos/simulate/sim/sim_server.h",
            "src/qos/dmclock/dmclock_util.cc",
            "src/qos/dmclock/dmclock_util.h",
            "src/qos/dmclock/support/indirect_intrusive_heap.h",
            "src/qos/qos_iops_bw_tokenbucket.cpp",
            "src/qos/qos_iops_bw_tokenbucket.h",
            "src/qos/qos_tokenbucket.h",
            "src/qos/qos_credit_strategy.h",
            "src/qos/qos_dmclock_client_wrapper.h",
            "src/qos/qos_dmclock_server_wrapper.h",
            "src/qos/qos_manager.h",
            "src/qos/qos_server.h",
            "src/qos/qos_client.h",
            "src/qos/dmclock/dmclock_recs.h",
            "src/qos/dmclock/dmclock_server.h",
            "src/qos/dmclock/dmclock_client.h",
            "src/common/timer/timer_thread.cpp",
            "src/common/timer/timer_thread.h",
            "src/common/utility/time_utility.h",
            "src/curve_compiler_specific.h",
            "src/qos/qos_request.h"
            ]

COPTS = [
    "-DNO_TCMALLOC",
]

cc_binary(
    name = "curve_qos_simulate",
    srcs = BUILD_SRCS,
    linkopts = (["-pthread",
                "-std=c++11",
                "-ltcmalloc",
                "-DGFLAGS_NS=google"]),
    includes = (["src/qos/dmclock",
                "src/qos/dmclock/support/",
                "src/qos/simulate/sim/",
                "src/qos/",
                "src"]),
    defines = (["SIMULATE",
                "SIMULATE_PRIVATE2PUBLIC"]),
    deps = [
        "@googlegflags//:gflags"
        ],
    copts = COPTS,
)

TEST_DIRS = [
    "test/chunkserver/qos/qos_credit_strategy_unittest.cpp",
    "test/chunkserver/qos/qos_dmclock_client_wrapper_unittest.cpp",
    "test/chunkserver/qos/qos_dmclock_server_wrapper_unittest.cpp",
    "test/chunkserver/qos/qos_iops_bw_tokenbucket_unittest.cpp",
    "test/chunkserver/qos/qos_unittest_main.cpp",
    "test/chunkserver/qos/qos_manager_unittest.cpp",
    "test/chunkserver/qos/qos_tokenbucket_unittest.cpp",
    "test/chunkserver/qos/qos_client_unittest.cpp",
    "test/chunkserver/qos/qos_server_unittest.cpp",
    "src/qos/dmclock/dmclock_util.cc",
    "src/qos/dmclock/dmclock_util.h",
    "src/qos/dmclock/support/indirect_intrusive_heap.h",
    "src/qos/qos_iops_bw_tokenbucket.cpp",
    "src/qos/qos_iops_bw_tokenbucket.h",
    "src/qos/qos_tokenbucket.h",
    "src/qos/qos_credit_strategy.h",
    "src/qos/qos_dmclock_client_wrapper.h",
    "src/qos/qos_dmclock_server_wrapper.h",
    "src/qos/qos_manager.h",
    "src/qos/qos_server.h",
    "src/qos/qos_client.h",
    "src/qos/dmclock/dmclock_recs.h",
    "src/qos/dmclock/dmclock_server.h",
    "src/qos/dmclock/dmclock_client.h",
    "src/common/timer/timer_thread.cpp",
    "src/common/timer/timer_thread.h",
    "src/common/utility/time_utility.h",
    "src/curve_compiler_specific.h"
]


cc_binary(
    name = "curve_qos_unittest",
    srcs = TEST_DIRS,
    linkopts = (["-pthread",
                "-std=c++11",
                "-fPIC",
                "-DGFLAGS_NS=google"]),
    includes = (["src/qos/dmclock",
                "src/qos/dmclock/support/",
                "src/qos/simulate/sim/",
                "src/qos/",
                "src/"]),
    defines = (["UINTTEST",
                "UINTTEST_PROTECTED2PUBLIC",
                "UINTTEST_PRIVATE2PUBLIC"]),
    deps = [
        "@googletest//:gtest",
        "@googletest//:gtest_main",
        "@googlegflags//:gflags"
    ],
)

cc_binary(
    name = "curve_common_timer_unittest",
    srcs = (["src/common/timer/timer_thread.h",
            "src/common/timer/timer_thread.cpp",
            "test/common/timer/timer_thread_unittest.cpp",
            "src/common/utility/time_utility.h"]),
    linkopts = (["-pthread",
                "-std=c++11"]),
    includes = (["src/qos/dmclock",
                "src/qos/dmclock/support/",
                "src/qos/simulate/sim/",
                "src/qos/",
                "src/"]),
    deps = [
        "@googletest//:gtest",
        "@googletest//:gtest_main",
        "@googlegflags//:gflags"
    ],
)


TEST_DMCLOCK_SUPPORT = [
    "test/chunkserver/qos/dmclock_test/support_test/test_indirect_intrusive_heap.cc",
    "test/chunkserver/qos/dmclock_test/support_test/test_intrusive_heap.cc",
    "src/qos/dmclock/support/run_every.cc",
    "src/qos/dmclock/support/run_every.h",
    "src/qos/dmclock/support/indirect_intrusive_heap.h",
    "src/qos/dmclock/support/intrusive_heap.h",
]

cc_binary(
    name = "curve_test_dmclock_support_unittest",
    srcs = TEST_DMCLOCK_SUPPORT,
    linkopts = (["-pthread",
                "-std=c++11"]),
    includes = (["src/qos/dmclock",
                "src/qos/dmclock/support/",
                "src/qos/simulate/sim/",
                "src/qos/",
                "src/"]),
    defines = (["UINTTEST",
                "UINTTEST_PROTECTED2PUBLIC",
                "UINTTEST_PRIVATE2PUBLIC"]),
    deps = [
        "@googletest//:gtest",
        "@googletest//:gtest_main",
    ],
)

TEST_DMCLOCK = [
    "test/chunkserver/qos/dmclock_test/src_test/sim/test_dmclock_client.cc",
    "test/chunkserver/qos/dmclock_test/src_test/sim/test_dmclock_server.cc",
    "test/chunkserver/qos/dmclock_test/src_test/sim/dmcPrCtl.cc",
    "test/chunkserver/qos/dmclock_test/src_test/sim/test_test_client.cc",
    "src/qos/dmclock/support/run_every.cc",
    "src/qos/dmclock/support/run_every.h",
    "src/qos/dmclock/support/indirect_intrusive_heap.h",
    "src/qos/dmclock/support/intrusive_heap.h",
    "src/qos/dmclock/dmclock_recs.h",
    "src/qos/dmclock/dmclock_server.h",
    "src/qos/dmclock/dmclock_client.h",
]

cc_binary(
    name = "curve_test_dmclock_unittest",
    srcs = TEST_DMCLOCK_SUPPORT,
    linkopts = (["-pthread",
                "-std=c++11"]),
    includes = (["src/qos/dmclock",
                "src/qos/dmclock/support/",
                "src/qos/simulate/sim/",
                "src/qos/",
                "src/"]),
    defines = (["UINTTEST",
                "UINTTEST_PROTECTED2PUBLIC",
                "UINTTEST_PRIVATE2PUBLIC"]),
    deps = [
        "@googletest//:gtest",
        "@googletest//:gtest_main",
    ],
)

BUILD_SRC_SFS_ADAPTOR_UINTTTEST = [
    "src/sfs/sfsMock.h",
    "src/sfs/sfsMock.cpp",
    "include/curve_compiler_specific.h",
    "include/chunkserver/chunkserver_common.h",
    "src/chunkserver/chunkserverStorage/adaptor_util.h",
    "src/chunkserver/chunkserverStorage/chunkserver_datastore_executor.cpp",
    "src/chunkserver/chunkserverStorage/chunkserver_datastore_executor.h",
    "src/chunkserver/chunkserverStorage/chunkserver_datastore_interface.h",
    "src/chunkserver/chunkserverStorage/chunkserver_datastore.h",
    "src/chunkserver/chunkserverStorage/chunkserver_datastore.cpp",
    "src/chunkserver/chunkserverStorage/chunkserver_sfs_adaptor.h",
    "src/chunkserver/chunkserverStorage/chunkserver_sfs_adaptor.cpp",
    "src/chunkserver/chunkserverStorage/chunkserver_sfs_localfs_impl.h",
    "src/chunkserver/chunkserverStorage/chunkserver_sfs_localfs_impl.cpp",
    "src/chunkserver/chunkserverStorage/chunkserver_storage.h",
    "src/chunkserver/chunkserverStorage/chunkserver_storage.cpp",
    "test/chunkserver/chunkserverStorage/chunkserver_datastore_executor_unittest.cpp",
    "test/chunkserver/chunkserverStorage/chunkserver_datastore_unittest.cpp",
    "test/chunkserver/chunkserverStorage/chunkserver_sfs_adaptor_unittest.cpp",
    "test/chunkserver/chunkserverStorage/chunkserver_sfs_localfs_impl_unittest.cpp",
    "test/chunkserver/chunkserverStorage/chunkserver_storage_unittest_main.cpp",
]

cc_test(
    name = "curve_chunkserverStorage_unittest",
    srcs = BUILD_SRC_SFS_ADAPTOR_UINTTTEST,
    linkopts = (["-pthread",
                "-std=c++11",
                "-fPIC",
                "-DGFLAGS_NS=google"]),
    includes = ([]),
    deps = [
        "@googletest//:gtest",
        "@googletest//:gtest_main",
        "@googlegflags//:gflags"
    ],
)


# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_library(
    name = "fake_client",
    srcs = ["src/libcurve/interface/libcurve_fake.cpp"],
    hdrs = ["src/libcurve/interface/libcurve_fake.h"],
    deps = ["//proto:nameserver2_cc_proto"],
)