#BUILD_SRCS = [
#            "src/qos/simulate/sim/test_dmclock_main.cc",
#            "src/qos/simulate/sim/test_dmclock.cc",
#            "src/qos/simulate/sim/test_dmclock.h",
#            "src/qos/simulate/sim/str_list.cc",
#            "src/qos/simulate/sim/str_list.h",
#            "src/qos/simulate/sim/config.cc",
#            "src/qos/simulate/sim/config.h",
#            "src/qos/simulate/sim/ConfUtils.cc",
#            "src/qos/simulate/sim/ConfUtils.h",
#            "src/qos/simulate/sim/sim_recs.h",
#            "src/qos/simulate/sim/sim_client.h",
#            "src/qos/simulate/sim/simulate.h",
#            "src/qos/simulate/sim/sim_server.h",
#            "src/qos/dmclock/dmclock_util.cc",
#            "src/qos/dmclock/dmclock_util.h",
#            "src/qos/dmclock/support/indirect_intrusive_heap.h",
#            "src/qos/qos_iops_bw_tokenbucket.cpp",
#            "src/qos/qos_iops_bw_tokenbucket.h",
#            "src/qos/qos_tokenbucket.h",
#            "src/qos/qos_credit_strategy.h",
#            "src/qos/qos_dmclock_client_wrapper.h",
#            "src/qos/qos_dmclock_server_wrapper.h",
#            "src/qos/qos_manager.h",
#            "src/qos/qos_server.h",
#            "src/qos/qos_client.h",
#            "src/qos/dmclock/dmclock_recs.h",
#            "src/qos/dmclock/dmclock_server.h",
#            "src/qos/dmclock/dmclock_client.h",
#            "src/common/timer/timer_thread.cpp",
#            "src/common/timer/timer_thread.h",
#            "src/common/utility/time_utility.h",
#            "src/curve_compiler_specific.h",
#            "src/qos/qos_request.h"
#            ]
#
#COPTS = [
#    "-DNO_TCMALLOC",
#]
#
#cc_binary(
#    name = "curve_qos_simulate",
#    srcs = BUILD_SRCS,
#    linkopts = (["-pthread",
#                "-std=c++11",
#                "-ltcmalloc",
#                "-DGFLAGS_NS=google"]),
#    includes = (["src/qos/dmclock",
#                "src/qos/dmclock/support/",
#                "src/qos/simulate/sim/",
#                "src/qos/",
#                "src"]),
#    defines = (["SIMULATE",
#                "SIMULATE_PRIVATE2PUBLIC"]),
#    deps = [
#        "@googlegflags//:gflags"
#        ],
#    copts = COPTS,
#)
#
#TEST_DIRS = [
#    "test/chunkserver/qos/qos_credit_strategy_unittest.cpp",
#    "test/chunkserver/qos/qos_dmclock_client_wrapper_unittest.cpp",
#    "test/chunkserver/qos/qos_dmclock_server_wrapper_unittest.cpp",
#    "test/chunkserver/qos/qos_iops_bw_tokenbucket_unittest.cpp",
#    "test/chunkserver/qos/qos_unittest_main.cpp",
#    "test/chunkserver/qos/qos_manager_unittest.cpp",
#    "test/chunkserver/qos/qos_tokenbucket_unittest.cpp",
#    "test/chunkserver/qos/qos_client_unittest.cpp",
#    "test/chunkserver/qos/qos_server_unittest.cpp",
#    "src/qos/dmclock/dmclock_util.cc",
#    "src/qos/dmclock/dmclock_util.h",
#    "src/qos/dmclock/support/indirect_intrusive_heap.h",
#    "src/qos/qos_iops_bw_tokenbucket.cpp",
#    "src/qos/qos_iops_bw_tokenbucket.h",
#    "src/qos/qos_tokenbucket.h",
#    "src/qos/qos_credit_strategy.h",
#    "src/qos/qos_dmclock_client_wrapper.h",
#    "src/qos/qos_dmclock_server_wrapper.h",
#    "src/qos/qos_manager.h",
#    "src/qos/qos_server.h",
#    "src/qos/qos_client.h",
#    "src/qos/dmclock/dmclock_recs.h",
#    "src/qos/dmclock/dmclock_server.h",
#    "src/qos/dmclock/dmclock_client.h",
#    "src/common/timer/timer_thread.cpp",
#    "src/common/timer/timer_thread.h",
#    "src/common/utility/time_utility.h",
#    "src/curve_compiler_specific.h"
#]
#
#
#cc_binary(
#    name = "curve_qos_unittest",
#    srcs = TEST_DIRS,
#    linkopts = (["-pthread",
#                "-std=c++11",
#                "-fPIC",
#                "-DGFLAGS_NS=google"]),
#    includes = (["src/qos/dmclock",
#                "src/qos/dmclock/support/",
#                "src/qos/simulate/sim/",
#                "src/qos/",
#                "src/"]),
#    defines = (["UINTTEST",
#                "UINTTEST_PROTECTED2PUBLIC",
#                "UINTTEST_PRIVATE2PUBLIC"]),
#    deps = [
#        "@googletest//:gtest",
#        "@googletest//:gtest_main",
#        "@googlegflags//:gflags"
#    ],
#)
#
#cc_binary(
#    name = "curve_common_timer_unittest",
#    srcs = (["src/common/timer/timer_thread.h",
#            "src/common/timer/timer_thread.cpp",
#            "test/common/timer/timer_thread_unittest.cpp",
#            "src/common/utility/time_utility.h"]),
#    linkopts = (["-pthread",
#                "-std=c++11"]),
#    includes = (["src/qos/dmclock",
#                "src/qos/dmclock/support/",
#                "src/qos/simulate/sim/",
#                "src/qos/",
#                "src/"]),
#    deps = [
#        "@googletest//:gtest",
#        "@googletest//:gtest_main",
#        "@googlegflags//:gflags"
#    ],
#)
#
#
#TEST_DMCLOCK_SUPPORT = [
#    "test/chunkserver/qos/dmclock_test/support_test/test_indirect_intrusive_heap.cc",
#    "test/chunkserver/qos/dmclock_test/support_test/test_intrusive_heap.cc",
#    "src/qos/dmclock/support/run_every.cc",
#    "src/qos/dmclock/support/run_every.h",
#    "src/qos/dmclock/support/indirect_intrusive_heap.h",
#    "src/qos/dmclock/support/intrusive_heap.h",
#]
#
#cc_binary(
#    name = "curve_test_dmclock_support_unittest",
#    srcs = TEST_DMCLOCK_SUPPORT,
#    linkopts = (["-pthread",
#                "-std=c++11"]),
#    includes = (["src/qos/dmclock",
#                "src/qos/dmclock/support/",
#                "src/qos/simulate/sim/",
#                "src/qos/",
#                "src/"]),
#    defines = (["UINTTEST",
#                "UINTTEST_PROTECTED2PUBLIC",
#                "UINTTEST_PRIVATE2PUBLIC"]),
#    deps = [
#        "@googletest//:gtest",
#        "@googletest//:gtest_main",
#    ],
#)
#
#TEST_DMCLOCK = [
#    "test/chunkserver/qos/dmclock_test/src_test/sim/test_dmclock_client.cc",
#    "test/chunkserver/qos/dmclock_test/src_test/sim/test_dmclock_server.cc",
#    "test/chunkserver/qos/dmclock_test/src_test/sim/dmcPrCtl.cc",
#    "test/chunkserver/qos/dmclock_test/src_test/sim/test_test_client.cc",
#    "src/qos/dmclock/support/run_every.cc",
#    "src/qos/dmclock/support/run_every.h",
#    "src/qos/dmclock/support/indirect_intrusive_heap.h",
#    "src/qos/dmclock/support/intrusive_heap.h",
#    "src/qos/dmclock/dmclock_recs.h",
#    "src/qos/dmclock/dmclock_server.h",
#    "src/qos/dmclock/dmclock_client.h",
#]
#
#cc_binary(
#    name = "curve_test_dmclock_unittest",
#    srcs = TEST_DMCLOCK_SUPPORT,
#    linkopts = (["-pthread",
#                "-std=c++11"]),
#    includes = (["src/qos/dmclock",
#                "src/qos/dmclock/support/",
#                "src/qos/simulate/sim/",
#                "src/qos/",
#                "src/"]),
#    defines = (["UINTTEST",
#                "UINTTEST_PROTECTED2PUBLIC",
#                "UINTTEST_PRIVATE2PUBLIC"]),
#    deps = [
#        "@googletest//:gtest",
#        "@googletest//:gtest_main",
#    ],
#)
#
BUILD_SRCS_SFS_ADAPTOR = [
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
    "src/sfs/sfsMock.h",
    "src/sfs/sfsMock.cpp",
    "include/curve_compiler_specific.h",
    "include/chunkserver/chunkserver_common.h",
]

cc_library(
    name = "chunkserver-sfsadaptor",
    srcs = BUILD_SRCS_SFS_ADAPTOR,
    includes = (["src/"]),
    linkopts = ([
        "-pthread",
        "-std=c++11",
    ]),
    deps = [
        "//external:gflags",
        "//external:braft",
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
    includes = ([]),
    linkopts = ([
        "-pthread",
        "-std=c++11",
        "-fPIC",
        "-DGFLAGS_NS=google",
    ]),
    deps = [
        "//external:gflags",
        "//external:braft",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
#cc_library(
#    name = "fake_client",
#    srcs = ["src/libcurve/interface/libcurve_fake.cpp"],
#    hdrs = ["src/libcurve/interface/libcurve_fake.h"],
#    deps = ["//proto:nameserver2_cc_proto"],
#)
#
COPTS = [
    "-DGFLAGS=gflags",
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DHAVE_SSE42",
    "-DNDEBUG",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
    "-msse4.2",
    "-pthread",
    "-Wsign-compare",
    "-Wno-unused-parameter",
    "-Wno-unused-variable",
    "-Woverloaded-virtual",
    "-Wnon-virtual-dtor",
    "-Wno-missing-field-initializers",
    "-std=c++14",
]

cc_library(
    name = "common",
    srcs = glob([
        "src/common/*.h",
        "src/common/*.cpp",
    ]),
    copts = COPTS,
    linkopts = [
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "common-test",
    srcs = glob([
        "test/common/*.cpp",
    ]),
    copts = [
        "-Iexternal/gtest/include",
    ],
    deps = [
        "//:common",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_proto_library(
    name = "chunkserver-cc-protos",
    deps = [":chunkserver-protos"],
)

proto_library(
    name = "chunkserver-protos",
    srcs = glob([
        "proto/*.proto",
    ]),
)

cc_library(
    name = "chunkserver-lib",
    srcs = glob(
        ["src/chunkserver/*.cpp"],
        exclude = ["src/chunkserver/chunkserver_main.cpp"],
    ),
    hdrs = glob([
        "src/chunkserver/*.h",
        "src/sfs/sfsMock.h",
    ]),
    copts = COPTS,
    includes = [
        "-Iexternal/protobuf/include",
    ],
    linkopts = [
        "-lrt",
        "-lssl",
        "-lcrypto",
        "-ldl",
        "-lz",
        "-lpthread",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//:chunkserver-cc-protos",
        "//:chunkserver-sfsadaptor",
        "//:common",
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:leveldb",
        "//external:protobuf",
    ],
)

cc_binary(
    name = "chunkserver",
    srcs = glob([
        "src/chunkserver/chunkserver_main.cpp",
    ]),
    copts = COPTS,
    linkopts = [
        "-lrt",
        "-lssl",
        "-lcrypto",
        "-ldl",
        "-lz",
        "-lpthread",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//:chunkserver-cc-protos",
        "//:chunkserver-sfsadaptor",
        "//:chunkserver-lib",
        "//:common",
        "//external:braft",
        "//external:brpc",
        "//external:butil",
        "//external:gflags",
        "//external:leveldb",
        "//external:protobuf",
    ],
)

cc_binary(
    name = "server-test",
    srcs = [
        "test/chunkserver/server.cpp",
    ],
    deps = [
        "//:chunkserver-lib",
        "//:chunkserver-cc-protos",
        "//:common",
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:bvar",
        "//external:gflags",
        "//external:protobuf",
    ],
)

cc_binary(
    name = "client-test",
    srcs = [
        "test/chunkserver/client.cpp",
    ],
    deps = [
        "//:chunkserver-lib",
        "//:chunkserver-cc-protos",
        "//:common",
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:bvar",
        "//external:gflags",
        "//external:protobuf",
    ],
)

cc_binary(
    name = "multi-copyset-io-test",
    srcs = [
        "test/chunkserver/multiple_copysets_io_test.cpp",
    ],
    deps = [
        "//:chunkserver-lib",
        "//:chunkserver-cc-protos",
        "//:common",
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:bvar",
        "//external:gflags",
        "//external:protobuf",
    ],
)

cc_library(
    name = "curvewrapper",
    srcs = glob([
        "src/client/libcurve_wrapper.cpp",
    ]),
    hdrs = glob([
        "src/client/libcurve_wrapper.h",
    ]),
    copts = [
        "-DGFLAGS=gflags",
        "-DOS_LINUX",
        "-DSNAPPY",
        "-DHAVE_ZLIB",
        "-DHAVE_SSE42",
        "-DNDEBUG",
        "-fno-omit-frame-pointer",
        "-momit-leaf-frame-pointer",
        "-msse4.2",
        "-pthread",
        "-Wsign-compare",
        "-Wno-unused-parameter",
        "-Wno-unused-variable",
        "-Wno-missing-field-initializers",
    ],
    includes = [
        "src/client/libcurve.h",
    ],
    linkopts = [
        "-lrt",
        "-lssl",
        "-lcrypto",
        "-ldl",
        "-lz",
        "-lpthread",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//:chunkserver-lib",
        "//:chunkserver-cc-protos",
        "//:common",
        "//:curve",
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:bvar",
        "//external:gflags",
        "//external:protobuf",
    ],
)

cc_library(
    name = "curve",
    srcs = glob([
        "src/client/libcurve.cpp",
    ]),
    hdrs = glob([
        "src/client/libcurve.h",
    ]),
    copts = COPTS,
    includes = [
    ],
    linkopts = [
        "-lrt",
        "-lssl",
        "-lcrypto",
        "-ldl",
        "-lz",
        "-lpthread",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//:chunkserver-lib",
        "//:chunkserver-cc-protos",
        "//:common",
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:bvar",
        "//external:gflags",
        "//external:protobuf",
    ],
)

CHUNKSERVER_DEPS = [
    "//:chunkserver-sfsadaptor",
    "//:chunkserver-lib",
    "//:chunkserver-cc-protos",
    "//:common",
    "//external:braft",
    "//external:brpc",
    "//external:bthread",
    "//external:butil",
    "//external:bvar",
    "//external:gflags",
    "//external:leveldb",
    "//external:protobuf",
    "@com_google_googletest//:gtest_main",
]

cc_binary(
    name = "chunkserver-test",
    srcs = glob([
        "test/chunkserver/cli_test.cpp",
        "test/chunkserver/chunk_service_test.cpp",
        "test/chunkserver/copyset_node_manager_test.cpp",
        "test/chunkserver/op_request_test.cpp",
    ]),
    copts = [
        "-Iexternal/gtest/include",
        "-g",
    ],
    deps = CHUNKSERVER_DEPS,
)

cc_test(
    name = "chunkserver-copyset-test",
    srcs = glob([
        "test/chunkserver/copyset_service_test.cpp",
    ]),
    copts = [
        "-Iexternal/gtest/include",
    ],
    deps = CHUNKSERVER_DEPS,
)

# tools

cc_binary(
    name = "curve-cli",
    srcs = [
        "src/tools/curve_cli.cpp",
    ],
    deps = [
        "//:chunkserver-lib",
        "//:chunkserver-cc-protos",
        "//:common",
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:bvar",
        "//external:gflags",
        "//external:protobuf",
    ],
)
