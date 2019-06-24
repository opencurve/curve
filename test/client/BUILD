# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
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
    #"-faligned-new"
]

cc_test(
    name = "client_test_unittest",
    srcs = glob([
        "*.h",
        "*.cpp"],
        exclude = ["client_session_unittest.cpp",
                "snapshot_service_unittest.cpp",
                "client_metric_test.cpp",
                "libcbd_libcurve_test.cpp"
                ]
    ),
    copts = COPTS,
    defines = ["UNIT_TEST", "FIU_ENABLE"],
    linkopts = ["-lfiu"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//include/client:include_client",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "//test/client/fake:fake_lib",
        "//src/common/concurrent:curve_concurrent",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "client_libcbd_unittest",
    srcs = glob([
        "libcbd_libcurve_test.cpp"
    ]),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    linkopts = ["-lfiu"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//include/client:include_client",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "//test/client/fake:fake_lib",
        "//src/common/concurrent:curve_concurrent",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "client_session_unittest",
    srcs = glob([
        "client_session_unittest.cpp"
    ]),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//include/client:include_client",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "//test/client/fake:fake_lib",
        "//src/common/concurrent:curve_concurrent",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "client_snapshot_test_unittest",
    srcs = glob([
        "*.h",
        "snapshot_service_unittest.cpp"]
    ),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    linkopts = ["-lfiu"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//include/client:include_client",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "//test/client/fake:fake_lib",
        "//src/common/concurrent:curve_concurrent",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)


cc_test(
    name = "client_metric_test",
    srcs = glob([
        "*.h",
        "client_metric_test.cpp"]
    ),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    linkopts = ["-lfiu"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//include/client:include_client",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "//test/client/fake:fake_lib",
        "//src/common/concurrent:curve_concurrent",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "client_mock",
    srcs = [
            "mock_file_client.h",
            ],
    deps = [
            "//src/client:curve_client",
            ],
    visibility = ["//visibility:public"],
)
