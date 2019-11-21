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

cc_binary(
    name = "curve_data_check",
    srcs = glob([
                "data_consistency_check_test.cpp"
                ]),
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:brpc",
        "//external:braft",
        "//external:protobuf",
        "//src/common:curve_common",
        "//include/client:include_client",
        "//proto:nameserver2_cc_proto",
        "//proto:chunkserver-cc-protos",
        "//src/client:curve_client",
        "//test/client/fake:fake_lib",
        "//src/tools:consistency_check_lib"
    ],
    copts = COPTS
)

cc_binary(
    name = "curve_status_tool_test",
    srcs = glob([
                "status_tool_test.cpp"
                ]),
    linkopts = [
        "-lmysqlcppconn",
    ],
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//src/tools:status_tool_lib",
        "//test/mds/mock:common_mock"
    ],
)

cc_binary(
    name = "curve_namespace_tool_test",
    srcs = glob([
                "namespace_tool_test.cpp"
                ]),
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//src/tools:namespace_tool_lib",
        "//test/client/fake:fake_lib",
    ],
)

cc_binary(
    name = "curve_copyset_check_test",
    srcs = glob([
                "copyset_check_test.cpp"
                ]),
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//src/tools:copyset_check_lib",
        "//test/client/fake:fake_lib",
    ],
)