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
        "//src/tools:curve_tool_lib",
        "//test/tools:tools_mock",
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
        "//src/tools:curve_tool_lib",
        "//test/tools:tools_mock",
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
        "//src/tools:curve_tool_lib",
        "//test/tools:tools_mock",
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
        "//src/tools:curve_tool_lib",
	"//test/tools:tools_mock",
    ],
)

cc_binary(
    name = "curve_mds_client_test",
    srcs = glob([
        "mds_client_test.cpp",
    ]),
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//src/tools:curve_tool_lib",
        "//test/client/fake:fake_lib",
    ],
)

cc_binary(
    name = "curve_chunkserver_client_test",
    srcs = glob([
        "chunkserver_client_test.cpp",
    ]),
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//src/tools:curve_tool_lib",
        "//test/client/fake:fake_lib",
    ],
)

cc_binary(
    name = "curve_copyset_check_core_test",
    srcs = glob([
        "copyset_check_core_test.cpp",
    ]),
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//src/tools:curve_tool_lib",
        "//test/tools:tools_mock",
    ],
)

cc_binary(
    name = "curve_namespace_tool_core_test",
    srcs = glob([
        "namespace_tool_core_test.cpp",
    ]),
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//src/tools:curve_tool_lib",
        "//test/tools:tools_mock",
    ],
)

cc_binary(
    name = "curve_snapshot_read_test",
    srcs = glob([
                "snapshot_read_test.cpp"
                ]),
    linkopts = [
        "-lmysqlcppconn",
    ],
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//src/tools:curve_tool_lib",
        "//test/common:common_mock",
        "//test/snapshotcloneserver:mock_repo",
    ],
)

cc_binary(
    name = "curve_etcd_client_test",
    srcs = glob([
        "etcd_client_test.cpp",
    ]),
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//src/tools:curve_tool_lib",
        "//src/common:curve_common",
    ],
)

cc_binary(
    name = "curve_cli_test",
    srcs = glob([
        "curve_cli_test.cpp",
    ]),
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//src/tools:curve_tool_lib",
        "//test/tools:tools_mock",
    ],
)

cc_binary(
    name = "curve_tool_factory_test",
    srcs = glob([
        "curve_tool_factory_test.cpp",
    ]),
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//src/tools:curve_tool_lib",
    ],
)

cc_library(
    name = "tools_mock",
    srcs = glob([
        "mock_*.h",
    ]),
    deps = [
            "//src/tools:curve_tool_lib",
            "//proto:chunkserver-cc-protos",
            ],
    visibility = ["//visibility:public"],
)
cc_binary(
    name = "curve_snapshot_check_test",
    srcs = glob([
                "snapshot_check_test.cpp"
                ]),
    linkopts = [
        "-lmysqlcppconn",
    ],
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//src/tools:curve_tool_lib",
        "//test/client:client_mock",
        "//test/tools:tools_mock",
    ],
)
