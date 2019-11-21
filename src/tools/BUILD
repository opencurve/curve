# tools
cc_library(
    name = "curve_cli_lib",
    srcs = [
        "curve_cli.h",
    ],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:bvar",
        "//external:gflags",
        "//external:glog",
        "//external:protobuf",
        "//proto:chunkserver-cc-protos",
        "//src/chunkserver:chunkserver-lib",
        "//src/common:curve_common",
    ],
)

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
]

cc_test(
    name = "createFile",
    srcs = ["createtool.cpp"],
    copts = COPTS,
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
        "//src/client:curve_client",
        "//src/common:curve_common",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "consistency_check_lib",
    srcs = glob(
        ["consistency_check.cpp"]
    ),
    hdrs = glob(["consistency_check.h"]),
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
        "//src/client:curve_client"
    ],
    visibility = ["//visibility:public"],
    copts = COPTS
)

cc_binary(
    name = "curve_format",
    srcs = [
        "curve_format_main.cpp",
    ],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:bvar",
        "//external:gflags",
        "//external:glog",
        "//external:protobuf",
        "//proto:chunkserver-cc-protos",
        "//src/chunkserver:chunkserver-lib",
        "//src/common:curve_common",
        "//src/fs:lfs",
        "//external:json"
    ],
)

cc_binary(
    name = "curve_tool",
    srcs = ["curve_tool_main.cpp"],

    copts = COPTS,
    linkopts = [
        "-lmysqlcppconn",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:gflags",
        "//src/tools:status_tool_lib",
        "//src/tools:namespace_tool_lib",
        "//src/tools:curve_cli_lib",
        "//src/tools:consistency_check_lib",
        "//src/tools:copyset_check_lib",
    ],
)

cc_library(
    name = "status_tool_lib",
    srcs = glob(
        ["status_tool.cpp", "status_tool.h"]
    ),
    hdrs = glob(["status_tool.h"]),
    deps = [
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:protobuf",
        "//proto:chunkserver-cc-protos",
        "//proto:nameserver2_cc_proto",
        "//proto:topology_cc_proto",
        "//src/common:curve_common",
        "//src/mds/dao:mdsrepo",
        "//src/mds/common:mds_common",
        "//src/repo:repo"
    ],
    visibility = ["//visibility:public"],
    copts = COPTS
)

cc_library(
    name = "namespace_tool_lib",
    srcs = glob(
        ["namespace_tool.cpp", "namespace_tool.h"],
    ),
    hdrs = glob(["namespace_tool.h"]),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:gflags",
        "//external:protobuf",
        "//proto:nameserver2_cc_proto",
        "//src/common:curve_common",
        "//src/mds/common:mds_common",
        "//src/mds/nameserver2:nameserver2",
    ],
)

cc_library(
    name = "copyset_check_lib",
    srcs = glob(
        ["copyset_check.cpp", "copyset_check.h"],
    ),
    hdrs = glob(["copyset_check.h"]),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:gflags",
        "//external:protobuf",
        "//external:brpc",
        "//external:braft",
        "//proto:topology_cc_proto",
        "//src/common:curve_common",
        "//src/mds/common:mds_common",
    ],
)
