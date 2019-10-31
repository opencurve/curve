# tools
cc_binary(
    name = "curve-cli",
    srcs = [
        "curve_cli.cpp",
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
    name = "checkConsistecny",
    srcs = ["curve_data_check_main.cpp"],
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
        "//src/tools:consistency_check_lib",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
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
    name = "curve_status_tool",
    srcs = ["status_tool.cpp", "status_tool.h", "status_tool_main.cpp"],
    copts = COPTS,
    linkopts = [
        "-lmysqlcppconn",
    ],
    visibility = ["//visibility:public"],
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
