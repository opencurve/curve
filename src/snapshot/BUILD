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
    name = "snapshot_server_lib",
    srcs = glob([
        "*.cpp","*.h"
    ],
    exclude = ["snapshot_main.cpp",
               "snapshot_server.cpp",
               "curvefs_client.cpp"],
    ),
    copts = COPTS,
    linkopts = [
        "-lrt",
        "-lssl",
        "-lcrypto",
        "-ldl",
        "-lz",
        "-lpthread",
        "-laws-cpp-sdk-core",
        "-laws-cpp-sdk-s3",
        "-lmysqlcppconn",
        "-luuid",
        "-L/usr/local/lib",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:leveldb",
        "//external:protobuf",
        "//proto:snapshotserver_cc_proto",
        "//src/common:curve_common",
        "//proto:nameserver2_cc_proto",
        "//proto:chunkserver-cc-protos",
        "//src/snapshot/dao:snapshotrepo",
        "//src/client:curve_client"
    ],
)

cc_binary(
    name = "snapshot-server",
    srcs = glob([
        "*.cpp","*.h"
    ]),
    copts = COPTS,
    linkopts = [
        "-lrt",
        "-lssl",
        "-lcrypto",
        "-ldl",
        "-lz",
        "-lpthread",
        "-laws-cpp-sdk-core",
        "-laws-cpp-sdk-s3",
        "-lmysqlcppconn",
        "-luuid",
        "-L/usr/local/lib",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:leveldb",
        "//external:protobuf",
        "//proto:snapshotserver_cc_proto",
        "//src/common:curve_common",
        "//proto:nameserver2_cc_proto",
        "//proto:chunkserver-cc-protos",
        "//src/snapshot/dao:snapshotrepo",
        "//src/client:curve_client"
    ],
)

cc_library(
    name = "s3_adapter",
    srcs = [
        "s3_adapter.cpp",
        "s3_adapter.h",
    ],
    copts = COPTS,
    linkopts = [
        "-laws-cpp-sdk-core",
        "-laws-cpp-sdk-s3",
        "-L/usr/local/lib",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:gflags",
        "//external:glog",
    ],
)
