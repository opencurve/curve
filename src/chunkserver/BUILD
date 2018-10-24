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
    name = "chunkserver-lib",
    srcs = glob(
        ["*.cpp"],
        exclude = ["chunkserver_main.cpp"],
    ),
    hdrs = glob([
        "*.h",
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
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//proto:chunkserver-cc-protos",
        "//src/chunkserver/chunkserverStorage:chunkserver-sfsadaptor-lib",
        "//src/common:curve_common",
    ],
)

# chunkserver exec
cc_binary(
    name = "chunkserver",
    srcs = glob([
        "chunkserver_main.cpp",
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
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:leveldb",
        "//external:protobuf",
        "//proto:chunkserver-cc-protos",
        "//src/chunkserver:chunkserver-lib",
        "//src/chunkserver/chunkserverStorage:chunkserver-sfsadaptor-lib",
        "//src/common:curve_common",
    ],
)
