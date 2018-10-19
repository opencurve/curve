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
    deps = ["@com_google_googletest//:gtest",
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
    ],
    visibility = ["//visibility:public"],
    copts = COPTS
)

