# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_binary
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
    name = "curve_client_workflow",
    srcs = glob(["*.cpp", 
                "*.h"],
                exclude = ["client_workflow_test4snap.cpp",
                            "mds_workflow_test.cpp",],),
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
        "//proto:heartbeat_cc_proto",
        "//proto:chunkserver-cc-protos",
        "//src/client:curve_client"
    ],
    copts = COPTS
)

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_library(
    name = "fake_lib",
    srcs = glob(
        ["*.cpp"],
        exclude = ["client_workflow_test.cpp",
                    "mds_workflow_test.cpp",
                    "client_workflow_test4snap.cpp"
                    ],
    ),
    hdrs = glob(["*.h"]),
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
        "//proto:heartbeat_cc_proto",
        "//proto:chunkserver-cc-protos",
        "//src/client:curve_client"
    ],
    visibility = ["//visibility:public"],
    copts = COPTS
)


cc_binary(
    name = "curve_fake_mds",
    srcs = glob(["mds_workflow_test.cpp",
                "fakeMDS.cpp", 
                "*.h"]),
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
        "//proto:heartbeat_cc_proto",
        "//proto:nameserver2_cc_proto",
        "//proto:chunkserver-cc-protos",
        "//src/client:curve_client"
    ],
    copts = COPTS
)

cc_binary(
    name = "curve_client_workflow4snap",
    srcs = glob(["*.cpp", 
                "*.h"],
                exclude = ["client_workflow_test.cpp",
                "mds_workflow_test.cpp"],),
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
        "//proto:heartbeat_cc_proto",
        "//proto:nameserver2_cc_proto",
        "//proto:chunkserver-cc-protos",
        "//src/client:curve_client"
    ],
    copts = COPTS
)
