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
    name = "client_test_unittest",
    srcs = glob(["*.cpp",
                "*.h",]),
    defines = ["UNIT_TEST"],
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
        "//proto:topology_cc_proto",
        "//proto:chunkserver-cc-protos",
        "//src/client:curve_client",
        "//test/client/fake:fake_lib"
    ],
    visibility = ["//visibility:public"],
    copts = COPTS
)

