cc_library(
    name = "datastore_mock",
    srcs = [
            "mock_chunkfile_pool.h",
            "mock_datastore.h"
            ],
    deps = [
            "//src/chunkserver/datastore:chunkserver_datastore",
            ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "curve_datastore_unittest",
    srcs = [
        "chunkfilepool_unittest.cpp",
        "datastore_real_unittest.cpp",
        "datastore_mock_unittest.cpp",
        "datastore_unittest_main.cpp",
        "chunkfilepool_helper.cpp",
        "chunkfilepool_helper.h",
    ],
    includes = ([]),
    copts = ["-std=c++14"],
    deps = [
        "@com_google_googletest//:gtest",
        "//external:braft",
        "//external:gflags",
        "//include:include-common",
        "//include/chunkserver:include-chunkserver",
        "//src/common:curve_common",
        "//src/chunkserver/datastore:chunkserver_datastore",
        "//test/fs:fs_mock",
        "//test/chunkserver/datastore:datastore_mock",
        "//external:json"
    ],
)
