cc_test(
    name = "curve_chunkserverStorage_unittest",
    srcs = [
        "chunkserver_datastore_executor_unittest.cpp",
        "chunkserver_datastore_unittest.cpp",
        "chunkserver_sfs_adaptor_unittest.cpp",
        "chunkserver_sfs_localfs_impl_unittest.cpp",
        "chunkserver_storage_unittest_main.cpp",
    ],
    includes = ([]),
    linkopts = ([
        "-pthread",
        "-std=c++11",
        "-fPIC",
        "-DGFLAGS_NS=google",
    ]),
    deps = [
        "//external:braft",
        "//external:gflags",
        "//external:gtest",
        "//include:include-common",
        "//include/chunkserver:include-chunkserver",
        "//src/chunkserver/chunkserverStorage:chunkserver-sfsadaptor-lib",
        "//src/sfs:chunkserver-sfsmock-lib",
    ],
)
