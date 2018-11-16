BUILD_SRCS_SFS_ADAPTOR = [
    "chunkserver_adaptor_util.cpp",
    "chunkserver_adaptor_util.h",
    "chunkserver_datastore_executor.cpp",
    "chunkserver_datastore_executor.h",
    "chunkserver_datastore_interface.h",
    "chunkserver_datastore.h",
    "chunkserver_datastore.cpp",
    "chunkserver_sfs_adaptor.h",
    "chunkserver_sfs_adaptor.cpp",
    "chunkserver_sfs_localfs_impl.h",
    "chunkserver_sfs_localfs_impl.cpp",
    "chunkserver_storage.h",
    "chunkserver_storage.cpp",
]

cc_library(
    name = "chunkserver-sfsadaptor-lib",
    srcs = BUILD_SRCS_SFS_ADAPTOR,
    linkopts = ([
        "-pthread",
        "-std=c++11",
    ]),
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:gflags",
        "//include:include-common",
        "//src/common:curve_common",
        "//include/chunkserver:include-chunkserver",
        "//src/sfs:chunkserver-sfsmock-lib",
    ],
)
