cc_library(
    name = "chunkserver_datastore",
    srcs = glob([
                "*.cpp",
                "*.h"
           ]),
    linkopts = ([
        "-pthread",
        "-std=c++11",
    ]),
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:gflags",
        "//external:glog",
        "//include:include-common",
        "//include/chunkserver:include-chunkserver",
        "//src/fs:lfs",
        "//src/common:curve_common",
        "//external:json"
    ],
)
