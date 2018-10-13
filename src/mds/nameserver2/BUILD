cc_library(
    name="nameserver2",
    hdrs=glob(["*.h"]),
    srcs=glob(["*.cpp"]),
    visibility = ["//visibility:public"],
    deps = [
            "//external:glog",
            "//external:gflags",
            "//external:leveldb",
            "//external:brpc",
            "//proto:nameserver2_cc_proto",
            "//src/common:curve_common",
            "//src/mds/topology:topology"

    ]
)
