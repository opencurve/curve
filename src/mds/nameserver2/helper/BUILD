cc_library(
    name="helper",
    hdrs=glob(["*.h"]),
    srcs=glob(["*.cpp"]),
    visibility = ["//visibility:public"],
    deps = [
            "//external:glog",
            "//external:gflags",
            "//src/common:curve_common",
            "//src/mds/common:mds_common",
            "//proto:nameserver2_cc_proto",
    ],
)