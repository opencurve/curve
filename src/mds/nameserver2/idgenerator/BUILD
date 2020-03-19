cc_library(
    name="idgenerator",
    hdrs=glob(["*.h"]),
    srcs=glob(["*.cpp"]),
    visibility = ["//visibility:public"],
    deps = [
            "//external:glog",
            "//external:gflags",
            "//src/common:curve_common",
            "//src/mds/common:mds_common",
            "//src/kvstorageclient:kvstorage_client",
            "//src/mds/nameserver2/helper:helper",
    ],
)
