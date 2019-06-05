cc_library(
    name = "lfs",
    srcs = glob([
                "*.cpp",
                "ext4_filesystem_impl.h",
                "ext4_util.h",
                "wrap_posix.h"
           ]),
    hdrs = ["local_filesystem.h","fs_common.h"],
    linkopts = ([
        "-std=c++11",
    ]),
    deps = [
                "//src/common:curve_common",
                "//external:glog"
            ],
    visibility = ["//visibility:public"],
)
