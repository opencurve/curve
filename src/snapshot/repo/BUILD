cc_library(
    name = "repo",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = [
        "-std=c++11",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:glog",
        "//external:json",
    ],
)
