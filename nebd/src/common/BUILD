cc_library(
    name = "nebd_common",
    srcs = glob([
        "*.h",
        "*.cpp",
        ]
    ),
    visibility = ["//visibility:public"],
    deps = [
        "//external:glog",
        "//external:butil",
        "//external:bthread",
        "//external:bvar",
        "//src/common:curve_uncopy",
        "//src/common:curve_common",
    ],
)
