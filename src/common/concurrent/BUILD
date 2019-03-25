COPTS = [
    "-DGFLAGS=gflags",
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DHAVE_SSE42",
    "-DNDEBUG",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
    "-msse4.2",
    "-pthread",
    "-Wsign-compare",
    "-Wno-unused-parameter",
    "-Wno-unused-variable",
    "-Woverloaded-virtual",
    "-Wnon-virtual-dtor",
    "-Wno-missing-field-initializers",
    "-std=c++14",
]

cc_library(
    name = "curve_concurrent",
    srcs = glob([
        "*.h",
        "*.cpp",
    ]),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:glog",
        "//src/common:curve_uncopy"
    ],
)
