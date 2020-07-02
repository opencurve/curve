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
    "-I /usr/include/libnl3",
]

cc_library(
    name = "curvenbd",
    srcs = glob([
        "*.h",
        "*.cpp",
        ],exclude = ["main.cpp"]
    ),
    deps = [
        "//external:gflags",
        "//external:glog",
        "//nebd/src/part1:nebdclient",
    ],
    copts = COPTS,
    linkopts = [
        "-lnl-3",
        "-lnl-genl-3",
    ],
    visibility = ["//visibility:public"],
)

cc_binary(
    name = "curve-nbd",
    srcs = glob([
        "main.cpp"
        ]
    ),
    deps = [
        "//nbd/src:curvenbd"
    ],
    copts = COPTS,
)
