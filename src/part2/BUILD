COPTS = [
    "-DGFLAGS=gflags",
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DHAVE_ZLIB",
    "-DHAVE_SSE42",
    "-DNDEBUG",
    "-std=c++0x",
    "-pipe",
    "-W",
    "-Wall",
    "-Wno-unused-parameter",
    "-fPIC",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
    "-msse4.2",
    "-pthread",
    "-Wsign-compare",
    "-Wno-unused-variable",
    "-Wno-missing-field-initializers",
    "-Woverloaded-virtual",
    "-Wnon-virtual-dtor"
]

cc_library(
    name = "nebdserver",
    srcs = glob([
        "*.h",
        "*.cpp",
        ],exclude = ["main.cpp"]
    ),
    visibility = ["//visibility:public"],
    deps = [
        "//external:glog",
	"//external:brpc",
	"//external:json",
	"//external:gflags",
	"//external:curve",
	"//src/common:nebd_common",
	"//proto:client_cc_proto"
    ],
    copts = COPTS,
)

cc_binary(
    name = "nebd-server",
    srcs = glob([
        "main.cpp",
        ]
    ),
    deps = [
        "//src/part2:nebdserver",
    ],
    copts = COPTS,
)


