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

ARM64_COPTS = [
    "-DGFLAGS=gflags",
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DNDEBUG",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
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
    name = "curve_tool_mock",
    srcs = glob([
                "*.h",
                "*.cpp",
                ]),
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//external:gflags",
        "//src/tools:curve_tool_lib",
    ],
    copts = select({
        ":arm64_build": ARM64_COPTS,
        ":x86_build": COPTS,
        "//conditions:default": COPTS,
    })
)

config_setting(
    name = "arm64_build",
    values = {"cpu": "aarch64"},
)

config_setting(
    name = "x86_build",
    values = {"cpu": "x86_64"},
)
