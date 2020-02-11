cc_binary(
    name = "nebd_metacache_unittest",
    srcs = glob([
        "nebd_metacache_unittest.cpp",
    ]),
    deps = [
        "//external:gflags",
        "//src/part1:nebdclient",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "heartbeat_manager_unittest",
    srcs = glob([
        "heartbeat_manager_unittest.cpp",
    ]),
    deps = [
        "//external:gflags",
        "//src/part1:nebdclient",
	"//tests/part1:fake_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "nebd_client_unittest",
    srcs = glob([
        "nebd_client_unittest.cpp",
    ]),
    deps = [
        "//external:gflags",
        "//src/part1:nebdclient",
	"//tests/part1:fake_lib",
        "@com_google_googletest//:gtest_main",
    ],
    linkstatic = False,
)

cc_library(
    name = "fake_lib",
    srcs = glob([
         "fake_*",
	 "mock_*",
    ]),
    hdrs = glob(["*.h"]),
    deps = [
        "@com_google_googletest//:gtest",
        "//src/part1:nebdclient",
    ],
    visibility = ["//visibility:public"],
)
