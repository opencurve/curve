cc_binary(
    name = "filerecordmap_test",
    srcs = glob([
        "file_record_map_unittest.cpp",
    ]),
    deps = [
        "//external:gflags",
        "//src/part2:nebdserver",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "filemanager_test",
    srcs = glob([
        "file_manager_unittest.cpp",
    ]),
    deps = [
        "//external:gflags",
        "//src/part2:nebdserver",
	"//tests/part2:mock_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "fileservice_test",
    srcs = glob([
        "file_service_unittest.cpp",
    ]),
    deps = [
        "//external:gflags",
        "//src/part2:nebdserver",
	"//tests/part2:mock_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "heartbeat_service_test",
    srcs = glob([
        "heartbeat_service_test.cpp",
    ]),
    deps = [
        "//external:gflags",
        "//src/part2:nebdserver",
	"//tests/part2:mock_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "metafile_manager_test",
    srcs = glob([
        "metafile_manager_test.cpp",
    ]),
    deps = [
        "//external:gflags",
        "//src/part2:nebdserver",
	"//tests/part2:mock_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "test_nebd_server",
    srcs = glob([
        "test_nebd_server.cpp",
    ]),
    deps = [
        "//external:gflags",
        "//src/part2:nebdserver",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "test_request_executor_curve",
    srcs = glob([
        "test_request_executor_curve.cpp",
    ]),
    deps = [
        "//external:gflags",
        "//src/part2:nebdserver",
	"//tests/part2:mock_lib",
        "@com_google_googletest//:gtest_main",
    ],
    linkopts = ["-lpthread"],
)

cc_library(
    name = "mock_lib",
    srcs = glob([
	 "mock_*",
    ]),
    hdrs = glob(["*.h"]),
    deps = [
        "@com_google_googletest//:gtest",
        "//src/part2:nebdserver",
    ],
    visibility = ["//visibility:public"],
)
