cc_test(
    name = "mds_heartbeat_basic_integration",
    srcs = glob([
        "common.h",
        "common.cpp",
        "heartbeat_basic_test.cpp"]),
    deps = [
        "//src/mds/heartbeat:heartbeat",
        "//src/mds/nameserver2:nameserver2",
        "//src/common:curve_common",
        "//src/mds/topology:topology",
        "//src/mds/schedule",
        "@com_google_googletest//:gtest_main",
        "@com_google_googletest//:gtest",
        "//external:brpc"
    ]
)

cc_test(
    name = "mds_heartbeat_exception_integration",
    srcs = glob([
        "common.h",
        "common.cpp",
        "heartbeat_exception_test.cpp"]),
    deps = [
        "//src/mds/heartbeat:heartbeat",
        "//src/mds/nameserver2:nameserver2",
        "//src/common:curve_common",
        "//src/mds/topology:topology",
        "//src/mds/schedule",
        "@com_google_googletest//:gtest_main",
        "@com_google_googletest//:gtest",
        "//external:brpc"
    ]
)
