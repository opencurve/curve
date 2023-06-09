load("//:copts.bzl", "CURVE_TEST_COPTS")

cc_test(
    name = "mysqlstorage_client_test",
    srcs = glob(["*.cpp"]),
    deps = [
        "//external:json",
        "//src/mysqlstorageclient:mysqlstorage_client",
        "//src/mds/nameserver2:nameserver2",
        "//src/mds/common:mds_common",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
    copts = CURVE_TEST_COPTS,
)