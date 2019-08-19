cc_test(
    name = "kvstorage_client_test",
    srcs = glob(["*.cpp"]),
    deps = [
        "//external:json",
        "//src/mds/kvstorageclient:kvstorage_client",
        "//src/mds/nameserver2:nameserver2",
        "//src/mds/common:mds_common",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)