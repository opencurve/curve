load("//:copts.bzl", "CURVE_TEST_COPTS", "SPDK_COPTS", "SPDK_LINK_OPTS", "SPDK_DEPS")

cc_test(
    name = "curve-filesystem_adaptor-unittest",
    srcs = glob([
        "*.cpp",
        "*.h",
    ]),
    copts = CURVE_TEST_COPTS + select({
        "//:with_spdk": SPDK_COPTS,
        "//conditions:default": [],
    }),
    linkopts = select({
        "//:with_spdk": SPDK_LINK_OPTS,
        "//conditions:default": [],
    }),
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//external:braft",
        "//src/chunkserver/filesystem_adaptor:filesystem_adaptor",
        "//src/fs:lfs",
    ] + select({
        "//:with_spdk": SPDK_DEPS,
        "//conditions:default": [],
    }),
)
