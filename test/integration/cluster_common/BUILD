cc_library(
    name = "integration_cluster_common",
    srcs = glob([
        "cluster.h",
        "cluster.cpp",
    ]),
    copts = ["-std=c++11"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/common:curve_common",
        "//src/mds/dao:mdsrepo",
        "//src/snapshotcloneserver/dao:snapshotrepo",
        "//src/client:curve_client",
        "//test/util:test_util",
        "//external:gflags",
        "//external:glog",
        "@com_google_googletest//:gtest",
    ],
    linkopts = [
        "-lmysqlcppconn",
    ]
)

#cc_test(
#   name = "cluster_basic_test",
#   srcs = glob(["cluster_basic_test.cpp"]),
#   copts = ["-std=c++11"],
#   deps = [
#       "@com_google_googletest//:gtest_main",
#       "@com_google_googletest//:gtest",
#       "//test/integration/cluster_common:integration_cluster_common",
#   ]
#)
