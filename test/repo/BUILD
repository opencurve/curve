cc_test(
    name = "mysql-test",
    srcs = glob(["mysql_test.cpp"]),
    linkopts = [
        "-lmysqlcppconn",
    ],
    deps = [
        "//external:json",
        "//src/repo:repo",
        "@com_google_googletest//:gtest",
    ],
)
cc_test(
    name = "connpool-test",
    srcs = glob(["test_connpool.cpp"]),
    linkopts = [
        "-lmysqlcppconn",
    ],
    deps = [
        "//external:json",
        "//src/repo:repo",
        "@com_google_googletest//:gtest",
    ],
)
