cc_test(
    name = "repo-test",
    srcs = glob(["*.cpp"]),
    linkopts = [
        "-ljsoncpp",
        "-lmysqlcppconn",
    ],
    deps = [
        "//external:gtest",
        "//src/repo",
    ],
)
