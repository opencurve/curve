cc_test(
    name = "repo-test",
    srcs = glob(["*.cpp"]),
    linkopts = [
        "-lmysqlcppconn",
    ],
    deps = [
        "@com_google_googletest//:gtest",
        "//src/repo",
        "//external:json",
    ],
)
