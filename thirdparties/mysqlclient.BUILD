cc_library(
    name = "mysqlclient",
    srcs = glob([
        "lib64/*",
    ]),
    hdrs = glob([
        "include/jdbc/*.h",
        "include/jdbc/cppconn/*.h",
    ]),
    includes = [
        "."
    ],
    visibility = [
        "//visibility:public"
    ],
)