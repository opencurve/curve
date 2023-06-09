cc_library(
    name = "mysqlclient",
    srcs = glob([
        "libmysqlcppconn8.so",
        "libmysqlcppconn.so",
        "libmysqlcppconn.so.9",
        "libmysqlcppconn8.so.2",
        "libmysqlcppconn.so.9.8.0.32",
        "libmysqlcppconn8.so.2.8.0.32",
    ]),
    hdrs = glob([
        "mysql_connection.h ",
        "mysql_driver.h",
        "mysql_error.h",
    ]),
    includes = [
        "."
    ],
    visibility = [
        "//visibility:public"
    ],
)