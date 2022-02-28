cc_library(
    name = "rocksdb_lib",
    srcs = glob([
        "lib/librocksdb.a",
    ]),
    hdrs = glob([
        "include/rocksdb/*.h",
    ]),
    linkopts = [
        '-llz4',
        '-lsnappy',
    ],
    includes = [
        "include"
    ],
    visibility = [
        "//visibility:public"
    ],
)

