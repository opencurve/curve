config_setting(
    name = "io_uring_support",
    values = {
        "define": "IO_URING_SUPPORT=1"
    },
)

DEFAULT_LINK_OPTS = [
    '-llz4',
    '-lsnappy',
]

LINK_URING_OPT = [
#    '-luring',
]

LINK_OPTS = select({
    ":io_uring_support": DEFAULT_LINK_OPTS + LINK_URING_OPT,
    "//conditions:default": DEFAULT_LINK_OPTS,
})

cc_library(
    name = "rocksdb_lib",
    srcs = glob([
        "lib/librocksdb.a",
    ]),
    hdrs = glob([
        "include/rocksdb/*.h",
    ]),
    linkopts = LINK_OPTS,
    includes = [
        "include"
    ],
    visibility = [
        "//visibility:public"
    ],
)