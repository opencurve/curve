# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_binary
cc_binary(
    name = "curvemds",
    srcs = glob(["*.cpp"]),
    copts = [],
    visibility = ["//visibility:public"],
    deps = [
        "//src/mds/server:mds",
    ],
)
