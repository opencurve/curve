# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_library(
    name = "mock_space_allocator",
    srcs = [],
    hdrs = glob(["*.h"]),
    deps = [
        "//curvefs/src/space_allocator:curve_fs_space_allocator"
    ],
    visibility = ["//visibility:public"],
)