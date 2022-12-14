load("//:copts.bzl", "CURVE_DEFAULT_COPTS")

cc_library(
    name = "metaserver_volume_lib",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/src/volume:volume",
        "//curvefs/src/metaserver:metaserver_fsinfo_lib",
#         # "//curvefs/proto:metaserver_cc_proto",
#         # "//curvefs/src/common:curvefs_common",
#         # "//curvefs/proto:mds_cc_proto",
#         # "//external:gflags",
#         # "//external:glog",
#         # "//src/common:curve_s3_adapter",
#         # "//curvefs/src/client/common",
#         # "//curvefs/src/client/metric:client_metric",
#         # "//curvefs/src/client/rpcclient",
#         "//curvefs/src/volume:volume"
    ],
)