cc_proto_library(
    name = "client_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":client_proto"],
)

proto_library(
    name = "client_proto",
    srcs = glob([
        "nebd-common.proto",
        "client.proto",
	    "heartbeat.proto"
        ]
    ),
)
