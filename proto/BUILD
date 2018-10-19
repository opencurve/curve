cc_proto_library (
    name = "nameserver2_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":nameserver2_proto",],
)

proto_library(
    name = "nameserver2_proto",
    srcs = ["nameserver2.proto"],
)

cc_proto_library(
    name = "topology_cc_proto",
    deps = [":topology_proto"],
    visibility = ["//visibility:public"],
)

proto_library(
    name = "topology_proto",
    srcs = ["topology.proto"],
)

cc_proto_library(
    name = "chunkserver-cc-protos",
    visibility = ["//visibility:public"],
    deps = [":chunkserver-protos"],
)

proto_library(
    name = "chunkserver-protos",
    srcs = glob([
        "chunk.proto",
        "cli.proto",
        "copyset.proto",
        "integrity.proto",
        "configuration.proto",
        "chunkserver.proto",
    ]),
    visibility = ["//visibility:public"],
)

