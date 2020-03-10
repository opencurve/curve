cc_proto_library(
    name = "nameserver2_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":nameserver2_proto"],
)

proto_library(
    name = "nameserver2_proto",
    srcs = ["nameserver2.proto"],
)

cc_proto_library(
    name = "topology_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":topology_proto"],
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
        "heartbeat.proto",
        "common.proto",
        "chunk.proto",
        "cli.proto",
        "cli2.proto",
        "copyset.proto",
        "integrity.proto",
        "configuration.proto",
        "chunkserver.proto",
    ]),
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "snapshotcloneserver_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":snapshotcloneserver_proto"],
)

proto_library(
    name = "snapshotcloneserver_proto",
    srcs = ["snapshotcloneserver.proto"],
)

cc_proto_library(
    name = "schedule_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":schedule_proto"],
)

proto_library(
    name = "schedule_proto",
    srcs = ["schedule.proto"],
)

