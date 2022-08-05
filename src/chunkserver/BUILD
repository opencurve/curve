#
#  Copyright (c) 2020 NetEase Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

load("//:copts.bzl", "CURVE_DEFAULT_COPTS", "CURVE_TEST_COPTS", "SPDK_COPTS", "SPDK_LINK_OPTS", "SPDK_DEPS")

cc_library(
    name = "chunkserver-lib",
    srcs = glob(
        ["*.cpp"],
        exclude = [
            "chunkserver_main.cpp",
            "spdk_hook.cpp",
        ],
    ) + select({
        "//:with_spdk": ["spdk_hook.cpp"],
        "//conditions:default": [],
    }),
    hdrs = glob(
        ["*.h"],
        exclude = [
            "spdk_hook.h",
        ],
    ) + select({
        "//:with_spdk": ["spdk_hook.h"],
        "//conditions:default": [],
    }),
    copts = CURVE_DEFAULT_COPTS + select({
        "//:with_spdk": SPDK_COPTS,
        "//conditions:default": [],
    }),

    linkopts = select({
        "//:with_spdk": SPDK_LINK_OPTS,
        "//conditions:default": [],
    }) + [
        "-std=c++11",
        "-luuid",
        "-lrt",
        "-lssl",
        "-lcrypto",
        "-ldl",
        "-lz",
        "-lpthread",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//proto:chunkserver-cc-protos",
        "//proto:topology_cc_proto",
        "//src/chunkserver/datastore:chunkserver_datastore",
        "//src/chunkserver/concurrent_apply:chunkserver_concurrent_apply",
        "//src/chunkserver/raftsnapshot:chunkserver-raft-snapshot",
        "//src/chunkserver/raftlog:chunkserver-raft-log",
        "//src/common:curve_common",
        "//src/common:curve_s3_adapter",
        "//src/fs:lfs",
        "//src/client:curve_client",
	    "//proto:scan_cc_proto",
	    "//include/client:include_client",
    ] + select({
        "//:with_spdk": SPDK_DEPS,
        "//conditions:default": [],
    }),
)

cc_library(
    name = "chunkserver-test-lib",
    srcs = glob(
        ["*.cpp"],
        exclude = [
            "chunkserver_main.cpp",
            "spdk_hook.cpp",
        ],
    ) + select({
        "//:with_spdk": ["spdk_hook.cpp"],
        "//conditions:default": [],
    }),
    hdrs = glob(
        ["*.h"],
        exclude = [
            "spdk_hook.h",
        ],
    ) + select({
        "//:with_spdk": ["spdk_hook.h"],
        "//conditions:default": [],
    }),
    copts = CURVE_DEFAULT_COPTS + select({
        "//:with_spdk": SPDK_COPTS,
        "//conditions:default": [],
    }),
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    linkopts = select({
        "//:with_spdk": SPDK_LINK_OPTS,
        "//conditions:default": [],
    }) + [
        "-std=c++11",
        "-luuid",
        "-lrt",
        "-lssl",
        "-lcrypto",
        "-ldl",
        "-lz",
        "-lpthread",
    ],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//proto:chunkserver-cc-protos",
        "//proto:topology_cc_proto",
        "//src/chunkserver/datastore:chunkserver_datastore",
        "//src/chunkserver/concurrent_apply:chunkserver_concurrent_apply",
        "//src/chunkserver/raftsnapshot:chunkserver-raft-snapshot",
        "//src/chunkserver/raftlog:chunkserver-raft-log",
        "//src/common:curve_common",
        "//src/common:curve_s3_adapter",
        "//src/fs:lfs",
        "//src/client:curve_client",
        "//proto:scan_cc_proto",
    ] + select({
        "//:with_spdk": SPDK_DEPS,
        "//conditions:default": [],
    }),
)

# chunkserver exec
cc_binary(
    name = "chunkserver",
    srcs = glob([
        "chunkserver_main.cpp",
    ]),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//external:protobuf",
        "//proto:chunkserver-cc-protos",
        "//src/chunkserver:chunkserver-lib",
        "//src/chunkserver/datastore:chunkserver_datastore",
        "//src/chunkserver/concurrent_apply:chunkserver_concurrent_apply",
        "//src/chunkserver/raftsnapshot:chunkserver-raft-snapshot",
        "//src/chunkserver/raftlog:chunkserver-raft-log",
        "//src/common:curve_common",
        "//src/common:curve_s3_adapter",
        "//src/fs:lfs",
    ],
)
