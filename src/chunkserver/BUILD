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

COPTS = [
    "-DGFLAGS=gflags",
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DHAVE_SSE42",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
    "-msse4.2",
    "-pthread",
    "-Wsign-compare",
    "-Wno-unused-parameter",
    "-Wno-unused-variable",
    "-Woverloaded-virtual",
    "-Wnon-virtual-dtor",
    "-Wno-missing-field-initializers",
    "-std=c++11",
]

cc_library(
    name = "chunkserver-lib",
    srcs = glob(
        ["*.cpp"],
        exclude = [
            "chunkserver_main.cpp",
        ],
    ),
    hdrs = glob([
        "*.h",
    ]),
    copts = COPTS,
    linkopts = [
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
	"//include/client:include_client",
        "//proto:scan_cc_proto",
    ],
)

cc_library(
    name = "chunkserver-test-lib",
    srcs = glob(
        ["*.cpp"],
        exclude = [
            "chunkserver_main.cpp",
        ],
    ),
    hdrs = glob([
        "*.h",
    ]),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    linkopts = [
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
    ],
)

# chunkserver exec
cc_binary(
    name = "chunkserver",
    srcs = glob([
        "chunkserver_main.cpp",
    ]),
    copts = COPTS,
    linkopts = [
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
