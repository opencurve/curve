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

cc_test(
    name = "raft-log-replication-test",
    srcs = glob([
        "raft_log_replication_test.cpp",
    ]),
    copts = ["-std=c++11"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//proto:chunkserver-cc-protos",
        "//src/chunkserver:chunkserver-test-lib",
        "//src/common:curve_common",
        "//test/integration/common:integration-test-common",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "raft-config-change-test",
    srcs = glob([
        "raft_config_change_test.cpp",
    ]),
    copts = ["-std=c++11"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//proto:chunkserver-cc-protos",
        "//src/chunkserver:chunkserver-test-lib",
        "//src/common:curve_common",
        "//test/integration/common:integration-test-common",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "raft-vote-test",
    srcs = glob([
        "peer_cluster.h",
        "peer_cluster.cpp",
        "raft_vote_test.cpp",
    ]),
    copts = ["-std=c++11"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//proto:chunkserver-cc-protos",
        "//src/chunkserver:chunkserver-test-lib",
        "//src/common:curve_common",
        "//test/integration/common:integration-test-common",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "raft-snapshot-test",
    srcs = glob([
        "raft_snapshot_test.cpp",
    ]),
    copts = ["-std=c++11"],
    deps = [
        "//external:braft",
        "//external:brpc",
        "//external:bthread",
        "//external:butil",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//proto:chunkserver-cc-protos",
        "//src/chunkserver:chunkserver-test-lib",
        "//src/common:curve_common",
        "//test/integration/common:integration-test-common",
        "@com_google_googletest//:gtest_main",
    ],
)
