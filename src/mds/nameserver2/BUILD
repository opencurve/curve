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

load("//:copts.bzl", "CURVE_DEFAULT_COPTS")

cc_library(
    name = "nameserver2",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = CURVE_DEFAULT_COPTS + [
        "-Wno-write-strings",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:brpc",
        "//external:gflags",
        "//external:glog",
        "//external:leveldb",
        "//proto:nameserver2_cc_proto",
        "//src/common:curve_auth",
        "//src/common:curve_common",
        "//src/common/concurrent:curve_dlock",
        "//src/kvstorageclient:kvstorage_client",
        "//src/mysqlstorageclient:mysqlstorage_client",
        "//src/mds/chunkserverclient",
        "//src/mds/common:mds_common",
        "//src/mds/nameserver2/allocstatistic:alloc_statistic",
        "//src/mds/nameserver2/helper",
        "//src/mds/nameserver2/idgenerator:nameserver_idgenerator",
        "//src/mds/snapshotcloneclient",
        "//src/mds/topology",
        "@com_google_absl//absl/strings",
    ],
)
