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

cc_library(
    name="nameserver2",
    hdrs=glob(["*.h"]),
    srcs=glob(["*.cpp"]),
    visibility = ["//visibility:public"],
    deps = [
            "//external:glog",
            "//external:gflags",
            "//external:leveldb",
            "//external:brpc",
            "//proto:nameserver2_cc_proto",
            "//src/common:curve_common",
            "//src/mds/topology:topology",
            "//src/mds/nameserver2/allocstatistic:alloc_statistic",
            "//src/mds/chunkserverclient:chunkserverclient",
            "//src/mds/snapshotcloneclient:snapshotcloneclient",
            "//src/mds/common:mds_common",
            "//src/mds/nameserver2/helper:helper",
            "//src/mds/nameserver2/idgenerator:idgenerator",
            "//src/common:curve_auth",
            "//src/kvstorageclient:kvstorage_client",
    ],
)

