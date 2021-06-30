#
#  Copyright (c) 2021 NetEase Inc.
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

cc_proto_library(
    name = "curvefs_common_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":curvefs_common_proto"],
)

proto_library(
    name = "curvefs_common_proto",
    srcs = ["common.proto"],
)

cc_proto_library(
    name = "metaserver_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":metaserver_proto"],
)

proto_library(
    name = "metaserver_proto",
    srcs = ["metaserver.proto"],
)

cc_proto_library(
    name = "mds_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":mds_proto", ],
)

proto_library(
    name = "mds_proto",
    srcs = ["mds.proto"],
    deps = [":curvefs_common_proto"],
)

cc_proto_library(
    name = "space_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":space_proto"],
)

proto_library(
    name = "space_proto",
    srcs = ["space.proto"],
    deps = [":curvefs_common_proto",":mds_proto"],
)