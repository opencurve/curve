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

workspace(name = "curve")

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

git_repository(
    name = "com_netease_storage_gerrit_curve_curve_braft",
    remote = "http://gerrit.storage.netease.com/curve/curve-braft",
    commit = "b17ebad68d1d1b84440f7bce984755ff47095137",
)

bind(
    name = "braft",
    actual = "@com_netease_storage_gerrit_curve_curve_braft//:braft",
)

# proto_library, cc_proto_library, and java_proto_library rules implicitly
# depend on @com_google_protobuf for protoc and proto runtimes.
# This statement defines the @com_google_protobuf repo.
http_archive(
    name = "com_google_protobuf",
    sha256 = "cef7f1b5a7c5fba672bec2a319246e8feba471f04dcebfe362d55930ee7c1c30",
    strip_prefix = "protobuf-3.5.0",
    urls = ["https://github.com/google/protobuf/archive/v3.5.0.zip"],
)

bind(
    name = "protobuf",
    actual = "@com_google_protobuf//:protobuf",
)

#import the  gtest files.
new_git_repository(
    name = "com_google_googletest",
    build_file = "bazel/gmock.BUILD",
    remote = "https://github.com/google/googletest",
    tag = "release-1.8.0",
)

bind(
    name = "gtest",
    actual = "@com_google_googletest//:gtest",
)

#Import the glog files.
# brpc内BUILD文件在依赖glog时, 直接指定的依赖是"@com_github_google_glog//:glog"
git_repository(
    name = "com_github_google_glog",
    remote = "http://gerrit.storage.netease.com/curve/curve-glog",
    commit = "4cc89c9e2b452db579397887c37f302fb28f6ca1",
    patch_args = ["-p1"],
    patches = ["//:thirdparties/glog/glog.patch"],
)

bind(
    name = "glog",
    actual = "@com_github_google_glog//:glog"
)

# glog depends on gflags-2.2.2
http_archive(
    name = "com_github_gflags_gflags",
    strip_prefix = "gflags-2.2.2",
    urls = [
        "https://mirror.bazel.build/github.com/gflags/gflags/archive/v2.2.2.tar.gz",
        "https://github.com/gflags/gflags/archive/v2.2.2.tar.gz",
    ],
)

bind(
    name = "gflags",
    actual = "@com_github_gflags_gflags//:gflags",
)

new_http_archive(
    name = "com_github_google_leveldb",
    build_file = "bazel/leveldb.BUILD",
    strip_prefix = "leveldb-a53934a3ae1244679f812d998a4f16f2c7f309a6",
    url = "https://github.com/google/leveldb/archive/a53934a3ae1244679f812d998a4f16f2c7f309a6.tar.gz",
)

bind(
    name = "leveldb",
    actual = "@com_github_google_leveldb//:leveldb",
)

git_repository(
    name = "com_netease_storage_gerrit_curve_curve_brpc",
    remote = "http://gerrit.storage.netease.com/curve/curve-brpc",
    commit = "1b9e00641cbec1c8803da6a1f7f555398c954cb0",
    patches = ["//:thirdparties/brpc/brpc.patch"],
    patch_args = ["-p1"],
)

bind(
    name = "brpc",
    actual = "@com_netease_storage_gerrit_curve_curve_brpc//:brpc",
)

bind(
    name = "butil",
    actual = "@com_netease_storage_gerrit_curve_curve_brpc//:butil",
)

bind(
    name = "bthread",
    actual = "@com_netease_storage_gerrit_curve_curve_brpc//:bthread",
)

bind(
    name = "bvar",
    actual = "@com_netease_storage_gerrit_curve_curve_brpc//:bvar",
)

# jsoncpp
new_git_repository(
    name = "jsoncpp",
    build_file = "bazel/jsoncpp.BUILD",
    remote = "https://github.com/open-source-parsers/jsoncpp.git",
    tag = "1.8.4",
)

bind(
    name = "json",
    actual = "@jsoncpp//:json",
)

new_local_repository(
    name = "aws_sdk",
    build_file = "bazel/aws-sdk.BUILD",
    path = "thirdparties/aws-sdk/usr",
)

new_local_repository(
    name = "etcdclient",
    build_file = "bazel/etcdclient.BUILD",
    path = "thirdparties/etcdclient",
)
