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
    name = "com_github_baidu_braft",
    remote = "https://github.com/baidu/braft",
    commit = "e255c0e4b18d1a8a5d484d4b647f41ff1385ef1e",
    patch_args = ["-p1"],
    patches = ["//:thirdparties/braft/braft.patch"],
)

bind(
    name = "braft",
    actual = "@com_github_baidu_braft//:braft",
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
    remote = "https://github.com/google/glog",
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
    name = "com_github_apache_brpc",
    remote = "https://github.com/apache/incubator-brpc",
    commit = "1b9e00641cbec1c8803da6a1f7f555398c954cb0",
    patches = ["//:thirdparties/brpc/brpc.patch"],
    patch_args = ["-p1"],
)

bind(
    name = "brpc",
    actual = "@com_github_apache_brpc//:brpc",
)

bind(
    name = "butil",
    actual = "@com_github_apache_brpc//:butil",
)

bind(
    name = "bthread",
    actual = "@com_github_apache_brpc//:bthread",
)

bind(
    name = "bvar",
    actual = "@com_github_apache_brpc//:bvar",
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
    name = "etcdclient",
    build_file = "bazel/etcdclient.BUILD",
    path = "thirdparties/etcdclient",
)

new_http_archive(
    name = "aws",
    urls = [
        "https://github.com/aws/aws-sdk-cpp/archive/1.7.340.tar.gz",
        "https://mirror.bazel.build/github.com/aws/aws-sdk-cpp/archive/1.7.340.tar.gz",
    ],
    sha256 = "2e82517045efb55409cff1408c12829d9e8aea22c1e2888529cb769b7473b0bf",
    strip_prefix = "aws-sdk-cpp-1.7.340",
    build_file = "//:thirdparties/aws/aws.BUILD",
)

new_http_archive(
    name = "aws_c_common",
    urls = [
        "https://github.com/awslabs/aws-c-common/archive/v0.4.29.tar.gz",
        "https://mirror.tensorflow.org/github.com/awslabs/aws-c-common/archive/v0.4.29.tar.gz",
    ],
    sha256 = "01c2a58553a37b3aa5914d9e0bf7bf14507ff4937bc5872a678892ca20fcae1f",
    strip_prefix = "aws-c-common-0.4.29",
    build_file = "//:thirdparties/aws/aws-c-common.BUILD",
)

new_http_archive(
    name = "aws_c_event_stream",
    urls = [
        "https://github.com/awslabs/aws-c-event-stream/archive/v0.1.4.tar.gz",
        "https://mirror.tensorflow.org/github.com/awslabs/aws-c-event-stream/archive/v0.1.4.tar.gz",
    ],
    sha256 = "31d880d1c868d3f3df1e1f4b45e56ac73724a4dc3449d04d47fc0746f6f077b6",
    strip_prefix = "aws-c-event-stream-0.1.4",
    build_file = "//:thirdparties/aws/aws-c-event-stream.BUILD",
)

new_http_archive(
    name = "aws_checksums",
    urls = [
        "https://github.com/awslabs/aws-checksums/archive/v0.1.5.tar.gz",
        "https://mirror.tensorflow.org/github.com/awslabs/aws-checksums/archive/v0.1.5.tar.gz",
    ],
    sha256 = "6e6bed6f75cf54006b6bafb01b3b96df19605572131a2260fddaf0e87949ced0",
    strip_prefix = "aws-checksums-0.1.5",
    build_file = "//:thirdparties/aws/aws-checksums.BUILD",
)
