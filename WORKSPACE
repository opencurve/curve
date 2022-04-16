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
load("@bazel_tools//tools/build_defs/repo:git.bzl", "new_git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# skylib
http_archive(
    name = "bazel_skylib",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.2.0/bazel-skylib-1.2.0.tar.gz",
        "https://github.com/bazelbuild/bazel-skylib/releases/download/1.2.0/bazel-skylib-1.2.0.tar.gz",
    ],
    sha256 = "af87959afe497dc8dfd4c6cb66e1279cb98ccc84284619ebfec27d9c09a903de",
)
load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")
bazel_skylib_workspace()

git_repository(
    name = "com_github_baidu_braft",
    remote = "https://gitee.com/baidu/braft",
    commit = "e255c0e4b18d1a8a5d484d4b647f41ff1385ef1e",
)

bind(
    name = "braft",
    actual = "@com_github_baidu_braft//:braft",
)

# proto_library, cc_proto_library, and java_proto_library rules implicitly
# depend on @com_google_protobuf for protoc and proto runtimes.
# This statement defines the @com_google_protobuf repo.

# zlib
http_archive(
    name = "net_zlib",
    build_file = "@com_google_protobuf//:third_party/zlib.BUILD",
    sha256 = "c3e5e9fdd5004dcb542feda5ee4f0ff0744628baf8ed2dd5d66f8ca1197cb1a1",
    strip_prefix = "zlib-1.2.11",
    urls = ["https://curve-build.nos-eastchina1.126.net/zlib-1.2.11.tar.gz"],
)

bind(
    name = "zlib",
    actual = "@net_zlib//:zlib",
)

http_archive(
    name = "com_google_protobuf",
    strip_prefix = "protobuf-3.6.1.3",
    patch_args = ["-p1"],
    patches = ["//:thirdparties/protobuf/protobuf.patch"],
    sha256 = "9510dd2afc29e7245e9e884336f848c8a6600a14ae726adb6befdb4f786f0be2",
    urls = ["https://curve-build.nos-eastchina1.126.net/protobuf-3.6.1.3.zip"],
)

bind(
    name = "protobuf",
    actual = "@com_google_protobuf//:protobuf",
)

#import the  gtest files.
new_git_repository(
    name = "com_google_googletest",
    build_file = "bazel/gmock.BUILD",
    remote = "https://gitee.com/mirrors/googletest",
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
    remote = "https://gitee.com/mirrors/glog",
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
    urls = ["https://curve-build.nos-eastchina1.126.net/gflags-2.2.2.tar.gz"],
)

bind(
    name = "gflags",
    actual = "@com_github_gflags_gflags//:gflags",
)

http_archive(
    name = "com_github_google_leveldb",
    build_file = "bazel/leveldb.BUILD",
    strip_prefix = "leveldb-a53934a3ae1244679f812d998a4f16f2c7f309a6",
    urls = ["https://curve-build.nos-eastchina1.126.net/leveldb-a53934a3ae1244679f812d998a4f16f2c7f309a6.tar.gz"],
)

bind(
    name = "leveldb",
    actual = "@com_github_google_leveldb//:leveldb",
)

git_repository(
    name = "com_github_apache_brpc",
    remote = "https://gitee.com/baidu/BRPC",
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
    remote = "https://gitee.com/mirrors/jsoncpp",
    tag = "1.8.4",
)

bind(
    name = "json",
    actual = "@jsoncpp//:json",
)

new_local_repository(
    name = "etcdclient",
    build_file = "external/bazel/etcdclient.BUILD",
    path = "thirdparties/etcdclient",
)

http_archive(
    name = "aws",
    urls = ["https://curve-build.nos-eastchina1.126.net/aws-sdk-cpp-1.7.340.tar.gz"],
    sha256 = "2e82517045efb55409cff1408c12829d9e8aea22c1e2888529cb769b7473b0bf",
    strip_prefix = "aws-sdk-cpp-1.7.340",
    build_file = "//:thirdparties/aws/aws.BUILD",
)

http_archive(
    name = "aws_c_common",
    urls = ["https://curve-build.nos-eastchina1.126.net/aws-c-common-0.4.29.tar.gz"],
    sha256 = "01c2a58553a37b3aa5914d9e0bf7bf14507ff4937bc5872a678892ca20fcae1f",
    strip_prefix = "aws-c-common-0.4.29",
    build_file = "//:thirdparties/aws/aws-c-common.BUILD",
)

http_archive(
    name = "aws_c_event_stream",
    urls = ["https://curve-build.nos-eastchina1.126.net/aws-c-event-stream-0.1.4.tar.gz"],
    sha256 = "31d880d1c868d3f3df1e1f4b45e56ac73724a4dc3449d04d47fc0746f6f077b6",
    strip_prefix = "aws-c-event-stream-0.1.4",
    build_file = "//:thirdparties/aws/aws-c-event-stream.BUILD",
)

http_archive(
    name = "aws_checksums",
    urls = ["https://curve-build.nos-eastchina1.126.net/aws-checksums-0.1.5.tar.gz"],
    sha256 = "6e6bed6f75cf54006b6bafb01b3b96df19605572131a2260fddaf0e87949ced0",
    strip_prefix = "aws-checksums-0.1.5",
    build_file = "//:thirdparties/aws/aws-checksums.BUILD",
)

# C++ rules for Bazel.
http_archive(
    name = "rules_cc",
    urls = ["https://github.com/bazelbuild/rules_cc/archive/9e10b8a6db775b1ecd358d8ddd3dab379a2c29a5.zip"],
    strip_prefix = "rules_cc-9e10b8a6db775b1ecd358d8ddd3dab379a2c29a5",
    sha256 = "954b7a3efc8752da957ae193a13b9133da227bdacf5ceb111f2e11264f7e8c95",
)

# abseil-cpp
http_archive(
  name = "com_google_absl",
  urls = ["https://curve-build.nos-eastchina1.126.net/abseil-cpp-20210324.2.tar.gz"],
  strip_prefix = "abseil-cpp-20210324.2",
  sha256 = "59b862f50e710277f8ede96f083a5bb8d7c9595376146838b9580be90374ee1f",
)

# Bazel platform rules.
http_archive(
    name = "platforms",
    sha256 = "b601beaf841244de5c5a50d2b2eddd34839788000fa1be4260ce6603ca0d8eb7",
    strip_prefix = "platforms-98939346da932eef0b54cf808622f5bb0928f00b",
    urls = ["https://curve-build.nos-eastchina1.126.net/platforms-98939346da932eef0b54cf808622f5bb0928f00b.zip"],
)

# RocksDB
new_local_repository(
    name = "rocksdb",
    build_file = "external/bazel/rocksdb.BUILD",
    path = "thirdparties/rocksdb",
)
