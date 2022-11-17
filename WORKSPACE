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

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

load("//thirdparties/brpc:brpc_workspace.bzl", "brpc_workspace")

brpc_workspace();

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
    remote = "https://github.com/baidu/braft",
    commit = "d0277bf2aea66908d1fd7376d191bd098371966e",
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
    urls = ["https://zlib.net/zlib-1.2.11.tar.gz"],
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
    urls = ["https://github.com/google/protobuf/archive/v3.6.1.3.zip"],
)

bind(
    name = "protobuf",
    actual = "@com_google_protobuf//:protobuf",
)

# import the  gtest files.
new_git_repository(
    name = "com_google_googletest",
    build_file = "//:thirdparties/gmock.BUILD",
    remote = "https://github.com/google/googletest",
    tag = "release-1.8.0",
)

bind(
    name = "gtest",
    actual = "@com_google_googletest//:gtest",
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

bind(
    name = "gflags",
    actual = "@com_github_gflags_gflags//:gflags",
)

http_archive(
    name = "com_github_google_leveldb",
    build_file = "@com_github_brpc_brpc//:leveldb.BUILD",
    strip_prefix = "leveldb-a53934a3ae1244679f812d998a4f16f2c7f309a6",
    urls = ["https://github.com/google/leveldb/archive/a53934a3ae1244679f812d998a4f16f2c7f309a6.tar.gz"],
)

bind(
    name = "leveldb",
    actual = "@com_github_google_leveldb//:leveldb",
)


bind(
    name = "brpc",
    actual = "@com_github_brpc_brpc//:brpc",
)

bind(
    name = "butil",
    actual = "@com_github_brpc_brpc//:butil",
)

bind(
    name = "bthread",
    actual = "@com_github_brpc_brpc//:bthread",
)

bind(
    name = "bvar",
    actual = "@com_github_brpc_brpc//:bvar",
)

# jsoncpp
new_git_repository(
    name = "jsoncpp",
    build_file = "//:thirdparties/jsoncpp.BUILD",
    remote = "https://github.com/open-source-parsers/jsoncpp.git",
    tag = "1.8.4",
)

bind(
    name = "json",
    actual = "@jsoncpp//:json",
)

new_local_repository(
    name = "etcdclient",
    build_file = "//:thirdparties/etcdclient.BUILD",
    path = "thirdparties/etcdclient",
)

new_local_repository(
    name = "aws",
    build_file = "//:thirdparties/aws/aws.BUILD",
    path = "thirdparties/aws/aws-sdk-cpp",
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
  urls = ["https://github.com/abseil/abseil-cpp/archive/refs/tags/20210324.2.tar.gz"],
  strip_prefix = "abseil-cpp-20210324.2",
  sha256 = "59b862f50e710277f8ede96f083a5bb8d7c9595376146838b9580be90374ee1f",
)

# Bazel platform rules.
http_archive(
    name = "platforms",
    sha256 = "b601beaf841244de5c5a50d2b2eddd34839788000fa1be4260ce6603ca0d8eb7",
    strip_prefix = "platforms-98939346da932eef0b54cf808622f5bb0928f00b",
    urls = ["https://github.com/bazelbuild/platforms/archive/98939346da932eef0b54cf808622f5bb0928f00b.zip"],
)

# RocksDB
new_local_repository(
    name = "rocksdb",
    build_file = "//:thirdparties/rocksdb.BUILD",
    path = "thirdparties/rocksdb",
)

# Hedron's Compile Commands Extractor for Bazel
# https://github.com/hedronvision/bazel-compile-commands-extractor
http_archive(
    name = "hedron_compile_commands",

    # Replace the commit hash in both places (below) with the latest, rather than using the stale one here. 
    # Even better, set up Renovate and let it do the work for you (see "Suggestion: Updates" in the README).
    urls = [
        "https://curve-build.nos-eastchina1.126.net/bazel-compile-commands-extractor-af9af15f7bc16fc3e407e2231abfcb62907d258f.tar.gz",
        "https://github.com/hedronvision/bazel-compile-commands-extractor/archive/af9af15f7bc16fc3e407e2231abfcb62907d258f.tar.gz",
    ],
    strip_prefix = "bazel-compile-commands-extractor-af9af15f7bc16fc3e407e2231abfcb62907d258f",
    # When you first run this tool, it'll recommend a sha256 hash to put here with a message like: "DEBUG: Rule 'hedron_compile_commands' indicated that a canonical reproducible form can be obtained by modifying arguments sha256 = ..." 
)
load("@hedron_compile_commands//:workspace_setup.bzl", "hedron_compile_commands_setup")
hedron_compile_commands_setup()
